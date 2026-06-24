import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from grpc import aio
from t_tech.invest import AsyncClient

from bd.growth_candle_cache import (
    get_growth_candle_cache,
    save_growth_candle_cache,
)
from bd.price_snapshot import save_price_snapshot
from bd.settings_storage import load_app_settings, load_selected_shares
from bot.growth_candle_intervals import (
    GrowthCandleInterval,
    get_growth_candle_interval,
    is_candle_cache_actual,
)
from tbank.candles import get_candles
from tbank.last_prices import (
    TBankLastPrice,
    get_last_prices_batched,
    map_last_prices_by_instrument_uid,
)
from tbank.shares import TBankShare


BATCH_SIZE = 100


@dataclass(frozen=True)
class GrowthBaseCandle:
    open_price: Decimal
    candle_time_utc: datetime
    is_complete: bool
    source: str


@dataclass(frozen=True)
class GrowthScanResult:
    ticker: str
    class_code: str
    instrument_uid: str
    name: str
    lot: int
    currency: str
    current_price: Decimal
    candle_open_price: Decimal
    growth_percent: Decimal
    candle_time_utc: datetime
    candle_is_complete: bool
    last_price_time_utc: datetime
    base_source: str


@dataclass(frozen=True)
class GrowthScanReport:
    interval_label: str
    growth_threshold_percent: Decimal
    max_price_age_seconds: int
    total_selected_shares: int
    total_prices_received: int
    snapshot_rows_saved: int
    results: list[GrowthScanResult]
    signals: list[GrowthScanResult]
    skipped: list[str]
    candle_cache_hits: int
    candle_api_requests: int


def _calculate_growth_percent(
    current_price: Decimal,
    base_price: Decimal,
) -> Decimal:
    if base_price <= 0:
        raise ValueError("base_price должен быть больше 0.")

    return (current_price / base_price - Decimal("1")) * Decimal("100")


async def _get_growth_base_candle(
    client,
    share: TBankShare,
    interval: GrowthCandleInterval,
    now_utc: datetime,
) -> GrowthBaseCandle:
    cached_candle = get_growth_candle_cache(
        instrument_uid=share.uid,
        interval_label=interval.label,
    )

    if cached_candle is not None and is_candle_cache_actual(
        interval=interval,
        candle_time_utc=cached_candle.candle_time_utc,
        now_utc=now_utc,
    ):
        return GrowthBaseCandle(
            open_price=cached_candle.open_price,
            candle_time_utc=cached_candle.candle_time_utc,
            is_complete=cached_candle.is_complete,
            source="cache",
        )

    candles = await get_candles(
        client=client,
        instrument_id=share.uid,
        from_time=now_utc - interval.lookback,
        to_time=now_utc,
        interval=interval.api_interval,
        limit=10,
    )

    if not candles:
        raise RuntimeError(
            f"Свечи не получены: {share.ticker}_{share.class_code}, "
            f"interval={interval.label}"
        )

    candle = max(candles, key=lambda item: item.time)

    save_growth_candle_cache(
        instrument_uid=share.uid,
        interval_label=interval.label,
        candle_time_utc=candle.time,
        open_price=candle.open,
        is_complete=candle.is_complete,
        updated_at_utc=now_utc,
    )

    return GrowthBaseCandle(
        open_price=candle.open,
        candle_time_utc=candle.time,
        is_complete=candle.is_complete,
        source="api",
    )


async def scan_growth_once() -> GrowthScanReport:
    settings = load_app_settings()
    selected_shares = load_selected_shares()

    token = settings["token"]

    if not token.strip():
        raise ValueError("В настройках сохранён пустой токен.")

    if not selected_shares:
        raise ValueError("Рабочий список акций пуст.")

    growth_threshold_percent = Decimal(settings["growth_percent"])
    interval = get_growth_candle_interval(settings["growth_candle_interval"])

    max_price_age_seconds_raw = settings["max_price_age_seconds"]

    try:
        max_price_age_seconds = int(max_price_age_seconds_raw)
    except ValueError as error:
        raise ValueError("max_price_age_seconds должен быть целым числом.") from error

    if max_price_age_seconds <= 0:
        raise ValueError("max_price_age_seconds должен быть больше 0.")

    if growth_threshold_percent <= 0:
        raise ValueError("Рост для покупки должен быть больше 0.")

    instrument_ids = [
        share.uid
        for share in selected_shares
    ]

    now_utc = datetime.now(timezone.utc)

    async with AsyncClient(token) as client:
        prices = await get_last_prices_batched(
            client=client,
            instrument_ids=instrument_ids,
            batch_size=BATCH_SIZE,
        )

        if not prices:
            raise RuntimeError("T-Invest API не вернул цены для рабочих акций.")

        snapshot_rows_saved = save_price_snapshot(
            prices=prices,
            captured_at_utc=now_utc,
        )

        prices_by_uid = map_last_prices_by_instrument_uid(prices)

        results: list[GrowthScanResult] = []
        skipped: list[str] = []
        candle_cache_hits = 0
        candle_api_requests = 0

        for share in selected_shares:
            last_price = prices_by_uid.get(share.uid)

            if last_price is None:
                skipped.append(
                    f"{share.ticker}_{share.class_code}: последняя цена не получена"
                )
                continue

            price_age_seconds = (
                now_utc - last_price.time.astimezone(timezone.utc)
            ).total_seconds()

            if price_age_seconds > max_price_age_seconds:
                skipped.append(
                    f"{share.ticker}_{share.class_code}: "
                    f"цена устарела, age={price_age_seconds:.2f} сек., "
                    f"limit={max_price_age_seconds} сек., "
                    f"last_price_time_utc={last_price.time}"
                )
                continue

            try:
                base_candle = await _get_growth_base_candle(
                    client=client,
                    share=share,
                    interval=interval,
                    now_utc=now_utc,
                )

                if base_candle.source == "cache":
                    candle_cache_hits += 1
                elif base_candle.source == "api":
                    candle_api_requests += 1
                else:
                    raise RuntimeError(f"Неизвестный источник свечи: {base_candle.source}")

                growth_percent = _calculate_growth_percent(
                    current_price=last_price.price,
                    base_price=base_candle.open_price,
                )
            except Exception as error:
                skipped.append(
                    f"{share.ticker}_{share.class_code}: "
                    f"{type(error).__name__}: {error}"
                )
                continue

            results.append(
                GrowthScanResult(
                    ticker=share.ticker,
                    class_code=share.class_code,
                    instrument_uid=share.uid,
                    name=share.name,
                    lot=share.lot,
                    currency=share.currency,
                    current_price=last_price.price,
                    candle_open_price=base_candle.open_price,
                    growth_percent=growth_percent,
                    candle_time_utc=base_candle.candle_time_utc,
                    candle_is_complete=base_candle.is_complete,
                    last_price_time_utc=last_price.time,
                    base_source=base_candle.source,
                )
            )

    results = sorted(
        results,
        key=lambda item: item.growth_percent,
        reverse=True,
    )

    signals = [
        result
        for result in results
        if result.growth_percent >= growth_threshold_percent
    ]

    return GrowthScanReport(
        interval_label=interval.label,
        growth_threshold_percent=growth_threshold_percent,
        max_price_age_seconds=max_price_age_seconds,
        total_selected_shares=len(selected_shares),
        total_prices_received=len(prices),
        snapshot_rows_saved=snapshot_rows_saved,
        results=results,
        signals=signals,
        skipped=skipped,
        candle_cache_hits=candle_cache_hits,
        candle_api_requests=candle_api_requests,
    )


def _format_percent(value: Decimal) -> str:
    return f"{value:.4f}%"


def print_growth_report(report: GrowthScanReport) -> None:
    print("=== Growth scan report ===")
    print(f"Интервал расчёта роста: {report.interval_label}")
    print(f"Порог роста: {_format_percent(report.growth_threshold_percent)}")
    print(f"Макс. возраст цены: {report.max_price_age_seconds} сек.")
    print(f"Рабочих акций: {report.total_selected_shares}")
    print(f"Цен получено: {report.total_prices_received}")
    print(f"Строк snapshot сохранено: {report.snapshot_rows_saved}")
    print(f"Результатов рассчитано: {len(report.results)}")
    print(f"Сигналов найдено: {len(report.signals)}")
    print(f"Пропущено: {len(report.skipped)}")
    print(f"Свечи из cache: {report.candle_cache_hits}")
    print(f"Свечи из API: {report.candle_api_requests}")
    print()

    if report.signals:
        print("=== Сигналы ===")

        for number, result in enumerate(report.signals, start=1):
            print(
                f"{number}. {result.ticker}_{result.class_code} "
                f"{_format_percent(result.growth_percent)} "
                f"current={result.current_price} "
                f"open={result.candle_open_price} "
                f"candle_time_utc={result.candle_time_utc} "
                f"complete={result.candle_is_complete} "
                f"source={result.base_source}"
            )

        print()

    print("=== Top 20 по росту ===")

    for number, result in enumerate(report.results[:20], start=1):
        print(
            f"{number}. {result.ticker}_{result.class_code} "
            f"{_format_percent(result.growth_percent)} "
            f"current={result.current_price} "
            f"open={result.candle_open_price} "
            f"candle_time_utc={result.candle_time_utc} "
            f"complete={result.candle_is_complete} "
            f"source={result.base_source}"
        )

    if report.skipped:
        print()
        print("=== Пропущено ===")

        for item in report.skipped[:50]:
            print(f"- {item}")

        if len(report.skipped) > 50:
            print(f"... ещё {len(report.skipped) - 50}")


async def main() -> None:
    try:
        report = await scan_growth_once()
    except KeyError as error:
        print(f"Ошибка настроек: отсутствует ключ {error}.")
        return
    except aio.AioRpcError as error:
        print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
        return
    except Exception as error:
        print(f"Ошибка: {type(error).__name__}: {error}")
        return

    print_growth_report(report)


if __name__ == "__main__":
    asyncio.run(main())
