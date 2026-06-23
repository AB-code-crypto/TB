import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from grpc import aio
from t_tech.invest import AsyncClient

from bd.price_snapshot import save_price_snapshot
from bd.settings_storage import load_app_settings, load_selected_shares
from bot.growth_candle_intervals import GrowthCandleInterval, get_growth_candle_interval
from tbank.candles import TBankCandle, get_candles
from tbank.last_prices import TBankLastPrice, get_last_prices_batched, map_last_prices_by_instrument_uid
from tbank.shares import TBankShare


BATCH_SIZE = 100


@dataclass(frozen=True)
class GrowthScanResult:
    ticker: str
    class_code: str
    instrument_uid: str
    name: str
    current_price: Decimal
    candle_open_price: Decimal
    growth_percent: Decimal
    candle_time_utc: datetime
    candle_is_complete: bool
    last_price_time_utc: datetime


@dataclass(frozen=True)
class GrowthScanReport:
    interval_label: str
    growth_threshold_percent: Decimal
    total_selected_shares: int
    total_prices_received: int
    snapshot_rows_saved: int
    results: list[GrowthScanResult]
    signals: list[GrowthScanResult]
    skipped: list[str]


def _calculate_growth_percent(
    current_price: Decimal,
    base_price: Decimal,
) -> Decimal:
    if base_price <= 0:
        raise ValueError("base_price должен быть больше 0.")

    return (current_price / base_price - Decimal("1")) * Decimal("100")


async def _get_current_growth_candle(
    client,
    share: TBankShare,
    interval: GrowthCandleInterval,
    now_utc: datetime,
) -> TBankCandle:
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

    return max(candles, key=lambda candle: candle.time)


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

        for share in selected_shares:
            last_price = prices_by_uid.get(share.uid)

            if last_price is None:
                skipped.append(
                    f"{share.ticker}_{share.class_code}: последняя цена не получена"
                )
                continue

            try:
                candle = await _get_current_growth_candle(
                    client=client,
                    share=share,
                    interval=interval,
                    now_utc=now_utc,
                )

                growth_percent = _calculate_growth_percent(
                    current_price=last_price.price,
                    base_price=candle.open,
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
                    current_price=last_price.price,
                    candle_open_price=candle.open,
                    growth_percent=growth_percent,
                    candle_time_utc=candle.time,
                    candle_is_complete=candle.is_complete,
                    last_price_time_utc=last_price.time,
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
        total_selected_shares=len(selected_shares),
        total_prices_received=len(prices),
        snapshot_rows_saved=snapshot_rows_saved,
        results=results,
        signals=signals,
        skipped=skipped,
    )


def _format_percent(value: Decimal) -> str:
    return f"{value:.4f}%"


def print_growth_report(report: GrowthScanReport) -> None:
    print("=== Growth scan report ===")
    print(f"Интервал расчёта роста: {report.interval_label}")
    print(f"Порог роста: {_format_percent(report.growth_threshold_percent)}")
    print(f"Рабочих акций: {report.total_selected_shares}")
    print(f"Цен получено: {report.total_prices_received}")
    print(f"Строк snapshot сохранено: {report.snapshot_rows_saved}")
    print(f"Результатов рассчитано: {len(report.results)}")
    print(f"Сигналов найдено: {len(report.signals)}")
    print(f"Пропущено: {len(report.skipped)}")
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
                f"complete={result.candle_is_complete}"
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
            f"complete={result.candle_is_complete}"
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
