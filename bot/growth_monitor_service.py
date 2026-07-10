import asyncio
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from bd.growth_current_state import (
    GrowthCurrentStateInput,
    save_growth_current_states,
)
from bd.growth_scan_cycle import (
    GROWTH_SCAN_CYCLE_STATUS_ERROR,
    GROWTH_SCAN_CYCLE_STATUS_SUCCESS,
    count_growth_scan_cycles,
    save_growth_scan_cycle,
)
from bd.growth_signal import count_growth_signals, save_growth_signal
from bd.buy_intent import (
    BUY_INTENT_SIDE_BUY,
    BUY_INTENT_STATUS_PLANNED,
    BUY_INTENT_STATUS_SKIPPED,
    save_buy_intent,
)
from bd.price_snapshot import delete_price_snapshots_older_than
from bd.settings_storage import load_app_settings
from bot.growth_scanner import GrowthScanReport, GrowthScanResult, scan_growth_once
from bot.auto_trade_executor import execute_auto_trading_cycle
from tbank.shares import MOEX_SHARE_CURRENCY


LogCallback = Callable[[str], None]
StopCallback = Callable[[], bool]




def _get_scan_interval_seconds() -> int:
    settings = load_app_settings()
    raw_value = settings["scan_interval_seconds"]

    try:
        scan_interval_seconds = int(raw_value)
    except ValueError as error:
        raise ValueError("scan_interval_seconds должен быть целым числом.") from error

    if scan_interval_seconds <= 0:
        raise ValueError("scan_interval_seconds должен быть больше 0.")

    return scan_interval_seconds


def _parse_non_negative_decimal_setting(
    settings: dict[str, str],
    key: str,
    label: str,
) -> Decimal:
    raw_value = settings[key].strip().replace(",", ".").replace(" ", "")

    try:
        value = Decimal(raw_value)
    except InvalidOperation as error:
        raise ValueError(
            f"{label}: некорректное число в настройках: {settings[key]!r}"
        ) from error

    if value < 0:
        raise ValueError(f"{label} не может быть меньше 0.")

    return value


def _format_percent(value) -> str:
    return f"{value:.4f}%"


def _emit_lines(on_log: LogCallback, lines: list[str]) -> None:
    for line in lines:
        on_log(line)


def save_report_signals(
    report: GrowthScanReport,
) -> tuple[int, int, list[tuple[int, GrowthScanResult]]]:
    detected_at_utc = datetime.now(timezone.utc)

    new_signals_count = 0
    duplicate_signals_count = 0
    new_signal_records: list[tuple[int, GrowthScanResult]] = []

    for signal in report.signals:
        signal_id = save_growth_signal(
            detected_at_utc=detected_at_utc,
            instrument_uid=signal.instrument_uid,
            ticker=signal.ticker,
            class_code=signal.class_code,
            name=signal.name,
            interval_label=report.interval_label,
            candle_time_utc=signal.candle_time_utc,
            current_price=signal.current_price,
            candle_open_price=signal.candle_open_price,
            growth_percent=signal.growth_percent,
            threshold_percent=report.growth_threshold_percent,
            last_price_time_utc=signal.last_price_time_utc,
            base_source=signal.base_source,
        )

        if signal_id is None:
            duplicate_signals_count += 1
        else:
            new_signals_count += 1
            new_signal_records.append((signal_id, signal))

    return new_signals_count, duplicate_signals_count, new_signal_records


def save_dry_run_buy_intents(
    new_signal_records: list[tuple[int, GrowthScanResult]],
    report: GrowthScanReport,
) -> tuple[int, int, Decimal]:
    if not new_signal_records:
        return 0, 0, Decimal("0")

    settings = load_app_settings()

    allow_buy = settings["allow_buy"] == "1"
    requested_amount = _parse_non_negative_decimal_setting(
        settings=settings,
        key="auto_buy_amount_rub",
        label="Сумма автопокупки RUB",
    )
    remaining_limit = _parse_non_negative_decimal_setting(
        settings=settings,
        key="bot_money_limit_rub",
        label="Лимит денег для бота RUB",
    )

    created_at_utc = datetime.now(timezone.utc)

    planned_count = 0
    skipped_count = 0
    planned_amount = Decimal("0")

    for signal_id, signal in new_signal_records:
        status = BUY_INTENT_STATUS_SKIPPED
        reason = ""
        quantity_lots = 0
        quantity_shares = 0
        estimated_order_amount = Decimal("0")
        currency = signal.currency.upper()
        signal_requested_amount = (
            requested_amount
            if currency == MOEX_SHARE_CURRENCY
            else Decimal("0")
        )

        if not allow_buy:
            reason = "Покупки запрещены настройкой allow_buy."
        elif currency != MOEX_SHARE_CURRENCY:
            reason = (
                "Робот торгует только RUB-акциями MOEX: "
                f"currency={currency}."
            )
        elif requested_amount <= 0:
            reason = (
                "Сумма автопокупки RUB равна 0; "
                "покупки отключены."
            )
        elif remaining_limit <= 0:
            reason = (
                "Лимит денег бота RUB равен 0 или уже исчерпан; "
                "покупки отключены."
            )
        else:
            one_lot_amount = signal.current_price * Decimal(signal.lot)

            if one_lot_amount <= 0:
                reason = "Стоимость одного лота должна быть больше 0."
            else:
                quantity_lots = int(requested_amount // one_lot_amount)

                if quantity_lots <= 0:
                    reason = (
                        "Сумма одной покупки меньше стоимости одного лота: "
                        f"amount={requested_amount} RUB, "
                        f"one_lot={one_lot_amount} RUB."
                    )
                else:
                    quantity_shares = quantity_lots * signal.lot
                    estimated_order_amount = (
                        signal.current_price * Decimal(quantity_shares)
                    )

                    if estimated_order_amount > remaining_limit:
                        reason = (
                            "Недостаточно лимита денег бота в текущем цикле: "
                            f"need={estimated_order_amount} RUB, "
                            f"remaining={remaining_limit} RUB."
                        )
                    else:
                        status = BUY_INTENT_STATUS_PLANNED
                        reason = (
                            "Dry-run: покупка рассчитана, "
                            "заявка брокеру не отправлена."
                        )

        intent_id = save_buy_intent(
            created_at_utc=created_at_utc,
            growth_signal_id=signal_id,
            instrument_uid=signal.instrument_uid,
            ticker=signal.ticker,
            class_code=signal.class_code,
            name=signal.name,
            side=BUY_INTENT_SIDE_BUY,
            status=status,
            reason=reason,
            current_price=signal.current_price,
            growth_percent=signal.growth_percent,
            threshold_percent=report.growth_threshold_percent,
            requested_amount=signal_requested_amount,
            lot=signal.lot,
            quantity_lots=quantity_lots,
            quantity_shares=quantity_shares,
            estimated_order_amount=estimated_order_amount,
            currency=currency,
            is_dry_run=True,
        )

        if intent_id is None:
            continue

        if status == BUY_INTENT_STATUS_PLANNED:
            planned_count += 1
            planned_amount += estimated_order_amount
            remaining_limit -= estimated_order_amount
        elif status == BUY_INTENT_STATUS_SKIPPED:
            skipped_count += 1
        else:
            raise RuntimeError(f"Неизвестный статус buy_intent: {status}")

    return planned_count, skipped_count, planned_amount




def build_buy_intent_log_lines(
    planned_count: int,
    skipped_count: int,
    planned_amount: Decimal,
) -> list[str]:
    if planned_count == 0 and skipped_count == 0:
        return []

    return [
        (
            "Dry-run автопокупок: "
            f"планов={planned_count}, "
            f"пропущено={skipped_count}, "
            f"плановая сумма={planned_amount:.2f} RUB."
        ),
        "Реальные заявки брокеру не отправлялись.",
    ]




def save_success_cycle(
    started_at_utc: datetime,
    finished_at_utc: datetime,
    report: GrowthScanReport,
    new_signals_count: int,
    duplicate_signals_count: int,
) -> int:
    return save_growth_scan_cycle(
        started_at_utc=started_at_utc,
        finished_at_utc=finished_at_utc,
        status=GROWTH_SCAN_CYCLE_STATUS_SUCCESS,
        interval_label=report.interval_label,
        threshold_percent=report.growth_threshold_percent,
        selected_shares_count=report.total_selected_shares,
        prices_received_count=report.total_prices_received,
        snapshot_rows_saved=report.snapshot_rows_saved,
        results_count=len(report.results),
        signals_count=len(report.signals),
        new_signals_count=new_signals_count,
        duplicate_signals_count=duplicate_signals_count,
        skipped_count=len(report.skipped),
        candle_cache_hits=report.candle_cache_hits,
        candle_api_requests=report.candle_api_requests,
    )


def save_error_cycle(
    started_at_utc: datetime,
    finished_at_utc: datetime,
    error: Exception,
) -> int:
    error_text = str(error).strip()

    if not error_text:
        error_text = repr(error)

    return save_growth_scan_cycle(
        started_at_utc=started_at_utc,
        finished_at_utc=finished_at_utc,
        status=GROWTH_SCAN_CYCLE_STATUS_ERROR,
        error_type=type(error).__name__,
        error_text=error_text,
    )


def build_success_log_lines(
    scan_cycle_id: int,
    report: GrowthScanReport,
    new_signals_count: int,
    duplicate_signals_count: int,
    deleted_old_price_snapshots_count: int,
) -> list[str]:
    lines = [
        f"Growth monitor cycle #{scan_cycle_id}",
        f"Интервал расчёта роста: {report.interval_label}",
        f"Порог роста: {_format_percent(report.growth_threshold_percent)}",
        f"Макс. возраст цены: {report.max_price_age_seconds} сек.",
        f"Рабочих акций: {report.total_selected_shares}",
        f"Цен получено: {report.total_prices_received}",
        f"Snapshot сохранено: {report.snapshot_rows_saved}",
        f"Рассчитано: {len(report.results)}",
        f"Текущий рост обновлён: {len(report.results)}",
        f"Сигналов в расчёте: {len(report.signals)}",
        f"Новых сигналов сохранено: {new_signals_count}",
        f"Дубликатов сигнала пропущено: {duplicate_signals_count}",
        f"Удалено snapshot предыдущих свечей: {deleted_old_price_snapshots_count}",
        f"Всего сигналов в БД: {count_growth_signals()}",
        f"Всего циклов в БД: {count_growth_scan_cycles()}",
        f"Пропущено инструментов: {len(report.skipped)}",
        f"Свечи из cache: {report.candle_cache_hits}",
        f"Свечи из API: {report.candle_api_requests}",
    ]

    if report.skipped:
        lines.append("Причины пропуска инструментов:")

        for number, skipped_item in enumerate(report.skipped[:10], start=1):
            lines.append(f"{number}. {skipped_item}")

        if len(report.skipped) > 10:
            lines.append(f"... ещё {len(report.skipped) - 10}")

    if report.signals:
        lines.append("Сигналы текущего расчёта:")

        for number, signal in enumerate(report.signals[:20], start=1):
            lines.append(
                f"{number}. {signal.ticker}_{signal.class_code} "
                f"{_format_percent(signal.growth_percent)} "
                f"current={signal.current_price} "
                f"open={signal.candle_open_price} "
                f"candle_time_utc={signal.candle_time_utc} "
                f"source={signal.base_source}"
            )

    return lines




def save_current_growth_state(
    scan_cycle_id: int,
    calculated_at_utc: datetime,
    report: GrowthScanReport,
) -> int:
    rows = [
        GrowthCurrentStateInput(
            scan_cycle_id=scan_cycle_id,
            calculated_at_utc=calculated_at_utc,
            instrument_uid=result.instrument_uid,
            ticker=result.ticker,
            class_code=result.class_code,
            name=result.name,
            interval_label=report.interval_label,
            threshold_percent=report.growth_threshold_percent,
            current_price=result.current_price,
            candle_open_price=result.candle_open_price,
            growth_percent=result.growth_percent,
            candle_time_utc=result.candle_time_utc,
            candle_is_complete=result.candle_is_complete,
            last_price_time_utc=result.last_price_time_utc,
            base_source=result.base_source,
            is_signal=result.growth_percent >= report.growth_threshold_percent,
        )
        for result in report.results
    ]

    return save_growth_current_states(rows)
async def _sleep_with_stop(
    seconds: float,
    should_stop: StopCallback,
) -> None:
    if seconds <= 0:
        return

    remaining_seconds = seconds

    while remaining_seconds > 0 and not should_stop():
        chunk_seconds = min(0.2, remaining_seconds)
        await asyncio.sleep(chunk_seconds)
        remaining_seconds -= chunk_seconds


async def run_growth_monitor_service(
    should_stop: StopCallback,
    on_log: LogCallback,
) -> None:
    on_log("Growth monitor service запущен.")

    while not should_stop():
        cycle_started_at = datetime.now(timezone.utc)

        try:
            scan_interval_seconds = _get_scan_interval_seconds()

            report = await scan_growth_once()
            new_signals_count, duplicate_signals_count, new_signal_records = save_report_signals(report)

            if report.results:
                current_candle_start_utc = min(
                    result.candle_time_utc
                    for result in report.results
                )
                deleted_old_price_snapshots_count = (
                    delete_price_snapshots_older_than(
                        cutoff_time_utc=current_candle_start_utc,
                    )
                )
            else:
                deleted_old_price_snapshots_count = 0

            cycle_finished_at = datetime.now(timezone.utc)

            scan_cycle_id = save_success_cycle(
                started_at_utc=cycle_started_at,
                finished_at_utc=cycle_finished_at,
                report=report,
                new_signals_count=new_signals_count,
                duplicate_signals_count=duplicate_signals_count,
            )
            current_growth_rows_saved = save_current_growth_state(
                scan_cycle_id=scan_cycle_id,
                calculated_at_utc=cycle_finished_at,
                report=report,
            )
            settings = load_app_settings()
            auto_trading_enabled = settings["auto_trading_enabled"] == "1"

            if auto_trading_enabled:
                trade_log_lines = await execute_auto_trading_cycle(
                    report=report,
                    new_signal_records=new_signal_records,
                )
            else:
                (
                    planned_buy_intents_count,
                    skipped_buy_intents_count,
                    planned_buy_intents_amount,
                ) = save_dry_run_buy_intents(
                    new_signal_records=new_signal_records,
                    report=report,
                )
                trade_log_lines = build_buy_intent_log_lines(
                    planned_count=planned_buy_intents_count,
                    skipped_count=skipped_buy_intents_count,
                    planned_amount=planned_buy_intents_amount,
                )
        except Exception as error:
            cycle_finished_at = datetime.now(timezone.utc)

            try:
                error_cycle_id = save_error_cycle(
                    started_at_utc=cycle_started_at,
                    finished_at_utc=cycle_finished_at,
                    error=error,
                )
            except Exception as storage_error:
                error_cycle_id = 0
                on_log(
                    "Ошибка сохранения неуспешного цикла: "
                    f"{type(storage_error).__name__}: {storage_error}"
                )

            error_text = str(error).strip()

            if not error_text:
                error_text = repr(error)

            on_log(
                f"Ошибка цикла #{error_cycle_id}: "
                f"{type(error).__name__}: {error_text}"
            )

            await _sleep_with_stop(
                seconds=5,
                should_stop=should_stop,
            )
            continue

        success_log_lines = build_success_log_lines(
            scan_cycle_id=scan_cycle_id,
            report=report,
            new_signals_count=new_signals_count,
            duplicate_signals_count=duplicate_signals_count,
            deleted_old_price_snapshots_count=deleted_old_price_snapshots_count,
        )
        success_log_lines.extend(trade_log_lines)

        _emit_lines(
            on_log=on_log,
            lines=success_log_lines,
        )

        elapsed_seconds = (cycle_finished_at - cycle_started_at).total_seconds()
        sleep_seconds = scan_interval_seconds - elapsed_seconds

        if sleep_seconds <= 0:
            on_log(
                f"Цикл занял {elapsed_seconds:.2f} сек., "
                f"что больше интервала проверки {scan_interval_seconds} сек. "
                "Следующий цикл начнётся сразу."
            )

        await _sleep_with_stop(
            seconds=sleep_seconds,
            should_stop=should_stop,
        )

    on_log("Growth monitor service остановлен.")
