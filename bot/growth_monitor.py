import asyncio
from datetime import datetime, timezone

from bd.growth_scan_cycle import (
    GROWTH_SCAN_CYCLE_STATUS_ERROR,
    GROWTH_SCAN_CYCLE_STATUS_SUCCESS,
    count_growth_scan_cycles,
    save_growth_scan_cycle,
)
from bd.growth_signal import count_growth_signals, save_growth_signal
from bd.price_snapshot import cleanup_old_price_snapshots
from bd.settings_storage import load_app_settings
from bot.growth_scanner import GrowthScanReport, scan_growth_once


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



def _get_price_snapshot_retention_days() -> int:
    settings = load_app_settings()
    raw_value = settings["price_snapshot_retention_days"]

    try:
        retention_days = int(raw_value)
    except ValueError as error:
        raise ValueError("price_snapshot_retention_days должен быть целым числом.") from error

    if retention_days <= 0:
        raise ValueError("price_snapshot_retention_days должен быть больше 0.")

    return retention_days


def _format_percent(value) -> str:
    return f"{value:.4f}%"


def save_report_signals(report: GrowthScanReport) -> tuple[int, int]:
    detected_at_utc = datetime.now(timezone.utc)

    new_signals_count = 0
    duplicate_signals_count = 0

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

    return new_signals_count, duplicate_signals_count


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
    return save_growth_scan_cycle(
        started_at_utc=started_at_utc,
        finished_at_utc=finished_at_utc,
        status=GROWTH_SCAN_CYCLE_STATUS_ERROR,
        error_type=type(error).__name__,
        error_text=str(error),
    )


def print_monitor_report(
    scan_cycle_id: int,
    report: GrowthScanReport,
    new_signals_count: int,
    duplicate_signals_count: int,
    deleted_old_price_snapshots_count: int,
) -> None:
    now_utc = datetime.now(timezone.utc).isoformat()

    print()
    print(f"[{now_utc}] Growth monitor cycle #{scan_cycle_id}")
    print(f"Интервал расчёта роста: {report.interval_label}")
    print(f"Порог роста: {_format_percent(report.growth_threshold_percent)}")
    print(f"Макс. возраст цены: {report.max_price_age_seconds} сек.")
    print(f"Рабочих акций: {report.total_selected_shares}")
    print(f"Цен получено: {report.total_prices_received}")
    print(f"Snapshot сохранено: {report.snapshot_rows_saved}")
    print(f"Рассчитано: {len(report.results)}")
    print(f"Сигналов в расчёте: {len(report.signals)}")
    print(f"Новых сигналов сохранено: {new_signals_count}")
    print(f"Дубликатов сигнала пропущено: {duplicate_signals_count}")
    print(f"Удалено старых snapshot-строк: {deleted_old_price_snapshots_count}")
    print(f"Всего сигналов в БД: {count_growth_signals()}")
    print(f"Всего циклов в БД: {count_growth_scan_cycles()}")
    print(f"Пропущено инструментов: {len(report.skipped)}")
    print(f"Свечи из cache: {report.candle_cache_hits}")
    print(f"Свечи из API: {report.candle_api_requests}")

    if report.signals:
        print("Сигналы текущего расчёта:")

        for number, signal in enumerate(report.signals[:20], start=1):
            print(
                f"{number}. {signal.ticker}_{signal.class_code} "
                f"{_format_percent(signal.growth_percent)} "
                f"current={signal.current_price} "
                f"open={signal.candle_open_price} "
                f"candle_time_utc={signal.candle_time_utc} "
                f"source={signal.base_source}"
            )


async def run_growth_monitor() -> None:
    print("Growth monitor запущен. Остановка: Ctrl+C")

    while True:
        cycle_started_at = datetime.now(timezone.utc)

        try:
            scan_interval_seconds = _get_scan_interval_seconds()
            price_snapshot_retention_days = _get_price_snapshot_retention_days()
            report = await scan_growth_once()
            new_signals_count, duplicate_signals_count = save_report_signals(report)
            deleted_old_price_snapshots_count = cleanup_old_price_snapshots(
                retention_days=price_snapshot_retention_days,
            )
            cycle_finished_at = datetime.now(timezone.utc)
            scan_cycle_id = save_success_cycle(
                started_at_utc=cycle_started_at,
                finished_at_utc=cycle_finished_at,
                report=report,
                new_signals_count=new_signals_count,
                duplicate_signals_count=duplicate_signals_count,
            deleted_old_price_snapshots_count=deleted_old_price_snapshots_count,
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
                print()
                print(
                    f"[{datetime.now(timezone.utc).isoformat()}] "
                    f"Ошибка сохранения неуспешного цикла: "
                    f"{type(storage_error).__name__}: {storage_error}"
                )

            print()
            print(
                f"[{datetime.now(timezone.utc).isoformat()}] "
                f"Ошибка цикла #{error_cycle_id}: {type(error).__name__}: {error}"
            )

            await asyncio.sleep(5)
            continue

        print_monitor_report(
            scan_cycle_id=scan_cycle_id,
            report=report,
            new_signals_count=new_signals_count,
            duplicate_signals_count=duplicate_signals_count,
        )

        elapsed_seconds = (cycle_finished_at - cycle_started_at).total_seconds()
        sleep_seconds = scan_interval_seconds - elapsed_seconds

        if sleep_seconds > 0:
            await asyncio.sleep(sleep_seconds)
        else:
            print(
                f"Цикл занял {elapsed_seconds:.2f} сек., "
                f"что больше интервала проверки {scan_interval_seconds} сек. "
                f"Следующий цикл начнётся сразу."
            )


async def main() -> None:
    await run_growth_monitor()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print()
        print("Growth monitor остановлен пользователем.")
