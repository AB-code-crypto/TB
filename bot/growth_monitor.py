import asyncio
from datetime import datetime, timezone

from bot.growth_scanner import GrowthScanReport, scan_growth_once
from bd.settings_storage import load_app_settings


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


def _format_percent(value) -> str:
    return f"{value:.4f}%"


def print_monitor_report(report: GrowthScanReport) -> None:
    now_utc = datetime.now(timezone.utc).isoformat()

    print()
    print(f"[{now_utc}] Growth monitor cycle")
    print(f"Интервал расчёта роста: {report.interval_label}")
    print(f"Порог роста: {_format_percent(report.growth_threshold_percent)}")
    print(f"Рабочих акций: {report.total_selected_shares}")
    print(f"Цен получено: {report.total_prices_received}")
    print(f"Snapshot сохранено: {report.snapshot_rows_saved}")
    print(f"Рассчитано: {len(report.results)}")
    print(f"Сигналов: {len(report.signals)}")
    print(f"Пропущено: {len(report.skipped)}")
    print(f"Свечи из cache: {report.candle_cache_hits}")
    print(f"Свечи из API: {report.candle_api_requests}")

    if report.signals:
        print("Сигналы:")

        for number, signal in enumerate(report.signals[:20], start=1):
            print(
                f"{number}. {signal.ticker}_{signal.class_code} "
                f"{_format_percent(signal.growth_percent)} "
                f"current={signal.current_price} "
                f"open={signal.candle_open_price} "
                f"source={signal.base_source}"
            )


async def run_growth_monitor() -> None:
    print("Growth monitor запущен. Остановка: Ctrl+C")

    while True:
        cycle_started_at = datetime.now(timezone.utc)

        try:
            scan_interval_seconds = _get_scan_interval_seconds()
            report = await scan_growth_once()
        except Exception as error:
            print()
            print(
                f"[{datetime.now(timezone.utc).isoformat()}] "
                f"Ошибка цикла: {type(error).__name__}: {error}"
            )

            await asyncio.sleep(5)
            continue

        print_monitor_report(report)

        cycle_finished_at = datetime.now(timezone.utc)
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
