from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from bd.database import get_connection


GROWTH_SCAN_CYCLE_STATUS_SUCCESS = "SUCCESS"
GROWTH_SCAN_CYCLE_STATUS_ERROR = "ERROR"


@dataclass(frozen=True)
class GrowthScanCycle:
    id: int
    started_at_utc: datetime
    finished_at_utc: datetime
    duration_seconds: Decimal
    status: str
    interval_label: str | None
    threshold_percent: Decimal | None
    selected_shares_count: int | None
    prices_received_count: int | None
    snapshot_rows_saved: int | None
    results_count: int | None
    signals_count: int | None
    new_signals_count: int | None
    duplicate_signals_count: int | None
    skipped_count: int | None
    candle_cache_hits: int | None
    candle_api_requests: int | None
    error_type: str | None
    error_text: str | None


def init_growth_scan_cycle_storage() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS growth_scan_cycle (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at_utc TEXT NOT NULL,
                finished_at_utc TEXT NOT NULL,
                duration_seconds TEXT NOT NULL,
                status TEXT NOT NULL,
                interval_label TEXT,
                threshold_percent TEXT,
                selected_shares_count INTEGER,
                prices_received_count INTEGER,
                snapshot_rows_saved INTEGER,
                results_count INTEGER,
                signals_count INTEGER,
                new_signals_count INTEGER,
                duplicate_signals_count INTEGER,
                skipped_count INTEGER,
                candle_cache_hits INTEGER,
                candle_api_requests INTEGER,
                error_type TEXT,
                error_text TEXT
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_growth_scan_cycle_started_at
            ON growth_scan_cycle (started_at_utc)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_growth_scan_cycle_status
            ON growth_scan_cycle (status)
            """
        )


def _datetime_to_storage_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("datetime должен быть timezone-aware.")

    return value.astimezone(timezone.utc).isoformat()


def _datetime_from_storage_text(value: str) -> datetime:
    parsed_value = datetime.fromisoformat(value)

    if parsed_value.tzinfo is None:
        raise RuntimeError(f"В БД сохранён datetime без timezone: {value}")

    return parsed_value.astimezone(timezone.utc)


def _optional_decimal_from_storage_text(value: str | None) -> Decimal | None:
    if value is None:
        return None

    return Decimal(value)


def _row_to_growth_scan_cycle(row) -> GrowthScanCycle:
    return GrowthScanCycle(
        id=row["id"],
        started_at_utc=_datetime_from_storage_text(row["started_at_utc"]),
        finished_at_utc=_datetime_from_storage_text(row["finished_at_utc"]),
        duration_seconds=Decimal(row["duration_seconds"]),
        status=row["status"],
        interval_label=row["interval_label"],
        threshold_percent=_optional_decimal_from_storage_text(row["threshold_percent"]),
        selected_shares_count=row["selected_shares_count"],
        prices_received_count=row["prices_received_count"],
        snapshot_rows_saved=row["snapshot_rows_saved"],
        results_count=row["results_count"],
        signals_count=row["signals_count"],
        new_signals_count=row["new_signals_count"],
        duplicate_signals_count=row["duplicate_signals_count"],
        skipped_count=row["skipped_count"],
        candle_cache_hits=row["candle_cache_hits"],
        candle_api_requests=row["candle_api_requests"],
        error_type=row["error_type"],
        error_text=row["error_text"],
    )


def save_growth_scan_cycle(
    started_at_utc: datetime,
    finished_at_utc: datetime,
    status: str,
    interval_label: str | None = None,
    threshold_percent: Decimal | None = None,
    selected_shares_count: int | None = None,
    prices_received_count: int | None = None,
    snapshot_rows_saved: int | None = None,
    results_count: int | None = None,
    signals_count: int | None = None,
    new_signals_count: int | None = None,
    duplicate_signals_count: int | None = None,
    skipped_count: int | None = None,
    candle_cache_hits: int | None = None,
    candle_api_requests: int | None = None,
    error_type: str | None = None,
    error_text: str | None = None,
) -> int:
    init_growth_scan_cycle_storage()

    if started_at_utc.tzinfo is None:
        raise ValueError("started_at_utc должен быть timezone-aware.")

    if finished_at_utc.tzinfo is None:
        raise ValueError("finished_at_utc должен быть timezone-aware.")

    if finished_at_utc < started_at_utc:
        raise ValueError("finished_at_utc не может быть меньше started_at_utc.")

    if status not in {
        GROWTH_SCAN_CYCLE_STATUS_SUCCESS,
        GROWTH_SCAN_CYCLE_STATUS_ERROR,
    }:
        raise ValueError(f"Некорректный статус цикла мониторинга: {status}")

    duration_seconds = Decimal(
        str((finished_at_utc - started_at_utc).total_seconds())
    )

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO growth_scan_cycle (
                started_at_utc,
                finished_at_utc,
                duration_seconds,
                status,
                interval_label,
                threshold_percent,
                selected_shares_count,
                prices_received_count,
                snapshot_rows_saved,
                results_count,
                signals_count,
                new_signals_count,
                duplicate_signals_count,
                skipped_count,
                candle_cache_hits,
                candle_api_requests,
                error_type,
                error_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _datetime_to_storage_text(started_at_utc),
                _datetime_to_storage_text(finished_at_utc),
                str(duration_seconds),
                status,
                interval_label,
                str(threshold_percent) if threshold_percent is not None else None,
                selected_shares_count,
                prices_received_count,
                snapshot_rows_saved,
                results_count,
                signals_count,
                new_signals_count,
                duplicate_signals_count,
                skipped_count,
                candle_cache_hits,
                candle_api_requests,
                error_type,
                error_text,
            ),
        )

    return cursor.lastrowid


def list_recent_growth_scan_cycles(limit: int = 50) -> list[GrowthScanCycle]:
    init_growth_scan_cycle_storage()

    if limit <= 0:
        raise ValueError("limit должен быть больше 0.")

    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM growth_scan_cycle
            ORDER BY started_at_utc DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [
        _row_to_growth_scan_cycle(row)
        for row in rows
    ]


def count_growth_scan_cycles() -> int:
    init_growth_scan_cycle_storage()

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM growth_scan_cycle
            """
        ).fetchone()

    return row["total"]
