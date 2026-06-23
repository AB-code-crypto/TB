from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from bd.database import get_connection


@dataclass(frozen=True)
class GrowthCandleCache:
    instrument_uid: str
    interval_label: str
    candle_time_utc: datetime
    open_price: Decimal
    is_complete: bool
    updated_at_utc: datetime


def init_growth_candle_cache_storage() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS growth_candle_cache (
                instrument_uid TEXT NOT NULL,
                interval_label TEXT NOT NULL,
                candle_time_utc TEXT NOT NULL,
                open_price TEXT NOT NULL,
                is_complete INTEGER NOT NULL,
                updated_at_utc TEXT NOT NULL,
                PRIMARY KEY (instrument_uid, interval_label)
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_growth_candle_cache_interval
            ON growth_candle_cache (interval_label, candle_time_utc)
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


def _row_to_growth_candle_cache(row) -> GrowthCandleCache:
    return GrowthCandleCache(
        instrument_uid=row["instrument_uid"],
        interval_label=row["interval_label"],
        candle_time_utc=_datetime_from_storage_text(row["candle_time_utc"]),
        open_price=Decimal(row["open_price"]),
        is_complete=bool(row["is_complete"]),
        updated_at_utc=_datetime_from_storage_text(row["updated_at_utc"]),
    )


def save_growth_candle_cache(
    instrument_uid: str,
    interval_label: str,
    candle_time_utc: datetime,
    open_price: Decimal,
    is_complete: bool,
    updated_at_utc: datetime | None = None,
) -> None:
    init_growth_candle_cache_storage()

    if not instrument_uid.strip():
        raise ValueError("instrument_uid не может быть пустым.")

    if not interval_label.strip():
        raise ValueError("interval_label не может быть пустым.")

    if open_price <= 0:
        raise ValueError("open_price должен быть больше 0.")

    if updated_at_utc is None:
        updated_at_utc = datetime.now(timezone.utc)

    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO growth_candle_cache (
                instrument_uid,
                interval_label,
                candle_time_utc,
                open_price,
                is_complete,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_uid, interval_label) DO UPDATE SET
                candle_time_utc = excluded.candle_time_utc,
                open_price = excluded.open_price,
                is_complete = excluded.is_complete,
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                instrument_uid,
                interval_label,
                _datetime_to_storage_text(candle_time_utc),
                str(open_price),
                1 if is_complete else 0,
                _datetime_to_storage_text(updated_at_utc),
            ),
        )


def get_growth_candle_cache(
    instrument_uid: str,
    interval_label: str,
) -> GrowthCandleCache | None:
    init_growth_candle_cache_storage()

    if not instrument_uid.strip():
        raise ValueError("instrument_uid не может быть пустым.")

    if not interval_label.strip():
        raise ValueError("interval_label не может быть пустым.")

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT *
            FROM growth_candle_cache
            WHERE instrument_uid = ?
              AND interval_label = ?
            LIMIT 1
            """,
            (instrument_uid, interval_label),
        ).fetchone()

    if row is None:
        return None

    return _row_to_growth_candle_cache(row)
