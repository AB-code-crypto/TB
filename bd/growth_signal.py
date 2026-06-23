from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from bd.database import get_connection


GROWTH_SIGNAL_STATUS_NEW = "NEW"


@dataclass(frozen=True)
class GrowthSignal:
    id: int
    detected_at_utc: datetime
    instrument_uid: str
    ticker: str
    class_code: str
    name: str
    interval_label: str
    candle_time_utc: datetime
    current_price: Decimal
    candle_open_price: Decimal
    growth_percent: Decimal
    threshold_percent: Decimal
    last_price_time_utc: datetime
    base_source: str
    status: str


def init_growth_signal_storage() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS growth_signal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                detected_at_utc TEXT NOT NULL,
                instrument_uid TEXT NOT NULL,
                ticker TEXT NOT NULL,
                class_code TEXT NOT NULL,
                name TEXT NOT NULL,
                interval_label TEXT NOT NULL,
                candle_time_utc TEXT NOT NULL,
                current_price TEXT NOT NULL,
                candle_open_price TEXT NOT NULL,
                growth_percent TEXT NOT NULL,
                threshold_percent TEXT NOT NULL,
                last_price_time_utc TEXT NOT NULL,
                base_source TEXT NOT NULL,
                status TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_growth_signal_uid_interval_candle
            ON growth_signal (instrument_uid, interval_label, candle_time_utc)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_growth_signal_detected_at
            ON growth_signal (detected_at_utc)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_growth_signal_status
            ON growth_signal (status)
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


def _row_to_growth_signal(row) -> GrowthSignal:
    return GrowthSignal(
        id=row["id"],
        detected_at_utc=_datetime_from_storage_text(row["detected_at_utc"]),
        instrument_uid=row["instrument_uid"],
        ticker=row["ticker"],
        class_code=row["class_code"],
        name=row["name"],
        interval_label=row["interval_label"],
        candle_time_utc=_datetime_from_storage_text(row["candle_time_utc"]),
        current_price=Decimal(row["current_price"]),
        candle_open_price=Decimal(row["candle_open_price"]),
        growth_percent=Decimal(row["growth_percent"]),
        threshold_percent=Decimal(row["threshold_percent"]),
        last_price_time_utc=_datetime_from_storage_text(row["last_price_time_utc"]),
        base_source=row["base_source"],
        status=row["status"],
    )


def signal_exists_for_candle(
    instrument_uid: str,
    interval_label: str,
    candle_time_utc: datetime,
) -> bool:
    init_growth_signal_storage()

    if not instrument_uid.strip():
        raise ValueError("instrument_uid не может быть пустым.")

    if not interval_label.strip():
        raise ValueError("interval_label не может быть пустым.")

    candle_time_text = _datetime_to_storage_text(candle_time_utc)

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id
            FROM growth_signal
            WHERE instrument_uid = ?
              AND interval_label = ?
              AND candle_time_utc = ?
            LIMIT 1
            """,
            (instrument_uid, interval_label, candle_time_text),
        ).fetchone()

    return row is not None


def save_growth_signal(
    detected_at_utc: datetime,
    instrument_uid: str,
    ticker: str,
    class_code: str,
    name: str,
    interval_label: str,
    candle_time_utc: datetime,
    current_price: Decimal,
    candle_open_price: Decimal,
    growth_percent: Decimal,
    threshold_percent: Decimal,
    last_price_time_utc: datetime,
    base_source: str,
    status: str = GROWTH_SIGNAL_STATUS_NEW,
) -> int | None:
    init_growth_signal_storage()

    if not instrument_uid.strip():
        raise ValueError("instrument_uid не может быть пустым.")

    if not ticker.strip():
        raise ValueError("ticker не может быть пустым.")

    if not class_code.strip():
        raise ValueError("class_code не может быть пустым.")

    if not interval_label.strip():
        raise ValueError("interval_label не может быть пустым.")

    if current_price <= 0:
        raise ValueError("current_price должен быть больше 0.")

    if candle_open_price <= 0:
        raise ValueError("candle_open_price должен быть больше 0.")

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO growth_signal (
                detected_at_utc,
                instrument_uid,
                ticker,
                class_code,
                name,
                interval_label,
                candle_time_utc,
                current_price,
                candle_open_price,
                growth_percent,
                threshold_percent,
                last_price_time_utc,
                base_source,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _datetime_to_storage_text(detected_at_utc),
                instrument_uid,
                ticker,
                class_code,
                name,
                interval_label,
                _datetime_to_storage_text(candle_time_utc),
                str(current_price),
                str(candle_open_price),
                str(growth_percent),
                str(threshold_percent),
                _datetime_to_storage_text(last_price_time_utc),
                base_source,
                status,
            ),
        )

    if cursor.rowcount == 0:
        return None

    return cursor.lastrowid


def list_recent_growth_signals(limit: int = 50) -> list[GrowthSignal]:
    init_growth_signal_storage()

    if limit <= 0:
        raise ValueError("limit должен быть больше 0.")

    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM growth_signal
            ORDER BY detected_at_utc DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [
        _row_to_growth_signal(row)
        for row in rows
    ]


def count_growth_signals() -> int:
    init_growth_signal_storage()

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM growth_signal
            """
        ).fetchone()

    return row["total"]
