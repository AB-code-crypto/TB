from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from bd.database import get_connection


@dataclass(frozen=True)
class GrowthCurrentStateInput:
    scan_cycle_id: int
    calculated_at_utc: datetime
    instrument_uid: str
    ticker: str
    class_code: str
    name: str
    interval_label: str
    threshold_percent: Decimal
    current_price: Decimal
    candle_open_price: Decimal
    growth_percent: Decimal
    candle_time_utc: datetime
    candle_is_complete: bool
    last_price_time_utc: datetime
    base_source: str
    is_signal: bool


@dataclass(frozen=True)
class GrowthCurrentState:
    scan_cycle_id: int
    calculated_at_utc: datetime
    instrument_uid: str
    ticker: str
    class_code: str
    name: str
    interval_label: str
    threshold_percent: Decimal
    current_price: Decimal
    candle_open_price: Decimal
    growth_percent: Decimal
    candle_time_utc: datetime
    candle_is_complete: bool
    last_price_time_utc: datetime
    base_source: str
    is_signal: bool


def init_growth_current_state_storage() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS growth_current_state (
                instrument_uid TEXT PRIMARY KEY,
                scan_cycle_id INTEGER NOT NULL,
                calculated_at_utc TEXT NOT NULL,
                ticker TEXT NOT NULL,
                class_code TEXT NOT NULL,
                name TEXT NOT NULL,
                interval_label TEXT NOT NULL,
                threshold_percent TEXT NOT NULL,
                current_price TEXT NOT NULL,
                candle_open_price TEXT NOT NULL,
                growth_percent TEXT NOT NULL,
                candle_time_utc TEXT NOT NULL,
                candle_is_complete INTEGER NOT NULL,
                last_price_time_utc TEXT NOT NULL,
                base_source TEXT NOT NULL,
                is_signal INTEGER NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_growth_current_state_growth
            ON growth_current_state (growth_percent)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_growth_current_state_cycle
            ON growth_current_state (scan_cycle_id)
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


def _row_to_growth_current_state(row) -> GrowthCurrentState:
    return GrowthCurrentState(
        scan_cycle_id=row["scan_cycle_id"],
        calculated_at_utc=_datetime_from_storage_text(row["calculated_at_utc"]),
        instrument_uid=row["instrument_uid"],
        ticker=row["ticker"],
        class_code=row["class_code"],
        name=row["name"],
        interval_label=row["interval_label"],
        threshold_percent=Decimal(row["threshold_percent"]),
        current_price=Decimal(row["current_price"]),
        candle_open_price=Decimal(row["candle_open_price"]),
        growth_percent=Decimal(row["growth_percent"]),
        candle_time_utc=_datetime_from_storage_text(row["candle_time_utc"]),
        candle_is_complete=bool(row["candle_is_complete"]),
        last_price_time_utc=_datetime_from_storage_text(row["last_price_time_utc"]),
        base_source=row["base_source"],
        is_signal=bool(row["is_signal"]),
    )


def save_growth_current_states(rows: list[GrowthCurrentStateInput]) -> int:
    init_growth_current_state_storage()

    db_rows = [
        (
            row.instrument_uid,
            row.scan_cycle_id,
            _datetime_to_storage_text(row.calculated_at_utc),
            row.ticker,
            row.class_code,
            row.name,
            row.interval_label,
            str(row.threshold_percent),
            str(row.current_price),
            str(row.candle_open_price),
            str(row.growth_percent),
            _datetime_to_storage_text(row.candle_time_utc),
            1 if row.candle_is_complete else 0,
            _datetime_to_storage_text(row.last_price_time_utc),
            row.base_source,
            1 if row.is_signal else 0,
        )
        for row in rows
    ]

    with get_connection() as connection:
        connection.execute("DELETE FROM growth_current_state")

        if not db_rows:
            return 0

        cursor = connection.executemany(
            """
            INSERT INTO growth_current_state (
                instrument_uid,
                scan_cycle_id,
                calculated_at_utc,
                ticker,
                class_code,
                name,
                interval_label,
                threshold_percent,
                current_price,
                candle_open_price,
                growth_percent,
                candle_time_utc,
                candle_is_complete,
                last_price_time_utc,
                base_source,
                is_signal
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            db_rows,
        )

    return cursor.rowcount


def list_growth_current_states(limit: int = 500) -> list[GrowthCurrentState]:
    init_growth_current_state_storage()

    if limit <= 0:
        raise ValueError("limit должен быть больше 0.")

    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM growth_current_state
            ORDER BY CAST(growth_percent AS REAL) DESC, ticker ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [
        _row_to_growth_current_state(row)
        for row in rows
    ]


def count_growth_current_states() -> int:
    init_growth_current_state_storage()

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM growth_current_state
            """
        ).fetchone()

    return row["total"]
