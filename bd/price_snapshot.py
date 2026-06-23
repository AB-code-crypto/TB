from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from bd.database import get_connection
from tbank.last_prices import TBankLastPrice


@dataclass(frozen=True)
class PriceSnapshot:
    id: int
    captured_at_utc: datetime
    instrument_uid: str
    figi: str
    ticker: str
    class_code: str
    price: Decimal
    price_time_utc: datetime
    last_price_type: str


def init_price_snapshot_storage() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS price_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                captured_at_utc TEXT NOT NULL,
                instrument_uid TEXT NOT NULL,
                figi TEXT NOT NULL,
                ticker TEXT NOT NULL,
                class_code TEXT NOT NULL,
                price TEXT NOT NULL,
                price_time_utc TEXT NOT NULL,
                last_price_type TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_price_snapshot_uid_captured_at
            ON price_snapshot (instrument_uid, captured_at_utc)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_price_snapshot_uid_price_time
            ON price_snapshot (instrument_uid, price_time_utc)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_price_snapshot_ticker_captured_at
            ON price_snapshot (ticker, class_code, captured_at_utc)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_price_snapshot_captured_at
            ON price_snapshot (captured_at_utc)
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


def _row_to_price_snapshot(row) -> PriceSnapshot:
    return PriceSnapshot(
        id=row["id"],
        captured_at_utc=_datetime_from_storage_text(row["captured_at_utc"]),
        instrument_uid=row["instrument_uid"],
        figi=row["figi"],
        ticker=row["ticker"],
        class_code=row["class_code"],
        price=Decimal(row["price"]),
        price_time_utc=_datetime_from_storage_text(row["price_time_utc"]),
        last_price_type=row["last_price_type"],
    )


def save_price_snapshot(
    prices: list[TBankLastPrice],
    captured_at_utc: datetime | None = None,
) -> int:
    init_price_snapshot_storage()

    if not prices:
        raise ValueError("prices не может быть пустым.")

    if captured_at_utc is None:
        captured_at_utc = datetime.now(timezone.utc)

    captured_at_text = _datetime_to_storage_text(captured_at_utc)

    rows = [
        (
            captured_at_text,
            price.instrument_uid,
            price.figi,
            price.ticker,
            price.class_code,
            str(price.price),
            _datetime_to_storage_text(price.time),
            price.last_price_type,
        )
        for price in prices
    ]

    with get_connection() as connection:
        cursor = connection.executemany(
            """
            INSERT INTO price_snapshot (
                captured_at_utc,
                instrument_uid,
                figi,
                ticker,
                class_code,
                price,
                price_time_utc,
                last_price_type
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    return cursor.rowcount


def get_latest_price_snapshot(instrument_uid: str) -> PriceSnapshot | None:
    init_price_snapshot_storage()

    if not instrument_uid.strip():
        raise ValueError("instrument_uid не может быть пустым.")

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT *
            FROM price_snapshot
            WHERE instrument_uid = ?
            ORDER BY captured_at_utc DESC, id DESC
            LIMIT 1
            """,
            (instrument_uid,),
        ).fetchone()

    if row is None:
        return None

    return _row_to_price_snapshot(row)


def get_price_snapshot_at_or_before(
    instrument_uid: str,
    target_time_utc: datetime,
) -> PriceSnapshot | None:
    init_price_snapshot_storage()

    if not instrument_uid.strip():
        raise ValueError("instrument_uid не может быть пустым.")

    target_time_text = _datetime_to_storage_text(target_time_utc)

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT *
            FROM price_snapshot
            WHERE instrument_uid = ?
              AND captured_at_utc <= ?
            ORDER BY captured_at_utc DESC, id DESC
            LIMIT 1
            """,
            (instrument_uid, target_time_text),
        ).fetchone()

    if row is None:
        return None

    return _row_to_price_snapshot(row)


def get_first_price_snapshot_at_or_after(
    instrument_uid: str,
    target_time_utc: datetime,
) -> PriceSnapshot | None:
    init_price_snapshot_storage()

    if not instrument_uid.strip():
        raise ValueError("instrument_uid не может быть пустым.")

    target_time_text = _datetime_to_storage_text(target_time_utc)

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT *
            FROM price_snapshot
            WHERE instrument_uid = ?
              AND captured_at_utc >= ?
            ORDER BY captured_at_utc ASC, id ASC
            LIMIT 1
            """,
            (instrument_uid, target_time_text),
        ).fetchone()

    if row is None:
        return None

    return _row_to_price_snapshot(row)


def count_price_snapshots() -> int:
    init_price_snapshot_storage()

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM price_snapshot
            """
        ).fetchone()

    return row["total"]



def delete_price_snapshots_older_than(cutoff_time_utc: datetime) -> int:
    init_price_snapshot_storage()

    if cutoff_time_utc.tzinfo is None:
        raise ValueError("cutoff_time_utc должен быть timezone-aware.")

    cutoff_time_text = _datetime_to_storage_text(cutoff_time_utc)

    with get_connection() as connection:
        cursor = connection.execute(
            """
            DELETE FROM price_snapshot
            WHERE captured_at_utc < ?
            """,
            (cutoff_time_text,),
        )

    return cursor.rowcount


def cleanup_old_price_snapshots(
    retention_days: int,
    now_utc: datetime | None = None,
) -> int:
    if retention_days <= 0:
        raise ValueError("retention_days должен быть больше 0.")

    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    if now_utc.tzinfo is None:
        raise ValueError("now_utc должен быть timezone-aware.")

    cutoff_time_utc = now_utc.astimezone(timezone.utc) - timedelta(days=retention_days)

    return delete_price_snapshots_older_than(cutoff_time_utc)
