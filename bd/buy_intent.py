from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from bd.database import get_connection


BUY_INTENT_SIDE_BUY = "BUY"
BUY_INTENT_STATUS_PLANNED = "DRY_RUN_PLANNED"
BUY_INTENT_STATUS_SKIPPED = "SKIPPED"


@dataclass(frozen=True)
class BuyIntent:
    id: int
    created_at_utc: datetime
    growth_signal_id: int
    instrument_uid: str
    ticker: str
    class_code: str
    name: str
    side: str
    status: str
    reason: str
    current_price: Decimal
    growth_percent: Decimal
    threshold_percent: Decimal
    requested_amount: Decimal
    lot: int
    quantity_lots: int
    quantity_shares: int
    estimated_order_amount: Decimal
    currency: str
    is_dry_run: bool


def init_buy_intent_storage() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS buy_intent (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at_utc TEXT NOT NULL,
                growth_signal_id INTEGER NOT NULL,
                instrument_uid TEXT NOT NULL,
                ticker TEXT NOT NULL,
                class_code TEXT NOT NULL,
                name TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                current_price TEXT NOT NULL,
                growth_percent TEXT NOT NULL,
                threshold_percent TEXT NOT NULL,
                requested_amount TEXT NOT NULL,
                lot INTEGER NOT NULL,
                quantity_lots INTEGER NOT NULL,
                quantity_shares INTEGER NOT NULL,
                estimated_order_amount TEXT NOT NULL,
                currency TEXT NOT NULL,
                is_dry_run INTEGER NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_buy_intent_signal_side
            ON buy_intent (growth_signal_id, side)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_buy_intent_created_at
            ON buy_intent (created_at_utc)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_buy_intent_status
            ON buy_intent (status)
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


def _row_to_buy_intent(row) -> BuyIntent:
    return BuyIntent(
        id=row["id"],
        created_at_utc=_datetime_from_storage_text(row["created_at_utc"]),
        growth_signal_id=row["growth_signal_id"],
        instrument_uid=row["instrument_uid"],
        ticker=row["ticker"],
        class_code=row["class_code"],
        name=row["name"],
        side=row["side"],
        status=row["status"],
        reason=row["reason"],
        current_price=Decimal(row["current_price"]),
        growth_percent=Decimal(row["growth_percent"]),
        threshold_percent=Decimal(row["threshold_percent"]),
        requested_amount=Decimal(row["requested_amount"]),
        lot=row["lot"],
        quantity_lots=row["quantity_lots"],
        quantity_shares=row["quantity_shares"],
        estimated_order_amount=Decimal(row["estimated_order_amount"]),
        currency=row["currency"],
        is_dry_run=bool(row["is_dry_run"]),
    )


def save_buy_intent(
    created_at_utc: datetime,
    growth_signal_id: int,
    instrument_uid: str,
    ticker: str,
    class_code: str,
    name: str,
    side: str,
    status: str,
    reason: str,
    current_price: Decimal,
    growth_percent: Decimal,
    threshold_percent: Decimal,
    requested_amount: Decimal,
    lot: int,
    quantity_lots: int,
    quantity_shares: int,
    estimated_order_amount: Decimal,
    currency: str,
    is_dry_run: bool,
) -> int | None:
    init_buy_intent_storage()

    if growth_signal_id <= 0:
        raise ValueError("growth_signal_id должен быть больше 0.")

    if not instrument_uid.strip():
        raise ValueError("instrument_uid не может быть пустым.")

    if not ticker.strip():
        raise ValueError("ticker не может быть пустым.")

    if not class_code.strip():
        raise ValueError("class_code не может быть пустым.")

    if not side.strip():
        raise ValueError("side не может быть пустым.")

    if not status.strip():
        raise ValueError("status не может быть пустым.")

    if current_price <= 0:
        raise ValueError("current_price должен быть больше 0.")

    if requested_amount <= 0:
        raise ValueError("requested_amount должен быть больше 0.")

    if lot <= 0:
        raise ValueError("lot должен быть больше 0.")

    if quantity_lots < 0:
        raise ValueError("quantity_lots не может быть меньше 0.")

    if quantity_shares < 0:
        raise ValueError("quantity_shares не может быть меньше 0.")

    if estimated_order_amount < 0:
        raise ValueError("estimated_order_amount не может быть меньше 0.")

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO buy_intent (
                created_at_utc,
                growth_signal_id,
                instrument_uid,
                ticker,
                class_code,
                name,
                side,
                status,
                reason,
                current_price,
                growth_percent,
                threshold_percent,
                requested_amount,
                lot,
                quantity_lots,
                quantity_shares,
                estimated_order_amount,
                currency,
                is_dry_run
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _datetime_to_storage_text(created_at_utc),
                growth_signal_id,
                instrument_uid,
                ticker,
                class_code,
                name,
                side,
                status,
                reason,
                str(current_price),
                str(growth_percent),
                str(threshold_percent),
                str(requested_amount),
                lot,
                quantity_lots,
                quantity_shares,
                str(estimated_order_amount),
                currency,
                1 if is_dry_run else 0,
            ),
        )

    if cursor.rowcount == 0:
        return None

    return cursor.lastrowid


def list_recent_buy_intents(limit: int = 100) -> list[BuyIntent]:
    init_buy_intent_storage()

    if limit <= 0:
        raise ValueError("limit должен быть больше 0.")

    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                id,
                created_at_utc,
                growth_signal_id,
                instrument_uid,
                ticker,
                class_code,
                name,
                side,
                status,
                reason,
                current_price,
                growth_percent,
                threshold_percent,
                requested_amount,
                lot,
                quantity_lots,
                quantity_shares,
                estimated_order_amount,
                currency,
                is_dry_run
            FROM buy_intent
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [
        _row_to_buy_intent(row)
        for row in rows
    ]
