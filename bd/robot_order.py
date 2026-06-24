from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from bd.database import get_connection


ROBOT_ORDER_SIDE_BUY = "BUY"
ROBOT_ORDER_SIDE_SELL = "SELL"

ROBOT_ORDER_STATUS_PREPARED = "PREPARED"
ROBOT_ORDER_STATUS_SENT = "SENT"
ROBOT_ORDER_STATUS_FAILED = "FAILED"


@dataclass(frozen=True)
class RobotOrder:
    id: int
    created_at_utc: datetime
    updated_at_utc: datetime
    account_id: str
    order_request_id: str
    broker_order_id: str
    side: str
    status: str
    execution_report_status: str
    order_type: str
    instrument_uid: str
    ticker: str
    class_code: str
    name: str
    quantity_lots: int
    quantity_shares: int
    limit_price: Decimal
    requested_amount: Decimal
    lots_executed: int
    executed_order_price: Decimal
    total_order_amount: Decimal
    source: str
    error_text: str


def init_robot_order_storage() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS robot_order (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                account_id TEXT NOT NULL,
                order_request_id TEXT NOT NULL,
                broker_order_id TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                execution_report_status TEXT NOT NULL,
                order_type TEXT NOT NULL,
                instrument_uid TEXT NOT NULL,
                ticker TEXT NOT NULL,
                class_code TEXT NOT NULL,
                name TEXT NOT NULL,
                quantity_lots INTEGER NOT NULL,
                quantity_shares INTEGER NOT NULL,
                limit_price TEXT NOT NULL,
                requested_amount TEXT NOT NULL,
                lots_executed INTEGER NOT NULL,
                executed_order_price TEXT NOT NULL,
                total_order_amount TEXT NOT NULL,
                source TEXT NOT NULL,
                error_text TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_robot_order_request_id
            ON robot_order (order_request_id)
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_robot_order_created
            ON robot_order (created_at_utc)
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


def _row_to_robot_order(row) -> RobotOrder:
    return RobotOrder(
        id=row["id"],
        created_at_utc=_datetime_from_storage_text(row["created_at_utc"]),
        updated_at_utc=_datetime_from_storage_text(row["updated_at_utc"]),
        account_id=row["account_id"],
        order_request_id=row["order_request_id"],
        broker_order_id=row["broker_order_id"],
        side=row["side"],
        status=row["status"],
        execution_report_status=row["execution_report_status"],
        order_type=row["order_type"],
        instrument_uid=row["instrument_uid"],
        ticker=row["ticker"],
        class_code=row["class_code"],
        name=row["name"],
        quantity_lots=row["quantity_lots"],
        quantity_shares=row["quantity_shares"],
        limit_price=Decimal(row["limit_price"]),
        requested_amount=Decimal(row["requested_amount"]),
        lots_executed=row["lots_executed"],
        executed_order_price=Decimal(row["executed_order_price"]),
        total_order_amount=Decimal(row["total_order_amount"]),
        source=row["source"],
        error_text=row["error_text"],
    )


def create_robot_order(
    account_id: str,
    order_request_id: str,
    side: str,
    order_type: str,
    instrument_uid: str,
    ticker: str,
    class_code: str,
    name: str,
    quantity_lots: int,
    quantity_shares: int,
    limit_price: Decimal,
    requested_amount: Decimal,
    source: str,
) -> int:
    init_robot_order_storage()

    if quantity_lots <= 0:
        raise ValueError("quantity_lots должен быть больше 0.")

    if quantity_shares <= 0:
        raise ValueError("quantity_shares должен быть больше 0.")

    now_utc = datetime.now(timezone.utc)

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO robot_order (
                created_at_utc,
                updated_at_utc,
                account_id,
                order_request_id,
                broker_order_id,
                side,
                status,
                execution_report_status,
                order_type,
                instrument_uid,
                ticker,
                class_code,
                name,
                quantity_lots,
                quantity_shares,
                limit_price,
                requested_amount,
                lots_executed,
                executed_order_price,
                total_order_amount,
                source,
                error_text
            )
            VALUES (?, ?, ?, ?, '', ?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, '0', '0', ?, '')
            """,
            (
                _datetime_to_storage_text(now_utc),
                _datetime_to_storage_text(now_utc),
                account_id,
                order_request_id,
                side,
                ROBOT_ORDER_STATUS_PREPARED,
                order_type,
                instrument_uid,
                ticker,
                class_code,
                name,
                quantity_lots,
                quantity_shares,
                str(limit_price),
                str(requested_amount),
                source,
            ),
        )

        return int(cursor.lastrowid)


def mark_robot_order_sent(
    robot_order_id: int,
    broker_order_id: str,
    execution_report_status: str,
    lots_executed: int,
    executed_order_price: Decimal,
    total_order_amount: Decimal,
) -> None:
    init_robot_order_storage()
    now_utc = datetime.now(timezone.utc)

    with get_connection() as connection:
        connection.execute(
            """
            UPDATE robot_order
            SET updated_at_utc = ?,
                broker_order_id = ?,
                status = ?,
                execution_report_status = ?,
                lots_executed = ?,
                executed_order_price = ?,
                total_order_amount = ?,
                error_text = ''
            WHERE id = ?
            """,
            (
                _datetime_to_storage_text(now_utc),
                broker_order_id,
                ROBOT_ORDER_STATUS_SENT,
                execution_report_status,
                lots_executed,
                str(executed_order_price),
                str(total_order_amount),
                robot_order_id,
            ),
        )


def mark_robot_order_failed(
    robot_order_id: int,
    error_text: str,
) -> None:
    init_robot_order_storage()
    now_utc = datetime.now(timezone.utc)

    with get_connection() as connection:
        connection.execute(
            """
            UPDATE robot_order
            SET updated_at_utc = ?,
                status = ?,
                error_text = ?
            WHERE id = ?
            """,
            (
                _datetime_to_storage_text(now_utc),
                ROBOT_ORDER_STATUS_FAILED,
                error_text,
                robot_order_id,
            ),
        )


def list_recent_robot_orders(limit: int = 100) -> list[RobotOrder]:
    init_robot_order_storage()

    if limit <= 0:
        raise ValueError("limit должен быть больше 0.")

    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM robot_order
            ORDER BY created_at_utc DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [
        _row_to_robot_order(row)
        for row in rows
    ]
