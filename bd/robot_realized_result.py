from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from bd.database import get_connection


RESULT_CURRENCIES = ("RUB", "USD", "EUR")


@dataclass(frozen=True)
class RobotRealizedResult:
    id: int
    closed_at_utc: datetime
    account_id: str
    robot_order_id: int | None
    instrument_uid: str
    ticker: str
    class_code: str
    name: str
    currency: str
    lot: int
    executed_lots: int
    executed_shares: int
    average_buy_price: Decimal
    sell_price: Decimal
    buy_amount: Decimal
    sell_amount: Decimal
    gross_result: Decimal
    source: str


def init_robot_realized_result_storage() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS robot_realized_result (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                closed_at_utc TEXT NOT NULL,
                account_id TEXT NOT NULL,
                robot_order_id INTEGER,
                instrument_uid TEXT NOT NULL,
                ticker TEXT NOT NULL,
                class_code TEXT NOT NULL,
                name TEXT NOT NULL,
                currency TEXT NOT NULL,
                lot INTEGER NOT NULL,
                executed_lots INTEGER NOT NULL,
                executed_shares INTEGER NOT NULL,
                average_buy_price TEXT NOT NULL,
                sell_price TEXT NOT NULL,
                buy_amount TEXT NOT NULL,
                sell_amount TEXT NOT NULL,
                gross_result TEXT NOT NULL,
                source TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
            idx_robot_realized_result_order
            ON robot_realized_result (robot_order_id)
            WHERE robot_order_id IS NOT NULL
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_robot_realized_result_account_closed
            ON robot_realized_result (account_id, closed_at_utc)
            """
        )


def _datetime_to_storage_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("datetime должен быть timezone-aware.")

    return value.astimezone(timezone.utc).isoformat()


def save_robot_realized_result(
    account_id: str,
    robot_order_id: int | None,
    instrument_uid: str,
    ticker: str,
    class_code: str,
    name: str,
    currency: str,
    lot: int,
    executed_lots: int,
    average_buy_price: Decimal,
    sell_price: Decimal,
    source: str,
) -> int | None:
    init_robot_realized_result_storage()

    if not account_id.strip():
        raise ValueError("account_id не может быть пустым.")

    if not instrument_uid.strip():
        raise ValueError("instrument_uid не может быть пустым.")

    clean_currency = currency.strip().upper()

    if not clean_currency:
        raise ValueError("currency не может быть пустой.")

    if lot <= 0:
        raise ValueError("lot должен быть больше 0.")

    if executed_lots <= 0:
        raise ValueError("executed_lots должен быть больше 0.")

    if average_buy_price < 0:
        raise ValueError("average_buy_price не может быть меньше 0.")

    if sell_price < 0:
        raise ValueError("sell_price не может быть меньше 0.")

    executed_shares = executed_lots * lot
    buy_amount = average_buy_price * Decimal(executed_shares)
    sell_amount = sell_price * Decimal(executed_shares)
    gross_result = sell_amount - buy_amount
    closed_at_utc = datetime.now(timezone.utc)

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO robot_realized_result (
                closed_at_utc,
                account_id,
                robot_order_id,
                instrument_uid,
                ticker,
                class_code,
                name,
                currency,
                lot,
                executed_lots,
                executed_shares,
                average_buy_price,
                sell_price,
                buy_amount,
                sell_amount,
                gross_result,
                source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _datetime_to_storage_text(closed_at_utc),
                account_id,
                robot_order_id,
                instrument_uid,
                ticker,
                class_code,
                name,
                clean_currency,
                lot,
                executed_lots,
                executed_shares,
                str(average_buy_price),
                str(sell_price),
                str(buy_amount),
                str(sell_amount),
                str(gross_result),
                source.strip(),
            ),
        )

        if cursor.rowcount == 0:
            return None

        return int(cursor.lastrowid)


def sum_robot_realized_results(
    account_id: str,
    started_at_utc: datetime | None = None,
    finished_at_utc: datetime | None = None,
) -> dict[str, Decimal]:
    init_robot_realized_result_storage()

    if not account_id.strip():
        return {
            currency: Decimal("0")
            for currency in RESULT_CURRENCIES
        }

    conditions = ["account_id = ?"]
    parameters: list[object] = [account_id]

    if started_at_utc is not None:
        conditions.append("closed_at_utc >= ?")
        parameters.append(_datetime_to_storage_text(started_at_utc))

    if finished_at_utc is not None:
        conditions.append("closed_at_utc <= ?")
        parameters.append(_datetime_to_storage_text(finished_at_utc))

    query = (
        "SELECT currency, gross_result "
        "FROM robot_realized_result "
        f"WHERE {' AND '.join(conditions)}"
    )

    with get_connection() as connection:
        rows = connection.execute(
            query,
            tuple(parameters),
        ).fetchall()

    totals: dict[str, Decimal] = {
        currency: Decimal("0")
        for currency in RESULT_CURRENCIES
    }

    for row in rows:
        currency = str(row["currency"]).upper()
        totals.setdefault(currency, Decimal("0"))
        totals[currency] += Decimal(row["gross_result"])

    return totals
