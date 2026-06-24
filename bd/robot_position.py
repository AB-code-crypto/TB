from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from bd.database import get_connection
from tbank.positions import TBankPortfolioPosition


ROBOT_POSITION_EVENT_SYNC_REDUCED = "SYNC_REDUCED"
ROBOT_POSITION_EVENT_SYNC_ZEROED = "SYNC_ZEROED"


@dataclass(frozen=True)
class RobotPosition:
    account_id: str
    instrument_uid: str
    ticker: str
    class_code: str
    name: str
    lot: int
    robot_lots: int
    robot_shares: int
    avg_price: Decimal
    last_broker_lots: int
    external_lots: int
    sync_note: str
    last_sync_at_utc: datetime | None
    updated_at_utc: datetime


@dataclass(frozen=True)
class RobotPositionSyncReport:
    checked_count: int
    reduced_count: int
    zeroed_count: int
    unchanged_count: int


def init_robot_position_storage() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS robot_position (
                account_id TEXT NOT NULL,
                instrument_uid TEXT NOT NULL,
                ticker TEXT NOT NULL,
                class_code TEXT NOT NULL,
                name TEXT NOT NULL,
                lot INTEGER NOT NULL,
                robot_lots INTEGER NOT NULL,
                robot_shares INTEGER NOT NULL,
                avg_price TEXT NOT NULL,
                last_broker_lots INTEGER NOT NULL,
                external_lots INTEGER NOT NULL,
                sync_note TEXT NOT NULL,
                last_sync_at_utc TEXT,
                updated_at_utc TEXT NOT NULL,
                PRIMARY KEY (account_id, instrument_uid)
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_robot_position_account
            ON robot_position (account_id)
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS robot_position_event (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at_utc TEXT NOT NULL,
                account_id TEXT NOT NULL,
                instrument_uid TEXT NOT NULL,
                ticker TEXT NOT NULL,
                class_code TEXT NOT NULL,
                event_type TEXT NOT NULL,
                reason TEXT NOT NULL,
                old_robot_lots INTEGER NOT NULL,
                new_robot_lots INTEGER NOT NULL,
                broker_lots INTEGER NOT NULL,
                external_lots INTEGER NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_robot_position_event_account_created
            ON robot_position_event (account_id, created_at_utc)
            """
        )


def _datetime_to_storage_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("datetime должен быть timezone-aware.")

    return value.astimezone(timezone.utc).isoformat()


def _datetime_from_storage_text(value: str | None) -> datetime | None:
    if value is None:
        return None

    parsed_value = datetime.fromisoformat(value)

    if parsed_value.tzinfo is None:
        raise RuntimeError(f"В БД сохранён datetime без timezone: {value}")

    return parsed_value.astimezone(timezone.utc)


def _position_row_to_dataclass(row) -> RobotPosition:
    return RobotPosition(
        account_id=row["account_id"],
        instrument_uid=row["instrument_uid"],
        ticker=row["ticker"],
        class_code=row["class_code"],
        name=row["name"],
        lot=row["lot"],
        robot_lots=row["robot_lots"],
        robot_shares=row["robot_shares"],
        avg_price=Decimal(row["avg_price"]),
        last_broker_lots=row["last_broker_lots"],
        external_lots=row["external_lots"],
        sync_note=row["sync_note"],
        last_sync_at_utc=_datetime_from_storage_text(row["last_sync_at_utc"]),
        updated_at_utc=_datetime_from_storage_text(row["updated_at_utc"]),
    )


def _decimal_lots_to_int(value: Decimal) -> int:
    if value <= 0:
        return 0

    return int(value)


def _save_position_event(
    created_at_utc: datetime,
    account_id: str,
    instrument_uid: str,
    ticker: str,
    class_code: str,
    event_type: str,
    reason: str,
    old_robot_lots: int,
    new_robot_lots: int,
    broker_lots: int,
    external_lots: int,
) -> None:
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO robot_position_event (
                created_at_utc,
                account_id,
                instrument_uid,
                ticker,
                class_code,
                event_type,
                reason,
                old_robot_lots,
                new_robot_lots,
                broker_lots,
                external_lots
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _datetime_to_storage_text(created_at_utc),
                account_id,
                instrument_uid,
                ticker,
                class_code,
                event_type,
                reason,
                old_robot_lots,
                new_robot_lots,
                broker_lots,
                external_lots,
            ),
        )


def list_robot_positions(account_id: str | None = None) -> list[RobotPosition]:
    init_robot_position_storage()

    with get_connection() as connection:
        if account_id is None:
            rows = connection.execute(
                """
                SELECT *
                FROM robot_position
                ORDER BY account_id, ticker, class_code
                """
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT *
                FROM robot_position
                WHERE account_id = ?
                ORDER BY ticker, class_code
                """,
                (account_id,),
            ).fetchall()

    return [
        _position_row_to_dataclass(row)
        for row in rows
    ]


def update_robot_position_after_fill(
    account_id: str,
    instrument_uid: str,
    ticker: str,
    class_code: str,
    name: str,
    lot: int,
    robot_lots: int,
    avg_price: Decimal,
) -> None:
    init_robot_position_storage()

    if not account_id.strip():
        raise ValueError("account_id не может быть пустым.")

    if not instrument_uid.strip():
        raise ValueError("instrument_uid не может быть пустым.")

    if not ticker.strip():
        raise ValueError("ticker не может быть пустым.")

    if lot <= 0:
        raise ValueError("lot должен быть больше 0.")

    if robot_lots < 0:
        raise ValueError("robot_lots не может быть меньше 0.")

    if avg_price < 0:
        raise ValueError("avg_price не может быть меньше 0.")

    now_utc = datetime.now(timezone.utc)

    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO robot_position (
                account_id,
                instrument_uid,
                ticker,
                class_code,
                name,
                lot,
                robot_lots,
                robot_shares,
                avg_price,
                last_broker_lots,
                external_lots,
                sync_note,
                last_sync_at_utc,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, NULL, ?)
            ON CONFLICT(account_id, instrument_uid) DO UPDATE SET
                ticker = excluded.ticker,
                class_code = excluded.class_code,
                name = excluded.name,
                lot = excluded.lot,
                robot_lots = excluded.robot_lots,
                robot_shares = excluded.robot_shares,
                avg_price = excluded.avg_price,
                sync_note = excluded.sync_note,
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                account_id,
                instrument_uid,
                ticker,
                class_code,
                name,
                lot,
                robot_lots,
                robot_lots * lot,
                str(avg_price),
                "Позиция обновлена после сделки робота.",
                _datetime_to_storage_text(now_utc),
            ),
        )


def sync_robot_positions_with_broker(
    account_id: str,
    broker_positions: list[TBankPortfolioPosition],
) -> RobotPositionSyncReport:
    init_robot_position_storage()

    if not account_id.strip():
        raise ValueError("account_id не может быть пустым.")

    now_utc = datetime.now(timezone.utc)
    positions = list_robot_positions(account_id=account_id)

    broker_lots_by_uid = {
        position.instrument_uid: _decimal_lots_to_int(position.quantity_lots)
        for position in broker_positions
    }

    reduced_count = 0
    zeroed_count = 0
    unchanged_count = 0

    with get_connection() as connection:
        for position in positions:
            broker_lots = broker_lots_by_uid.get(position.instrument_uid, 0)
            old_robot_lots = position.robot_lots

            if broker_lots < old_robot_lots:
                new_robot_lots = broker_lots
                external_lots = 0

                if new_robot_lots == 0:
                    event_type = ROBOT_POSITION_EVENT_SYNC_ZEROED
                    sync_note = "Позиция робота обнулена: у брокера нет нужного количества."
                    zeroed_count += 1
                else:
                    event_type = ROBOT_POSITION_EVENT_SYNC_REDUCED
                    sync_note = "Позиция робота уменьшена: у брокера меньше лотов."
                    reduced_count += 1

                connection.execute(
                    """
                    UPDATE robot_position
                    SET robot_lots = ?,
                        robot_shares = ?,
                        last_broker_lots = ?,
                        external_lots = ?,
                        sync_note = ?,
                        last_sync_at_utc = ?,
                        updated_at_utc = ?
                    WHERE account_id = ?
                      AND instrument_uid = ?
                    """,
                    (
                        new_robot_lots,
                        new_robot_lots * position.lot,
                        broker_lots,
                        external_lots,
                        sync_note,
                        _datetime_to_storage_text(now_utc),
                        _datetime_to_storage_text(now_utc),
                        account_id,
                        position.instrument_uid,
                    ),
                )

                _save_position_event(
                    created_at_utc=now_utc,
                    account_id=account_id,
                    instrument_uid=position.instrument_uid,
                    ticker=position.ticker,
                    class_code=position.class_code,
                    event_type=event_type,
                    reason=sync_note,
                    old_robot_lots=old_robot_lots,
                    new_robot_lots=new_robot_lots,
                    broker_lots=broker_lots,
                    external_lots=external_lots,
                )
                continue

            external_lots = broker_lots - old_robot_lots

            if external_lots > 0:
                sync_note = f"У брокера есть внешняя позиция клиента: {external_lots} лот(ов)."
            else:
                sync_note = "Позиция робота совпадает с брокером."

            connection.execute(
                """
                UPDATE robot_position
                SET last_broker_lots = ?,
                    external_lots = ?,
                    sync_note = ?,
                    last_sync_at_utc = ?,
                    updated_at_utc = ?
                WHERE account_id = ?
                  AND instrument_uid = ?
                """,
                (
                    broker_lots,
                    external_lots,
                    sync_note,
                    _datetime_to_storage_text(now_utc),
                    _datetime_to_storage_text(now_utc),
                    account_id,
                    position.instrument_uid,
                ),
            )
            unchanged_count += 1

    return RobotPositionSyncReport(
        checked_count=len(positions),
        reduced_count=reduced_count,
        zeroed_count=zeroed_count,
        unchanged_count=unchanged_count,
    )
