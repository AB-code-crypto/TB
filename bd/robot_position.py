from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from bd.database import get_connection
from bd.robot_realized_result import save_robot_realized_result
from tbank.positions import TBankPortfolioPosition
from tbank.shares import TBankShare


ROBOT_POSITION_EVENT_SYNC_REDUCED = "SYNC_REDUCED"
ROBOT_POSITION_EVENT_SYNC_ZEROED = "SYNC_ZEROED"
ROBOT_POSITION_EVENT_MANUAL_ADJUSTMENT = "MANUAL_ADJUSTMENT"


@dataclass(frozen=True)
class RobotPosition:
    account_id: str
    instrument_uid: str
    ticker: str
    class_code: str
    name: str
    currency: str
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
                currency TEXT NOT NULL,
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
        currency=row["currency"],
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



def _infer_lot_from_broker_position(position: TBankPortfolioPosition) -> int:
    broker_lots = _decimal_lots_to_int(position.quantity_lots)

    if broker_lots <= 0:
        return 0

    if position.quantity <= 0:
        return 0

    return int(position.quantity / Decimal(broker_lots))


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
                ORDER BY account_id, name, ticker, class_code
                """
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT *
                FROM robot_position
                WHERE account_id = ?
                ORDER BY name, ticker, class_code
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
    currency: str,
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

    clean_currency = currency.strip().upper()

    if not clean_currency:
        raise ValueError("currency не может быть пустой.")

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
                currency,
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, NULL, ?)
            ON CONFLICT(account_id, instrument_uid) DO UPDATE SET
                ticker = excluded.ticker,
                class_code = excluded.class_code,
                name = excluded.name,
                currency = excluded.currency,
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
                clean_currency,
                lot,
                robot_lots,
                robot_lots * lot,
                str(avg_price),
                "Позиция обновлена после сделки робота.",
                _datetime_to_storage_text(now_utc),
            ),
        )


def _resolve_position_identity(
    position: RobotPosition,
    shares_by_uid: dict[str, TBankShare],
) -> tuple[str, str, str, str, int]:
    share = shares_by_uid.get(position.instrument_uid)

    if share is None:
        return (
            position.ticker,
            position.class_code,
            position.name,
            position.currency,
            position.lot,
        )

    return (
        share.ticker,
        share.class_code,
        share.name,
        share.currency,
        share.lot,
    )


def _resolve_broker_position_identity(
    position: TBankPortfolioPosition,
    shares_by_uid: dict[str, TBankShare],
) -> tuple[str, str, str, str, int]:
    share = shares_by_uid.get(position.instrument_uid)

    if share is None:
        return (
            position.ticker,
            position.class_code,
            position.ticker,
            position.currency,
            _infer_lot_from_broker_position(position),
        )

    return (
        share.ticker,
        share.class_code,
        share.name,
        share.currency,
        share.lot,
    )


def set_robot_position_lots(
    account_id: str,
    instrument_uid: str,
    robot_lots: int,
    reason: str,
) -> bool:
    init_robot_position_storage()

    if not account_id.strip():
        raise ValueError("account_id не может быть пустым.")

    if not instrument_uid.strip():
        raise ValueError("instrument_uid не может быть пустым.")

    if robot_lots < 0:
        raise ValueError("Количество лотов робота не может быть меньше 0.")

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT *
            FROM robot_position
            WHERE account_id = ?
              AND instrument_uid = ?
            LIMIT 1
            """,
            (
                account_id,
                instrument_uid,
            ),
        ).fetchone()

    if row is None:
        raise ValueError(
            "Позиция робота не найдена для ручной корректировки: "
            f"account_id={account_id}, instrument_uid={instrument_uid}"
        )

    position = _position_row_to_dataclass(row)

    if robot_lots > position.last_broker_lots:
        raise ValueError(
            "Лотов у робота не может быть больше, чем лотов у брокера: "
            f"robot_lots={robot_lots}, broker_lots={position.last_broker_lots}"
        )

    if robot_lots == position.robot_lots:
        return False

    now_utc = datetime.now(timezone.utc)
    external_lots = position.last_broker_lots - robot_lots
    clean_reason = reason.strip()

    if not clean_reason:
        clean_reason = "Ручная корректировка позиции робота."

    sync_note = (
        "Позиция робота скорректирована вручную: "
        f"{position.robot_lots} → {robot_lots} лот(ов)."
    )

    with get_connection() as connection:
        connection.execute(
            """
            UPDATE robot_position
            SET robot_lots = ?,
                robot_shares = ?,
                external_lots = ?,
                sync_note = ?,
                updated_at_utc = ?
            WHERE account_id = ?
              AND instrument_uid = ?
            """,
            (
                robot_lots,
                robot_lots * position.lot,
                external_lots,
                sync_note,
                _datetime_to_storage_text(now_utc),
                account_id,
                instrument_uid,
            ),
        )

    _save_position_event(
        created_at_utc=now_utc,
        account_id=account_id,
        instrument_uid=instrument_uid,
        ticker=position.ticker,
        class_code=position.class_code,
        event_type=ROBOT_POSITION_EVENT_MANUAL_ADJUSTMENT,
        reason=clean_reason,
        old_robot_lots=position.robot_lots,
        new_robot_lots=robot_lots,
        broker_lots=position.last_broker_lots,
        external_lots=external_lots,
    )

    return True


def get_robot_position(
    account_id: str,
    instrument_uid: str,
) -> RobotPosition | None:
    init_robot_position_storage()

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT *
            FROM robot_position
            WHERE account_id = ?
              AND instrument_uid = ?
            LIMIT 1
            """,
            (
                account_id,
                instrument_uid,
            ),
        ).fetchone()

    if row is None:
        return None

    return _position_row_to_dataclass(row)


def apply_robot_order_fill(
    account_id: str,
    share: TBankShare,
    side: str,
    executed_lots: int,
    executed_price: Decimal,
    robot_order_id: int | None = None,
    source: str = "",
) -> None:
    if executed_lots <= 0:
        return

    current_position = get_robot_position(
        account_id=account_id,
        instrument_uid=share.uid,
    )

    if side == "BUY":
        if current_position is None or current_position.robot_lots <= 0:
            new_robot_lots = executed_lots
            new_avg_price = executed_price
        else:
            old_robot_lots = current_position.robot_lots
            new_robot_lots = old_robot_lots + executed_lots
            old_amount = current_position.avg_price * Decimal(old_robot_lots)
            new_amount = executed_price * Decimal(executed_lots)
            new_avg_price = (old_amount + new_amount) / Decimal(new_robot_lots)

        update_robot_position_after_fill(
            account_id=account_id,
            instrument_uid=share.uid,
            ticker=share.ticker,
            class_code=share.class_code,
            name=share.name,
            currency=share.currency,
            lot=share.lot,
            robot_lots=new_robot_lots,
            avg_price=new_avg_price,
        )
        return

    if side == "SELL":
        if current_position is None:
            raise ValueError("Позиция робота для продажи не найдена.")

        if executed_lots > current_position.robot_lots:
            raise ValueError(
                "Исполнено больше лотов, чем было в позиции робота: "
                f"executed_lots={executed_lots}, robot_lots={current_position.robot_lots}"
            )

        save_robot_realized_result(
            account_id=account_id,
            robot_order_id=robot_order_id,
            instrument_uid=share.uid,
            ticker=share.ticker,
            class_code=share.class_code,
            name=share.name,
            currency=share.currency,
            lot=share.lot,
            executed_lots=executed_lots,
            average_buy_price=current_position.avg_price,
            sell_price=executed_price,
            source=source,
        )

        new_robot_lots = current_position.robot_lots - executed_lots
        new_avg_price = (
            current_position.avg_price
            if new_robot_lots > 0
            else Decimal("0")
        )

        update_robot_position_after_fill(
            account_id=account_id,
            instrument_uid=share.uid,
            ticker=share.ticker,
            class_code=share.class_code,
            name=share.name,
            currency=share.currency,
            lot=share.lot,
            robot_lots=new_robot_lots,
            avg_price=new_avg_price,
        )
        return

    raise ValueError(f"Неизвестная сторона заявки: {side}")


def sync_robot_positions_with_broker(
    account_id: str,
    broker_positions: list[TBankPortfolioPosition],
    shares: list[TBankShare],
) -> RobotPositionSyncReport:
    init_robot_position_storage()

    if not account_id.strip():
        raise ValueError("account_id не может быть пустым.")

    now_utc = datetime.now(timezone.utc)
    positions = list_robot_positions(account_id=account_id)

    shares_by_uid = {
        share.uid: share
        for share in shares
    }
    tracked_uids = set(shares_by_uid) | {
        position.instrument_uid
        for position in positions
    }
    broker_positions_by_uid = {
        position.instrument_uid: position
        for position in broker_positions
        if position.instrument_uid in tracked_uids
    }
    broker_lots_by_uid = {
        position.instrument_uid: _decimal_lots_to_int(position.quantity_lots)
        for position in broker_positions_by_uid.values()
    }
    known_uids = {
        position.instrument_uid
        for position in positions
    }

    reduced_count = 0
    zeroed_count = 0
    unchanged_count = 0

    with get_connection() as connection:
        for position in positions:
            broker_lots = broker_lots_by_uid.get(position.instrument_uid, 0)
            old_robot_lots = position.robot_lots
            ticker, class_code, name, currency, lot = _resolve_position_identity(
                position=position,
                shares_by_uid=shares_by_uid,
            )

            if old_robot_lots == 0 and broker_lots == 0:
                connection.execute(
                    """
                    DELETE FROM robot_position
                    WHERE account_id = ?
                      AND instrument_uid = ?
                    """,
                    (
                        account_id,
                        position.instrument_uid,
                    ),
                )
                continue

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
                    SET ticker = ?,
                        class_code = ?,
                        name = ?,
                        currency = ?,
                        lot = ?,
                        robot_lots = ?,
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
                        ticker,
                        class_code,
                        name,
                        currency,
                        lot,
                        new_robot_lots,
                        new_robot_lots * lot,
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
                    ticker=ticker,
                    class_code=class_code,
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
                SET ticker = ?,
                    class_code = ?,
                    name = ?,
                    currency = ?,
                    lot = ?,
                    last_broker_lots = ?,
                    external_lots = ?,
                    sync_note = ?,
                    last_sync_at_utc = ?,
                    updated_at_utc = ?
                WHERE account_id = ?
                  AND instrument_uid = ?
                """,
                (
                    ticker,
                    class_code,
                    name,
                    currency,
                    lot,
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

        for instrument_uid, broker_position in broker_positions_by_uid.items():
            if instrument_uid in known_uids:
                continue

            broker_lots = broker_lots_by_uid[instrument_uid]

            if broker_lots <= 0:
                continue

            ticker, class_code, name, currency, lot = _resolve_broker_position_identity(
                position=broker_position,
                shares_by_uid=shares_by_uid,
            )

            if lot <= 0:
                continue

            sync_note = "Внешняя позиция клиента. Робот её не трогает."

            connection.execute(
                """
                INSERT INTO robot_position (
                    account_id,
                    instrument_uid,
                    ticker,
                    class_code,
                    name,
                    currency,
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
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?)
                """,
                (
                    account_id,
                    broker_position.instrument_uid,
                    ticker,
                    class_code,
                    name,
                    currency,
                    lot,
                    str(broker_position.average_position_price),
                    broker_lots,
                    broker_lots,
                    sync_note,
                    _datetime_to_storage_text(now_utc),
                    _datetime_to_storage_text(now_utc),
                ),
            )
            unchanged_count += 1

    synced_positions_count = len(list_robot_positions(account_id=account_id))

    return RobotPositionSyncReport(
        checked_count=synced_positions_count,
        reduced_count=reduced_count,
        zeroed_count=zeroed_count,
        unchanged_count=unchanged_count,
    )
