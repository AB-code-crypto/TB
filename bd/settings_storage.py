from dataclasses import asdict, fields
from datetime import datetime, timezone
from decimal import Decimal
import json

from bd.database import get_connection
from tbank.shares import TBankShare


def init_settings_storage() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS selected_shares (
                uid TEXT PRIMARY KEY,
                sort_order INTEGER NOT NULL,
                payload TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            )
            """
        )


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_app_settings(settings: dict[str, str]) -> None:
    init_settings_storage()

    updated_at_utc = _utc_now_text()

    rows = [
        (key, value, updated_at_utc)
        for key, value in settings.items()
    ]

    with get_connection() as connection:
        connection.executemany(
            """
            INSERT INTO app_settings (key, value, updated_at_utc)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at_utc = excluded.updated_at_utc
            """,
            rows,
        )


def load_app_settings() -> dict[str, str]:
    init_settings_storage()

    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT key, value
            FROM app_settings
            """
        ).fetchall()

    return {
        row["key"]: row["value"]
        for row in rows
    }


def _share_to_payload(share: TBankShare) -> str:
    data = asdict(share)
    data["min_price_increment"] = str(share.min_price_increment)

    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _share_from_payload(payload: str) -> TBankShare:
    data = json.loads(payload)

    required_field_names = [
        field.name
        for field in fields(TBankShare)
    ]

    missing_fields = [
        field_name
        for field_name in required_field_names
        if field_name not in data
    ]

    if missing_fields:
        raise RuntimeError(
            f"В сохранённой акции не хватает полей: {', '.join(missing_fields)}"
        )

    data["min_price_increment"] = Decimal(data["min_price_increment"])

    return TBankShare(
        **{
            field_name: data[field_name]
            for field_name in required_field_names
        }
    )


def save_selected_shares(shares: list[TBankShare]) -> None:
    init_settings_storage()

    updated_at_utc = _utc_now_text()

    rows = [
        (
            share.uid,
            sort_order,
            _share_to_payload(share),
            updated_at_utc,
        )
        for sort_order, share in enumerate(shares, start=1)
    ]

    with get_connection() as connection:
        connection.execute("DELETE FROM selected_shares")
        connection.executemany(
            """
            INSERT INTO selected_shares (uid, sort_order, payload, updated_at_utc)
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )


def load_selected_shares() -> list[TBankShare]:
    init_settings_storage()

    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT payload
            FROM selected_shares
            ORDER BY sort_order ASC
            """
        ).fetchall()

    return [
        _share_from_payload(row["payload"])
        for row in rows
    ]



def reset_app_storage() -> None:
    init_settings_storage()

    with get_connection() as connection:
        connection.execute("DELETE FROM app_settings")
        connection.execute("DELETE FROM selected_shares")
