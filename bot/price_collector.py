import asyncio
from datetime import datetime, timezone

from grpc import aio
from t_tech.invest import AsyncClient

from bd.price_snapshot import count_price_snapshots, save_price_snapshot
from bd.settings_storage import load_app_settings, load_selected_shares
from tbank.last_prices import get_last_prices_batched


BATCH_SIZE = 100


async def collect_price_snapshot() -> int:
    settings = load_app_settings()
    selected_shares = load_selected_shares()

    token = settings["token"]

    if not token.strip():
        raise ValueError("В настройках сохранён пустой токен.")

    if not selected_shares:
        raise ValueError("Рабочий список акций пуст.")

    instrument_ids = [
        share.uid
        for share in selected_shares
    ]

    async with AsyncClient(token) as client:
        prices = await get_last_prices_batched(
            client=client,
            instrument_ids=instrument_ids,
            batch_size=BATCH_SIZE,
        )

    if not prices:
        raise RuntimeError("T-Invest API не вернул цены для рабочих акций.")

    captured_at_utc = datetime.now(timezone.utc)

    saved_count = save_price_snapshot(
        prices=prices,
        captured_at_utc=captured_at_utc,
    )

    return saved_count


async def main() -> None:
    try:
        saved_count = await collect_price_snapshot()
    except KeyError as error:
        print(f"Ошибка настроек: отсутствует ключ {error}.")
        return
    except aio.AioRpcError as error:
        print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
        return
    except Exception as error:
        print(f"Ошибка: {type(error).__name__}: {error}")
        return

    print(f"Сохранено цен в snapshot: {saved_count}")
    print(f"Всего записей price_snapshot в БД: {count_price_snapshots()}")


if __name__ == "__main__":
    asyncio.run(main())
