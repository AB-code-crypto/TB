from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol

from t_tech.invest.grpc import marketdata_pb2

NANO = Decimal("1000000000")


class MarketDataService(Protocol):
    stub: Any
    metadata: Any


class MarketDataApiClient(Protocol):
    market_data: MarketDataService


@dataclass(frozen=True)
class TBankLastPrice:
    figi: str
    instrument_uid: str
    ticker: str
    class_code: str
    price: Decimal
    time: datetime
    last_price_type: str


def _quotation_to_decimal(value) -> Decimal:
    return Decimal(value.units) + Decimal(value.nano) / NANO


async def get_last_prices(
        client: MarketDataApiClient,
        instrument_ids: list[str],
) -> list[TBankLastPrice]:
    """
    Получить последние цены сделок по списку инструментов.

    instrument_id может быть:
    - instrument_uid;
    - figi;
    - ticker + '_' + class_code.

    Для рабочего робота предпочтительно использовать instrument_uid.
    Для ручной проверки удобно использовать SBER_TQBR, GAZP_TQBR и т.д.
    """
    cleaned_instrument_ids = [
        instrument_id.strip()
        for instrument_id in instrument_ids
        if instrument_id.strip()
    ]

    if not cleaned_instrument_ids:
        raise ValueError("instrument_ids не может быть пустым.")

    response = await client.market_data.stub.GetLastPrices(
        request=marketdata_pb2.GetLastPricesRequest(
            instrument_id=cleaned_instrument_ids,
        ),
        metadata=client.market_data.metadata,
    )

    prices: list[TBankLastPrice] = []

    for last_price in response.last_prices:
        prices.append(
            TBankLastPrice(
                figi=last_price.figi,
                instrument_uid=last_price.instrument_uid,
                ticker=last_price.ticker,
                class_code=last_price.class_code,
                price=_quotation_to_decimal(last_price.price),
                time=last_price.time.ToDatetime(tzinfo=timezone.utc),
                last_price_type=marketdata_pb2.LastPriceType.Name(
                    last_price.last_price_type
                ),
            )
        )

    return prices


async def get_last_price(
        client: MarketDataApiClient,
        instrument_id: str,
) -> TBankLastPrice:
    """
    Получить последнюю цену одного инструмента.

    Если API не вернул цену, падаем явно.
    """
    prices = await get_last_prices(
        client=client,
        instrument_ids=[instrument_id],
    )

    if not prices:
        raise RuntimeError(f"Последняя цена для инструмента {instrument_id} не получена.")

    if len(prices) > 1:
        raise RuntimeError(
            f"Для инструмента {instrument_id} получено несколько цен: {len(prices)}."
        )

    return prices[0]


async def get_last_prices_batched(
        client: MarketDataApiClient,
        instrument_ids: list[str],
        batch_size: int = 100,
) -> list[TBankLastPrice]:
    """
    Получить последние цены по большому списку инструментов пачками.

    Это нужно для рыночного среза, чтобы не делать отдельный запрос
    на каждый инструмент.

    instrument_ids:
    - instrument_uid;
    - figi;
    - ticker + '_' + class_code.

    batch_size задаём явно. Если API/лимиты изменятся, поменяем его в одном месте.
    """
    if batch_size <= 0:
        raise ValueError("batch_size должен быть больше 0.")

    cleaned_instrument_ids = [
        instrument_id.strip()
        for instrument_id in instrument_ids
        if instrument_id.strip()
    ]

    if not cleaned_instrument_ids:
        raise ValueError("instrument_ids не может быть пустым.")

    prices: list[TBankLastPrice] = []

    for start in range(0, len(cleaned_instrument_ids), batch_size):
        batch = cleaned_instrument_ids[start:start + batch_size]

        batch_prices = await get_last_prices(
            client=client,
            instrument_ids=batch,
        )

        prices.extend(batch_prices)

    return prices


def map_last_prices_by_instrument_uid(
        prices: list[TBankLastPrice],
) -> dict[str, TBankLastPrice]:
    """
    Преобразовать список последних цен в словарь по instrument_uid.
    """
    return {
        price.instrument_uid: price
        for price in prices
    }


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv
    from grpc import aio
    from t_tech.invest import AsyncClient

    TEST_INSTRUMENT_IDS = [
        "SBER_TQBR",
        "GAZP_TQBR",
        "LKOH_TQBR",
        "YDEX_TQBR",
        "VTBR_TQBR",
        "ROSN_TQBR",
        "GMKN_TQBR",
        "TATN_TQBR",
        "SNGS_TQBR",
        "NVTK_TQBR",
    ]

    TEST_BATCH_SIZE = 3


    def print_last_prices(prices: list[TBankLastPrice]) -> None:
        print(f"Получено последних цен: {len(prices)}")
        print()

        if not prices:
            print("Цены не получены.")
            return

        for number, price in enumerate(prices, start=1):
            print(f"{number}. {price.ticker}_{price.class_code}")
            print(f"   price:      {price.price}")
            print(f"   time UTC:   {price.time}")
            print(f"   figi:       {price.figi}")
            print(f"   uid:        {price.instrument_uid}")
            print(f"   type:       {price.last_price_type}")
            print()


    async def main() -> None:
        load_dotenv()

        token = os.environ["INVEST_TOKEN"]

        try:
            async with AsyncClient(token) as client:
                prices = await get_last_prices_batched(
                    client=client,
                    instrument_ids=TEST_INSTRUMENT_IDS,
                    batch_size=TEST_BATCH_SIZE,
                )
        except KeyError as error:
            print(f"Ошибка: в .env не задана переменная {error}.")
            return
        except aio.AioRpcError as error:
            print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
            return

        print_last_prices(prices)


    asyncio.run(main())
