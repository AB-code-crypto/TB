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
    ]

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
                prices = await get_last_prices(
                    client=client,
                    instrument_ids=TEST_INSTRUMENT_IDS,
                )
        except KeyError as error:
            print(f"Ошибка: в .env не задана переменная {error}.")
            return
        except aio.AioRpcError as error:
            print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
            return

        print_last_prices(prices)

    asyncio.run(main())