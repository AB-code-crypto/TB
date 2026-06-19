from dataclasses import dataclass
from typing import Any, Protocol

from t_tech.invest.grpc import common_pb2, marketdata_pb2


class MarketDataService(Protocol):
    stub: Any
    metadata: Any


class MarketDataApiClient(Protocol):
    market_data: MarketDataService


@dataclass(frozen=True)
class TBankTradingStatus:
    figi: str
    instrument_uid: str
    ticker: str
    class_code: str
    trading_status: str
    limit_order_available_flag: bool
    market_order_available_flag: bool
    api_trade_available_flag: bool
    bestprice_order_available_flag: bool
    only_best_price: bool


async def get_trading_status(
    client: MarketDataApiClient,
    instrument_id: str,
) -> TBankTradingStatus:
    """
    Получить текущий торговый статус инструмента.

    instrument_id может быть:
    - instrument_uid;
    - figi;
    - ticker + '_' + class_code.

    Для рабочего робота предпочтительно использовать instrument_uid.
    Для ручной проверки удобно использовать вариант вроде SBER_TQBR.
    """
    if not instrument_id.strip():
        raise ValueError("instrument_id не может быть пустым.")

    response = await client.market_data.stub.GetTradingStatus(
        request=marketdata_pb2.GetTradingStatusRequest(
            instrument_id=instrument_id,
        ),
        metadata=client.market_data.metadata,
    )

    return TBankTradingStatus(
        figi=response.figi,
        instrument_uid=response.instrument_uid,
        ticker=response.ticker,
        class_code=response.class_code,
        trading_status=common_pb2.SecurityTradingStatus.Name(
            response.trading_status
        ),
        limit_order_available_flag=response.limit_order_available_flag,
        market_order_available_flag=response.market_order_available_flag,
        api_trade_available_flag=response.api_trade_available_flag,
        bestprice_order_available_flag=response.bestprice_order_available_flag,
        only_best_price=response.only_best_price,
    )


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv
    from grpc import aio
    from t_tech.invest import AsyncClient

    TEST_INSTRUMENT_ID = "SBER_TQBR"

    def print_trading_status(status: TBankTradingStatus) -> None:
        print("Торговый статус инструмента:")
        print()
        print(f"ticker:         {status.ticker}")
        print(f"class_code:     {status.class_code}")
        print(f"figi:           {status.figi}")
        print(f"uid:            {status.instrument_uid}")
        print(f"status:         {status.trading_status}")
        print()
        print("Доступность заявок:")
        print(f"  api_trade:    {status.api_trade_available_flag}")
        print(f"  limit_order:  {status.limit_order_available_flag}")
        print(f"  market_order: {status.market_order_available_flag}")
        print(f"  bestprice:    {status.bestprice_order_available_flag}")
        print(f"  only_best:    {status.only_best_price}")

    async def main() -> None:
        load_dotenv()

        token = os.environ["INVEST_TOKEN"]

        try:
            async with AsyncClient(token) as client:
                status = await get_trading_status(
                    client=client,
                    instrument_id=TEST_INSTRUMENT_ID,
                )
        except KeyError as error:
            print(f"Ошибка: в .env не задана переменная {error}.")
            return
        except aio.AioRpcError as error:
            print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
            return

        print_trading_status(status)

    asyncio.run(main())