from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Protocol

from google.protobuf.timestamp_pb2 import Timestamp
from t_tech.invest.grpc import marketdata_pb2


NANO = Decimal("1000000000")


class MarketDataService(Protocol):
    stub: Any
    metadata: Any


class MarketDataApiClient(Protocol):
    market_data: MarketDataService


@dataclass(frozen=True)
class TBankCandle:
    time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    is_complete: bool


def _quotation_to_decimal(value) -> Decimal:
    return Decimal(value.units) + Decimal(value.nano) / NANO


def _datetime_to_timestamp(value: datetime) -> Timestamp:
    if value.tzinfo is None:
        raise ValueError("datetime должен быть timezone-aware.")

    timestamp = Timestamp()
    timestamp.FromDatetime(value.astimezone(timezone.utc))

    return timestamp


async def get_candles(
    client: MarketDataApiClient,
    instrument_id: str,
    from_time: datetime,
    to_time: datetime,
    interval: int,
    limit: int | None = None,
) -> list[TBankCandle]:
    """
    Получить исторические свечи по инструменту.

    instrument_id может быть:
    - instrument_uid;
    - figi;
    - ticker + '_' + class_code.

    Для рабочего робота предпочтительно использовать instrument_uid.
    Для ручной проверки удобно использовать SBER_TQBR.
    """
    if not instrument_id.strip():
        raise ValueError("instrument_id не может быть пустым.")

    if from_time.tzinfo is None:
        raise ValueError("from_time должен быть timezone-aware.")

    if to_time.tzinfo is None:
        raise ValueError("to_time должен быть timezone-aware.")

    if from_time >= to_time:
        raise ValueError("from_time должен быть меньше to_time.")

    if interval == marketdata_pb2.CANDLE_INTERVAL_UNSPECIFIED:
        raise ValueError("interval не может быть CANDLE_INTERVAL_UNSPECIFIED.")

    if limit is not None and limit <= 0:
        raise ValueError("limit должен быть больше 0.")

    request_data = {
        "instrument_id": instrument_id,
        "from": _datetime_to_timestamp(from_time),
        "to": _datetime_to_timestamp(to_time),
        "interval": interval,
    }

    if limit is not None:
        request_data["limit"] = limit

    response = await client.market_data.stub.GetCandles(
        request=marketdata_pb2.GetCandlesRequest(**request_data),
        metadata=client.market_data.metadata,
    )

    candles: list[TBankCandle] = []

    for candle in response.candles:
        candles.append(
            TBankCandle(
                time=candle.time.ToDatetime(tzinfo=timezone.utc),
                open=_quotation_to_decimal(candle.open),
                high=_quotation_to_decimal(candle.high),
                low=_quotation_to_decimal(candle.low),
                close=_quotation_to_decimal(candle.close),
                volume=candle.volume,
                is_complete=candle.is_complete,
            )
        )

    return candles


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv
    from grpc import aio
    from t_tech.invest import AsyncClient

    TEST_INSTRUMENT_ID = "SBER_TQBR"
    TEST_INTERVAL = marketdata_pb2.CANDLE_INTERVAL_1_MIN
    TEST_LIMIT = 50

    def print_candles(candles: list[TBankCandle]) -> None:
        print(f"Получено свечей: {len(candles)}")
        print()

        if not candles:
            print("Свечи не получены.")
            return

        for number, candle in enumerate(candles, start=1):
            print(f"Свеча #{number}")
            print(f"  time UTC:    {candle.time}")
            print(f"  open:        {candle.open}")
            print(f"  high:        {candle.high}")
            print(f"  low:         {candle.low}")
            print(f"  close:       {candle.close}")
            print(f"  volume:      {candle.volume}")
            print(f"  is_complete: {candle.is_complete}")
            print()

    async def main() -> None:
        load_dotenv()

        token = os.environ["INVEST_TOKEN"]

        to_time = datetime.now(timezone.utc)
        from_time = to_time - timedelta(days=1)

        try:
            async with AsyncClient(token) as client:
                candles = await get_candles(
                    client=client,
                    instrument_id=TEST_INSTRUMENT_ID,
                    from_time=from_time,
                    to_time=to_time,
                    interval=TEST_INTERVAL,
                    limit=TEST_LIMIT,
                )
        except KeyError as error:
            print(f"Ошибка: в .env не задана переменная {error}.")
            return
        except aio.AioRpcError as error:
            print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
            return

        print_candles(candles)

    asyncio.run(main())