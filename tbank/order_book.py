from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

from t_tech.invest.grpc import marketdata_pb2


NANO = Decimal("1000000000")


class MarketDataService(Protocol):
    stub: Any
    metadata: Any


class OrderBookApiClient(Protocol):
    market_data: MarketDataService


@dataclass(frozen=True)
class TBankBestOrderBookPrices:
    best_bid: Decimal
    best_ask: Decimal


def _quotation_to_decimal(value) -> Decimal:
    return Decimal(value.units) + Decimal(value.nano) / NANO


async def get_best_order_book_prices(
    client: OrderBookApiClient,
    instrument_id: str,
    depth: int = 1,
) -> TBankBestOrderBookPrices:
    if not instrument_id.strip():
        raise ValueError("instrument_id не может быть пустым.")

    if depth <= 0:
        raise ValueError("depth должен быть больше 0.")

    request = marketdata_pb2.GetOrderBookRequest()
    fields = request.DESCRIPTOR.fields_by_name

    if "instrument_id" in fields:
        request.instrument_id = instrument_id

    if "figi" in fields:
        request.figi = instrument_id

    if "depth" in fields:
        request.depth = depth

    response = await client.market_data.stub.GetOrderBook(
        request=request,
        metadata=client.market_data.metadata,
    )

    bids = [
        _quotation_to_decimal(order.price)
        for order in response.bids
    ]
    asks = [
        _quotation_to_decimal(order.price)
        for order in response.asks
    ]

    if not bids:
        raise RuntimeError(f"В стакане нет заявок на покупку: {instrument_id}")

    if not asks:
        raise RuntimeError(f"В стакане нет заявок на продажу: {instrument_id}")

    return TBankBestOrderBookPrices(
        best_bid=max(bids),
        best_ask=min(asks),
    )
