from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol

from t_tech.invest.grpc import orders_pb2


NANO = Decimal("1000000000")


class OrdersService(Protocol):
    stub: Any
    metadata: Any


class OrdersApiClient(Protocol):
    orders: OrdersService


@dataclass(frozen=True)
class TBankActiveOrder:
    order_id: str
    order_request_id: str
    execution_report_status: str
    direction: str
    order_type: str

    figi: str
    instrument_uid: str
    ticker: str
    class_code: str
    currency: str

    lots_requested: int
    lots_executed: int

    initial_order_price: Decimal
    executed_order_price: Decimal
    total_order_amount: Decimal
    average_position_price: Decimal
    initial_security_price: Decimal

    order_date: datetime


def _money_to_decimal(value) -> Decimal:
    return Decimal(value.units) + Decimal(value.nano) / NANO


async def get_active_orders(
    client: OrdersApiClient,
    account_id: str,
) -> list[TBankActiveOrder]:
    """
    Получить список активных заявок по счёту.

    По умолчанию GetOrders возвращает только активные заявки.
    Исполненные и отменённые заявки в этом списке обычно не остаются.
    """
    if not account_id.strip():
        raise ValueError("account_id не может быть пустым.")

    response = await client.orders.stub.GetOrders(
        request=orders_pb2.GetOrdersRequest(
            account_id=account_id,
        ),
        metadata=client.orders.metadata,
    )

    orders: list[TBankActiveOrder] = []

    for order in response.orders:
        orders.append(
            TBankActiveOrder(
                order_id=order.order_id,
                order_request_id=order.order_request_id,
                execution_report_status=orders_pb2.OrderExecutionReportStatus.Name(
                    order.execution_report_status
                ),
                direction=orders_pb2.OrderDirection.Name(order.direction),
                order_type=orders_pb2.OrderType.Name(order.order_type),
                figi=order.figi,
                instrument_uid=order.instrument_uid,
                ticker=order.ticker,
                class_code=order.class_code,
                currency=order.currency.upper(),
                lots_requested=order.lots_requested,
                lots_executed=order.lots_executed,
                initial_order_price=_money_to_decimal(order.initial_order_price),
                executed_order_price=_money_to_decimal(order.executed_order_price),
                total_order_amount=_money_to_decimal(order.total_order_amount),
                average_position_price=_money_to_decimal(order.average_position_price),
                initial_security_price=_money_to_decimal(order.initial_security_price),
                order_date=order.order_date.ToDatetime(tzinfo=timezone.utc),
            )
        )

    return orders


async def get_active_limit_orders(
    client: OrdersApiClient,
    account_id: str,
) -> list[TBankActiveOrder]:
    """
    Получить только активные лимитные заявки по счёту.
    """
    active_orders = await get_active_orders(
        client=client,
        account_id=account_id,
    )

    return [
        order
        for order in active_orders
        if order.order_type == "ORDER_TYPE_LIMIT"
    ]


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv
    from grpc import aio
    from t_tech.invest import AsyncClient

    ONLY_LIMIT_ORDERS = True

    def print_active_orders(orders: list[TBankActiveOrder]) -> None:
        print(f"Активных заявок найдено: {len(orders)}")
        print()

        if not orders:
            print("Активных заявок нет.")
            return

        for number, order in enumerate(orders, start=1):
            print(f"Заявка #{number}")
            print(f"  order_id:             {order.order_id}")
            print(f"  order_request_id:     {order.order_request_id}")
            print(f"  status:               {order.execution_report_status}")
            print(f"  direction:            {order.direction}")
            print(f"  order_type:           {order.order_type}")
            print()
            print(f"  ticker:               {order.ticker}_{order.class_code}")
            print(f"  figi:                 {order.figi}")
            print(f"  instrument_uid:       {order.instrument_uid}")
            print(f"  currency:             {order.currency}")
            print()
            print(f"  lots_requested:       {order.lots_requested}")
            print(f"  lots_executed:        {order.lots_executed}")
            print(f"  initial_order_price:  {order.initial_order_price:.2f}")
            print(f"  initial_security_price:{order.initial_security_price:.2f}")
            print(f"  executed_order_price: {order.executed_order_price:.2f}")
            print(f"  total_order_amount:   {order.total_order_amount:.2f}")
            print(f"  average_position_price:{order.average_position_price:.2f}")
            print()
            print(f"  order_date UTC:       {order.order_date}")
            print()

    async def main() -> None:
        load_dotenv()

        token = os.environ["INVEST_TOKEN"]
        account_id = os.environ["INVEST_ACCOUNT_ID"]

        try:
            async with AsyncClient(token) as client:
                if ONLY_LIMIT_ORDERS:
                    orders = await get_active_limit_orders(
                        client=client,
                        account_id=account_id,
                    )
                else:
                    orders = await get_active_orders(
                        client=client,
                        account_id=account_id,
                    )
        except KeyError as error:
            print(f"Ошибка: в .env не задана переменная {error}.")
            return
        except aio.AioRpcError as error:
            print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
            return

        print_active_orders(orders)

    asyncio.run(main())