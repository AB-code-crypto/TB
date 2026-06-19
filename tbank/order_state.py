from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol

from t_tech.invest.grpc import common_pb2, orders_pb2

NANO = Decimal("1000000000")


class OrdersService(Protocol):
    stub: Any
    metadata: Any


class OrdersApiClient(Protocol):
    orders: OrdersService


@dataclass(frozen=True)
class TBankOrderState:
    order_id: str
    order_request_id: str
    execution_report_status: str
    lots_requested: int
    lots_executed: int
    direction: str
    order_type: str
    figi: str
    instrument_uid: str
    currency: str
    order_date: datetime

    initial_order_price: Decimal
    executed_order_price: Decimal
    total_order_amount: Decimal
    average_position_price: Decimal
    initial_security_price: Decimal
    initial_commission: Decimal
    executed_commission: Decimal
    service_commission: Decimal


def _money_to_decimal(value) -> Decimal:
    return Decimal(value.units) + Decimal(value.nano) / NANO


async def get_order_state(
        client: OrdersApiClient,
        account_id: str,
        order_id: str,
        order_id_type: int,
) -> TBankOrderState:
    """
    Получить состояние торгового поручения.

    order_id_type:
    - orders_pb2.ORDER_ID_TYPE_EXCHANGE — биржевой order_id;
    - orders_pb2.ORDER_ID_TYPE_REQUEST — order_request_id / ключ идемпотентности.
    """
    if not account_id.strip():
        raise ValueError("account_id не может быть пустым.")

    if not order_id.strip():
        raise ValueError("order_id не может быть пустым.")

    if order_id_type not in (
            orders_pb2.ORDER_ID_TYPE_EXCHANGE,
            orders_pb2.ORDER_ID_TYPE_REQUEST,
    ):
        raise ValueError("Некорректный order_id_type.")

    response = await client.orders.stub.GetOrderState(
        request=orders_pb2.GetOrderStateRequest(
            account_id=account_id,
            order_id=order_id,
            price_type=common_pb2.PRICE_TYPE_CURRENCY,
            order_id_type=order_id_type,
        ),
        metadata=client.orders.metadata,
    )

    return TBankOrderState(
        order_id=response.order_id,
        order_request_id=response.order_request_id,
        execution_report_status=orders_pb2.OrderExecutionReportStatus.Name(
            response.execution_report_status
        ),
        lots_requested=response.lots_requested,
        lots_executed=response.lots_executed,
        direction=orders_pb2.OrderDirection.Name(response.direction),
        order_type=orders_pb2.OrderType.Name(response.order_type),
        figi=response.figi,
        instrument_uid=response.instrument_uid,
        currency=response.currency.upper(),
        order_date=response.order_date.ToDatetime(tzinfo=timezone.utc),
        initial_order_price=_money_to_decimal(response.initial_order_price),
        executed_order_price=_money_to_decimal(response.executed_order_price),
        total_order_amount=_money_to_decimal(response.total_order_amount),
        average_position_price=_money_to_decimal(response.average_position_price),
        initial_security_price=_money_to_decimal(response.initial_security_price),
        initial_commission=_money_to_decimal(response.initial_commission),
        executed_commission=_money_to_decimal(response.executed_commission),
        service_commission=_money_to_decimal(response.service_commission),
    )


async def get_order_state_by_exchange_order_id(
        client: OrdersApiClient,
        account_id: str,
        order_id: str,
) -> TBankOrderState:
    """
    Получить состояние заявки по биржевому order_id.
    """
    return await get_order_state(
        client=client,
        account_id=account_id,
        order_id=order_id,
        order_id_type=orders_pb2.ORDER_ID_TYPE_EXCHANGE,
    )


async def get_order_state_by_request_order_id(
        client: OrdersApiClient,
        account_id: str,
        order_request_id: str,
) -> TBankOrderState:
    """
    Получить состояние заявки по order_request_id / ключу идемпотентности.
    """
    return await get_order_state(
        client=client,
        account_id=account_id,
        order_id=order_request_id,
        order_id_type=orders_pb2.ORDER_ID_TYPE_REQUEST,
    )


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv
    from grpc import aio
    from t_tech.invest import AsyncClient

    # Для проверки твоей покупки SBER:
    TEST_ORDER_ID = "80411824954"
    TEST_ORDER_ID_TYPE = orders_pb2.ORDER_ID_TYPE_EXCHANGE


    # Если хочешь проверить по request_id, используй так:
    # TEST_ORDER_ID = "e0bb6ec2-763a-42e4-b36e-ec915bc23827"
    # TEST_ORDER_ID_TYPE = orders_pb2.ORDER_ID_TYPE_REQUEST

    def print_order_state(state: TBankOrderState) -> None:
        print("Состояние заявки:")
        print()
        print(f"order_id:                {state.order_id}")
        print(f"order_request_id:        {state.order_request_id}")
        print(f"status:                  {state.execution_report_status}")
        print(f"direction:               {state.direction}")
        print(f"order_type:              {state.order_type}")
        print(f"figi:                    {state.figi}")
        print(f"instrument_uid:          {state.instrument_uid}")
        print(f"currency:                {state.currency}")
        print(f"order_date UTC:          {state.order_date}")
        print()
        print(f"lots_requested:          {state.lots_requested}")
        print(f"lots_executed:           {state.lots_executed}")

        if state.lots_executed > 0:
            average_price = state.total_order_amount / state.lots_executed
            print(f"average_price_per_lot:   {average_price:.2f}")

        commission = state.executed_commission + state.service_commission

        if state.direction == "ORDER_DIRECTION_BUY":
            cash_amount = state.total_order_amount + commission
            cash_amount_label = "cash_spent"
        elif state.direction == "ORDER_DIRECTION_SELL":
            cash_amount = state.total_order_amount - commission
            cash_amount_label = "cash_received"
        else:
            raise RuntimeError(f"Неизвестное направление заявки: {state.direction}")

        print()
        print(f"gross_amount:            {state.total_order_amount:.2f}")
        print(f"commission:              {commission:.2f}")
        print(f"{cash_amount_label}:     {cash_amount:.2f}")
        print(f"initial_order_price:     {state.initial_order_price:.2f}")
        print(f"executed_order_price:    {state.executed_order_price:.2f}")
        print(f"average_position_price:  {state.average_position_price:.2f}")
        print(f"initial_security_price:  {state.initial_security_price:.2f}")
        print(f"total_order_amount:      {state.total_order_amount:.2f}")
        print()
        print(f"initial_commission:      {state.initial_commission:.2f}")
        print(f"executed_commission:     {state.executed_commission:.2f}")
        print(f"service_commission:      {state.service_commission:.2f}")


    async def main() -> None:
        load_dotenv()

        token = os.environ["INVEST_TOKEN"]
        account_id = os.environ["INVEST_ACCOUNT_ID"]

        try:
            async with AsyncClient(token) as client:
                state = await get_order_state(
                    client=client,
                    account_id=account_id,
                    order_id=TEST_ORDER_ID,
                    order_id_type=TEST_ORDER_ID_TYPE,
                )
        except KeyError as error:
            print(f"Ошибка: в .env не задана переменная {error}.")
            return
        except aio.AioRpcError as error:
            print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
            return

        print_order_state(state)


    asyncio.run(main())
