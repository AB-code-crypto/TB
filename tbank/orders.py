from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol
from uuid import uuid4

from t_tech.invest.grpc import common_pb2, orders_pb2

NANO = Decimal("1000000000")


class OrdersService(Protocol):
    stub: Any
    metadata: Any


class OrdersApiClient(Protocol):
    orders: OrdersService


@dataclass(frozen=True)
class TBankOrderResult:
    order_id: str
    order_request_id: str
    execution_report_status: str
    lots_requested: int
    lots_executed: int
    direction: str
    order_type: str
    figi: str
    instrument_uid: str
    initial_order_price: Decimal
    executed_order_price: Decimal
    total_order_amount: Decimal
    message: str


def _money_to_decimal(value) -> Decimal:
    return Decimal(value.units) + Decimal(value.nano) / NANO


def _decimal_to_quotation(value: Decimal) -> common_pb2.Quotation:
    """
    Преобразовать Decimal в protobuf Quotation.

    Используется для цены лимитной заявки.
    Не округляем цену молча: если больше 9 знаков после запятой,
    падаем явно.
    """
    if value <= 0:
        raise ValueError("Цена должна быть больше 0.")

    units = int(value)
    nano_decimal = (value - Decimal(units)) * NANO

    if nano_decimal != nano_decimal.to_integral_value():
        raise ValueError("Цена не может содержать больше 9 знаков после запятой.")

    return common_pb2.Quotation(
        units=units,
        nano=int(nano_decimal),
    )


async def post_market_order(
        client: OrdersApiClient,
        account_id: str,
        instrument_id: str,
        quantity_lots: int,
        direction: int,
) -> TBankOrderResult:
    """
    Выставить рыночную заявку.

    quantity_lots — количество лотов, не количество акций.
    instrument_id может быть:
    - instrument_uid;
    - figi;
    - ticker + '_' + class_code.

    direction:
    - orders_pb2.ORDER_DIRECTION_BUY
    - orders_pb2.ORDER_DIRECTION_SELL
    """
    if not account_id.strip():
        raise ValueError("account_id не может быть пустым.")

    if not instrument_id.strip():
        raise ValueError("instrument_id не может быть пустым.")

    if quantity_lots <= 0:
        raise ValueError("quantity_lots должен быть больше 0.")

    if direction not in (
            orders_pb2.ORDER_DIRECTION_BUY,
            orders_pb2.ORDER_DIRECTION_SELL,
    ):
        raise ValueError("Некорректное направление заявки.")

    response = await client.orders.stub.PostOrder(
        request=orders_pb2.PostOrderRequest(
            quantity=quantity_lots,
            price=common_pb2.Quotation(units=0, nano=0),
            direction=direction,
            account_id=account_id,
            order_type=orders_pb2.ORDER_TYPE_MARKET,
            order_id=str(uuid4()),
            instrument_id=instrument_id,
            time_in_force=orders_pb2.TIME_IN_FORCE_DAY,
            price_type=common_pb2.PRICE_TYPE_CURRENCY,
            confirm_margin_trade=False,
        ),
        metadata=client.orders.metadata,
    )

    return TBankOrderResult(
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
        initial_order_price=_money_to_decimal(response.initial_order_price),
        executed_order_price=_money_to_decimal(response.executed_order_price),
        total_order_amount=_money_to_decimal(response.total_order_amount),
        message=response.message,
    )


async def post_limit_order(
        client: OrdersApiClient,
        account_id: str,
        instrument_id: str,
        quantity_lots: int,
        price: Decimal,
        direction: int,
) -> TBankOrderResult:
    """
    Выставить лимитную заявку.

    quantity_lots — количество лотов, не количество акций.
    price — лимитная цена за инструмент в валюте расчётов.

    direction:
    - orders_pb2.ORDER_DIRECTION_BUY
    - orders_pb2.ORDER_DIRECTION_SELL
    """
    if not account_id.strip():
        raise ValueError("account_id не может быть пустым.")

    if not instrument_id.strip():
        raise ValueError("instrument_id не может быть пустым.")

    if quantity_lots <= 0:
        raise ValueError("quantity_lots должен быть больше 0.")

    if direction not in (
            orders_pb2.ORDER_DIRECTION_BUY,
            orders_pb2.ORDER_DIRECTION_SELL,
    ):
        raise ValueError("Некорректное направление заявки.")

    response = await client.orders.stub.PostOrder(
        request=orders_pb2.PostOrderRequest(
            quantity=quantity_lots,
            price=_decimal_to_quotation(price),
            direction=direction,
            account_id=account_id,
            order_type=orders_pb2.ORDER_TYPE_LIMIT,
            order_id=str(uuid4()),
            instrument_id=instrument_id,
            time_in_force=orders_pb2.TIME_IN_FORCE_DAY,
            price_type=common_pb2.PRICE_TYPE_CURRENCY,
            confirm_margin_trade=False,
        ),
        metadata=client.orders.metadata,
    )

    return TBankOrderResult(
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
        initial_order_price=_money_to_decimal(response.initial_order_price),
        executed_order_price=_money_to_decimal(response.executed_order_price),
        total_order_amount=_money_to_decimal(response.total_order_amount),
        message=response.message,
    )


async def buy_market_order(
        client: OrdersApiClient,
        account_id: str,
        instrument_id: str,
        quantity_lots: int,
) -> TBankOrderResult:
    """
    Купить инструмент рыночной заявкой.
    """
    return await post_market_order(
        client=client,
        account_id=account_id,
        instrument_id=instrument_id,
        quantity_lots=quantity_lots,
        direction=orders_pb2.ORDER_DIRECTION_BUY,
    )


async def sell_market_order(
        client: OrdersApiClient,
        account_id: str,
        instrument_id: str,
        quantity_lots: int,
) -> TBankOrderResult:
    """
    Продать инструмент рыночной заявкой.
    """
    return await post_market_order(
        client=client,
        account_id=account_id,
        instrument_id=instrument_id,
        quantity_lots=quantity_lots,
        direction=orders_pb2.ORDER_DIRECTION_SELL,
    )


async def buy_limit_order(
        client: OrdersApiClient,
        account_id: str,
        instrument_id: str,
        quantity_lots: int,
        price: Decimal,
) -> TBankOrderResult:
    """
    Купить инструмент лимитной заявкой.
    """
    return await post_limit_order(
        client=client,
        account_id=account_id,
        instrument_id=instrument_id,
        quantity_lots=quantity_lots,
        price=price,
        direction=orders_pb2.ORDER_DIRECTION_BUY,
    )


async def sell_limit_order(
        client: OrdersApiClient,
        account_id: str,
        instrument_id: str,
        quantity_lots: int,
        price: Decimal,
) -> TBankOrderResult:
    """
    Продать инструмент лимитной заявкой.
    """
    return await post_limit_order(
        client=client,
        account_id=account_id,
        instrument_id=instrument_id,
        quantity_lots=quantity_lots,
        price=price,
        direction=orders_pb2.ORDER_DIRECTION_SELL,
    )


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv
    from grpc import aio
    from t_tech.invest import AsyncClient

    # ВНИМАНИЕ:
    # Если используется боевой токен, это реальная заявка на реальном брокерском счёте.
    # Чтобы случайно не купить/продать при запуске файла, по умолчанию выключено.
    ALLOW_REAL_ORDER = True

    # BUY  — купить
    # SELL — продать
    TEST_ORDER_ACTION = "BUY"

    # MARKET — рыночная заявка
    # LIMIT  — лимитная заявка
    TEST_ORDER_TYPE = "LIMIT"

    TEST_INSTRUMENT_ID = "SBER_TQBR"
    TEST_QUANTITY_LOTS = 1
    TEST_LIMIT_PRICE = Decimal("300.00")


    def print_order_result(result: TBankOrderResult) -> None:
        print("Результат выставления заявки:")
        print()
        print(f"order_id:                {result.order_id}")
        print(f"order_request_id:        {result.order_request_id}")
        print(f"status:                  {result.execution_report_status}")
        print(f"direction:               {result.direction}")
        print(f"order_type:              {result.order_type}")
        print(f"figi:                    {result.figi}")
        print(f"instrument_uid:          {result.instrument_uid}")
        print(f"lots_requested:          {result.lots_requested}")
        print(f"lots_executed:           {result.lots_executed}")

        if result.lots_executed > 0:
            average_price = result.total_order_amount / result.lots_executed
            print(f"average_price_per_lot:   {average_price:.2f}")

        print(f"initial_order_price:     {result.initial_order_price:.2f}")
        print(f"executed_order_price:    {result.executed_order_price:.2f}")
        print(f"total_order_amount:      {result.total_order_amount:.2f}")
        print(f"message:                 {result.message}")


    async def main() -> None:
        load_dotenv()

        action = TEST_ORDER_ACTION.strip().upper()
        order_type = TEST_ORDER_TYPE.strip().upper()

        if action not in ("BUY", "SELL"):
            print("Ошибка: TEST_ORDER_ACTION должен быть BUY или SELL.")
            return
        if order_type not in ("MARKET", "LIMIT"):
            print("Ошибка: TEST_ORDER_TYPE должен быть MARKET или LIMIT.")
            return

        if not ALLOW_REAL_ORDER:
            print("Заявка НЕ отправлена.")
            print("Для реальной отправки заявки установи:")
            print("ALLOW_REAL_ORDER = True")
            print()
            print(f"Действие:   {action}")
            print(f"Инструмент: {TEST_INSTRUMENT_ID}")
            print(f"Количество: {TEST_QUANTITY_LOTS} лот(ов)")
            print(f"Тип заявки: {order_type}")

            if order_type == "LIMIT":
                print(f"Лимитная цена: {TEST_LIMIT_PRICE}")
            return

        account_id = os.environ["INVEST_ACCOUNT_ID"]
        token = os.environ["INVEST_TOKEN"]

        try:
            async with AsyncClient(token) as client:
                if action == "BUY" and order_type == "MARKET":
                    result = await buy_market_order(
                        client=client,
                        account_id=account_id,
                        instrument_id=TEST_INSTRUMENT_ID,
                        quantity_lots=TEST_QUANTITY_LOTS,
                    )
                elif action == "SELL" and order_type == "MARKET":
                    result = await sell_market_order(
                        client=client,
                        account_id=account_id,
                        instrument_id=TEST_INSTRUMENT_ID,
                        quantity_lots=TEST_QUANTITY_LOTS,
                    )
                elif action == "BUY" and order_type == "LIMIT":
                    result = await buy_limit_order(
                        client=client,
                        account_id=account_id,
                        instrument_id=TEST_INSTRUMENT_ID,
                        quantity_lots=TEST_QUANTITY_LOTS,
                        price=TEST_LIMIT_PRICE,
                    )
                elif action == "SELL" and order_type == "LIMIT":
                    result = await sell_limit_order(
                        client=client,
                        account_id=account_id,
                        instrument_id=TEST_INSTRUMENT_ID,
                        quantity_lots=TEST_QUANTITY_LOTS,
                        price=TEST_LIMIT_PRICE,
                    )
                else:
                    raise RuntimeError(
                        f"Неизвестная комбинация: action={action}, order_type={order_type}"
                    )

        except KeyError as error:
            print(f"Ошибка: в .env не задана переменная {error}.")
            return
        except aio.AioRpcError as error:
            print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
            return

        print_order_result(result)


    asyncio.run(main())
