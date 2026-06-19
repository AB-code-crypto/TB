from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from t_tech.invest.grpc import orders_pb2


class OrdersService(Protocol):
    stub: Any
    metadata: Any


class OrdersApiClient(Protocol):
    orders: OrdersService


@dataclass(frozen=True)
class TBankCancelOrderResult:
    order_id: str
    order_id_type: str
    cancelled_at: datetime


async def cancel_order(
    client: OrdersApiClient,
    account_id: str,
    order_id: str,
    order_id_type: int,
) -> TBankCancelOrderResult:
    """
    Отменить активную заявку.

    order_id_type:
    - orders_pb2.ORDER_ID_TYPE_EXCHANGE — биржевой order_id;
    - orders_pb2.ORDER_ID_TYPE_REQUEST — order_request_id / ключ идемпотентности.

    Важно: уже исполненную заявку отменить нельзя.
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

    response = await client.orders.stub.CancelOrder(
        request=orders_pb2.CancelOrderRequest(
            account_id=account_id,
            order_id=order_id,
            order_id_type=order_id_type,
        ),
        metadata=client.orders.metadata,
    )

    return TBankCancelOrderResult(
        order_id=order_id,
        order_id_type=orders_pb2.OrderIdType.Name(order_id_type),
        cancelled_at=response.time.ToDatetime(tzinfo=timezone.utc),
    )


async def cancel_order_by_exchange_order_id(
    client: OrdersApiClient,
    account_id: str,
    order_id: str,
) -> TBankCancelOrderResult:
    """
    Отменить заявку по биржевому order_id.
    """
    return await cancel_order(
        client=client,
        account_id=account_id,
        order_id=order_id,
        order_id_type=orders_pb2.ORDER_ID_TYPE_EXCHANGE,
    )


async def cancel_order_by_request_order_id(
    client: OrdersApiClient,
    account_id: str,
    order_request_id: str,
) -> TBankCancelOrderResult:
    """
    Отменить заявку по order_request_id / ключу идемпотентности.
    """
    return await cancel_order(
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

    # ВНИМАНИЕ:
    # Если используется боевой токен, это реальная отмена заявки.
    # По умолчанию выключено, чтобы случайно не отменить активную заявку.
    ALLOW_REAL_CANCEL = True

    # Для проверки вставляешь order_id активной лимитной заявки.
    TEST_ORDER_ID = "80412370163"
    TEST_ORDER_ID_TYPE = orders_pb2.ORDER_ID_TYPE_EXCHANGE

    # Если хочешь отменить по request_id:
    # TEST_ORDER_ID = "uuid-request-id"
    # TEST_ORDER_ID_TYPE = orders_pb2.ORDER_ID_TYPE_REQUEST

    def print_cancel_result(result: TBankCancelOrderResult) -> None:
        print("Результат отмены заявки:")
        print()
        print(f"order_id:        {result.order_id}")
        print(f"order_id_type:   {result.order_id_type}")
        print(f"cancelled_at UTC:{result.cancelled_at}")

    async def main() -> None:
        load_dotenv()

        if not ALLOW_REAL_CANCEL:
            print("Отмена заявки НЕ отправлена.")
            print("Для реальной отмены заявки установи:")
            print("ALLOW_REAL_CANCEL = True")
            print()
            print(f"order_id:      {TEST_ORDER_ID}")
            print(f"order_id_type: {orders_pb2.OrderIdType.Name(TEST_ORDER_ID_TYPE)}")
            return

        account_id = os.environ["INVEST_ACCOUNT_ID"]
        token = os.environ["INVEST_TOKEN"]

        try:
            async with AsyncClient(token) as client:
                result = await cancel_order(
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

        print_cancel_result(result)

    asyncio.run(main())