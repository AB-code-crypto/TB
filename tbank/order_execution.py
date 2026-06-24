from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any, Protocol

from t_tech.invest.grpc import orders_pb2

from tbank.shares import TBankShare


NANO = Decimal("1000000000")


class OrdersService(Protocol):
    stub: Any
    metadata: Any


class OrdersApiClient(Protocol):
    orders: OrdersService


@dataclass(frozen=True)
class TBankPostOrderResult:
    broker_order_id: str
    order_request_id: str
    execution_report_status: str
    lots_requested: int
    lots_executed: int
    initial_order_price: Decimal
    executed_order_price: Decimal
    total_order_amount: Decimal


def _decimal_to_units_nano(value: Decimal) -> tuple[int, int]:
    normalized_value = value.quantize(Decimal("0.000000001"), rounding=ROUND_DOWN)
    units = int(normalized_value)
    nano = int((normalized_value - Decimal(units)) * NANO)

    return units, nano


def _set_decimal_to_quotation(value: Decimal, target) -> None:
    units, nano = _decimal_to_units_nano(value)
    target.units = units
    target.nano = nano


def _money_to_decimal(value) -> Decimal:
    return Decimal(value.units) + Decimal(value.nano) / NANO


def _enum_value(name: str) -> int:
    if hasattr(orders_pb2, name):
        return getattr(orders_pb2, name)

    raise RuntimeError(f"В orders_pb2 не найден enum: {name}")


def _side_to_order_direction(side: str) -> int:
    if side == "BUY":
        return _enum_value("ORDER_DIRECTION_BUY")

    if side == "SELL":
        return _enum_value("ORDER_DIRECTION_SELL")

    raise ValueError(f"Неизвестная сторона заявки: {side}")


def _status_name(value: int) -> str:
    try:
        return orders_pb2.OrderExecutionReportStatus.Name(value)
    except ValueError:
        return f"UNKNOWN_STATUS_{value}"


def _build_post_order_request(
    account_id: str,
    order_request_id: str,
    share: TBankShare,
    side: str,
    quantity_lots: int,
    limit_price: Decimal,
):
    request = orders_pb2.PostOrderRequest()
    fields = request.DESCRIPTOR.fields_by_name

    if "account_id" in fields:
        request.account_id = account_id

    if "order_id" in fields:
        request.order_id = order_request_id

    if "figi" in fields:
        request.figi = share.figi

    if "instrument_id" in fields:
        request.instrument_id = share.uid

    if "instrument_id_type" in fields and hasattr(orders_pb2, "INSTRUMENT_ID_TYPE_UID"):
        request.instrument_id_type = orders_pb2.INSTRUMENT_ID_TYPE_UID

    if "quantity" in fields:
        request.quantity = quantity_lots

    if "direction" in fields:
        request.direction = _side_to_order_direction(side)

    if "order_type" in fields:
        request.order_type = _enum_value("ORDER_TYPE_LIMIT")

    if "price" in fields:
        _set_decimal_to_quotation(limit_price, request.price)

    return request


async def post_limit_order(
    client: OrdersApiClient,
    account_id: str,
    order_request_id: str,
    share: TBankShare,
    side: str,
    quantity_lots: int,
    limit_price: Decimal,
) -> TBankPostOrderResult:
    if not account_id.strip():
        raise ValueError("account_id не может быть пустым.")

    if not order_request_id.strip():
        raise ValueError("order_request_id не может быть пустым.")

    if quantity_lots <= 0:
        raise ValueError("Количество лотов должно быть больше 0.")

    if limit_price <= 0:
        raise ValueError("Лимитная цена должна быть больше 0.")

    request = _build_post_order_request(
        account_id=account_id,
        order_request_id=order_request_id,
        share=share,
        side=side,
        quantity_lots=quantity_lots,
        limit_price=limit_price,
    )

    response = await client.orders.stub.PostOrder(
        request=request,
        metadata=client.orders.metadata,
    )

    return TBankPostOrderResult(
        broker_order_id=getattr(response, "order_id", ""),
        order_request_id=order_request_id,
        execution_report_status=_status_name(response.execution_report_status),
        lots_requested=getattr(response, "lots_requested", quantity_lots),
        lots_executed=getattr(response, "lots_executed", 0),
        initial_order_price=_money_to_decimal(getattr(response, "initial_order_price")),
        executed_order_price=_money_to_decimal(getattr(response, "executed_order_price")),
        total_order_amount=_money_to_decimal(getattr(response, "total_order_amount")),
    )
