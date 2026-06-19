from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

from t_tech.invest.grpc import operations_pb2


NANO = Decimal("1000000000")


class OperationsService(Protocol):
    stub: Any
    metadata: Any


class PositionsApiClient(Protocol):
    operations: OperationsService


@dataclass(frozen=True)
class TBankPortfolioPosition:
    figi: str
    instrument_uid: str
    position_uid: str
    ticker: str
    class_code: str
    instrument_type: str

    quantity: Decimal
    quantity_lots: Decimal
    blocked: bool
    blocked_lots: Decimal

    average_position_price: Decimal
    average_position_price_fifo: Decimal
    current_price: Decimal

    expected_yield: Decimal
    expected_yield_fifo: Decimal
    daily_yield: Decimal

    currency: str


def _quotation_to_decimal(value) -> Decimal:
    return Decimal(value.units) + Decimal(value.nano) / NANO


def _money_to_decimal(value) -> Decimal:
    return Decimal(value.units) + Decimal(value.nano) / NANO


async def get_portfolio_positions(
    client: PositionsApiClient,
    account_id: str,
) -> list[TBankPortfolioPosition]:
    """
    Получить текущие позиции портфеля по брокерскому счёту.

    Используем прямой gRPC-вызов stub.GetPortfolio(), а не deprecated-обёртку
    client.operations.get_portfolio().
    """
    if not account_id.strip():
        raise ValueError("account_id не может быть пустым.")

    response = await client.operations.stub.GetPortfolio(
        request=operations_pb2.PortfolioRequest(account_id=account_id),
        metadata=client.operations.metadata,
    )

    positions: list[TBankPortfolioPosition] = []

    for position in response.positions:
        positions.append(
            TBankPortfolioPosition(
                figi=position.figi,
                instrument_uid=position.instrument_uid,
                position_uid=position.position_uid,
                ticker=position.ticker,
                class_code=position.class_code,
                instrument_type=position.instrument_type,
                quantity=_quotation_to_decimal(position.quantity),
                quantity_lots=_quotation_to_decimal(position.quantity_lots),
                blocked=position.blocked,
                blocked_lots=_quotation_to_decimal(position.blocked_lots),
                average_position_price=_money_to_decimal(
                    position.average_position_price
                ),
                average_position_price_fifo=_money_to_decimal(
                    position.average_position_price_fifo
                ),
                current_price=_money_to_decimal(position.current_price),
                expected_yield=_money_to_decimal(position.expected_yield),
                expected_yield_fifo=_money_to_decimal(position.expected_yield_fifo),
                daily_yield=_money_to_decimal(position.daily_yield),
                currency=position.average_position_price.currency.upper(),
            )
        )

    return positions


def find_position_by_instrument_uid(
    positions: list[TBankPortfolioPosition],
    instrument_uid: str,
) -> TBankPortfolioPosition | None:
    """
    Найти позицию по instrument_uid.
    """
    if not instrument_uid.strip():
        raise ValueError("instrument_uid не может быть пустым.")

    for position in positions:
        if position.instrument_uid == instrument_uid:
            return position

    return None


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv
    from grpc import aio
    from t_tech.invest import AsyncClient

    # SBER из твоих логов:
    TEST_INSTRUMENT_UID = "e6123145-9665-43e0-8413-cd61b8aa9b13"

    def print_position(position: TBankPortfolioPosition) -> None:
        print(f"{position.ticker}_{position.class_code} | {position.instrument_type}")
        print(f"  figi:                         {position.figi}")
        print(f"  instrument_uid:               {position.instrument_uid}")
        print(f"  position_uid:                 {position.position_uid}")
        print()
        print(f"  quantity:                     {position.quantity}")
        print(f"  quantity_lots:                {position.quantity_lots}")
        print(f"  blocked:                      {position.blocked}")
        print(f"  blocked_lots:                 {position.blocked_lots}")
        print()
        print(f"  average_position_price:       {position.average_position_price:.2f} {position.currency}")
        print(f"  average_position_price_fifo:  {position.average_position_price_fifo:.2f} {position.currency}")
        print(f"  current_price:                {position.current_price:.2f} {position.currency}")
        print()
        print(f"  expected_yield:               {position.expected_yield:.2f} {position.currency}")
        print(f"  expected_yield_fifo:          {position.expected_yield_fifo:.2f} {position.currency}")
        print(f"  daily_yield:                  {position.daily_yield:.2f} {position.currency}")
        print()

    def print_positions(positions: list[TBankPortfolioPosition]) -> None:
        print(f"Всего позиций в портфеле: {len(positions)}")
        print()

        if not positions:
            print("Позиции не найдены.")
            return

        for number, position in enumerate(positions, start=1):
            print(f"Позиция #{number}")
            print_position(position)

    async def main() -> None:
        load_dotenv()

        token = os.environ["INVEST_TOKEN"]
        account_id = os.environ["INVEST_ACCOUNT_ID"]

        try:
            async with AsyncClient(token) as client:
                positions = await get_portfolio_positions(
                    client=client,
                    account_id=account_id,
                )
        except KeyError as error:
            print(f"Ошибка: в .env не задана переменная {error}.")
            return
        except aio.AioRpcError as error:
            print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
            return

        print_positions(positions)

        print("Проверка конкретного инструмента:")
        print()

        selected_position = find_position_by_instrument_uid(
            positions=positions,
            instrument_uid=TEST_INSTRUMENT_UID,
        )

        if selected_position is None:
            print(f"Позиция по instrument_uid={TEST_INSTRUMENT_UID} не найдена.")
            return

        print_position(selected_position)

    asyncio.run(main())