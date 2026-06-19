from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

from t_tech.invest.grpc import operations_pb2


NANO = Decimal("1000000000")


class OperationsService(Protocol):
    stub: Any
    metadata: Any


class OperationsApiClient(Protocol):
    operations: OperationsService


@dataclass(frozen=True)
class MoneyBalance:
    currency: str
    total: Decimal
    blocked: Decimal
    available: Decimal


@dataclass(frozen=True)
class PortfolioBalance:
    account_id: str
    total_amount_portfolio: Decimal
    total_amount_currencies: Decimal
    total_amount_shares: Decimal
    total_amount_bonds: Decimal
    total_amount_etf: Decimal
    money: list[MoneyBalance]


def _money_to_decimal(value) -> Decimal:
    """
    Преобразовать protobuf MoneyValue в Decimal.
    """
    return Decimal(value.units) + Decimal(value.nano) / NANO


def _collect_money_by_currency(values) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}

    for value in values:
        currency = value.currency.upper()
        amount = _money_to_decimal(value)

        if currency not in result:
            result[currency] = Decimal("0")

        result[currency] += amount

    return result


async def get_balance(client: OperationsApiClient, account_id: str) -> PortfolioBalance:
    """
    Получить баланс и общую оценку портфеля по конкретному брокерскому счету.

    Используем прямые gRPC-вызовы:
    - stub.GetPortfolio()
    - stub.GetPositions()

    Не используем deprecated-обёртки:
    - client.operations.get_portfolio()
    - client.operations.get_positions()
    """
    portfolio = await client.operations.stub.GetPortfolio(
        request=operations_pb2.PortfolioRequest(account_id=account_id),
        metadata=client.operations.metadata,
    )

    positions = await client.operations.stub.GetPositions(
        request=operations_pb2.PositionsRequest(account_id=account_id),
        metadata=client.operations.metadata,
    )

    money_by_currency = _collect_money_by_currency(positions.money)
    blocked_by_currency = _collect_money_by_currency(positions.blocked)

    currencies = sorted(set(money_by_currency) | set(blocked_by_currency))

    money: list[MoneyBalance] = []

    for currency in currencies:
        total = money_by_currency.get(currency, Decimal("0"))
        blocked = blocked_by_currency.get(currency, Decimal("0"))
        available = total - blocked

        money.append(
            MoneyBalance(
                currency=currency,
                total=total,
                blocked=blocked,
                available=available,
            )
        )

    return PortfolioBalance(
        account_id=account_id,
        total_amount_portfolio=_money_to_decimal(portfolio.total_amount_portfolio),
        total_amount_currencies=_money_to_decimal(portfolio.total_amount_currencies),
        total_amount_shares=_money_to_decimal(portfolio.total_amount_shares),
        total_amount_bonds=_money_to_decimal(portfolio.total_amount_bonds),
        total_amount_etf=_money_to_decimal(portfolio.total_amount_etf),
        money=money,
    )


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv
    from grpc import aio
    from t_tech.invest import AsyncClient

    def print_balance(balance: PortfolioBalance) -> None:
        print(f"Счёт: {balance.account_id}")
        print()
        print("Портфель:")
        print(f"  Всего:      {balance.total_amount_portfolio:.2f}")
        print(f"  Валюта:     {balance.total_amount_currencies:.2f}")
        print(f"  Акции:      {balance.total_amount_shares:.2f}")
        print(f"  Облигации:  {balance.total_amount_bonds:.2f}")
        print(f"  Фонды:      {balance.total_amount_etf:.2f}")
        print()

        print("Деньги:")

        if not balance.money:
            print("  Денежные позиции не найдены.")
            return

        for money in balance.money:
            print(f"  {money.currency}:")
            print(f"    всего:     {money.total:.2f}")
            print(f"    заблок.:   {money.blocked:.2f}")
            print(f"    доступно:  {money.available:.2f}")
            print()

    async def main() -> None:
        load_dotenv()

        token = os.environ["INVEST_TOKEN"]
        account_id = os.environ["INVEST_ACCOUNT_ID"]

        try:
            async with AsyncClient(token) as client:
                balance = await get_balance(client=client, account_id=account_id)
        except KeyError as error:
            print(f"Ошибка: в .env не задана переменная {error}.")
            return
        except aio.AioRpcError as error:
            print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
            return

        print_balance(balance)

    asyncio.run(main())