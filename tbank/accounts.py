from dataclasses import dataclass
from typing import Any, Protocol

from t_tech.invest.grpc import users_pb2


class UsersService(Protocol):
    stub: Any
    metadata: Any


class AccountsApiClient(Protocol):
    users: UsersService


@dataclass(frozen=True)
class TBankAccount:
    account_id: str
    name: str
    account_type: str
    status: str
    access_level: str


async def get_accounts(client: AccountsApiClient) -> list[TBankAccount]:
    """
    Получить список брокерских счетов пользователя T-Invest.

    Используем прямой gRPC-вызов stub.GetAccounts(), а не deprecated-обёртку
    client.users.get_accounts().
    """
    response = await client.users.stub.GetAccounts(
        request=users_pb2.GetAccountsRequest(),
        metadata=client.users.metadata,
    )

    accounts: list[TBankAccount] = []

    for account in response.accounts:
        accounts.append(
            TBankAccount(
                account_id=account.id,
                name=account.name,
                account_type=users_pb2.AccountType.Name(account.type),
                status=users_pb2.AccountStatus.Name(account.status),
                access_level=users_pb2.AccessLevel.Name(account.access_level),
            )
        )

    return accounts


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv
    from grpc import aio
    from t_tech.invest import AsyncClient

    def print_accounts(accounts: list[TBankAccount]) -> None:
        if not accounts:
            print("Брокерские счета не найдены.")
            return

        print("Доступные брокерские счета:")
        print()

        for number, account in enumerate(accounts, start=1):
            print(f"{number}. {account.name}")
            print(f"   account_id:   {account.account_id}")
            print(f"   type:         {account.account_type}")
            print(f"   status:       {account.status}")
            print(f"   access_level: {account.access_level}")
            print()

    async def main() -> None:
        load_dotenv()

        token = os.environ["INVEST_TOKEN"]

        try:
            async with AsyncClient(token) as client:
                accounts = await get_accounts(client)
        except KeyError as error:
            print(f"Ошибка: в .env не задана переменная {error}.")
            return
        except aio.AioRpcError as error:
            print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
            return

        print_accounts(accounts)

    asyncio.run(main())