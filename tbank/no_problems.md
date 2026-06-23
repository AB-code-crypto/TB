Да. Чтобы мы не искали это заново, скажи мне примерно так:

> **У нас в T-Invest SDK deprecated high-level методы. Мы решили не подавлять warning, а обходить deprecated-обёртки через прямой gRPC `stub` + `*_pb2` внутри папки `tbank`. Проверь исходник deprecated-метода через `inspect.getsource`, найди там `self.stub.Get...`, используй соответствующий protobuf request из `t_tech.invest.grpc.*_pb2`, а наружу возвращай наш DTO. Не тащи сырой SDK/protobuf за пределы `tbank`.**

Этого будет достаточно.

## Шпаргалка по решению

Что **не делаем**:

```python
await client.users.get_accounts()
await client.operations.get_portfolio(account_id=account_id)
await client.operations.get_positions(account_id=account_id)
```

Потому что это deprecated high-level wrappers.

Что **делаем**:

```python
response = await client.users.stub.GetAccounts(
    request=users_pb2.GetAccountsRequest(),
    metadata=client.users.metadata,
)
```

```python
portfolio = await client.operations.stub.GetPortfolio(
    request=operations_pb2.PortfolioRequest(account_id=account_id),
    metadata=client.operations.metadata,
)
```

```python
positions = await client.operations.stub.GetPositions(
    request=operations_pb2.PositionsRequest(account_id=account_id),
    metadata=client.operations.metadata,
)
```

## Алгоритм для новых deprecated-warning

Если опять увидим warning типа:

```text
DeprecatedWarning: some_method is deprecated as of 1.0.0.
```

то действуем так:

```text
1. Не подавляем warning.
2. Смотрим исходник deprecated-метода через inspect.getsource().
3. Внутри ищем реальный вызов self.stub.GetSomething / PostSomething.
4. Находим request-класс в нужном *_pb2:
   users_pb2, operations_pb2, marketdata_pb2, instruments_pb2, orders_pb2 и т.д.
5. Вызываем stub напрямую:
   client.<service>.stub.<RpcMethod>(request=..., metadata=client.<service>.metadata)
6. Ответ protobuf маппим в наш DTO.
7. В bot/gui/bd protobuf не отдаём.
```

## Ещё одна важная договорённость

Если PyCharm снова подчёркивает:

```text
Unresolved attribute reference 'users' for class 'AsyncClient'
```

то не мучаем `AsyncClient` напрямую, а используем `Protocol`:

```python
from typing import Any, Protocol


class AccountsApiClient(Protocol):
    users: Any
```

И функция получает не `AsyncClient`, а наш протокол:

```python
async def get_accounts(client: AccountsApiClient) -> list[TBankAccount]:
    ...
```

## Для enum-полей

Не используем универсальный `getattr`. Делаем явно:

```python
users_pb2.AccountType.Name(account.type)
users_pb2.AccountStatus.Name(account.status)
users_pb2.AccessLevel.Name(account.access_level)
```

Это соответствует твоему правилу: **падаем быстро, явно и громко**, а не прячем ошибку.

Короткая команда для будущего:
**“Вспомни наше решение по deprecated T-Invest SDK: не suppress warnings, а прямой stub + pb2 внутри tbank, DTO наружу.”**
