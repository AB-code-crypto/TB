from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

from t_tech.invest.grpc import common_pb2, instruments_pb2


NANO = Decimal("1000000000")


class InstrumentsService(Protocol):
    stub: Any
    metadata: Any


class InstrumentsApiClient(Protocol):
    instruments: InstrumentsService


@dataclass(frozen=True)
class TBankShare:
    figi: str
    uid: str
    position_uid: str
    ticker: str
    class_code: str
    isin: str
    name: str
    lot: int
    currency: str

    exchange: str
    real_exchange: str
    instrument_exchange: str

    share_type: str
    trading_status: str

    api_trade_available_flag: bool
    buy_available_flag: bool
    sell_available_flag: bool

    for_iis_flag: bool
    for_qual_investor_flag: bool
    weekend_flag: bool
    blocked_tca_flag: bool
    liquidity_flag: bool

    required_tests: list[str]

    country_of_risk: str
    sector: str

    min_price_increment: Decimal


def _quotation_to_decimal(value) -> Decimal:
    return Decimal(value.units) + Decimal(value.nano) / NANO


async def get_shares(client: InstrumentsApiClient) -> list[TBankShare]:
    """
    Получить список всех акций из T-Invest API.

    Используем прямой gRPC-вызов stub.Shares(), а не high-level SDK-обёртку.
    Фильтрацию здесь не делаем: этот слой только получает и нормализует данные.
    """
    response = await client.instruments.stub.Shares(
        request=instruments_pb2.InstrumentsRequest(
            instrument_status=common_pb2.INSTRUMENT_STATUS_ALL,
        ),
        metadata=client.instruments.metadata,
    )

    shares: list[TBankShare] = []

    for share in response.instruments:
        shares.append(
            TBankShare(
                figi=share.figi,
                uid=share.uid,
                position_uid=share.position_uid,
                ticker=share.ticker,
                class_code=share.class_code,
                isin=share.isin,
                name=share.name,
                lot=share.lot,
                currency=share.currency.upper(),
                exchange=share.exchange,
                real_exchange=common_pb2.RealExchange.Name(share.real_exchange),
                instrument_exchange=instruments_pb2.InstrumentExchangeType.Name(
                    share.instrument_exchange
                ),
                share_type=instruments_pb2.ShareType.Name(share.share_type),
                trading_status=common_pb2.SecurityTradingStatus.Name(
                    share.trading_status
                ),
                api_trade_available_flag=share.api_trade_available_flag,
                buy_available_flag=share.buy_available_flag,
                sell_available_flag=share.sell_available_flag,
                for_iis_flag=share.for_iis_flag,
                for_qual_investor_flag=share.for_qual_investor_flag,
                weekend_flag=share.weekend_flag,
                blocked_tca_flag=share.blocked_tca_flag,
                liquidity_flag=share.liquidity_flag,
                required_tests=list(share.required_tests),
                country_of_risk=share.country_of_risk,
                sector=share.sector,
                min_price_increment=_quotation_to_decimal(share.min_price_increment),
            )
        )

    return shares


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv
    from grpc import aio
    from t_tech.invest import AsyncClient


    def print_shares_summary(shares: list[TBankShare]) -> None:
        total = len(shares)

        api_trade_available = sum(1 for share in shares if share.api_trade_available_flag)
        buy_available = sum(1 for share in shares if share.buy_available_flag)
        sell_available = sum(1 for share in shares if share.sell_available_flag)

        rub = sum(1 for share in shares if share.currency == "RUB")
        usd = sum(1 for share in shares if share.currency == "USD")
        eur = sum(1 for share in shares if share.currency == "EUR")

        moex = sum(1 for share in shares if share.real_exchange == "REAL_EXCHANGE_MOEX")
        dealer = sum(1 for share in shares if share.real_exchange == "REAL_EXCHANGE_DEALER")
        rts = sum(1 for share in shares if share.real_exchange == "REAL_EXCHANGE_RTS")
        unspecified_exchange = sum(
            1 for share in shares if share.real_exchange == "REAL_EXCHANGE_UNSPECIFIED"
        )

        normal_trading = sum(
            1
            for share in shares
            if share.trading_status == "SECURITY_TRADING_STATUS_NORMAL_TRADING"
        )
        break_in_trading = sum(
            1
            for share in shares
            if share.trading_status == "SECURITY_TRADING_STATUS_BREAK_IN_TRADING"
        )
        not_available = sum(
            1
            for share in shares
            if share.trading_status
            == "SECURITY_TRADING_STATUS_NOT_AVAILABLE_FOR_TRADING"
        )

        qualified_only = sum(1 for share in shares if share.for_qual_investor_flag)
        weekend_available = sum(1 for share in shares if share.weekend_flag)
        liquid = sum(1 for share in shares if share.liquidity_flag)

        print("Сводка по акциям:")
        print(f"  Всего акций:                 {total}")
        print()
        print("Доступность:")
        print(f"  Доступны через API:           {api_trade_available}")
        print(f"  Доступны на покупку:          {buy_available}")
        print(f"  Доступны на продажу:          {sell_available}")
        print()
        print("Валюты:")
        print(f"  RUB:                          {rub}")
        print(f"  USD:                          {usd}")
        print(f"  EUR:                          {eur}")
        print()
        print("Биржи / реальные площадки:")
        print(f"  MOEX:                         {moex}")
        print(f"  RTS / СПБ:                    {rts}")
        print(f"  DEALER / OTC:                 {dealer}")
        print(f"  UNSPECIFIED:                  {unspecified_exchange}")
        print()
        print("Торговые статусы:")
        print(f"  NORMAL_TRADING:               {normal_trading}")
        print(f"  BREAK_IN_TRADING:             {break_in_trading}")
        print(f"  NOT_AVAILABLE_FOR_TRADING:    {not_available}")
        print()
        print("Дополнительные признаки:")
        print(f"  Только для квалов:            {qualified_only}")
        print(f"  Доступны в выходные:          {weekend_available}")
        print(f"  liquidity_flag=True:          {liquid}")
        print()


    def print_shares(shares: list[TBankShare], limit: int = 30) -> None:
        print_shares_summary(shares)

        if not shares:
            print("Список акций пуст.")
            return

        print(f"Первые {min(limit, len(shares))} акций:")
        print()

        for number, share in enumerate(shares[:limit], start=1):
            print(f"{number}. {share.ticker} | {share.name}")
            print(f"   figi:       {share.figi}")
            print(f"   uid:        {share.uid}")
            print(f"   class_code: {share.class_code}")
            print(f"   lot:        {share.lot}")
            print(f"   currency:   {share.currency}")
            print(f"   exchange:   {share.exchange}")
            print(f"   real_exch:  {share.real_exchange}")
            print(f"   instr_exch: {share.instrument_exchange}")
            print(f"   status:     {share.trading_status}")
            print(f"   api_trade:  {share.api_trade_available_flag}")
            print(f"   buy:        {share.buy_available_flag}")
            print(f"   sell:       {share.sell_available_flag}")
            print()

    async def main() -> None:
        load_dotenv()

        token = os.environ["INVEST_TOKEN"]

        try:
            async with AsyncClient(token) as client:
                shares = await get_shares(client)
        except KeyError as error:
            print(f"Ошибка: в .env не задана переменная {error}.")
            return
        except aio.AioRpcError as error:
            print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
            return

        print_shares(shares)

    asyncio.run(main())