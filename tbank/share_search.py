from tbank.shares import TBankShare, get_shares


def find_shares_by_ticker(shares: list[TBankShare], ticker: str) -> list[TBankShare]:
    """
    Найти акции по точному тикеру.

    Функция не фильтрует по бирже, валюте, доступности API или торговому статусу.
    Она только ищет все инструменты с указанным ticker.
    """
    normalized_ticker = ticker.strip().upper()

    if not normalized_ticker:
        raise ValueError("ticker не может быть пустым.")

    return [share for share in shares if share.ticker.upper() == normalized_ticker]


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv
    from grpc import aio
    from t_tech.invest import AsyncClient

    TEST_TICKER = "SBER"

    def print_found_shares(ticker: str, shares: list[TBankShare]) -> None:
        print(f"Поиск акций по тикеру: {ticker.upper()}")
        print(f"Найдено инструментов: {len(shares)}")
        print()

        if not shares:
            print("Инструменты не найдены.")
            return

        for number, share in enumerate(shares, start=1):
            print(f"{number}. {share.ticker} | {share.name}")
            print(f"   figi:         {share.figi}")
            print(f"   uid:          {share.uid}")
            print(f"   position_uid: {share.position_uid}")
            print(f"   class_code:   {share.class_code}")
            print(f"   isin:         {share.isin}")
            print(f"   lot:          {share.lot}")
            print(f"   currency:     {share.currency}")
            print(f"   exchange:     {share.exchange}")
            print(f"   real_exch:    {share.real_exchange}")
            print(f"   instr_exch:   {share.instrument_exchange}")
            print(f"   status:       {share.trading_status}")
            print(f"   api_trade:    {share.api_trade_available_flag}")
            print(f"   buy:          {share.buy_available_flag}")
            print(f"   sell:         {share.sell_available_flag}")
            print(f"   qual_only:    {share.for_qual_investor_flag}")
            print(f"   sector:       {share.sector}")
            print(f"   risk_country: {share.country_of_risk}")
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

        found_shares = find_shares_by_ticker(shares=shares, ticker=TEST_TICKER)
        print_found_shares(ticker=TEST_TICKER, shares=found_shares)

    asyncio.run(main())