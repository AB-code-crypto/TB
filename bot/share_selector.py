from tbank.shares import TBankShare
from tbank.share_search import find_shares_by_ticker


class ShareSelectionError(Exception):
    pass


def select_trade_share_by_ticker(
    shares: list[TBankShare],
    ticker: str,
) -> TBankShare:
    """
    Выбрать основную торговую акцию по тикеру.

    Это уже логика робота, а не API-слой.

    Правила для первой версии:
    - ticker должен совпадать точно;
    - валюта RUB;
    - реальная площадка MOEX;
    - class_code TQBR;
    - инструмент доступен через API;
    - покупка доступна;
    - продажа доступна;
    - инструмент не только для квалифицированных инвесторов.

    Торговый статус NORMAL_TRADING здесь пока НЕ проверяем.
    Для него будет отдельный кирпич через MarketDataService.GetTradingStatus.
    """
    found_shares = find_shares_by_ticker(shares=shares, ticker=ticker)

    if not found_shares:
        raise ShareSelectionError(f"Акции с тикером {ticker.upper()} не найдены.")

    suitable_shares = [
        share
        for share in found_shares
        if share.currency == "RUB"
        and share.real_exchange == "REAL_EXCHANGE_MOEX"
        and share.class_code == "TQBR"
        and share.api_trade_available_flag
        and share.buy_available_flag
        and share.sell_available_flag
        and not share.for_qual_investor_flag
    ]

    if not suitable_shares:
        raise ShareSelectionError(
            f"Для тикера {ticker.upper()} не найден подходящий торговый инструмент."
        )

    if len(suitable_shares) > 1:
        raise ShareSelectionError(
            f"Для тикера {ticker.upper()} найдено несколько подходящих инструментов: "
            f"{len(suitable_shares)}. Нужен ручной разбор."
        )

    return suitable_shares[0]


if __name__ == "__main__":
    import asyncio
    import os

    from dotenv import load_dotenv
    from grpc import aio
    from t_tech.invest import AsyncClient

    from tbank.shares import get_shares

    TEST_TICKER = "SBER"

    def print_share(share: TBankShare) -> None:
        print("Выбран торговый инструмент:")
        print()
        print(f"ticker:       {share.ticker}")
        print(f"name:         {share.name}")
        print(f"figi:         {share.figi}")
        print(f"uid:          {share.uid}")
        print(f"position_uid: {share.position_uid}")
        print(f"class_code:   {share.class_code}")
        print(f"isin:         {share.isin}")
        print(f"lot:          {share.lot}")
        print(f"currency:     {share.currency}")
        print(f"exchange:     {share.exchange}")
        print(f"real_exch:    {share.real_exchange}")
        print(f"instr_exch:   {share.instrument_exchange}")
        print(f"status:       {share.trading_status}")
        print(f"api_trade:    {share.api_trade_available_flag}")
        print(f"buy:          {share.buy_available_flag}")
        print(f"sell:         {share.sell_available_flag}")
        print(f"qual_only:    {share.for_qual_investor_flag}")
        print(f"sector:       {share.sector}")
        print(f"risk_country: {share.country_of_risk}")
        print(f"min_step:     {share.min_price_increment}")

    async def main() -> None:
        load_dotenv()

        token = os.environ["INVEST_TOKEN"]

        try:
            async with AsyncClient(token) as client:
                shares = await get_shares(client)

            selected_share = select_trade_share_by_ticker(
                shares=shares,
                ticker=TEST_TICKER,
            )

        except KeyError as error:
            print(f"Ошибка: в .env не задана переменная {error}.")
            return
        except aio.AioRpcError as error:
            print(f"Ошибка gRPC T-Invest API: {error.code().name}: {error.details()}")
            return
        except ShareSelectionError as error:
            print(f"Ошибка выбора акции: {error}")
            return

        print_share(selected_share)

    asyncio.run(main())