from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from t_tech.invest import AsyncClient

from bd.robot_order import create_robot_order, mark_robot_order_failed, mark_robot_order_sent
from bd.robot_position import apply_robot_order_fill, list_robot_positions
from bd.settings_storage import load_app_settings, load_selected_shares
from bot.growth_scanner import GrowthScanReport, GrowthScanResult
from tbank.balance import PortfolioBalance, get_balance
from tbank.last_prices import get_last_price
from tbank.order_execution import TBankPostOrderResult, post_market_order
from tbank.shares import TBankShare


AUTO_ORDER_SOURCE = "AUTO_GROWTH_MONITOR"


@dataclass(frozen=True)
class RobotMarketOrderReport:
    share: TBankShare
    result: TBankPostOrderResult
    quantity_lots: int
    estimated_amount: Decimal


@dataclass(frozen=True)
class ExitOrdersResult:
    log_lines: list[str]
    exited_instrument_uids: set[str]
    portfolio_changed: bool


def _parse_positive_decimal_setting(
    settings: dict[str, str],
    key: str,
    label: str,
) -> Decimal:
    raw_value = settings[key].strip().replace(",", ".").replace(" ", "")

    try:
        value = Decimal(raw_value)
    except InvalidOperation as error:
        raise ValueError(
            f"{label}: некорректное число в настройках: {settings[key]!r}"
        ) from error

    if value <= 0:
        raise ValueError(f"{label} должен быть больше 0.")

    return value


def _format_percent(value: Decimal) -> str:
    return f"{value:.4f}%"


SUPPORTED_TRADE_CURRENCIES = ("RUB", "USD", "EUR")


def _get_available_money_by_currency(
    balance: PortfolioBalance,
) -> dict[str, Decimal]:
    return {
        money.currency.upper(): money.available
        for money in balance.money
    }


def _estimate_robot_used_money_by_currency(
    account_id: str,
) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = {
        currency: Decimal("0")
        for currency in SUPPORTED_TRADE_CURRENCIES
    }

    for position in list_robot_positions(account_id=account_id):
        if position.robot_lots <= 0:
            continue

        if position.avg_price <= 0:
            continue

        currency = position.currency.upper()
        totals.setdefault(currency, Decimal("0"))
        totals[currency] += (
            position.avg_price
            * Decimal(position.robot_lots * position.lot)
        )

    return totals


def _calculate_position_growth_percent(
    current_price: Decimal,
    avg_price: Decimal,
) -> Decimal:
    if avg_price <= 0:
        raise ValueError("avg_price должен быть больше 0.")

    return (
        current_price / avg_price - Decimal("1")
    ) * Decimal("100")


async def _send_robot_market_order(
    client,
    account_id: str,
    share: TBankShare,
    side: str,
    quantity_lots: int,
    estimated_amount: Decimal,
    source: str = AUTO_ORDER_SOURCE,
) -> RobotMarketOrderReport:
    order_request_id = uuid4().hex
    robot_order_id = create_robot_order(
        account_id=account_id,
        order_request_id=order_request_id,
        side=side,
        order_type="MARKET",
        instrument_uid=share.uid,
        ticker=share.ticker,
        class_code=share.class_code,
        name=share.name,
        quantity_lots=quantity_lots,
        quantity_shares=quantity_lots * share.lot,
        limit_price=Decimal("0"),
        requested_amount=estimated_amount,
        source=source,
    )

    try:
        result = await post_market_order(
            client=client,
            account_id=account_id,
            order_request_id=order_request_id,
            share=share,
            side=side,
            quantity_lots=quantity_lots,
        )
    except Exception as error:
        mark_robot_order_failed(
            robot_order_id=robot_order_id,
            error_text=str(error),
        )
        raise

    mark_robot_order_sent(
        robot_order_id=robot_order_id,
        broker_order_id=result.broker_order_id,
        execution_report_status=result.execution_report_status,
        lots_executed=result.lots_executed,
        executed_order_price=result.executed_order_price,
        total_order_amount=result.total_order_amount,
    )

    if result.lots_executed > 0:
        apply_robot_order_fill(
            account_id=account_id,
            share=share,
            side=side,
            executed_lots=result.lots_executed,
            executed_price=result.executed_order_price,
            robot_order_id=robot_order_id,
            source=source,
        )

    return RobotMarketOrderReport(
        share=share,
        result=result,
        quantity_lots=quantity_lots,
        estimated_amount=estimated_amount,
    )


async def _execute_exit_orders(
    client,
    account_id: str,
    shares_by_uid: dict[str, TBankShare],
    results_by_uid: dict[str, GrowthScanResult],
    take_profit_percent: Decimal,
    stop_loss_percent: Decimal,
    allow_sell: bool,
) -> ExitOrdersResult:
    log_lines: list[str] = []
    exited_instrument_uids: set[str] = set()
    portfolio_changed = False
    positions = [
        position
        for position in list_robot_positions(account_id=account_id)
        if position.robot_lots > 0
    ]

    if not positions:
        return ExitOrdersResult(
            log_lines=log_lines,
            exited_instrument_uids=exited_instrument_uids,
            portfolio_changed=False,
        )

    if not allow_sell:
        log_lines.append(
            "Автоторговля: продажи запрещены настройкой, выход из позиций не выполнялся."
        )
        return ExitOrdersResult(
            log_lines=log_lines,
            exited_instrument_uids=exited_instrument_uids,
            portfolio_changed=False,
        )

    for position in positions:
        share = shares_by_uid.get(position.instrument_uid)

        if share is None:
            log_lines.append(
                "Автопродажа пропущена: акция не найдена в рабочем списке: "
                f"{position.ticker}_{position.class_code}."
            )
            continue

        result = results_by_uid.get(position.instrument_uid)
        price_source = "текущий расчёт роста"

        if result is None:
            try:
                last_price = await get_last_price(
                    client=client,
                    instrument_id=share.uid,
                )
            except Exception as error:
                log_lines.append(
                    "Автопродажа пропущена: цена не получена ни из текущего расчёта, "
                    "ни отдельным запросом: "
                    f"{position.ticker}_{position.class_code}, "
                    f"error={type(error).__name__}: {error}"
                )
                continue

            current_price = last_price.price
            price_source = (
                "отдельный запрос последней цены, "
                f"time_utc={last_price.time}"
            )
            log_lines.append(
                "Цена для проверки выхода получена отдельным запросом: "
                f"{position.ticker}_{position.class_code}, "
                f"price={current_price}, "
                f"time_utc={last_price.time}."
            )
        else:
            current_price = result.current_price

        if position.avg_price <= 0:
            log_lines.append(
                "Автопродажа пропущена: средняя цена позиции некорректна: "
                f"{position.ticker}_{position.class_code}, avg_price={position.avg_price}."
            )
            continue

        position_growth_percent = _calculate_position_growth_percent(
            current_price=current_price,
            avg_price=position.avg_price,
        )

        if position_growth_percent >= take_profit_percent:
            exit_reason = (
                "take-profit "
                f"{_format_percent(position_growth_percent)} >= "
                f"{_format_percent(take_profit_percent)}"
            )
        elif position_growth_percent <= -stop_loss_percent:
            exit_reason = (
                "stop-loss "
                f"{_format_percent(position_growth_percent)} <= "
                f"-{_format_percent(stop_loss_percent)}"
            )
        else:
            continue

        estimated_amount = (
            current_price * Decimal(position.robot_lots * position.lot)
        )

        try:
            order_report = await _send_robot_market_order(
                client=client,
                account_id=account_id,
                share=share,
                side="SELL",
                quantity_lots=position.robot_lots,
                estimated_amount=estimated_amount,
            )
        except Exception as error:
            log_lines.append(
                "Автопродажа не отправлена: "
                f"{position.ticker}_{position.class_code}, "
                f"reason={exit_reason}, "
                f"price={current_price}, "
                f"price_source={price_source}, "
                f"error={type(error).__name__}: {error}"
            )
            continue

        if order_report.result.lots_executed > 0:
            exited_instrument_uids.add(position.instrument_uid)
            portfolio_changed = True

        log_lines.append(
            "Автопродажа отправлена: "
            f"{position.ticker}_{position.class_code}, "
            f"лотов={order_report.quantity_lots}, "
            f"причина={exit_reason}, "
            f"price={current_price}, "
            f"price_source={price_source}, "
            f"broker_order_id={order_report.result.broker_order_id}, "
            f"status={order_report.result.execution_report_status}, "
            f"исполнено={order_report.result.lots_executed}."
        )

    return ExitOrdersResult(
        log_lines=log_lines,
        exited_instrument_uids=exited_instrument_uids,
        portfolio_changed=portfolio_changed,
    )


async def _execute_entry_orders(
    client,
    account_id: str,
    shares_by_uid: dict[str, TBankShare],
    new_signal_records: list[tuple[int, GrowthScanResult]],
    requested_amounts_by_currency: dict[str, Decimal],
    bot_money_limits_by_currency: dict[str, Decimal],
    allow_buy: bool,
    blocked_entry_uids: set[str],
) -> tuple[list[str], bool]:
    log_lines: list[str] = []
    portfolio_changed = False

    if not new_signal_records:
        return log_lines, portfolio_changed

    if not allow_buy:
        log_lines.append(
            "Автопокупка: покупки запрещены настройкой, новые сигналы не исполнялись."
        )
        return log_lines, portfolio_changed

    open_positions = [
        position
        for position in list_robot_positions(account_id=account_id)
        if position.robot_lots > 0
    ]
    open_robot_uids = {
        position.instrument_uid
        for position in open_positions
    }
    used_money_by_currency = _estimate_robot_used_money_by_currency(
        account_id=account_id
    )
    remaining_limits_by_currency = {
        currency: (
            bot_money_limits_by_currency[currency]
            - used_money_by_currency.get(currency, Decimal("0"))
        )
        for currency in bot_money_limits_by_currency
    }

    balance = await get_balance(client, account_id)
    available_money_by_currency = _get_available_money_by_currency(balance)

    for signal_id, signal in new_signal_records:
        if signal.instrument_uid in blocked_entry_uids:
            log_lines.append(
                "Автопокупка пропущена после выхода в том же цикле: "
                f"{signal.ticker}_{signal.class_code}, signal_id={signal_id}."
            )
            continue

        if signal.instrument_uid in open_robot_uids:
            log_lines.append(
                "Автопокупка пропущена без пирамидинга: "
                f"{signal.ticker}_{signal.class_code}, "
                f"signal_id={signal_id}, у робота уже есть позиция."
            )
            continue

        share = shares_by_uid.get(signal.instrument_uid)

        if share is None:
            log_lines.append(
                "Автопокупка пропущена: акция не найдена в рабочем списке: "
                f"{signal.ticker}_{signal.class_code}, signal_id={signal_id}."
            )
            continue

        currency = signal.currency.upper()

        if currency not in requested_amounts_by_currency:
            log_lines.append(
                "Автопокупка пропущена: валюта не поддерживается настройками: "
                f"{signal.ticker}_{signal.class_code}, currency={currency}."
            )
            continue

        requested_amount = requested_amounts_by_currency[currency]
        remaining_bot_limit = remaining_limits_by_currency.get(
            currency,
            Decimal("0"),
        )
        available_money = available_money_by_currency.get(
            currency,
            Decimal("0"),
        )

        if remaining_bot_limit <= 0:
            log_lines.append(
                "Автопокупка пропущена: лимит денег бота в валюте уже занят: "
                f"{signal.ticker}_{signal.class_code}, currency={currency}."
            )
            continue

        if available_money <= 0:
            log_lines.append(
                "Автопокупка пропущена: свободных денег в валюте нет: "
                f"{signal.ticker}_{signal.class_code}, currency={currency}."
            )
            continue

        one_lot_amount = signal.current_price * Decimal(signal.lot)

        if one_lot_amount <= 0:
            log_lines.append(
                "Автопокупка пропущена: стоимость одного лота некорректна: "
                f"{signal.ticker}_{signal.class_code}, one_lot={one_lot_amount}."
            )
            continue

        quantity_lots = int(requested_amount // one_lot_amount)

        if quantity_lots <= 0:
            log_lines.append(
                "Автопокупка пропущена: суммы одной покупки не хватает на 1 лот: "
                f"{signal.ticker}_{signal.class_code}, "
                f"amount={requested_amount} {currency}, "
                f"one_lot={one_lot_amount} {currency}."
            )
            continue

        estimated_amount = one_lot_amount * Decimal(quantity_lots)

        if estimated_amount > remaining_bot_limit:
            log_lines.append(
                "Автопокупка пропущена: недостаточно лимита денег бота: "
                f"{signal.ticker}_{signal.class_code}, "
                f"need={estimated_amount:.2f} {currency}, "
                f"remaining={remaining_bot_limit:.2f} {currency}."
            )
            continue

        if estimated_amount > available_money:
            log_lines.append(
                "Автопокупка пропущена: недостаточно свободных денег: "
                f"{signal.ticker}_{signal.class_code}, "
                f"need={estimated_amount:.2f} {currency}, "
                f"available={available_money:.2f} {currency}."
            )
            continue

        try:
            order_report = await _send_robot_market_order(
                client=client,
                account_id=account_id,
                share=share,
                side="BUY",
                quantity_lots=quantity_lots,
                estimated_amount=estimated_amount,
            )
        except Exception as error:
            log_lines.append(
                "Автопокупка не отправлена: "
                f"{signal.ticker}_{signal.class_code}, "
                f"signal_id={signal_id}, "
                f"лотов={quantity_lots}, "
                f"примерная сумма={estimated_amount:.2f} {currency}, "
                f"lot={signal.lot}, "
                f"current_price={signal.current_price}, "
                f"error={type(error).__name__}: {error}"
            )
            continue

        spent_amount = (
            order_report.result.total_order_amount
            if order_report.result.total_order_amount > 0
            else estimated_amount
        )

        if order_report.result.lots_executed > 0:
            available_money_by_currency[currency] = available_money - spent_amount
            remaining_limits_by_currency[currency] = (
                remaining_bot_limit - spent_amount
            )
            open_robot_uids.add(signal.instrument_uid)
            portfolio_changed = True

        log_lines.append(
            "Автопокупка отправлена: "
            f"{signal.ticker}_{signal.class_code}, "
            f"signal_id={signal_id}, "
            f"лотов={quantity_lots}, "
            f"примерная сумма={estimated_amount:.2f} {currency}, "
            f"broker_order_id={order_report.result.broker_order_id}, "
            f"status={order_report.result.execution_report_status}, "
            f"исполнено={order_report.result.lots_executed}."
        )

    return log_lines, portfolio_changed


BULK_CLOSE_ORDER_SOURCE = "BULK_CLOSE_BUTTON"


@dataclass(frozen=True)
class BulkRobotSellItem:
    ticker: str
    class_code: str
    currency: str
    requested_lots: int
    executed_lots: int
    broker_order_id: str
    execution_report_status: str
    error_text: str


@dataclass(frozen=True)
class BulkRobotSellReport:
    items: list[BulkRobotSellItem]

    @property
    def total_positions_count(self) -> int:
        return len(self.items)

    @property
    def fully_sold_count(self) -> int:
        return sum(
            1
            for item in self.items
            if (
                not item.error_text
                and item.executed_lots >= item.requested_lots
            )
        )

    @property
    def partially_sold_count(self) -> int:
        return sum(
            1
            for item in self.items
            if (
                not item.error_text
                and 0 < item.executed_lots < item.requested_lots
            )
        )

    @property
    def failed_count(self) -> int:
        return sum(
            1
            for item in self.items
            if item.error_text or item.executed_lots <= 0
        )

    @property
    def total_executed_lots(self) -> int:
        return sum(item.executed_lots for item in self.items)


async def sell_all_robot_positions(
    client,
    account_id: str,
    shares: list[TBankShare],
) -> BulkRobotSellReport:
    shares_by_uid = {
        share.uid: share
        for share in shares
    }
    positions = [
        position
        for position in list_robot_positions(account_id=account_id)
        if position.robot_lots > 0
    ]
    items: list[BulkRobotSellItem] = []

    for position in positions:
        share = shares_by_uid.get(position.instrument_uid)

        if share is None:
            items.append(
                BulkRobotSellItem(
                    ticker=position.ticker,
                    class_code=position.class_code,
                    currency=position.currency,
                    requested_lots=position.robot_lots,
                    executed_lots=0,
                    broker_order_id="",
                    execution_report_status="",
                    error_text=(
                        "Акция не найдена в справочнике T-Invest. "
                        "Рыночная заявка не отправлена."
                    ),
                )
            )
            continue

        if not share.api_trade_available_flag:
            items.append(
                BulkRobotSellItem(
                    ticker=position.ticker,
                    class_code=position.class_code,
                    currency=position.currency,
                    requested_lots=position.robot_lots,
                    executed_lots=0,
                    broker_order_id="",
                    execution_report_status="",
                    error_text="Торговля инструментом через API недоступна.",
                )
            )
            continue

        if not share.sell_available_flag:
            items.append(
                BulkRobotSellItem(
                    ticker=position.ticker,
                    class_code=position.class_code,
                    currency=position.currency,
                    requested_lots=position.robot_lots,
                    executed_lots=0,
                    broker_order_id="",
                    execution_report_status="",
                    error_text="Продажа инструмента сейчас недоступна.",
                )
            )
            continue

        if share.blocked_tca_flag:
            items.append(
                BulkRobotSellItem(
                    ticker=position.ticker,
                    class_code=position.class_code,
                    currency=position.currency,
                    requested_lots=position.robot_lots,
                    executed_lots=0,
                    broker_order_id="",
                    execution_report_status="",
                    error_text="Инструмент заблокирован для торговли.",
                )
            )
            continue

        estimated_amount = (
            position.avg_price
            * Decimal(position.robot_lots * position.lot)
        )

        try:
            order_report = await _send_robot_market_order(
                client=client,
                account_id=account_id,
                share=share,
                side="SELL",
                quantity_lots=position.robot_lots,
                estimated_amount=estimated_amount,
                source=BULK_CLOSE_ORDER_SOURCE,
            )
        except Exception as error:
            error_text = str(error).strip() or repr(error)
            items.append(
                BulkRobotSellItem(
                    ticker=position.ticker,
                    class_code=position.class_code,
                    currency=position.currency,
                    requested_lots=position.robot_lots,
                    executed_lots=0,
                    broker_order_id="",
                    execution_report_status="",
                    error_text=(
                        f"{type(error).__name__}: {error_text}"
                    ),
                )
            )
            continue

        result = order_report.result
        error_text = ""

        if result.lots_executed <= 0:
            error_text = (
                "Рыночная заявка отправлена, но ни один лот не исполнен."
            )

        items.append(
            BulkRobotSellItem(
                ticker=position.ticker,
                class_code=position.class_code,
                currency=position.currency,
                requested_lots=position.robot_lots,
                executed_lots=result.lots_executed,
                broker_order_id=result.broker_order_id,
                execution_report_status=result.execution_report_status,
                error_text=error_text,
            )
        )

    return BulkRobotSellReport(items=items)


async def execute_auto_trading_cycle(
    report: GrowthScanReport,
    new_signal_records: list[tuple[int, GrowthScanResult]],
) -> list[str]:
    settings = load_app_settings()

    if settings["auto_trading_enabled"] != "1":
        return []

    token = settings["token"].strip()
    account_id = settings["account_id"].strip()

    if not token:
        raise ValueError("В настройках сохранён пустой token.")

    if not account_id:
        raise ValueError("В настройках сохранён пустой account_id.")

    allow_buy = settings["allow_buy"] == "1"
    allow_sell = settings["allow_sell"] == "1"
    requested_amounts_by_currency = {
        currency: _parse_positive_decimal_setting(
            settings=settings,
            key=f"auto_buy_amount_{currency.lower()}",
            label=f"Сумма автопокупки {currency}",
        )
        for currency in SUPPORTED_TRADE_CURRENCIES
    }
    bot_money_limits_by_currency = {
        currency: _parse_positive_decimal_setting(
            settings=settings,
            key=f"bot_money_limit_{currency.lower()}",
            label=f"Лимит денег бота {currency}",
        )
        for currency in SUPPORTED_TRADE_CURRENCIES
    }
    take_profit_percent = _parse_positive_decimal_setting(
        settings=settings,
        key="take_profit_percent",
        label="Продать при прибыли, %",
    )
    stop_loss_percent = _parse_positive_decimal_setting(
        settings=settings,
        key="stop_loss_percent",
        label="Продать при убытке, %",
    )

    selected_shares = load_selected_shares()
    shares_by_uid = {
        share.uid: share
        for share in selected_shares
    }
    results_by_uid = {
        result.instrument_uid: result
        for result in report.results
    }

    log_lines = [
        "Реальная автоторговля включена: заявки отправляются рыночными ордерами."
    ]

    async with AsyncClient(token) as client:
        exit_result = await _execute_exit_orders(
            client=client,
            account_id=account_id,
            shares_by_uid=shares_by_uid,
            results_by_uid=results_by_uid,
            take_profit_percent=take_profit_percent,
            stop_loss_percent=stop_loss_percent,
            allow_sell=allow_sell,
        )
        entry_log_lines, entry_portfolio_changed = await _execute_entry_orders(
            client=client,
            account_id=account_id,
            shares_by_uid=shares_by_uid,
            new_signal_records=new_signal_records,
            requested_amounts_by_currency=requested_amounts_by_currency,
            bot_money_limits_by_currency=bot_money_limits_by_currency,
            allow_buy=allow_buy,
            blocked_entry_uids=exit_result.exited_instrument_uids,
        )

    log_lines.extend(exit_result.log_lines)
    log_lines.extend(entry_log_lines)

    portfolio_changed = (
        exit_result.portfolio_changed
        or entry_portfolio_changed
    )

    if portfolio_changed:
        log_lines.append(
            "Портфель робота изменён: обновляю свободные средства."
        )

    if len(log_lines) == 1:
        log_lines.append("Автоторговля: действий в этом цикле нет.")

    return log_lines
