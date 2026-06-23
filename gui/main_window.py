import os
from decimal import Decimal, InvalidOperation
from collections.abc import Callable, Coroutine
from datetime import datetime, timedelta, timezone
from typing import Any

from dotenv import load_dotenv
from PySide6.QtCore import Qt, QThread
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QGridLayout,
    QGroupBox,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)
from t_tech.invest import AsyncClient
from t_tech.invest.grpc import marketdata_pb2

from gui.worker import AsyncTaskWorker
from bd.settings_storage import (
    load_app_settings,
    load_selected_shares,
    reset_app_storage,
    save_app_settings,
    save_selected_shares,
)
from tbank.accounts import TBankAccount, get_accounts
from tbank.active_orders import TBankActiveOrder, get_active_orders
from tbank.balance import PortfolioBalance, get_balance
from tbank.candles import TBankCandle, get_candles
from tbank.last_prices import TBankLastPrice, get_last_prices_batched
from tbank.positions import TBankPortfolioPosition, get_portfolio_positions
from tbank.shares import TBankShare, get_shares


CANDLE_INTERVALS: dict[str, int] = {
    "1 минута": marketdata_pb2.CANDLE_INTERVAL_1_MIN,
    "5 минут": marketdata_pb2.CANDLE_INTERVAL_5_MIN,
    "15 минут": marketdata_pb2.CANDLE_INTERVAL_15_MIN,
    "1 час": marketdata_pb2.CANDLE_INTERVAL_HOUR,
    "1 день": marketdata_pb2.CANDLE_INTERVAL_DAY,
}


GROWTH_PERIOD_UNITS: dict[str, str] = {
    "секунд": "seconds",
    "минут": "minutes",
    "часов": "hours",
    "дней": "days",
    "недель": "weeks",
    "месяцев": "months",
}


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()

        load_dotenv()

        initial_token = ""
        initial_account_id = ""

        if "INVEST_TOKEN" in os.environ:
            initial_token = os.environ["INVEST_TOKEN"]

        if "INVEST_ACCOUNT_ID" in os.environ:
            initial_account_id = os.environ["INVEST_ACCOUNT_ID"]

        saved_settings = load_app_settings()

        if "token" in saved_settings:
            initial_token = saved_settings["token"]

        if "account_id" in saved_settings:
            initial_account_id = saved_settings["account_id"]

        self.threads: list[QThread] = []
        self.workers: list[AsyncTaskWorker] = []

        self.all_shares: list[TBankShare] = []
        self.available_shares: list[TBankShare] = []
        saved_selected_shares = load_selected_shares()
        self.selected_shares_by_uid: dict[str, TBankShare] = {
            share.uid: share
            for share in saved_selected_shares
        }
        self.robot_is_running = False

        self.setWindowTitle("TBank Robot — GUI v0.1")
        self.resize(1450, 900)

        self.token_edit = QLineEdit(initial_token)
        self.token_edit.setEchoMode(QLineEdit.EchoMode.Password)

        self.account_id_edit = QLineEdit(initial_account_id)
        self.instrument_ids_edit = QLineEdit("SBER_TQBR, GAZP_TQBR, LKOH_TQBR")
        self.candle_instrument_edit = QLineEdit("SBER_TQBR")
        self.candle_days_edit = QLineEdit("1")
        self.candle_limit_edit = QLineEdit("50")

        self.growth_percent_edit = QLineEdit("1.00")
        self.growth_period_value_edit = QLineEdit("30")
        self.growth_period_unit_combo = QComboBox()
        self.growth_period_unit_combo.addItems(list(GROWTH_PERIOD_UNITS.keys()))
        self.take_profit_percent_edit = QLineEdit("1.00")
        self.stop_loss_percent_edit = QLineEdit("1.00")
        self.bot_money_limit_edit = QLineEdit("10000.00")

        self.robot_status_label = QLabel("Робот: выключен")
        self.manual_mode_checkbox = QCheckBox("Ручной режим")

        self.allow_buy_checkbox = QCheckBox("Покупки разрешены")
        self.allow_buy_checkbox.setChecked(True)
        self.allow_buy_checkbox.setToolTip(
            "Если выключено — робот не будет открывать новые покупки."
        )

        self.allow_sell_checkbox = QCheckBox("Продажи разрешены")
        self.allow_sell_checkbox.setChecked(True)
        self.allow_sell_checkbox.setToolTip(
            "Если выключено — робот не будет продавать позиции автоматически."
        )

        self.manual_instrument_id_edit = QLineEdit("SBER_TQBR")
        self.manual_buy_amount_edit = QLineEdit("10000.00")
        self.manual_sell_lots_edit = QLineEdit("1")

        self.qualified_investor_checkbox = QCheckBox("Клиент — квалифицированный инвестор")
        self.qualified_investor_checkbox.setChecked(False)

        self.shares_filters_label = QLabel(self._get_shares_filter_text(False))
        self.shares_filters_label.setWordWrap(True)

        self.candle_interval_combo = QComboBox()
        self.candle_interval_combo.addItems(list(CANDLE_INTERVALS.keys()))
        self._apply_saved_settings(saved_settings)

        self.accounts_table = QTableWidget()
        self.money_table = QTableWidget()
        self.positions_table = QTableWidget()
        self.orders_table = QTableWidget()
        self.shares_table = QTableWidget()
        self.shares_tab_widget = QWidget()
        self.apply_checked_shares_button = QPushButton(
            "Обновить рабочие акции из отмеченных"
        )

        self.selected_shares_table = QTableWidget()
        self.prices_table = QTableWidget()
        self.candles_table = QTableWidget()

        self.log_edit = QPlainTextEdit()
        self.log_edit.setReadOnly(True)

        self._build_ui()
        self.refresh_selected_shares_table()

        self._log("GUI v0.1 запущен.")
        self._log(f"Рабочих акций загружено из SQLite: {len(self.selected_shares_by_uid)}")
        self._log(
            f"Поле токена: {'заполнено' if self.token_edit.text().strip() else 'пустое'}"
        )
        self._log(
            f"Account ID: {self.account_id_edit.text().strip() if self.account_id_edit.text().strip() else 'не задан'}"
        )
        self._log(f"Сохранённых рабочих акций загружено: {len(self.selected_shares_by_uid)}")

    def _build_ui(self) -> None:
        root = QWidget()
        root_layout = QVBoxLayout(root)

        controls = QGroupBox("Проверка API")
        controls_layout = QGridLayout(controls)

        controls_layout.addWidget(QLabel("Токен:"), 0, 0)
        controls_layout.addWidget(self.token_edit, 0, 1, 1, 3)

        controls_layout.addWidget(QLabel("Account ID:"), 1, 0)
        controls_layout.addWidget(self.account_id_edit, 1, 1, 1, 3)

        accounts_button = QPushButton("Получить аккаунты")
        balance_button = QPushButton("Получить баланс")
        positions_button = QPushButton("Получить позиции")
        active_orders_button = QPushButton("Активные заявки")

        accounts_button.clicked.connect(self.load_accounts)
        balance_button.clicked.connect(self.load_balance)
        positions_button.clicked.connect(self.load_positions)
        active_orders_button.clicked.connect(self.load_active_orders)

        controls_layout.addWidget(accounts_button, 2, 0)
        controls_layout.addWidget(balance_button, 2, 1)
        controls_layout.addWidget(positions_button, 2, 2)
        controls_layout.addWidget(active_orders_button, 2, 3)

        controls_layout.addWidget(QLabel("Акции:"), 3, 0)
        controls_layout.addWidget(self.qualified_investor_checkbox, 3, 1, 1, 2)

        shares_button = QPushButton("Получить акции")
        shares_button.clicked.connect(self.load_shares)
        controls_layout.addWidget(shares_button, 3, 3)

        self.qualified_investor_checkbox.toggled.connect(
            lambda checked: self.refresh_shares_filters_label()
        )

        controls_layout.addWidget(QLabel("Фильтры акций:"), 4, 0)
        controls_layout.addWidget(self.shares_filters_label, 4, 1, 1, 3)

        selected_prices_button = QPushButton("Last prices по рабочим акциям")
        selected_prices_button.clicked.connect(self.load_last_prices_for_selected_shares)

        clear_selected_button = QPushButton("Очистить рабочие акции")
        clear_selected_button.clicked.connect(self.clear_selected_shares)

        controls_layout.addWidget(QLabel("Рабочие акции:"), 5, 0)
        controls_layout.addWidget(selected_prices_button, 5, 1, 1, 2)
        controls_layout.addWidget(clear_selected_button, 5, 3)

        controls_layout.addWidget(QLabel("Instrument IDs:"), 6, 0)
        controls_layout.addWidget(self.instrument_ids_edit, 6, 1, 1, 2)

        last_prices_button = QPushButton("Получить last prices")
        last_prices_button.clicked.connect(self.load_last_prices)
        controls_layout.addWidget(last_prices_button, 6, 3)

        controls_layout.addWidget(QLabel("Свечи инструмент:"), 7, 0)
        controls_layout.addWidget(self.candle_instrument_edit, 7, 1)

        controls_layout.addWidget(QLabel("Интервал:"), 7, 2)
        controls_layout.addWidget(self.candle_interval_combo, 7, 3)

        controls_layout.addWidget(QLabel("Дней назад:"), 8, 0)
        controls_layout.addWidget(self.candle_days_edit, 8, 1)

        controls_layout.addWidget(QLabel("Лимит свечей:"), 8, 2)
        controls_layout.addWidget(self.candle_limit_edit, 8, 3)

        candles_button = QPushButton("Получить свечи")
        candles_button.clicked.connect(self.load_candles)
        controls_layout.addWidget(candles_button, 9, 0, 1, 4)
        strategy_controls = QGroupBox("Настройки стратегии и режим")
        strategy_layout = QGridLayout(strategy_controls)

        strategy_layout.addWidget(QLabel("Рост для покупки, %:"), 0, 0)
        strategy_layout.addWidget(self.growth_percent_edit, 0, 1)

        strategy_layout.addWidget(QLabel("Период роста:"), 0, 4)
        strategy_layout.addWidget(self.growth_period_value_edit, 0, 5)
        strategy_layout.addWidget(self.growth_period_unit_combo, 0, 6)

        strategy_layout.addWidget(QLabel("Продать при прибыли, %:"), 0, 2)
        strategy_layout.addWidget(self.take_profit_percent_edit, 0, 3)

        strategy_layout.addWidget(QLabel("Продать при убытке, %:"), 1, 0)
        strategy_layout.addWidget(self.stop_loss_percent_edit, 1, 1)

        strategy_layout.addWidget(QLabel("Лимит денег для бота, ₽:"), 1, 2)
        strategy_layout.addWidget(self.bot_money_limit_edit, 1, 3)

        start_robot_button = QPushButton("Включить робота")
        stop_robot_button = QPushButton("Выключить робота")

        start_robot_button.clicked.connect(self.start_robot_placeholder)
        stop_robot_button.clicked.connect(self.stop_robot_placeholder)

        strategy_layout.addWidget(self.robot_status_label, 2, 0)
        strategy_layout.addWidget(start_robot_button, 2, 1)
        strategy_layout.addWidget(stop_robot_button, 2, 2)
        strategy_layout.addWidget(self.manual_mode_checkbox, 2, 3)

        strategy_layout.addWidget(QLabel("Ручной инструмент:"), 3, 0)
        strategy_layout.addWidget(self.manual_instrument_id_edit, 3, 1)

        strategy_layout.addWidget(QLabel("Сумма покупки, ₽:"), 3, 2)
        strategy_layout.addWidget(self.manual_buy_amount_edit, 3, 3)

        strategy_layout.addWidget(QLabel("Объём продажи, лоты:"), 4, 0)
        strategy_layout.addWidget(self.manual_sell_lots_edit, 4, 1)

        manual_buy_button = QPushButton("Купить вручную")
        manual_sell_button = QPushButton("Продать вручную")

        manual_buy_button.clicked.connect(self.manual_buy_placeholder)
        manual_sell_button.clicked.connect(self.manual_sell_placeholder)

        strategy_layout.addWidget(manual_buy_button, 4, 2)
        strategy_layout.addWidget(manual_sell_button, 4, 3)
        strategy_layout.addWidget(QLabel("Разрешения:"), 5, 0)
        strategy_layout.addWidget(self.allow_buy_checkbox, 5, 1)
        strategy_layout.addWidget(self.allow_sell_checkbox, 5, 2)


        save_state_button = QPushButton("Сохранить настройки")
        save_state_button.clicked.connect(self.save_current_state)

        reset_state_button = QPushButton("Сбросить настройки")
        reset_state_button.clicked.connect(self.reset_current_state)
        strategy_layout.addWidget(save_state_button, 6, 0, 1, 2)
        strategy_layout.addWidget(reset_state_button, 6, 2, 1, 2)


        self.accounts_table.cellDoubleClicked.connect(self.select_account_from_table)
        self.apply_checked_shares_button.clicked.connect(
            self.apply_checked_shares_selection
        )
        self.selected_shares_table.cellDoubleClicked.connect(
            self.remove_selected_share_from_table
        )

        shares_tab_layout = QVBoxLayout(self.shares_tab_widget)
        shares_tab_layout.addWidget(self.shares_table)
        shares_tab_layout.addWidget(self.apply_checked_shares_button)

        self.tabs = QTabWidget()
        self.tabs.addTab(self.accounts_table, "Аккаунты")
        self.tabs.addTab(self.money_table, "Баланс")
        self.tabs.addTab(self.positions_table, "Позиции")
        self.tabs.addTab(self.orders_table, "Активные заявки")
        self.tabs.addTab(self.shares_tab_widget, "Акции")
        self.tabs.addTab(self.selected_shares_table, "Рабочие акции")
        self.tabs.addTab(self.prices_table, "Last prices")
        self.tabs.addTab(self.candles_table, "Свечи")
        self.tabs.addTab(self.log_edit, "Лог")

        root_layout.addWidget(controls)
        root_layout.addWidget(strategy_controls)
        root_layout.addWidget(self.tabs)

        self.setCentralWidget(root)

    def _apply_saved_settings(self, settings: dict[str, str]) -> None:
        if "instrument_ids" in settings:
            self.instrument_ids_edit.setText(settings["instrument_ids"])

        if "candle_instrument" in settings:
            self.candle_instrument_edit.setText(settings["candle_instrument"])

        if "candle_days" in settings:
            self.candle_days_edit.setText(settings["candle_days"])

        if "candle_limit" in settings:
            self.candle_limit_edit.setText(settings["candle_limit"])

        if "growth_percent" in settings:
            self.growth_percent_edit.setText(settings["growth_percent"])

        if "growth_period_value" in settings:
            self.growth_period_value_edit.setText(settings["growth_period_value"])

        if "growth_period_unit" in settings:
            growth_period_unit_index = self.growth_period_unit_combo.findText(
                settings["growth_period_unit"]
            )

            if growth_period_unit_index == -1:
                raise ValueError(
                    f"Сохранённая единица периода роста не найдена: {settings['growth_period_unit']}"
                )

            self.growth_period_unit_combo.setCurrentIndex(growth_period_unit_index)

        if "take_profit_percent" in settings:
            self.take_profit_percent_edit.setText(settings["take_profit_percent"])

        if "stop_loss_percent" in settings:
            self.stop_loss_percent_edit.setText(settings["stop_loss_percent"])

        if "bot_money_limit" in settings:
            self.bot_money_limit_edit.setText(settings["bot_money_limit"])

        if "manual_instrument_id" in settings:
            self.manual_instrument_id_edit.setText(settings["manual_instrument_id"])

        if "manual_buy_amount" in settings:
            self.manual_buy_amount_edit.setText(settings["manual_buy_amount"])

        if "manual_sell_lots" in settings:
            self.manual_sell_lots_edit.setText(settings["manual_sell_lots"])

        if "client_is_qualified" in settings:
            self.qualified_investor_checkbox.setChecked(
                settings["client_is_qualified"] == "1"
            )

        if "manual_mode" in settings:
            self.manual_mode_checkbox.setChecked(settings["manual_mode"] == "1")

        if "allow_buy" in settings:
            self.allow_buy_checkbox.setChecked(settings["allow_buy"] == "1")

        if "allow_sell" in settings:
            self.allow_sell_checkbox.setChecked(settings["allow_sell"] == "1")

        if "candle_interval" in settings:
            candle_interval_index = self.candle_interval_combo.findText(
                settings["candle_interval"]
            )

            if candle_interval_index == -1:
                raise ValueError(
                    f"Сохранённый интервал свечей не найден: {settings['candle_interval']}"
                )

            self.candle_interval_combo.setCurrentIndex(candle_interval_index)

        self.refresh_shares_filters_label()

    def save_current_state(self) -> None:
        settings = {
            "token": self.token_edit.text().strip(),
            "account_id": self.account_id_edit.text().strip(),
            "client_is_qualified": "1" if self.qualified_investor_checkbox.isChecked() else "0",
            "instrument_ids": self.instrument_ids_edit.text().strip(),
            "candle_instrument": self.candle_instrument_edit.text().strip(),
            "candle_days": self.candle_days_edit.text().strip(),
            "candle_limit": self.candle_limit_edit.text().strip(),
            "candle_interval": self.candle_interval_combo.currentText(),
            "growth_percent": self.growth_percent_edit.text().strip(),
            "growth_period_value": self.growth_period_value_edit.text().strip(),
            "growth_period_unit": self.growth_period_unit_combo.currentText(),
            "take_profit_percent": self.take_profit_percent_edit.text().strip(),
            "stop_loss_percent": self.stop_loss_percent_edit.text().strip(),
            "bot_money_limit": self.bot_money_limit_edit.text().strip(),
            "manual_mode": "1" if self.manual_mode_checkbox.isChecked() else "0",
            "allow_buy": "1" if self.allow_buy_checkbox.isChecked() else "0",
            "allow_sell": "1" if self.allow_sell_checkbox.isChecked() else "0",
            "manual_instrument_id": self.manual_instrument_id_edit.text().strip(),
            "manual_buy_amount": self.manual_buy_amount_edit.text().strip(),
            "manual_sell_lots": self.manual_sell_lots_edit.text().strip(),
        }

        save_app_settings(settings)
        save_selected_shares(list(self.selected_shares_by_uid.values()))

        self._log("Состояние GUI сохранено в SQLite.")
        self._log("Проверки токена, account_id и стратегии выполняются при запуске действий.")
        self._log(f"Сохранено рабочих акций: {len(self.selected_shares_by_uid)}")

    def reset_current_state(self) -> None:
        answer = QMessageBox.question(
            self,
            "Сброс настроек",
            (
                "Сбросить настройки приложения?\n\n"
                "Будут очищены сохранённые настройки, токен, account_id "
                "и рабочий список акций."
            ),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )

        if answer != QMessageBox.StandardButton.Yes:
            return

        reset_app_storage()

        self.token_edit.clear()
        self.account_id_edit.clear()

        self.qualified_investor_checkbox.setChecked(False)
        self.manual_mode_checkbox.setChecked(False)

        self.allow_buy_checkbox.setChecked(True)
        self.allow_sell_checkbox.setChecked(True)

        self.instrument_ids_edit.setText("SBER_TQBR, GAZP_TQBR, LKOH_TQBR")

        self.candle_instrument_edit.setText("SBER_TQBR")
        self.candle_days_edit.setText("1")
        self.candle_limit_edit.setText("50")
        self.candle_interval_combo.setCurrentText("1 минута")

        self.growth_percent_edit.setText("1.00")
        self.growth_period_value_edit.setText("30")
        self.growth_period_unit_combo.setCurrentText("секунд")
        self.take_profit_percent_edit.setText("1.00")
        self.stop_loss_percent_edit.setText("1.00")
        self.bot_money_limit_edit.setText("10000.00")

        self.manual_instrument_id_edit.setText("SBER_TQBR")
        self.manual_buy_amount_edit.setText("10000.00")
        self.manual_sell_lots_edit.setText("1")

        self.robot_is_running = False
        self.robot_status_label.setText("Робот: выключен")

        self.selected_shares_by_uid.clear()
        self.refresh_selected_shares_table()
        self.refresh_available_shares_table()
        self.refresh_shares_filters_label()

        self._log("Настройки сброшены.")
        self._log("Рабочий список акций очищен.")
        self._log("Покупки разрешены: да")
        self._log("Продажи разрешены: да")

    def _get_shares_filter_text(self, client_is_qualified: bool) -> str:
        qual_filter = (
            "for_qual_investor: допускаются"
            if client_is_qualified
            else "for_qual_investor=False"
        )

        return (
            "currency=RUB; "
            "real_exchange=REAL_EXCHANGE_MOEX; "
            "class_code=TQBR; "
            "api_trade=True; "
            "buy=True; "
            "sell=True; "
            "blocked_tca=False; "
            f"{qual_filter}; "
            "liquidity_flag — не фильтр, только колонка."
        )

    def refresh_shares_filters_label(self) -> None:
        self.shares_filters_label.setText(
            self._get_shares_filter_text(self.qualified_investor_checkbox.isChecked())
        )

    def _parse_decimal_field(self, line_edit: QLineEdit, field_name: str) -> Decimal:
        raw_value = line_edit.text().strip().replace(",", ".")

        if not raw_value:
            raise ValueError(f"{field_name}: поле не может быть пустым.")

        try:
            value = Decimal(raw_value)
        except InvalidOperation as error:
            raise ValueError(f"{field_name}: некорректное число.") from error

        return value

    def _get_strategy_settings(self) -> dict[str, object]:
        growth_percent = self._parse_decimal_field(
            self.growth_percent_edit,
            "Рост для покупки, %",
        )
        growth_period_raw = self.growth_period_value_edit.text().strip()

        try:
            growth_period_value = int(growth_period_raw)
        except ValueError as error:
            raise ValueError("Период роста должен быть целым числом.") from error

        if growth_period_value <= 0:
            raise ValueError("Период роста должен быть больше 0.")

        growth_period_unit = self.growth_period_unit_combo.currentText()

        if growth_period_unit not in GROWTH_PERIOD_UNITS:
            raise ValueError(f"Некорректная единица периода роста: {growth_period_unit}")
        take_profit_percent = self._parse_decimal_field(
            self.take_profit_percent_edit,
            "Продать при прибыли, %",
        )
        stop_loss_percent = self._parse_decimal_field(
            self.stop_loss_percent_edit,
            "Продать при убытке, %",
        )
        bot_money_limit = self._parse_decimal_field(
            self.bot_money_limit_edit,
            "Лимит денег для бота",
        )

        if growth_percent <= 0:
            raise ValueError("Рост для покупки должен быть больше 0.")

        if take_profit_percent <= 0:
            raise ValueError("Процент прибыли должен быть больше 0.")

        if stop_loss_percent <= 0:
            raise ValueError("Процент убытка должен быть больше 0.")

        if bot_money_limit <= 0:
            raise ValueError("Лимит денег для бота должен быть больше 0.")

        return {
            "growth_percent": growth_percent,
            "growth_period_value": growth_period_value,
            "growth_period_unit": growth_period_unit,
            "take_profit_percent": take_profit_percent,
            "stop_loss_percent": stop_loss_percent,
            "bot_money_limit": bot_money_limit,
        }

    def start_robot_placeholder(self) -> None:
        if not self.selected_shares_by_uid:
            QMessageBox.warning(
                self,
                "Ошибка",
                "Рабочий список акций пуст. Сначала выберите акции.",
            )
            return

        try:
            settings = self._get_strategy_settings()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка настроек стратегии", str(error))
            return
        if (
            not self.allow_buy_checkbox.isChecked()
            and not self.allow_sell_checkbox.isChecked()
        ):
            QMessageBox.warning(
                self,
                "Ошибка режима робота",
                "Покупки и продажи отключены. Робот не сможет ничего делать.",
            )
            return


        self.robot_is_running = True
        self.robot_status_label.setText("Робот: включен")

        self._log("Робот включен.")
        self._log(f"Рабочих акций: {len(self.selected_shares_by_uid)}")
        self._log(f"Рост для покупки: {settings['growth_percent']}%")
        self._log(f"Take profit: {settings['take_profit_percent']}%")
        self._log(f"Stop loss: {settings['stop_loss_percent']}%")
        self._log(f"Лимит денег для бота: {settings['bot_money_limit']} ₽")
        self._log(
            f"Покупки разрешены: {'да' if self.allow_buy_checkbox.isChecked() else 'нет'}"
        )
        self._log(
            f"Продажи разрешены: {'да' if self.allow_sell_checkbox.isChecked() else 'нет'}"
        )
        self._log("Автологика пока не подключена. GUI фиксирует настройки.")

    def stop_robot_placeholder(self) -> None:
        self.robot_is_running = False
        self.robot_status_label.setText("Робот: выключен")
        self._log("Робот выключен.")

    def _get_manual_instrument_id(self) -> str:
        instrument_id = self.manual_instrument_id_edit.text().strip()

        if not instrument_id:
            raise ValueError("Ручной инструмент не может быть пустым.")

        return instrument_id

    def manual_buy_placeholder(self) -> None:
        if self.robot_is_running:
            QMessageBox.warning(
                self,
                "Ошибка",
                "Для ручной сделки сначала выключите робота.",
            )
            return

        if not self.manual_mode_checkbox.isChecked():
            QMessageBox.warning(
                self,
                "Ошибка",
                "Для ручной сделки включите ручной режим.",
            )
            return

        try:
            instrument_id = self._get_manual_instrument_id()
            buy_amount = self._parse_decimal_field(
                self.manual_buy_amount_edit,
                "Сумма покупки",
            )
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка ручной покупки", str(error))
            return

        if buy_amount <= 0:
            QMessageBox.warning(
                self,
                "Ошибка ручной покупки",
                "Сумма покупки должна быть больше 0.",
            )
            return

        self._log(
            f"Ручная покупка подготовлена: instrument_id={instrument_id}, "
            f"сумма={buy_amount} ₽."
        )
        self._log("Реальная отправка заявки пока не подключена.")

    def manual_sell_placeholder(self) -> None:
        if self.robot_is_running:
            QMessageBox.warning(
                self,
                "Ошибка",
                "Для ручной сделки сначала выключите робота.",
            )
            return

        if not self.manual_mode_checkbox.isChecked():
            QMessageBox.warning(
                self,
                "Ошибка",
                "Для ручной сделки включите ручной режим.",
            )
            return

        try:
            instrument_id = self._get_manual_instrument_id()
            sell_lots_raw = self.manual_sell_lots_edit.text().strip()
            sell_lots = int(sell_lots_raw)
        except ValueError as error:
            QMessageBox.warning(
                self,
                "Ошибка ручной продажи",
                f"Объём продажи должен быть целым числом лотов. {error}",
            )
            return

        if sell_lots <= 0:
            QMessageBox.warning(
                self,
                "Ошибка ручной продажи",
                "Объём продажи должен быть больше 0.",
            )
            return

        self._log(
            f"Ручная продажа подготовлена: instrument_id={instrument_id}, "
            f"лоты={sell_lots}."
        )
        self._log("Реальная отправка заявки пока не подключена.")

    def _get_token(self) -> str:
        token = self.token_edit.text().strip()

        if not token:
            raise ValueError("Токен не может быть пустым.")

        return token

    def _get_account_id(self) -> str:
        account_id = self.account_id_edit.text().strip()

        if not account_id:
            raise ValueError("Account ID не может быть пустым.")

        return account_id

    def _run_async_task(
        self,
        name: str,
        task_factory: Callable[[], Coroutine[Any, Any, object]],
        on_success: Callable[[Any], None],
    ) -> None:
        self._log(f"Старт задачи: {name}")

        thread = QThread(self)
        worker = AsyncTaskWorker(task_factory)

        self.threads.append(thread)
        self.workers.append(worker)

        worker.moveToThread(thread)

        thread.started.connect(worker.run)

        worker.finished.connect(
            lambda result, task_name=name, success_handler=on_success: self._handle_success(
                task_name,
                result,
                success_handler,
            )
        )
        worker.failed.connect(
            lambda error, task_name=name: self._handle_error(
                task_name,
                error,
            )
        )

        worker.finished.connect(thread.quit)
        worker.failed.connect(thread.quit)

        thread.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(
            lambda current_thread=thread, current_worker=worker: self._cleanup_task(
                current_thread,
                current_worker,
            )
        )

        thread.start()

    def _cleanup_task(self, thread: QThread, worker: AsyncTaskWorker) -> None:
        if thread in self.threads:
            self.threads.remove(thread)

        if worker in self.workers:
            self.workers.remove(worker)

    def _handle_success(
        self,
        name: str,
        result: object,
        on_success: Callable[[Any], None],
    ) -> None:
        self._log(f"Задача выполнена: {name}")
        on_success(result)

    def _handle_error(self, name: str, error: str) -> None:
        self._log(f"Ошибка в задаче {name}: {error}")
        QMessageBox.critical(self, f"Ошибка: {name}", error)

    def _log(self, message: str) -> None:
        now = datetime.now().strftime("%H:%M:%S")
        self.log_edit.appendPlainText(f"[{now}] {message}")

    def _fill_table(
        self,
        table: QTableWidget,
        headers: list[str],
        rows: list[list[object]],
    ) -> None:
        table.clear()
        table.setColumnCount(len(headers))
        table.setRowCount(len(rows))
        table.setHorizontalHeaderLabels(headers)

        for row_index, row in enumerate(rows):
            for column_index, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                table.setItem(row_index, column_index, item)

        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        table.verticalHeader().setVisible(False)

    def _make_read_only_item(self, value: object) -> QTableWidgetItem:
        item = QTableWidgetItem(str(value))
        item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)

        return item

    def select_account_from_table(self, row: int, column: int) -> None:
        account_id_item = self.accounts_table.item(row, 0)

        if account_id_item is None:
            return

        self.account_id_edit.setText(account_id_item.text())
        self._log(f"Account ID выбран из таблицы: {account_id_item.text()}")

    def load_accounts(self) -> None:
        try:
            token = self._get_token()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка", str(error))
            return

        async def task():
            async with AsyncClient(token) as client:
                return await get_accounts(client)

        self._run_async_task("accounts", task, self.show_accounts)

    def show_accounts(self, accounts: list[TBankAccount]) -> None:
        rows = [
            [
                account.account_id,
                account.name,
                account.account_type,
                account.status,
                account.access_level,
            ]
            for account in accounts
        ]

        self._fill_table(
            self.accounts_table,
            ["account_id", "name", "type", "status", "access_level"],
            rows,
        )

        if len(accounts) == 1:
            self.account_id_edit.setText(accounts[0].account_id)
            self._log(f"Account ID выбран автоматически: {accounts[0].account_id}")

        self._log(f"Получено аккаунтов: {len(accounts)}")
        self.tabs.setCurrentWidget(self.accounts_table)

    def load_balance(self) -> None:
        try:
            token = self._get_token()
            account_id = self._get_account_id()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка", str(error))
            return

        async def task():
            async with AsyncClient(token) as client:
                return await get_balance(client, account_id)

        self._run_async_task("balance", task, self.show_balance)

    def show_balance(self, balance: PortfolioBalance) -> None:
        self._log(f"Портфель всего: {balance.total_amount_portfolio:.2f}")
        self._log(f"Валюта: {balance.total_amount_currencies:.2f}")
        self._log(f"Акции: {balance.total_amount_shares:.2f}")
        self._log(f"Облигации: {balance.total_amount_bonds:.2f}")
        self._log(f"Фонды: {balance.total_amount_etf:.2f}")

        rows = [
            [
                money.currency,
                f"{money.total:.2f}",
                f"{money.blocked:.2f}",
                f"{money.available:.2f}",
            ]
            for money in balance.money
        ]

        self._fill_table(
            self.money_table,
            ["currency", "total", "blocked", "available"],
            rows,
        )

        self.tabs.setCurrentWidget(self.money_table)

    def load_positions(self) -> None:
        try:
            token = self._get_token()
            account_id = self._get_account_id()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка", str(error))
            return

        async def task():
            async with AsyncClient(token) as client:
                return await get_portfolio_positions(client, account_id)

        self._run_async_task("positions", task, self.show_positions)

    def show_positions(self, positions: list[TBankPortfolioPosition]) -> None:
        rows = [
            [
                position.ticker,
                position.class_code,
                position.instrument_type,
                position.quantity,
                position.quantity_lots,
                position.average_position_price,
                position.average_position_price_fifo,
                position.current_price,
                position.expected_yield,
                position.currency,
            ]
            for position in positions
        ]

        self._fill_table(
            self.positions_table,
            [
                "ticker",
                "class_code",
                "type",
                "quantity",
                "lots",
                "avg_price",
                "avg_fifo",
                "current",
                "yield",
                "currency",
            ],
            rows,
        )

        self._log(f"Получено позиций: {len(positions)}")
        self.tabs.setCurrentWidget(self.positions_table)

    def load_active_orders(self) -> None:
        try:
            token = self._get_token()
            account_id = self._get_account_id()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка", str(error))
            return

        async def task():
            async with AsyncClient(token) as client:
                return await get_active_orders(client, account_id)

        self._run_async_task("active_orders", task, self.show_active_orders)

    def show_active_orders(self, orders: list[TBankActiveOrder]) -> None:
        rows = [
            [
                order.order_id,
                order.order_request_id,
                order.execution_report_status,
                order.direction,
                order.order_type,
                order.ticker,
                order.class_code,
                order.lots_requested,
                order.lots_executed,
                order.initial_order_price,
                order.total_order_amount,
                order.order_date,
            ]
            for order in orders
        ]

        self._fill_table(
            self.orders_table,
            [
                "order_id",
                "request_id",
                "status",
                "direction",
                "type",
                "ticker",
                "class_code",
                "lots_req",
                "lots_exec",
                "price",
                "amount",
                "date_utc",
            ],
            rows,
        )

        self._log(f"Получено активных заявок: {len(orders)}")
        self.tabs.setCurrentWidget(self.orders_table)

    def load_shares(self) -> None:
        try:
            token = self._get_token()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка", str(error))
            return

        client_is_qualified = self.qualified_investor_checkbox.isChecked()
        self.refresh_shares_filters_label()

        async def task():
            async with AsyncClient(token) as client:
                return await get_shares(client)

        self._run_async_task(
            "shares",
            task,
            lambda shares, qualified=client_is_qualified: self.show_shares(
                shares,
                qualified,
            ),
        )

    def _filter_available_shares(
        self,
        shares: list[TBankShare],
        client_is_qualified: bool,
    ) -> list[TBankShare]:
        filtered_shares: list[TBankShare] = []

        for share in shares:
            if share.currency != "RUB":
                continue

            if share.real_exchange != "REAL_EXCHANGE_MOEX":
                continue

            if share.class_code != "TQBR":
                continue

            if not share.api_trade_available_flag:
                continue

            if not share.buy_available_flag:
                continue

            if not share.sell_available_flag:
                continue

            if share.blocked_tca_flag:
                continue

            if share.for_qual_investor_flag and not client_is_qualified:
                continue

            filtered_shares.append(share)

        return filtered_shares

    def _sync_selected_shares_with_available(self) -> None:
        available_uids = {
            share.uid
            for share in self.available_shares
        }

        removed_uids = [
            uid
            for uid in self.selected_shares_by_uid
            if uid not in available_uids
        ]

        for uid in removed_uids:
            del self.selected_shares_by_uid[uid]

        if removed_uids:
            self._log(
                f"Из рабочих акций удалены недоступные после фильтра: {len(removed_uids)}"
            )

        self.refresh_selected_shares_table()

    def show_shares(
        self,
        shares: list[TBankShare],
        client_is_qualified: bool,
    ) -> None:
        self.all_shares = shares
        self.available_shares = self._filter_available_shares(
            shares=shares,
            client_is_qualified=client_is_qualified,
        )

        self.refresh_available_shares_table()

        qualified_count = sum(1 for share in shares if share.for_qual_investor_flag)

        self._log(f"Клиент квал: {'да' if client_is_qualified else 'нет'}")
        self._log(f"Фильтры акций: {self._get_shares_filter_text(client_is_qualified)}")
        self._log(f"Всего акций из API: {len(shares)}")
        self._log(f"Акций с признаком для квалов в общем списке: {qualified_count}")
        self._log(f"Рабочих акций после фильтра: {len(self.available_shares)}")

        self._sync_selected_shares_with_available()

        self.tabs.setCurrentWidget(self.shares_tab_widget)

    def refresh_available_shares_table(self) -> None:
        headers = [
            "✓",
            "#",
            "ticker",
            "name",
            "class_code",
            "lot",
            "currency",
            "real_exchange",
            "uid",
            "api",
            "buy",
            "sell",
            "qual",
            "blocked_tca",
            "liquidity",
            "required_tests",
        ]

        self.shares_table.clear()
        self.shares_table.setColumnCount(len(headers))
        self.shares_table.setRowCount(len(self.available_shares))
        self.shares_table.setHorizontalHeaderLabels(headers)

        for row_index, share in enumerate(self.available_shares):
            checkbox_item = QTableWidgetItem()
            checkbox_item.setFlags(
                Qt.ItemFlag.ItemIsEnabled
                | Qt.ItemFlag.ItemIsUserCheckable
                | Qt.ItemFlag.ItemIsSelectable
            )

            if share.uid in self.selected_shares_by_uid:
                checkbox_item.setCheckState(Qt.CheckState.Checked)
            else:
                checkbox_item.setCheckState(Qt.CheckState.Unchecked)

            self.shares_table.setItem(row_index, 0, checkbox_item)

            row_values = [
                row_index + 1,
                share.ticker,
                share.name,
                share.class_code,
                share.lot,
                share.currency,
                share.real_exchange,
                share.uid,
                share.api_trade_available_flag,
                share.buy_available_flag,
                share.sell_available_flag,
                share.for_qual_investor_flag,
                share.blocked_tca_flag,
                share.liquidity_flag,
                ", ".join(share.required_tests),
            ]

            for column_index, value in enumerate(row_values, start=1):
                self.shares_table.setItem(
                    row_index,
                    column_index,
                    self._make_read_only_item(value),
                )

        self.shares_table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents
        )
        self.shares_table.verticalHeader().setVisible(False)

    def apply_checked_shares_selection(self) -> None:
        selected_shares: dict[str, TBankShare] = {}

        for row in range(self.shares_table.rowCount()):
            checkbox_item = self.shares_table.item(row, 0)
            uid_item = self.shares_table.item(row, 8)

            if checkbox_item is None or uid_item is None:
                continue

            if checkbox_item.checkState() != Qt.CheckState.Checked:
                continue

            share = self._find_available_share_by_uid(uid_item.text())

            if share is None:
                continue

            selected_shares[share.uid] = share

        self.selected_shares_by_uid = selected_shares
        self.refresh_selected_shares_table()

        self._log(
            f"Рабочий список акций полностью обновлён. "
            f"Выбрано: {len(self.selected_shares_by_uid)}"
        )

        self.tabs.setCurrentWidget(self.selected_shares_table)

    def _find_available_share_by_uid(self, uid: str) -> TBankShare | None:
        for share in self.available_shares:
            if share.uid == uid:
                return share

        return None

    def add_selected_share_from_available_table(self, row: int, column: int) -> None:
        uid_item = self.shares_table.item(row, 8)

        if uid_item is None:
            return

        uid = uid_item.text()
        share = self._find_available_share_by_uid(uid)

        if share is None:
            self._log(f"Акция не найдена в доступном списке: {uid}")
            return

        if uid in self.selected_shares_by_uid:
            self._log(f"Акция уже есть в рабочем списке: {share.ticker}")
            return

        self.selected_shares_by_uid[uid] = share
        self.refresh_selected_shares_table()

        self._log(f"Акция добавлена в рабочий список: {share.ticker} / {share.name}")
        self._log(f"Всего рабочих акций выбрано: {len(self.selected_shares_by_uid)}")

        self.tabs.setCurrentWidget(self.selected_shares_table)

    def remove_selected_share_from_table(self, row: int, column: int) -> None:
        uid_item = self.selected_shares_table.item(row, 7)

        if uid_item is None:
            return

        uid = uid_item.text()

        if uid not in self.selected_shares_by_uid:
            return

        share = self.selected_shares_by_uid[uid]
        del self.selected_shares_by_uid[uid]

        self.refresh_selected_shares_table()
        self.refresh_available_shares_table()

        self._log(f"Акция удалена из рабочего списка: {share.ticker} / {share.name}")
        self._log(f"Всего рабочих акций выбрано: {len(self.selected_shares_by_uid)}")

    def clear_selected_shares(self) -> None:
        self.selected_shares_by_uid.clear()
        self.refresh_selected_shares_table()
        self.refresh_available_shares_table()
        self._log("Рабочий список акций очищен.")

    def refresh_selected_shares_table(self) -> None:
        rows = [
            [
                number,
                share.ticker,
                share.name,
                share.class_code,
                share.lot,
                share.currency,
                share.real_exchange,
                share.uid,
                share.for_qual_investor_flag,
                share.liquidity_flag,
            ]
            for number, share in enumerate(self.selected_shares_by_uid.values(), start=1)
        ]

        self._fill_table(
            self.selected_shares_table,
            [
                "#",
                "ticker",
                "name",
                "class_code",
                "lot",
                "currency",
                "real_exchange",
                "uid",
                "qual",
                "liquidity",
            ],
            rows,
        )

    def load_last_prices_for_selected_shares(self) -> None:
        try:
            token = self._get_token()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка", str(error))
            return

        if not self.selected_shares_by_uid:
            QMessageBox.warning(self, "Ошибка", "Рабочий список акций пуст.")
            return

        instrument_ids = [
            share.uid
            for share in self.selected_shares_by_uid.values()
        ]

        async def task():
            async with AsyncClient(token) as client:
                return await get_last_prices_batched(
                    client=client,
                    instrument_ids=instrument_ids,
                    batch_size=100,
                )

        self._run_async_task(
            "last_prices_selected_shares",
            task,
            self.show_last_prices,
        )

    def load_last_prices(self) -> None:
        try:
            token = self._get_token()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка", str(error))
            return

        instrument_ids = [
            value.strip()
            for value in self.instrument_ids_edit.text().split(",")
            if value.strip()
        ]

        if not instrument_ids:
            QMessageBox.warning(self, "Ошибка", "Список instrument_ids пуст.")
            return

        async def task():
            async with AsyncClient(token) as client:
                return await get_last_prices_batched(
                    client=client,
                    instrument_ids=instrument_ids,
                    batch_size=100,
                )

        self._run_async_task("last_prices", task, self.show_last_prices)

    def show_last_prices(self, prices: list[TBankLastPrice]) -> None:
        rows = [
            [
                number,
                price.ticker,
                price.class_code,
                price.price,
                price.time,
                price.instrument_uid,
                price.last_price_type,
            ]
            for number, price in enumerate(prices, start=1)
        ]

        self._fill_table(
            self.prices_table,
            ["#", "ticker", "class_code", "price", "time_utc", "uid", "type"],
            rows,
        )

        self._log(f"Получено last prices: {len(prices)}")
        self.tabs.setCurrentWidget(self.prices_table)

    def load_candles(self) -> None:
        try:
            token = self._get_token()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка", str(error))
            return

        instrument_id = self.candle_instrument_edit.text().strip()

        if not instrument_id:
            QMessageBox.warning(self, "Ошибка", "instrument_id не может быть пустым.")
            return

        try:
            days = int(self.candle_days_edit.text().strip())
            limit = int(self.candle_limit_edit.text().strip())
        except ValueError:
            QMessageBox.warning(self, "Ошибка", "Дни и лимит должны быть числами.")
            return

        if days <= 0:
            QMessageBox.warning(self, "Ошибка", "Количество дней должно быть больше 0.")
            return

        if limit <= 0:
            QMessageBox.warning(self, "Ошибка", "Лимит должен быть больше 0.")
            return

        interval_name = self.candle_interval_combo.currentText()
        interval = CANDLE_INTERVALS[interval_name]

        to_time = datetime.now(timezone.utc)
        from_time = to_time - timedelta(days=days)

        async def task():
            async with AsyncClient(token) as client:
                return await get_candles(
                    client=client,
                    instrument_id=instrument_id,
                    from_time=from_time,
                    to_time=to_time,
                    interval=interval,
                    limit=limit,
                )

        self._run_async_task("candles", task, self.show_candles)

    def show_candles(self, candles: list[TBankCandle]) -> None:
        rows = [
            [
                number,
                candle.time,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.is_complete,
            ]
            for number, candle in enumerate(candles, start=1)
        ]

        self._fill_table(
            self.candles_table,
            ["#", "time_utc", "open", "high", "low", "close", "volume", "complete"],
            rows,
        )

        self._log(f"Получено свечей: {len(candles)}")
        self.tabs.setCurrentWidget(self.candles_table)