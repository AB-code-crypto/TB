import os
from decimal import Decimal, InvalidOperation
from collections.abc import Callable, Coroutine
from datetime import datetime
from typing import Any

from dotenv import load_dotenv
from PySide6.QtCore import Qt, QThread, Signal
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
from gui.growth_monitor_worker import GrowthMonitorWorker
from gui.monitoring_tables import (
    fill_buy_intents_table,
    fill_growth_current_table,
    fill_growth_cycles_table,
    fill_growth_signals_table,
)
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
from tbank.positions import TBankPortfolioPosition, get_portfolio_positions
from tbank.shares import TBankShare, get_shares


CANDLE_INTERVALS: dict[str, int] = {
    "1 минута": marketdata_pb2.CANDLE_INTERVAL_1_MIN,
    "5 минут": marketdata_pb2.CANDLE_INTERVAL_5_MIN,
    "15 минут": marketdata_pb2.CANDLE_INTERVAL_15_MIN,
    "1 час": marketdata_pb2.CANDLE_INTERVAL_HOUR,
    "1 день": marketdata_pb2.CANDLE_INTERVAL_DAY,
}

GROWTH_CANDLE_INTERVALS: dict[str, int] = {
    "5 секунд": marketdata_pb2.CANDLE_INTERVAL_5_SEC,
    "10 секунд": marketdata_pb2.CANDLE_INTERVAL_10_SEC,
    "30 секунд": marketdata_pb2.CANDLE_INTERVAL_30_SEC,
    "1 минута": marketdata_pb2.CANDLE_INTERVAL_1_MIN,
    "2 минуты": marketdata_pb2.CANDLE_INTERVAL_2_MIN,
    "3 минуты": marketdata_pb2.CANDLE_INTERVAL_3_MIN,
    "5 минут": marketdata_pb2.CANDLE_INTERVAL_5_MIN,
    "10 минут": marketdata_pb2.CANDLE_INTERVAL_10_MIN,
    "15 минут": marketdata_pb2.CANDLE_INTERVAL_15_MIN,
    "30 минут": marketdata_pb2.CANDLE_INTERVAL_30_MIN,
    "1 час": marketdata_pb2.CANDLE_INTERVAL_HOUR,
    "2 часа": marketdata_pb2.CANDLE_INTERVAL_2_HOUR,
    "4 часа": marketdata_pb2.CANDLE_INTERVAL_4_HOUR,
    "1 день": marketdata_pb2.CANDLE_INTERVAL_DAY,
    "1 неделя": marketdata_pb2.CANDLE_INTERVAL_WEEK,
    "1 месяц": marketdata_pb2.CANDLE_INTERVAL_MONTH,
}



class MainWindow(QMainWindow):
    async_task_finished = Signal(str, object)
    async_task_failed = Signal(str, str)

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
        self.growth_monitor_thread: QThread | None = None
        self.growth_monitor_worker: GrowthMonitorWorker | None = None

        self.all_shares: list[TBankShare] = []
        self.available_shares: list[TBankShare] = []
        saved_selected_shares = load_selected_shares()
        self.selected_shares_by_uid: dict[str, TBankShare] = {
            share.uid: share
            for share in saved_selected_shares
        }
        self.robot_is_running = False

        self.async_task_counter = 0
        self.async_task_names: dict[str, str] = {}
        self.async_task_success_handlers: dict[str, Callable[[Any], None]] = {}
        self.async_task_error_handlers: dict[str, Callable[[str], None] | None] = {}

        self.setWindowTitle("TBank Robot — GUI v0.1")
        self.resize(1450, 900)

        self.token_edit = QLineEdit(initial_token)
        self.token_edit.setEchoMode(QLineEdit.EchoMode.Password)

        self.account_id_edit = QLineEdit(initial_account_id)

        self.growth_percent_edit = QLineEdit("1.00")
        self.growth_candle_interval_combo = QComboBox()
        self.growth_candle_interval_combo.addItems(list(GROWTH_CANDLE_INTERVALS.keys()))
        self.growth_candle_interval_combo.setCurrentText("30 секунд")
        self.scan_interval_seconds_edit = QLineEdit("10")
        self.max_price_age_seconds_edit = QLineEdit("30")
        self.price_snapshot_retention_days_edit = QLineEdit("7")
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

        self.qualified_investor_checkbox = QCheckBox("Я квалифицированный инвестор")
        self.qualified_investor_checkbox.setChecked(False)

        self.shares_filters_label = QLabel(self._get_shares_filter_text(False))
        self.shares_filters_label.setWordWrap(True)

        self._apply_saved_settings(saved_settings)

        self.accounts_table = QTableWidget()
        self.money_table = QTableWidget()
        self.positions_table = QTableWidget()
        self.orders_table = QTableWidget()
        self.info_tab_widget = QWidget()
        self.info_title_label = QLabel("")
        self.shares_table = QTableWidget()
        self.shares_tab_widget = QWidget()
        self.shares_search_edit = QLineEdit()
        self.shares_search_edit.setPlaceholderText("Поиск по тикеру или названию")
        self.shares_search_status_label = QLabel("")
        self.apply_checked_shares_button = QPushButton(
            "Обновить рабочие акции из отмеченных"
        )

        self.selected_shares_table = QTableWidget()
        self.growth_signals_table = QTableWidget()
        self.buy_intents_table = QTableWidget()
        self.growth_current_table = QTableWidget()
        self.growth_cycles_table = QTableWidget()
        self.monitoring_tabs = QTabWidget()

        self.log_edit = QPlainTextEdit()
        self.log_edit.setReadOnly(True)

        self.async_task_finished.connect(self._handle_async_task_finished)
        self.async_task_failed.connect(self._handle_async_task_failed)

        self._build_ui()
        self.refresh_selected_shares_table()
        self.refresh_growth_monitor_tables()

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

        self.accounts_button = QPushButton("Получить аккаунты")
        balance_button = QPushButton("Получить баланс")
        positions_button = QPushButton("Получить позиции")
        active_orders_button = QPushButton("Активные заявки")

        self.accounts_button.clicked.connect(self.load_accounts)
        balance_button.clicked.connect(self.load_balance)
        positions_button.clicked.connect(self.load_positions)
        active_orders_button.clicked.connect(self.load_active_orders)

        controls_layout.addWidget(self.accounts_button, 2, 0)
        controls_layout.addWidget(balance_button, 2, 1)
        controls_layout.addWidget(positions_button, 2, 2)
        controls_layout.addWidget(active_orders_button, 2, 3)

        controls_layout.addWidget(QLabel("Акции:"), 3, 0)
        controls_layout.addWidget(self.qualified_investor_checkbox, 3, 1, 1, 2)

        self.shares_button = QPushButton("Получить акции")
        self.shares_button.clicked.connect(self.load_shares)
        controls_layout.addWidget(self.shares_button, 3, 3)

        self.qualified_investor_checkbox.toggled.connect(
            lambda checked: self.refresh_shares_filters_label()
        )

        controls_layout.addWidget(QLabel("Фильтры акций:"), 4, 0)
        controls_layout.addWidget(self.shares_filters_label, 4, 1, 1, 3)

        self.clear_selected_shares_button = QPushButton("Очистить рабочие акции")
        self.clear_selected_shares_button.clicked.connect(self.clear_selected_shares)

        controls_layout.addWidget(QLabel("Рабочие акции:"), 5, 0)
        controls_layout.addWidget(self.clear_selected_shares_button, 5, 1, 1, 3)

        strategy_controls = QGroupBox("Настройки стратегии и режим")
        strategy_layout = QGridLayout(strategy_controls)

        strategy_layout.addWidget(QLabel("Рост для покупки, %:"), 0, 0)
        strategy_layout.addWidget(self.growth_percent_edit, 0, 1)

        strategy_layout.addWidget(QLabel("Интервал расчёта роста:"), 0, 4)
        strategy_layout.addWidget(self.growth_candle_interval_combo, 0, 5, 1, 2)
        strategy_layout.addWidget(QLabel("Интервал проверки, сек:"), 1, 4)
        strategy_layout.addWidget(self.scan_interval_seconds_edit, 1, 5, 1, 2)
        strategy_layout.addWidget(QLabel("Макс. возраст цены, сек:"), 2, 4)
        strategy_layout.addWidget(self.max_price_age_seconds_edit, 2, 5, 1, 2)
        strategy_layout.addWidget(QLabel("Хранить снимки цен, дней:"), 3, 4)
        strategy_layout.addWidget(self.price_snapshot_retention_days_edit, 3, 5, 1, 2)

        strategy_layout.addWidget(QLabel("Продать при прибыли, %:"), 0, 2)
        strategy_layout.addWidget(self.take_profit_percent_edit, 0, 3)

        strategy_layout.addWidget(QLabel("Продать при убытке, %:"), 1, 0)
        strategy_layout.addWidget(self.stop_loss_percent_edit, 1, 1)

        strategy_layout.addWidget(QLabel("Лимит денег для бота, ₽:"), 1, 2)
        strategy_layout.addWidget(self.bot_money_limit_edit, 1, 3)

        self.robot_toggle_button = QPushButton("Включить робота")
        self.robot_toggle_button.setCheckable(True)
        self.robot_toggle_button.toggled.connect(self.toggle_robot_monitoring)
        self._set_robot_visual_state("stopped")

        strategy_layout.addWidget(self.robot_status_label, 2, 0)
        strategy_layout.addWidget(self.robot_toggle_button, 2, 1, 1, 2)
        strategy_layout.addWidget(self.manual_mode_checkbox, 2, 3)

        strategy_layout.addWidget(QLabel("Ручной инструмент:"), 3, 0)
        strategy_layout.addWidget(self.manual_instrument_id_edit, 3, 1)

        strategy_layout.addWidget(QLabel("Сумма одной покупки, ₽:"), 3, 2)
        strategy_layout.addWidget(self.manual_buy_amount_edit, 3, 3)

        strategy_layout.addWidget(QLabel("Объём продажи, лоты:"), 4, 0)
        strategy_layout.addWidget(self.manual_sell_lots_edit, 4, 1)

        self.manual_buy_button = QPushButton("Купить вручную")
        self.manual_sell_button = QPushButton("Продать вручную")

        self.manual_buy_button.clicked.connect(self.manual_buy_placeholder)
        self.manual_sell_button.clicked.connect(self.manual_sell_placeholder)

        strategy_layout.addWidget(self.manual_buy_button, 4, 2)
        strategy_layout.addWidget(self.manual_sell_button, 4, 3)
        strategy_layout.addWidget(QLabel("Разрешения:"), 5, 0)
        strategy_layout.addWidget(self.allow_buy_checkbox, 5, 1)
        strategy_layout.addWidget(self.allow_sell_checkbox, 5, 2)


        self.save_state_button = QPushButton("Сохранить настройки")
        self.save_state_button.clicked.connect(self.save_current_state)

        self.reset_state_button = QPushButton("Сбросить настройки")
        self.reset_state_button.clicked.connect(self.reset_current_state)
        strategy_layout.addWidget(self.save_state_button, 6, 0, 1, 2)
        strategy_layout.addWidget(self.reset_state_button, 6, 2, 1, 2)


        self.accounts_table.cellDoubleClicked.connect(self.select_account_from_table)
        self.apply_checked_shares_button.clicked.connect(
            self.apply_checked_shares_selection
        )
        self.selected_shares_table.cellDoubleClicked.connect(
            self.remove_selected_share_from_table
        )

        shares_tab_layout = QVBoxLayout(self.shares_tab_widget)
        shares_tab_layout.addWidget(QLabel("Поиск акции:"))
        shares_tab_layout.addWidget(self.shares_search_edit)
        shares_tab_layout.addWidget(self.shares_search_status_label)
        shares_tab_layout.addWidget(self.shares_table)
        shares_tab_layout.addWidget(self.apply_checked_shares_button)

        self.shares_search_edit.textChanged.connect(
            lambda text: self.apply_shares_search_filter()
        )

        info_layout = QVBoxLayout(self.info_tab_widget)
        self.info_title_label.setStyleSheet("font-weight: bold;")
        self.info_title_label.setVisible(False)
        info_layout.addWidget(self.info_title_label)
        info_layout.addWidget(self.accounts_table)
        info_layout.addWidget(self.money_table)
        info_layout.addWidget(self.positions_table)
        info_layout.addWidget(self.orders_table)
        self._hide_info_tables()

        self.tabs = QTabWidget()
        self.tabs.addTab(self.info_tab_widget, "Инфо")
        self.tabs.addTab(self.shares_tab_widget, "Акции")
        self.tabs.addTab(self.selected_shares_table, "Рабочие акции")

        self.monitoring_tabs.addTab(self.growth_current_table, "Текущий рост")
        self.monitoring_tabs.addTab(self.growth_signals_table, "Сигналы роста")
        self.monitoring_tabs.addTab(self.buy_intents_table, "Планы покупок")
        self.monitoring_tabs.addTab(self.growth_cycles_table, "Циклы")
        self.tabs.addTab(self.monitoring_tabs, "Мониторинг")

        self.tabs.addTab(self.log_edit, "Лог")

        root_layout.addWidget(controls)
        root_layout.addWidget(strategy_controls)
        root_layout.addWidget(self.tabs)

        self.setCentralWidget(root)

    def _apply_saved_settings(self, settings: dict[str, str]) -> None:
        if "growth_percent" in settings:
            self.growth_percent_edit.setText(settings["growth_percent"])

        if "growth_candle_interval" in settings:
            growth_candle_interval_index = self.growth_candle_interval_combo.findText(
                settings["growth_candle_interval"]
            )

            if growth_candle_interval_index == -1:
                raise ValueError(
                    f"Сохранённый интервал расчёта роста не найден: {settings['growth_candle_interval']}"
                )

            self.growth_candle_interval_combo.setCurrentIndex(growth_candle_interval_index)

        if "scan_interval_seconds" in settings:
            self.scan_interval_seconds_edit.setText(settings["scan_interval_seconds"])

        if "max_price_age_seconds" in settings:
            self.max_price_age_seconds_edit.setText(settings["max_price_age_seconds"])

        if "price_snapshot_retention_days" in settings:
            self.price_snapshot_retention_days_edit.setText(settings["price_snapshot_retention_days"])

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

        self.refresh_shares_filters_label()

    def save_current_state(self) -> None:
        settings = {
            "token": self.token_edit.text().strip(),
            "account_id": self.account_id_edit.text().strip(),
            "client_is_qualified": "1" if self.qualified_investor_checkbox.isChecked() else "0",
            "growth_percent": self.growth_percent_edit.text().strip(),
            "growth_candle_interval": self.growth_candle_interval_combo.currentText(),
            "scan_interval_seconds": self.scan_interval_seconds_edit.text().strip(),
            "max_price_age_seconds": self.max_price_age_seconds_edit.text().strip(),
            "price_snapshot_retention_days": self.price_snapshot_retention_days_edit.text().strip(),
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
                "Сбросить настройки приложения?"
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

        self.growth_percent_edit.setText("1.00")
        self.growth_candle_interval_combo.setCurrentText("30 секунд")
        self.scan_interval_seconds_edit.setText("10")
        self.max_price_age_seconds_edit.setText("30")
        self.price_snapshot_retention_days_edit.setText("3")

        self.take_profit_percent_edit.setText("1.00")
        self.stop_loss_percent_edit.setText("1.00")
        self.bot_money_limit_edit.setText("10000.00")

        self.manual_instrument_id_edit.setText("SBER_TQBR")
        self.manual_buy_amount_edit.setText("10000.00")
        self.manual_sell_lots_edit.setText("1")

        self.robot_is_running = False
        self.robot_status_label.setText("Робот: выключен")
        self._set_robot_visual_state("stopped")
        self._set_robot_inputs_locked(False)

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

        growth_candle_interval = self.growth_candle_interval_combo.currentText()

        if growth_candle_interval not in GROWTH_CANDLE_INTERVALS:
            raise ValueError(
                f"Некорректный интервал расчёта роста: {growth_candle_interval}"
            )

        scan_interval_raw = self.scan_interval_seconds_edit.text().strip()

        try:
            scan_interval_seconds = int(scan_interval_raw)
        except ValueError as error:
            raise ValueError("Интервал проверки должен быть целым числом секунд.") from error

        if scan_interval_seconds <= 0:
            raise ValueError("Интервал проверки должен быть больше 0.")

        max_price_age_seconds_raw = self.max_price_age_seconds_edit.text().strip()

        try:
            max_price_age_seconds = int(max_price_age_seconds_raw)
        except ValueError as error:
            raise ValueError("Максимальный возраст цены должен быть целым числом секунд.") from error

        if max_price_age_seconds <= 0:
            raise ValueError("Максимальный возраст цены должен быть больше 0.")

        price_snapshot_retention_days_raw = self.price_snapshot_retention_days_edit.text().strip()

        try:
            price_snapshot_retention_days = int(price_snapshot_retention_days_raw)
        except ValueError as error:
            raise ValueError("Срок хранения снимков цен должен быть целым числом дней.") from error

        if price_snapshot_retention_days <= 0:
            raise ValueError("Срок хранения снимков цен должен быть больше 0.")

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
        manual_buy_amount = self._parse_decimal_field(
            self.manual_buy_amount_edit,
            "Сумма одной покупки, ₽",
        )

        if growth_percent <= 0:
            raise ValueError("Рост для покупки должен быть больше 0.")

        if take_profit_percent <= 0:
            raise ValueError("Процент прибыли должен быть больше 0.")

        if stop_loss_percent <= 0:
            raise ValueError("Процент убытка должен быть больше 0.")

        if bot_money_limit <= 0:
            raise ValueError("Лимит денег для бота должен быть больше 0.")

        if manual_buy_amount <= 0:
            raise ValueError("Сумма одной покупки должна быть больше 0.")

        return {
            "growth_percent": growth_percent,
            "growth_candle_interval": growth_candle_interval,
            "growth_candle_interval_value": GROWTH_CANDLE_INTERVALS[growth_candle_interval],
            "scan_interval_seconds": scan_interval_seconds,
            "max_price_age_seconds": max_price_age_seconds,
            "price_snapshot_retention_days": price_snapshot_retention_days,
            "take_profit_percent": take_profit_percent,
            "stop_loss_percent": stop_loss_percent,
            "bot_money_limit": bot_money_limit,
            "manual_buy_amount": manual_buy_amount,
        }

    def _set_robot_inputs_locked(self, locked: bool) -> None:
        enabled = not locked

        widgets = [
            self.token_edit,
            self.account_id_edit,
            self.qualified_investor_checkbox,
            self.growth_percent_edit,
            self.growth_candle_interval_combo,
            self.scan_interval_seconds_edit,
            self.max_price_age_seconds_edit,
            self.price_snapshot_retention_days_edit,
            self.take_profit_percent_edit,
            self.stop_loss_percent_edit,
            self.bot_money_limit_edit,
            self.manual_mode_checkbox,
            self.allow_buy_checkbox,
            self.allow_sell_checkbox,
            self.manual_instrument_id_edit,
            self.manual_buy_amount_edit,
            self.manual_sell_lots_edit,
            self.accounts_button,
            self.shares_button,
            self.apply_checked_shares_button,
            self.clear_selected_shares_button,
            self.save_state_button,
            self.reset_state_button,
            self.manual_buy_button,
            self.manual_sell_button,
            self.shares_table,
            self.selected_shares_table,
        ]

        for widget in widgets:
            widget.setEnabled(enabled)


    def _reject_robot_start(self, message: str) -> None:
        clean_message = message.strip()

        if not clean_message:
            clean_message = (
                "Запуск робота отклонён, но текст ошибки пуст. "
                "Проверь лог приложения и консольный вывод."
            )

        self.robot_is_running = False
        self._set_robot_inputs_locked(False)
        self._set_robot_visual_state("stopped")
        self._log(f"Робот не включён: {clean_message}")
        QMessageBox.warning(self, "Робот не включён", clean_message)

    def _get_robot_start_form(self) -> tuple[str, str, dict[str, object]]:
        token = self._get_token()
        account_id = self._get_account_id()

        if not self.selected_shares_by_uid:
            raise ValueError("Рабочий список акций пуст. Сначала выберите акции.")

        settings = self._get_strategy_settings()

        return token, account_id, settings

    def start_robot_placeholder(self) -> None:
        if self.robot_is_running or self.growth_monitor_worker is not None:
            self._set_robot_visual_state("running")
            QMessageBox.information(
                self,
                "Мониторинг уже запущен",
                "Мониторинг уже работает.",
            )
            return

        try:
            token, account_id, settings = self._get_robot_start_form()
        except ValueError as error:
            self._reject_robot_start(str(error))
            return

        client_is_qualified = self.qualified_investor_checkbox.isChecked()

        self._set_robot_inputs_locked(True)
        self._set_robot_visual_state("starting")
        self._log("Проверяю token/account_id через T-Invest API.")
        self._log("Проверяю рабочие акции через T-Invest API.")

        async def task():
            async with AsyncClient(token) as client:
                accounts = await get_accounts(client)
                account_exists = any(
                    account.account_id == account_id
                    for account in accounts
                )

                if not account_exists:
                    return accounts, []

                shares = await get_shares(client)

                return accounts, shares

        def on_success(result: tuple[list[TBankAccount], list[TBankShare]]) -> None:
            self._handle_robot_start_validation_success(
                result=result,
                account_id=account_id,
                settings=settings,
                client_is_qualified=client_is_qualified,
            )

        self._run_async_task(
            "robot_start_validation",
            task,
            on_success,
            self._handle_robot_start_validation_error,
        )

    def _handle_robot_start_validation_success(
        self,
        result: tuple[list[TBankAccount], list[TBankShare]],
        account_id: str,
        settings: dict[str, object],
        client_is_qualified: bool,
    ) -> None:
        accounts, shares = result

        account = next(
            (
                current_account
                for current_account in accounts
                if current_account.account_id == account_id
            ),
            None,
        )

        if account is None:
            available_account_ids = ", ".join(
                current_account.account_id
                for current_account in accounts
            )

            if not available_account_ids:
                available_account_ids = "нет доступных аккаунтов"

            self._reject_robot_start(
                f"Account ID не найден среди аккаунтов T-Invest: {account_id}. "
                f"Доступные account_id: {available_account_ids}"
            )
            return

        available_shares = self._filter_available_shares(
            shares=shares,
            client_is_qualified=client_is_qualified,
        )
        available_shares_by_uid = {
            share.uid: share
            for share in available_shares
        }
        selected_uids = list(self.selected_shares_by_uid)

        invalid_selected_shares: list[str] = []

        for uid in selected_uids:
            if uid in available_shares_by_uid:
                continue

            share = self.selected_shares_by_uid[uid]
            invalid_selected_shares.append(
                f"{share.ticker}_{share.class_code} ({uid})"
            )

        if invalid_selected_shares:
            shown_items = "\n".join(
                f"- {item}"
                for item in invalid_selected_shares[:20]
            )

            if len(invalid_selected_shares) > 20:
                shown_items += (
                    f"\n... ещё {len(invalid_selected_shares) - 20}"
                )

            self._reject_robot_start(
                "Некоторые рабочие акции не прошли текущую проверку через API "
                "и не могут использоваться при запуске робота:\n"
                f"{shown_items}"
            )
            return

        self.all_shares = shares
        self.available_shares = available_shares
        self.selected_shares_by_uid = {
            uid: available_shares_by_uid[uid]
            for uid in selected_uids
        }

        self.refresh_available_shares_table()
        self.refresh_selected_shares_table()

        self._start_robot_after_validation(
            settings=settings,
            account=account,
        )


    def _handle_robot_start_validation_error(self, error_text: str) -> None:
        clean_error_text = error_text.strip()

        if not clean_error_text:
            clean_error_text = (
                "T-Invest API или нижний сетевой слой вернул пустой текст ошибки."
            )

        self._reject_robot_start(
            "Проверка token/account_id через T-Invest API не пройдена.\n\n"
            f"{clean_error_text}"
        )

    def _start_robot_after_validation(
        self,
        settings: dict[str, object],
        account: TBankAccount,
    ) -> None:
        self.save_current_state()

        self.robot_is_running = True
        self._set_robot_inputs_locked(True)
        self._set_robot_visual_state("running")

        self._log("Проверка token/account_id через API пройдена.")
        self._log(f"Account ID подтверждён: {account.account_id} / {account.name}")
        self._log("Робот включён. Режим: наблюдение за ростом без отправки заявок.")
        self._log(f"Рабочих акций: {len(self.selected_shares_by_uid)}")
        self._log(f"Рост для покупки: {settings['growth_percent']}%")
        self._log(
            f"Интервал расчёта роста: {settings['growth_candle_interval']}"
        )
        self._log(f"Интервал проверки: {settings['scan_interval_seconds']} сек.")
        self._log(f"Макс. возраст цены: {settings['max_price_age_seconds']} сек.")
        self._log(
            f"Хранение снимков цен: {settings['price_snapshot_retention_days']} дней."
        )
        self._log(f"Сумма одной покупки: {settings['manual_buy_amount']} ₽")
        self._log(
            "Реальные торговые заявки пока не отправляются. "
            "Создаются dry-run планы покупок."
        )

        thread = QThread(self)
        worker = GrowthMonitorWorker()
        worker.moveToThread(thread)

        thread.started.connect(worker.run)

        worker.log_message.connect(self._handle_growth_monitor_log_message)
        worker.finished.connect(self._on_growth_monitor_finished)
        worker.failed.connect(self._on_growth_monitor_failed)

        worker.finished.connect(thread.quit)
        worker.failed.connect(thread.quit)

        thread.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(self._cleanup_growth_monitor_worker)

        self.growth_monitor_thread = thread
        self.growth_monitor_worker = worker

        thread.start()

    def stop_robot_placeholder(self) -> None:
        if self.growth_monitor_worker is None:
            self.robot_is_running = False
            self._set_robot_inputs_locked(False)
            self._set_robot_visual_state("stopped")
            self._log("Мониторинг не был запущен.")
            return

        self._set_robot_visual_state("stopping")
        self._log("Остановка мониторинга запрошена.")
        self.growth_monitor_worker.stop()

    def _handle_growth_monitor_log_message(self, message: str) -> None:
        self._log(message)

        if (
            message.startswith("Growth monitor cycle #")
            or message.startswith("Ошибка цикла #")
            or message == "Growth monitor service остановлен."
        ):
            self.refresh_growth_monitor_tables()

    def refresh_growth_monitor_tables(self) -> None:
        self.refresh_growth_current_table()
        self.refresh_growth_signals_table()
        self.refresh_buy_intents_table()
        self.refresh_growth_cycles_table()

    def refresh_growth_current_table(self) -> None:
        fill_growth_current_table(self.growth_current_table)

    def refresh_growth_signals_table(self) -> None:
        fill_growth_signals_table(self.growth_signals_table)

    def refresh_buy_intents_table(self) -> None:
        fill_buy_intents_table(self.buy_intents_table)

    def refresh_growth_cycles_table(self) -> None:
        fill_growth_cycles_table(self.growth_cycles_table)

    def _on_growth_monitor_finished(self) -> None:
        self.robot_is_running = False
        self._set_robot_inputs_locked(False)
        self._set_robot_visual_state("stopped")
        self._log("Мониторинг остановлен.")

    def _on_growth_monitor_failed(self, error_text: str) -> None:
        self.robot_is_running = False
        self._set_robot_inputs_locked(False)
        self._set_robot_visual_state("error")
        self._log(f"Ошибка мониторинга: {error_text}")

    def toggle_robot_monitoring(self, checked: bool) -> None:
        if checked:
            self.start_robot_placeholder()
        else:
            self.stop_robot_placeholder()

    def _set_robot_visual_state(self, state: str) -> None:
        if not hasattr(self, "robot_toggle_button"):
            return

        self.robot_toggle_button.blockSignals(True)

        if state == "starting":
            self.robot_is_running = False
            self.robot_status_label.setText("Робот: проверка запуска")
            self.robot_status_label.setStyleSheet(
                "font-weight: bold; color: #8a5a00;"
            )
            self.robot_toggle_button.setChecked(True)
            self.robot_toggle_button.setEnabled(False)
            self.robot_toggle_button.setText("Проверка запуска...")
            self.robot_toggle_button.setToolTip(
                "Проверяем token, account_id, рабочие акции и настройки."
            )
            self.robot_toggle_button.setStyleSheet(
                """
                QPushButton {
                    background-color: #fff0b3;
                    color: #6b4a00;
                    font-weight: bold;
                    border: 1px solid #d6b656;
                    border-radius: 5px;
                    padding: 6px 12px;
                }
                """
            )

        elif state == "running":
            self.robot_is_running = True
            self.robot_status_label.setText("Робот: включён")
            self.robot_status_label.setStyleSheet(
                "font-weight: bold; color: #0b6b20;"
            )
            self.robot_toggle_button.setChecked(True)
            self.robot_toggle_button.setEnabled(True)
            self.robot_toggle_button.setText("Робот включён")
            self.robot_toggle_button.setToolTip(
                "Робот включён в режиме наблюдения. Нажмите, чтобы выключить."
            )
            self.robot_toggle_button.setStyleSheet(
                """
                QPushButton {
                    background-color: #c8f7c5;
                    color: #0b4f19;
                    font-weight: bold;
                    border: 1px solid #6fbd6b;
                    border-radius: 5px;
                    padding: 6px 12px;
                }
                QPushButton:hover {
                    background-color: #b7efb1;
                }
                """
            )

        elif state == "stopping":
            self.robot_status_label.setText("Робот: выключается")
            self.robot_status_label.setStyleSheet(
                "font-weight: bold; color: #8a5a00;"
            )
            self.robot_toggle_button.setChecked(True)
            self.robot_toggle_button.setEnabled(False)
            self.robot_toggle_button.setText("Выключение робота...")
            self.robot_toggle_button.setToolTip("Идёт выключение робота.")
            self.robot_toggle_button.setStyleSheet(
                """
                QPushButton {
                    background-color: #fff0b3;
                    color: #6b4a00;
                    font-weight: bold;
                    border: 1px solid #d6b656;
                    border-radius: 5px;
                    padding: 6px 12px;
                }
                """
            )

        elif state == "error":
            self.robot_status_label.setText("Робот: ошибка")
            self.robot_status_label.setStyleSheet(
                "font-weight: bold; color: #8a1f11;"
            )
            self.robot_toggle_button.setChecked(False)
            self.robot_toggle_button.setEnabled(True)
            self.robot_toggle_button.setText("Ошибка. Включить робота")
            self.robot_toggle_button.setToolTip(
                "Робот остановлен из-за ошибки. Нажмите, чтобы включить снова."
            )
            self.robot_toggle_button.setStyleSheet(
                """
                QPushButton {
                    background-color: #ffd6d1;
                    color: #7a1a10;
                    font-weight: bold;
                    border: 1px solid #d68178;
                    border-radius: 5px;
                    padding: 6px 12px;
                }
                QPushButton:hover {
                    background-color: #ffc4bd;
                }
                """
            )

        elif state == "stopped":
            self.robot_is_running = False
            self.robot_status_label.setText("Робот: выключен")
            self.robot_status_label.setStyleSheet(
                "font-weight: bold; color: #555555;"
            )
            self.robot_toggle_button.setChecked(False)
            self.robot_toggle_button.setEnabled(True)
            self.robot_toggle_button.setText("Включить робота")
            self.robot_toggle_button.setToolTip(
                "Робот выключен. Нажмите, чтобы включить."
            )
            self.robot_toggle_button.setStyleSheet(
                """
                QPushButton {
                    background-color: #eeeeee;
                    color: #222222;
                    font-weight: bold;
                    border: 1px solid #999999;
                    border-radius: 5px;
                    padding: 6px 12px;
                }
                QPushButton:hover {
                    background-color: #e0e0e0;
                }
                """
            )

        else:
            self.robot_toggle_button.blockSignals(False)
            raise ValueError(f"Неизвестное визуальное состояние робота: {state}")

        self.robot_toggle_button.blockSignals(False)

    def _cleanup_growth_monitor_worker(self) -> None:
        self.growth_monitor_thread = None
        self.growth_monitor_worker = None

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
                "Сумма одной покупки, ₽",
            )
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка ручной покупки", str(error))
            return

        if buy_amount <= 0:
            QMessageBox.warning(
                self,
                "Ошибка ручной покупки",
                "Сумма одной покупки должна быть больше 0.",
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
        on_error: Callable[[str], None] | None = None,
    ) -> None:
        self._log(f"Старт задачи: {name}")

        self.async_task_counter += 1
        task_id = f"{name}:{self.async_task_counter}"

        self.async_task_names[task_id] = name
        self.async_task_success_handlers[task_id] = on_success
        self.async_task_error_handlers[task_id] = on_error

        thread = QThread(self)
        worker = AsyncTaskWorker(task_factory)

        self.threads.append(thread)
        self.workers.append(worker)

        worker.moveToThread(thread)

        thread.started.connect(worker.run)

        worker.finished.connect(
            lambda result, current_task_id=task_id: self.async_task_finished.emit(
                current_task_id,
                result,
            )
        )
        worker.failed.connect(
            lambda error, current_task_id=task_id: self.async_task_failed.emit(
                current_task_id,
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

    def _handle_async_task_finished(self, task_id: str, result: object) -> None:
        name = self.async_task_names.pop(task_id)
        on_success = self.async_task_success_handlers.pop(task_id)
        self.async_task_error_handlers.pop(task_id, None)

        self._handle_success(
            name=name,
            result=result,
            on_success=on_success,
        )

    def _handle_async_task_failed(self, task_id: str, error: str) -> None:
        name = self.async_task_names.pop(task_id)
        self.async_task_success_handlers.pop(task_id, None)
        on_error = self.async_task_error_handlers.pop(task_id, None)

        self._handle_error(
            name=name,
            error=error,
            on_error=on_error,
        )

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

    def _handle_error(
        self,
        name: str,
        error: str,
        on_error: Callable[[str], None] | None = None,
    ) -> None:
        self._log(f"Ошибка в задаче {name}: {error}")

        if on_error is not None:
            on_error(error)
            return

        QMessageBox.critical(self, f"Ошибка: {name}", error)

    def _log(self, message: str) -> None:
        now = datetime.now().strftime("%H:%M:%S")
        self.log_edit.appendPlainText(f"[{now}] {message}")

    def _hide_info_tables(self) -> None:
        self.info_title_label.setVisible(False)

        for table in (
            self.accounts_table,
            self.money_table,
            self.positions_table,
            self.orders_table,
        ):
            table.setVisible(False)

    def _show_info_table(self, title: str, table: QTableWidget) -> None:
        self.info_title_label.setText(title)
        self.info_title_label.setVisible(True)

        for current_table in (
            self.accounts_table,
            self.money_table,
            self.positions_table,
            self.orders_table,
        ):
            current_table.setVisible(current_table is table)

        self.tabs.setCurrentWidget(self.info_tab_widget)

    def _format_table_value(self, value: object) -> str:
        if value is None:
            return ""

        if isinstance(value, datetime):
            return value.replace(tzinfo=None).isoformat(
                sep=" ",
                timespec="seconds",
            )

        text = str(value)

        if "-" in text and ":" in text and ("T" in text or "+" in text):
            try:
                parsed_value = datetime.fromisoformat(text)
            except ValueError:
                return text

            return parsed_value.replace(tzinfo=None).isoformat(
                sep=" ",
                timespec="seconds",
            )

        return text

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
                item = QTableWidgetItem(self._format_table_value(value))
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                table.setItem(row_index, column_index, item)

        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        table.verticalHeader().setVisible(False)

    def _make_read_only_item(self, value: object) -> QTableWidgetItem:
        item = QTableWidgetItem(self._format_table_value(value))
        item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)

        return item

    def select_account_from_table(self, row: int, column: int) -> None:
        if self.robot_is_running:
            QMessageBox.warning(
                self,
                "Робот включён",
                "Account ID нельзя менять во время работы робота.",
            )
            return

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

        if len(accounts) == 1 and not self.robot_is_running:
            self.account_id_edit.setText(accounts[0].account_id)
            self._log(f"Account ID выбран автоматически: {accounts[0].account_id}")

        self._log(f"Получено аккаунтов: {len(accounts)}")
        self._show_info_table("Аккаунты", self.accounts_table)

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

        self._show_info_table("Баланс", self.money_table)

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
        self._show_info_table("Позиции", self.positions_table)

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
        self._show_info_table("Активные заявки", self.orders_table)

    def load_shares(self) -> None:
        if self.robot_is_running:
            QMessageBox.warning(
                self,
                "Робот включён",
                "Список акций нельзя обновлять во время работы робота.",
            )
            return

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

    def apply_shares_search_filter(self) -> None:
        query = self.shares_search_edit.text().strip().casefold()
        visible_count = 0
        total_count = self.shares_table.rowCount()

        for row in range(total_count):
            searchable_values = []

            for column in (2, 3, 4, 8):
                item = self.shares_table.item(row, column)

                if item is not None:
                    searchable_values.append(item.text())

            row_text = " ".join(searchable_values).casefold()
            row_matches = not query or query in row_text

            self.shares_table.setRowHidden(row, not row_matches)

            if row_matches:
                visible_count += 1

        if total_count == 0:
            self.shares_search_status_label.setText("")
        elif query:
            self.shares_search_status_label.setText(
                f"Найдено: {visible_count} из {total_count}"
            )
        else:
            self.shares_search_status_label.setText(
                f"Всего доступных акций: {total_count}"
            )

    def refresh_available_shares_table(self) -> None:
        self.shares_table.setSortingEnabled(False)

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
        self.shares_table.setColumnHidden(8, True)   # uid: внутренний идентификатор инструмента
        self.shares_table.setColumnHidden(15, True)  # required_tests: внутренний список тестов API
        self.shares_table.setSortingEnabled(True)
        self.shares_table.sortItems(2, Qt.SortOrder.AscendingOrder)
        self.apply_shares_search_filter()

    def apply_checked_shares_selection(self) -> None:
        if self.robot_is_running:
            QMessageBox.warning(
                self,
                "Робот включён",
                "Рабочие акции нельзя менять во время работы робота.",
            )
            return

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
        if self.robot_is_running:
            QMessageBox.warning(
                self,
                "Робот включён",
                "Рабочие акции нельзя очищать во время работы робота.",
            )
            return

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
        self.selected_shares_table.setColumnHidden(7, True)  # uid: нужен коду, но не нужен клиенту
