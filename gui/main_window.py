import asyncio
import os
from uuid import uuid4
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from collections.abc import Callable, Coroutine
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QGridLayout,
    QHBoxLayout,
    QGroupBox,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QFrame,
    QScrollArea,
    QSizePolicy,
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
from bot.auto_trade_executor import (
    BulkRobotSellReport,
    sell_all_robot_positions,
)
from gui.monitoring_tables import (
    fill_buy_intents_table,
    fill_growth_current_table,
    fill_growth_cycles_table,
    fill_growth_signals_table,
    fill_robot_orders_table,
    fill_robot_positions_table,
)
from bd.database_maintenance import (
    DatabaseMaintenanceResult,
    format_database_size,
    get_database_total_size_bytes,
    run_database_maintenance,
)
from bd.robot_realized_result import sum_robot_realized_results
from bd.robot_order import create_robot_order, mark_robot_order_cancelled_by_broker_order, mark_robot_order_failed, mark_robot_order_sent
from bd.robot_position import (
    apply_robot_order_fill,
    get_robot_position,
    list_robot_positions,
    set_robot_position_lots,
    sync_robot_positions_with_broker,
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
from tbank.order_book import get_best_order_book_prices
from tbank.order_execution import TBankPostOrderResult, cancel_order, post_limit_order, post_market_order
from tbank.last_prices import (
    TBankLastPrice,
    get_last_price,
    get_last_prices_batched,
    map_last_prices_by_instrument_uid,
)
from tbank.shares import (
    MOEX_REAL_EXCHANGE,
    MOEX_SHARE_CURRENCY,
    TBankShare,
    get_shares,
)


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


class CheckableTableWidgetItem(QTableWidgetItem):
    def __lt__(self, other) -> bool:
        return self.checkState().value < other.checkState().value


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
        self.pending_robot_start_settings: dict[str, object] | None = None
        self.pending_robot_start_account: TBankAccount | None = None
        self.robot_session_started_at_utc: datetime | None = None
        self.robot_session_finished_at_utc: datetime | None = None
        self.runtime_available_money_by_currency: dict[str, Decimal] = {}
        self.runtime_balance_refresh_in_progress = False

        self.all_shares: list[TBankShare] = []
        self.available_shares: list[TBankShare] = []
        self.available_share_prices_by_uid: dict[str, TBankLastPrice] = {}
        saved_selected_shares = [
            share
            for share in load_selected_shares()
            if (
                share.real_exchange == MOEX_REAL_EXCHANGE
                and share.currency.upper() == MOEX_SHARE_CURRENCY
            )
        ]
        self.selected_shares_by_uid: dict[str, TBankShare] = {
            share.uid: share
            for share in saved_selected_shares
        }
        self.robot_is_running = False

        self.async_task_counter = 0
        self.async_task_names: dict[str, str] = {}
        self.async_task_success_handlers: dict[str, Callable[[Any], None]] = {}
        self.async_task_error_handlers: dict[str, Callable[[str], None] | None] = {}

        self.setWindowTitle("TBank Robot — GUI v 1.0")
        self._apply_saved_window_size(saved_settings)

        self.token_edit = QLineEdit(initial_token)
        self.token_edit.setEchoMode(QLineEdit.EchoMode.Password)

        self.account_id_edit = QLineEdit(initial_account_id)

        self.growth_percent_edit = QLineEdit("1.00")
        self.growth_candle_interval_combo = QComboBox()
        self.growth_candle_interval_combo.addItems(list(GROWTH_CANDLE_INTERVALS.keys()))
        self.growth_candle_interval_combo.setCurrentText("1 минута")
        self.scan_interval_seconds_edit = QLineEdit("10")
        self.max_price_age_seconds_edit = QLineEdit("30")
        self.take_profit_percent_edit = QLineEdit("1.00")
        self.stop_loss_percent_edit = QLineEdit("1.00")
        self.bot_money_limit_edit = QLineEdit("0")
        self.auto_buy_amount_edit = QLineEdit("0")
        self.bot_money_limit_usd_edit = QLineEdit("0")
        self.auto_buy_amount_usd_edit = QLineEdit("0")
        self.bot_money_limit_eur_edit = QLineEdit("0")
        self.auto_buy_amount_eur_edit = QLineEdit("0")

        self.robot_status_label = QLabel("Робот: выключен")
        self.robot_mode_summary_label = QLabel("Режим: Тестирование")
        self.robot_account_summary_label = QLabel("Свободно:\n—")
        self.robot_selected_shares_summary_label = QLabel("Рабочих акций: 0")
        self.robot_open_positions_summary_label = QLabel("Открытых позиций: 0")
        self.robot_total_result_summary_label = QLabel("Результат всего, до комиссий: 0.00 RUB | 0.00 USD | 0.00 EUR")
        self.robot_session_result_summary_label = QLabel("Результат сессии, до комиссий: 0.00 RUB | 0.00 USD | 0.00 EUR")
        self.manual_mode_checkbox = QCheckBox("Ручной режим")
        self.manual_mode_checkbox.setChecked(False)

        self.auto_trading_enabled_checkbox = QCheckBox("Реальная автоторговля")
        self.auto_trading_enabled_checkbox.setChecked(False)
        self.auto_trading_enabled_checkbox.setToolTip(
            "Если включено — робот будет отправлять реальные рыночные заявки. "
            "По умолчанию всегда выключено."
        )

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
        self.manual_limit_offset_edit = QLineEdit("0")

        self.manual_last_price_button = QPushButton("Получить последнюю цену")
        self.manual_last_price_label = QLabel("—")
        self.manual_last_price_label.setWordWrap(True)

        self.qualified_investor_checkbox = QCheckBox("Я квалифицированный инвестор")
        self.qualified_investor_checkbox.setChecked(False)

        self.only_liquid_shares_checkbox = QCheckBox("Только ликвидные акции")
        self.only_liquid_shares_checkbox.setChecked(True)
        self.only_liquid_shares_checkbox.setToolTip(
            "Если включено — в рабочий список попадут только акции с liquidity_flag=True."
        )

        self.shares_filters_label = QLabel(
            self._get_shares_filter_text(False, True)
        )
        self.shares_filters_label.setWordWrap(True)

        self._apply_saved_settings(saved_settings)

        self.accounts_table = QTableWidget()
        self.money_table = QTableWidget()
        self.positions_table = QTableWidget()
        self.orders_table = QTableWidget()
        self.active_orders_actions_widget = QWidget()
        self.cancel_active_order_button = QPushButton("Отменить отмеченные активные заявки")
        self.cancel_all_limit_orders_button = QPushButton("Отменить все лимитные заявки")
        self.info_tab_widget = QWidget()
        self.info_title_label = QLabel("")
        self.startup_hint_label = QLabel(
            "Порядок запуска:\n"
            "1. Проверить подключение к T-Invest.\n"
            "2. Загрузить акции и выбрать рабочий список.\n"
            "3. Настроить стратегию и режим торговли.\n"
            "4. Синхронизировать позиции робота.\n"
            "5. Запустить мониторинг или боевую автоторговлю."
        )
        self.startup_hint_label.setWordWrap(True)
        self.startup_hint_label.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.startup_hint_label.setStyleSheet(
            "font-size: 14px; color: #555555; padding: 12px;"
        )
        self.shares_table = QTableWidget()
        self.shares_tab_widget = QWidget()
        self.shares_search_edit = QLineEdit()
        self.shares_search_edit.setPlaceholderText("Поиск по тикеру или названию")
        self.shares_search_status_label = QLabel("")
        self.apply_checked_shares_button = QPushButton(
            "Обновить рабочие акции из отмеченных"
        )
        self.update_robot_positions_button = QPushButton(
            "Обновить позиции и запустить робота"
        )
        self.cancel_robot_start_button = QPushButton("Отменить запуск")

        self.selected_shares_table = QTableWidget()
        self.selected_shares_tab_widget = QWidget()
        self.robot_positions_tab_widget = QWidget()
        self.robot_positions_table = QTableWidget()
        self.growth_signals_table = QTableWidget()
        self.buy_intents_table = QTableWidget()
        self.robot_orders_table = QTableWidget()
        self.growth_current_table = QTableWidget()
        self.growth_cycles_table = QTableWidget()
        self.monitoring_tabs = QTabWidget()

        self.log_tab_widget = QWidget()
        self.log_edit = QPlainTextEdit()
        self.log_edit.setReadOnly(True)
        self.clear_log_button = QPushButton("Очистить лог")
        self.clear_log_button.setToolTip(
            "Очистить только видимый лог. Данные БД не удаляются."
        )
        self.clear_log_button.clicked.connect(
            self.clear_log_view
        )

        self.async_task_finished.connect(self._handle_async_task_finished)
        self.async_task_failed.connect(self._handle_async_task_failed)

        self._build_ui()
        self.refresh_selected_shares_table()
        self.refresh_growth_monitor_tables()

        self._refresh_robot_summary()
        self._refresh_manual_trade_buttons_state()

        self._log("GUI v1.0 запущен.")
        self._log(f"Рабочих акций загружено из SQLite: {len(self.selected_shares_by_uid)}")
        self._log(
            f"Поле токена: {'заполнено' if self.token_edit.text().strip() else 'пустое'}"
        )
        self._log(
            f"Account ID: {self.account_id_edit.text().strip() if self.account_id_edit.text().strip() else 'не задан'}"
        )
        self._log(f"Сохранённых рабочих акций загружено: {len(self.selected_shares_by_uid)}")

    def _format_currency_totals(
        self,
        totals: dict[str, Decimal],
        show_positive_sign: bool = True,
    ) -> str:
        parts: list[str] = []

        for currency in ("RUB", "USD", "EUR"):
            amount = totals.get(currency, Decimal("0"))
            prefix = "+" if show_positive_sign and amount > 0 else ""
            parts.append(f"{prefix}{amount:.2f} {currency}")

        return " | ".join(parts)

    def _handle_account_id_changed(self) -> None:
        self.runtime_available_money_by_currency = {}
        self._refresh_robot_summary()

    def _apply_runtime_balance(
        self,
        balance: PortfolioBalance,
    ) -> None:
        self.runtime_available_money_by_currency = {
            money.currency.upper(): money.available
            for money in balance.money
        }
        self.runtime_balance_refresh_in_progress = False
        self._refresh_robot_summary()

    def _handle_runtime_balance_error(
        self,
        error_text: str,
    ) -> None:
        self.runtime_balance_refresh_in_progress = False

        if hasattr(self, "log_edit"):
            self._log(
                "Не удалось обновить свободные средства в статусе: "
                f"{error_text}"
            )

    def refresh_runtime_balance(self) -> None:
        if self.runtime_balance_refresh_in_progress:
            return

        token = self.token_edit.text().strip()
        account_id = self.account_id_edit.text().strip()

        if not token or not account_id:
            return

        self.runtime_balance_refresh_in_progress = True

        async def task():
            async with AsyncClient(token) as client:
                return await asyncio.wait_for(
                    get_balance(client, account_id),
                    timeout=20,
                )

        self._run_async_task(
            "runtime_balance",
            task,
            self._apply_runtime_balance,
            self._handle_runtime_balance_error,
        )

    def _finish_robot_session(self) -> None:
        if (
            self.robot_session_started_at_utc is not None
            and self.robot_session_finished_at_utc is None
        ):
            self.robot_session_finished_at_utc = datetime.now(timezone.utc)

    def _refresh_robot_summary(self) -> None:
        if not hasattr(self, "robot_mode_summary_label"):
            return

        mode_text = (
            "Режим: РЕАЛЬНАЯ ТОРГОВЛЯ"
            if self.auto_trading_enabled_checkbox.isChecked()
            else "Режим: Тестирование"
        )
        account_id = (
            self.account_id_edit.text().strip()
            if hasattr(self, "account_id_edit")
            else ""
        )

        open_positions_count = 0
        total_results = {
            currency: Decimal("0")
            for currency in ("RUB", "USD", "EUR")
        }
        session_results = {
            currency: Decimal("0")
            for currency in ("RUB", "USD", "EUR")
        }

        if account_id:
            try:
                open_positions_count = sum(
                    1
                    for position in list_robot_positions(
                        account_id=account_id
                    )
                    if position.robot_lots > 0
                )
                total_results = sum_robot_realized_results(
                    account_id=account_id,
                )

                if self.robot_session_started_at_utc is not None:
                    session_results = sum_robot_realized_results(
                        account_id=account_id,
                        started_at_utc=self.robot_session_started_at_utc,
                        finished_at_utc=self.robot_session_finished_at_utc,
                    )
            except Exception as error:
                if hasattr(self, "log_edit"):
                    self._log(
                        "Не удалось обновить сводку робота из БД: "
                        f"{type(error).__name__}: {error}"
                    )

        if self.runtime_available_money_by_currency:
            balance_text = self._format_currency_totals(
                self.runtime_available_money_by_currency,
                show_positive_sign=False,
            )
        else:
            balance_text = "—"

        self.robot_mode_summary_label.setText(mode_text)
        self.robot_account_summary_label.setText(
            f"Свободно:\n{balance_text}"
        )
        self.robot_selected_shares_summary_label.setText(
            f"Рабочих акций: {len(self.selected_shares_by_uid)}"
        )
        self.robot_open_positions_summary_label.setText(
            f"Открытых позиций: {open_positions_count}"
        )
        self.robot_total_result_summary_label.setText(
            "Результат всего, до комиссий: "
            f"{self._format_currency_totals(total_results)}"
        )
        self.robot_session_result_summary_label.setText(
            "Результат сессии, до комиссий: "
            f"{self._format_currency_totals(session_results)}"
        )

        if self.auto_trading_enabled_checkbox.isChecked():
            self.robot_mode_summary_label.setStyleSheet(
                "font-weight: bold; color: #8a1f11;"
            )
        else:
            self.robot_mode_summary_label.setStyleSheet(
                "font-weight: bold; color: #555555;"
            )

    def _refresh_manual_trade_buttons_state(self) -> None:
        if not hasattr(self, "manual_market_buy_button"):
            return

        enabled = (
            not self.robot_is_running
            and self.manual_mode_checkbox.isChecked()
        )

        for button in (
            self.manual_market_buy_button,
            self.manual_limit_buy_button,
            self.manual_market_sell_button,
            self.manual_limit_sell_button,
        ):
            button.setEnabled(enabled)

    def _apply_saved_window_size(self, settings: dict[str, str]) -> None:
        width = 900
        height = 900

        if "window_width" in settings and "window_height" in settings:
            try:
                width = int(settings["window_width"])
                height = int(settings["window_height"])
            except ValueError as error:
                raise ValueError("Сохранённый размер окна должен быть целым числом.") from error

            if width <= 0 or height <= 0:
                raise ValueError("Сохранённый размер окна должен быть больше 0.")

        self.resize(width, height)

    def _save_window_size(self) -> None:
        save_app_settings(
            {
                "window_width": str(self.width()),
                "window_height": str(self.height()),
            }
        )

    def closeEvent(self, event) -> None:
        self._save_window_size()
        super().closeEvent(event)

    def _build_ui(self) -> None:
        content = QWidget()
        root_layout = QVBoxLayout(content)

        status_controls = QGroupBox("Статус робота")
        status_layout = QGridLayout(status_controls)
        self.robot_status_label.setStyleSheet(
            "font-weight: bold; font-size: 15px; color: #555555;"
        )
        self.robot_mode_summary_label.setStyleSheet("font-weight: bold;")
        self.robot_account_summary_label.setAlignment(
            Qt.AlignmentFlag.AlignLeft
            | Qt.AlignmentFlag.AlignVCenter
        )
        status_layout.addWidget(self.robot_status_label, 0, 0)
        status_layout.addWidget(self.robot_mode_summary_label, 1, 0)

        status_layout.addWidget(
            self.robot_total_result_summary_label, 0, 1
        )
        status_layout.addWidget(
            self.robot_session_result_summary_label, 1, 1
        )

        status_layout.addWidget(
            self.robot_account_summary_label, 0, 2, 2, 1
        )

        status_layout.addWidget(
            self.robot_selected_shares_summary_label, 0, 3
        )
        status_layout.addWidget(
            self.robot_open_positions_summary_label, 1, 3
        )

        status_layout.setColumnStretch(0, 1)
        status_layout.setColumnStretch(1, 3)
        status_layout.setColumnStretch(2, 2)
        status_layout.setColumnStretch(3, 1)

        controls = QGroupBox("Подключение к T-Invest API")
        controls_layout = QGridLayout(controls)

        controls_layout.addWidget(QLabel("Токен:"), 0, 0)
        controls_layout.addWidget(self.token_edit, 0, 1, 1, 5)
        controls_layout.addWidget(QLabel("Account ID:"), 0, 6)
        controls_layout.addWidget(self.account_id_edit, 0, 7, 1, 3)

        for column in range(10):
            controls_layout.setColumnStretch(column, 1)
        self.account_id_edit.textChanged.connect(
            lambda text: self._handle_account_id_changed()
        )
        self.auto_trading_enabled_checkbox.toggled.connect(
            lambda checked: self._refresh_robot_summary()
        )

        self.accounts_button = QPushButton("Проверить подключение")
        balance_button = QPushButton("Получить баланс")
        active_orders_button = QPushButton("Активные заявки")
        self.shares_button = QPushButton("Загрузить акции")
        self.sell_all_robot_positions_button = QPushButton("Продать все позиции робота")
        self.sell_all_robot_positions_button.setToolTip(
            "Отправить реальные рыночные заявки на продажу "
            "только позиций, закреплённых за роботом."
        )

        self.accounts_button.clicked.connect(self.load_accounts)
        balance_button.clicked.connect(self.load_balance)
        active_orders_button.clicked.connect(self.load_active_orders)
        self.shares_button.clicked.connect(self.load_shares)
        self.sell_all_robot_positions_button.clicked.connect(
            self.start_sell_all_robot_positions
        )

        connection_buttons = (
            self.accounts_button,
            balance_button,
            active_orders_button,
            self.shares_button,
            self.sell_all_robot_positions_button,
        )

        for button in connection_buttons:
            button.setSizePolicy(
                QSizePolicy.Policy.Expanding,
                QSizePolicy.Policy.Fixed,
            )
            button.setMinimumHeight(32)
            button.setFont(self.font())
            button.setStyleSheet("")

        controls_layout.addWidget(self.accounts_button, 1, 0, 1, 2)
        controls_layout.addWidget(balance_button, 1, 2, 1, 2)
        controls_layout.addWidget(active_orders_button, 1, 4, 1, 2)
        controls_layout.addWidget(self.shares_button, 1, 6, 1, 2)
        controls_layout.addWidget(
            self.sell_all_robot_positions_button, 1, 8, 1, 2
        )

        self.qualified_investor_checkbox.toggled.connect(
            lambda checked: self.refresh_shares_after_filter_change()
        )
        self.only_liquid_shares_checkbox.toggled.connect(
            lambda checked: self.refresh_shares_after_filter_change()
        )

        self.clear_selected_shares_button = QPushButton("Очистить рабочие акции")
        self.clear_selected_shares_button.clicked.connect(self.clear_selected_shares)
        self.update_robot_positions_button.clicked.connect(self.update_robot_positions_from_table)

        strategy_controls = QGroupBox("Настройки стратегии")
        strategy_layout = QGridLayout(strategy_controls)

        strategy_layout.addWidget(QLabel("Лимит денег RUB:"), 0, 0)
        strategy_layout.addWidget(self.bot_money_limit_edit, 0, 1)
        strategy_layout.addWidget(QLabel("Сумма автопокупки RUB:"), 0, 2)
        strategy_layout.addWidget(self.auto_buy_amount_edit, 0, 3)
        strategy_layout.addWidget(QLabel("Интервал расчёта роста:"), 0, 4)
        strategy_layout.addWidget(self.growth_candle_interval_combo, 0, 5)

        strategy_layout.addWidget(QLabel("Лимит денег USD:"), 1, 0)
        strategy_layout.addWidget(self.bot_money_limit_usd_edit, 1, 1)
        strategy_layout.addWidget(QLabel("Сумма автопокупки USD:"), 1, 2)
        strategy_layout.addWidget(self.auto_buy_amount_usd_edit, 1, 3)
        strategy_layout.addWidget(QLabel("Интервал проверки, сек:"), 1, 4)
        strategy_layout.addWidget(self.scan_interval_seconds_edit, 1, 5)

        strategy_layout.addWidget(QLabel("Лимит денег EUR:"), 2, 0)
        strategy_layout.addWidget(self.bot_money_limit_eur_edit, 2, 1)
        strategy_layout.addWidget(QLabel("Сумма автопокупки EUR:"), 2, 2)
        strategy_layout.addWidget(self.auto_buy_amount_eur_edit, 2, 3)
        strategy_layout.addWidget(QLabel("Макс. возраст цены, сек:"), 2, 4)
        strategy_layout.addWidget(self.max_price_age_seconds_edit, 2, 5)

        strategy_separator = QFrame()
        strategy_separator.setFrameShape(QFrame.Shape.HLine)
        strategy_separator.setFrameShadow(QFrame.Shadow.Sunken)
        strategy_layout.addWidget(strategy_separator, 3, 0, 1, 6)

        strategy_layout.addWidget(QLabel("Купить при росте, %:"), 4, 0)
        strategy_layout.addWidget(self.growth_percent_edit, 4, 1)
        strategy_layout.addWidget(QLabel("Продать при прибыли, %:"), 4, 2)
        strategy_layout.addWidget(self.take_profit_percent_edit, 4, 3)
        strategy_layout.addWidget(QLabel("Продать при убытке, %:"), 4, 4)
        strategy_layout.addWidget(self.stop_loss_percent_edit, 4, 5)

        self.database_maintenance_button = QPushButton("Обслуживание БД")
        self.database_maintenance_button.setToolTip(
            "Проверить целостность, обрезать WAL и физически сжать SQLite."
        )
        self.database_maintenance_button.clicked.connect(
            self.start_database_maintenance
        )

        strategy_options_widget = QWidget()
        strategy_options_layout = QHBoxLayout(strategy_options_widget)
        strategy_options_layout.setContentsMargins(0, 0, 0, 0)

        self.auto_trading_enabled_checkbox.setText("Боевой режим")
        self.auto_trading_enabled_checkbox.setStyleSheet(
            "font-weight: bold; color: #8a1f11;"
        )

        strategy_option_checkboxes = (
            self.auto_trading_enabled_checkbox,
            self.qualified_investor_checkbox,
            self.only_liquid_shares_checkbox,
            self.allow_buy_checkbox,
            self.allow_sell_checkbox,
        )

        for checkbox in strategy_option_checkboxes:
            strategy_options_layout.addWidget(checkbox)

        strategy_options_layout.addStretch(1)
        strategy_layout.addWidget(
            strategy_options_widget, 5, 0, 1, 6
        )

        self.robot_toggle_button = QPushButton("Включить и синхронизировать")
        self.robot_toggle_button.setCheckable(True)
        self.robot_toggle_button.toggled.connect(self.toggle_robot_monitoring)
        self._set_robot_visual_state("stopped")

        self.save_state_button = QPushButton("Сохранить настройки")
        self.save_state_button.clicked.connect(self.save_current_state)

        self.reset_state_button = QPushButton("Сбросить настройки")
        self.reset_state_button.clicked.connect(self.reset_current_state)

        strategy_actions_widget = QWidget()
        strategy_actions_layout = QHBoxLayout(strategy_actions_widget)
        strategy_actions_layout.setContentsMargins(0, 0, 0, 0)

        strategy_action_buttons = (
            self.robot_toggle_button,
            self.save_state_button,
            self.reset_state_button,
            self.database_maintenance_button,
        )

        for button in strategy_action_buttons:
            button.setSizePolicy(
                QSizePolicy.Policy.Expanding,
                QSizePolicy.Policy.Fixed,
            )
            button.setMinimumHeight(32)
            strategy_actions_layout.addWidget(button, 1)

        strategy_layout.addWidget(
            strategy_actions_widget, 6, 0, 1, 6
        )

        strategy_layout.setColumnStretch(1, 1)
        strategy_layout.setColumnStretch(3, 1)
        strategy_layout.setColumnStretch(5, 1)

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

        selected_shares_tab_layout = QVBoxLayout(self.selected_shares_tab_widget)
        selected_shares_tab_layout.addWidget(self.selected_shares_table)
        selected_shares_tab_layout.addWidget(self.clear_selected_shares_button)

        self.shares_search_edit.textChanged.connect(
            lambda text: self.apply_shares_search_filter()
        )
        self.shares_table.itemChanged.connect(
            lambda item: self.apply_shares_search_filter()
        )

        self.manual_market_buy_button = QPushButton("Купить рынком")
        self.manual_limit_buy_button = QPushButton("Купить лимитом")
        self.manual_market_sell_button = QPushButton("Продать рынком")
        self.manual_limit_sell_button = QPushButton("Продать лимитом")

        self.manual_market_buy_button.clicked.connect(self.manual_market_buy)
        self.manual_limit_buy_button.clicked.connect(self.manual_limit_buy)
        self.manual_market_sell_button.clicked.connect(self.manual_market_sell)
        self.manual_limit_sell_button.clicked.connect(self.manual_limit_sell)
        self.manual_last_price_button.clicked.connect(self.refresh_manual_last_price)
        self.manual_mode_checkbox.toggled.connect(
            lambda checked: self._refresh_manual_trade_buttons_state()
        )

        manual_trading_controls = QGroupBox("Ручная торговля (проверка и аварийное управление)")
        manual_trading_controls_layout = QGridLayout(manual_trading_controls)

        manual_trading_controls_layout.addWidget(QLabel("Инструмент:"), 0, 0)
        manual_trading_controls_layout.addWidget(self.manual_instrument_id_edit, 0, 1, 1, 2)
        manual_trading_controls_layout.addWidget(self.manual_mode_checkbox, 0, 3)

        manual_trading_controls_layout.addWidget(
            QLabel("Сумма ручной покупки (валюта инструмента):"), 1, 0
        )
        manual_trading_controls_layout.addWidget(self.manual_buy_amount_edit, 1, 1)

        manual_trading_controls_layout.addWidget(QLabel("Объём продажи, лоты:"), 1, 2)
        manual_trading_controls_layout.addWidget(self.manual_sell_lots_edit, 1, 3)

        manual_trading_controls_layout.addWidget(QLabel("Отступ лимитной заявки, шагов:"), 2, 0)
        manual_trading_controls_layout.addWidget(self.manual_limit_offset_edit, 2, 1)

        manual_trading_controls_layout.addWidget(self.manual_last_price_button, 2, 2)
        manual_trading_controls_layout.addWidget(self.manual_last_price_label, 2, 3)

        manual_trade_buttons_widget = QWidget()
        manual_trade_buttons_layout = QHBoxLayout(
            manual_trade_buttons_widget
        )
        manual_trade_buttons_layout.setContentsMargins(0, 0, 0, 0)

        manual_trade_buttons = (
            self.manual_market_buy_button,
            self.manual_limit_buy_button,
            self.manual_market_sell_button,
            self.manual_limit_sell_button,
        )

        for button in manual_trade_buttons:
            button.setSizePolicy(
                QSizePolicy.Policy.Expanding,
                QSizePolicy.Policy.Fixed,
            )
            button.setMinimumHeight(32)
            button.setFont(self.font())
            button.setStyleSheet("")
            manual_trade_buttons_layout.addWidget(button, 1)

        manual_trading_controls_layout.addWidget(
            manual_trade_buttons_widget, 3, 0, 1, 4
        )

        info_layout = QVBoxLayout(self.info_tab_widget)
        self.info_title_label.setStyleSheet("font-weight: bold;")
        self.info_title_label.setVisible(False)
        info_layout.addWidget(self.startup_hint_label)
        info_layout.addWidget(self.info_title_label)
        info_layout.addWidget(self.accounts_table)
        info_layout.addWidget(self.money_table)
        info_layout.addWidget(self.positions_table)
        info_layout.addWidget(self.orders_table)

        active_orders_actions_layout = QGridLayout(self.active_orders_actions_widget)
        active_orders_actions_layout.addWidget(self.cancel_active_order_button, 0, 0)
        active_orders_actions_layout.addWidget(self.cancel_all_limit_orders_button, 0, 1)
        info_layout.addWidget(self.active_orders_actions_widget)

        self.cancel_active_order_button.clicked.connect(self.cancel_selected_active_order)
        self.cancel_all_limit_orders_button.clicked.connect(self.cancel_all_active_limit_orders)
        self._hide_info_tables()

        self.tabs = QTabWidget()
        self.tabs.addTab(self.info_tab_widget, "Инфо")
        self.tabs.addTab(self.shares_tab_widget, "Акции")
        self.tabs.addTab(self.selected_shares_tab_widget, "Рабочие акции")

        self.cancel_robot_start_button.clicked.connect(self.cancel_robot_start_after_sync)

        robot_positions_tab_layout = QVBoxLayout(self.robot_positions_tab_widget)
        robot_positions_tab_layout.addWidget(self.robot_positions_table)

        robot_positions_actions_layout = QGridLayout()
        robot_positions_actions_layout.addWidget(self.update_robot_positions_button, 0, 0)
        robot_positions_actions_layout.addWidget(self.cancel_robot_start_button, 0, 1)
        robot_positions_tab_layout.addLayout(robot_positions_actions_layout)

        self.monitoring_tabs.setStyleSheet(
            """
            QTabBar::tab {
                padding: 6px 12px;
                color: #555555;
            }
            QTabBar::tab:selected {
                font-weight: bold;
                color: #111111;
                background-color: #e8f0fe;
                border: 1px solid #8aa7e8;
                border-bottom: 3px solid #2f5fcb;
            }
            QTabBar::tab:!selected {
                background-color: #f3f3f3;
            }
            """
        )
        self.monitoring_tabs.addTab(self.growth_current_table, "Рост сейчас")
        self.monitoring_tabs.addTab(self.growth_signals_table, "Сигналы")
        self.monitoring_tabs.addTab(self.buy_intents_table, "Планы покупок")
        self.monitoring_tabs.addTab(self.robot_orders_table, "Заявки")
        self.monitoring_tabs.addTab(self.robot_positions_tab_widget, "Позиции")
        self.monitoring_tabs.addTab(self.growth_cycles_table, "Циклы сканирования")
        self.tabs.addTab(self.monitoring_tabs, "Мониторинг")

        log_tab_layout = QVBoxLayout(self.log_tab_widget)
        log_actions_layout = QGridLayout()
        log_actions_layout.addWidget(
            self.clear_log_button, 0, 0
        )
        log_actions_layout.setColumnStretch(1, 1)
        log_tab_layout.addLayout(log_actions_layout)
        log_tab_layout.addWidget(self.log_edit)
        self.tabs.addTab(self.log_tab_widget, "Лог")

        root_layout.addWidget(status_controls)
        root_layout.addWidget(controls)
        root_layout.addWidget(strategy_controls)
        root_layout.addWidget(manual_trading_controls)
        root_layout.addWidget(self.tabs)

        scroll_area = QScrollArea(self)
        scroll_area.setWidgetResizable(True)
        scroll_area.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded
        )
        scroll_area.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded
        )
        scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        scroll_area.setWidget(content)
        self.setCentralWidget(scroll_area)

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

        if "take_profit_percent" in settings:
            self.take_profit_percent_edit.setText(settings["take_profit_percent"])

        if "stop_loss_percent" in settings:
            self.stop_loss_percent_edit.setText(settings["stop_loss_percent"])

        if "bot_money_limit_rub" in settings:
            self.bot_money_limit_edit.setText(settings["bot_money_limit_rub"])

        if "auto_buy_amount_rub" in settings:
            self.auto_buy_amount_edit.setText(settings["auto_buy_amount_rub"])

        if "bot_money_limit_usd" in settings:
            self.bot_money_limit_usd_edit.setText(settings["bot_money_limit_usd"])

        if "auto_buy_amount_usd" in settings:
            self.auto_buy_amount_usd_edit.setText(settings["auto_buy_amount_usd"])

        if "bot_money_limit_eur" in settings:
            self.bot_money_limit_eur_edit.setText(settings["bot_money_limit_eur"])

        if "auto_buy_amount_eur" in settings:
            self.auto_buy_amount_eur_edit.setText(settings["auto_buy_amount_eur"])

        if "manual_instrument_id" in settings:
            self.manual_instrument_id_edit.setText(settings["manual_instrument_id"])

        if "manual_buy_amount" in settings:
            self.manual_buy_amount_edit.setText(settings["manual_buy_amount"])

        if "manual_sell_lots" in settings:
            self.manual_sell_lots_edit.setText(settings["manual_sell_lots"])

        if "manual_limit_offset" in settings:
            self.manual_limit_offset_edit.setText(settings["manual_limit_offset"])

        if "client_is_qualified" in settings:
            self.qualified_investor_checkbox.setChecked(
                settings["client_is_qualified"] == "1"
            )

        if "only_liquid_shares" in settings:
            self.only_liquid_shares_checkbox.setChecked(
                settings["only_liquid_shares"] == "1"
            )

        self.manual_mode_checkbox.setChecked(False)
        self.auto_trading_enabled_checkbox.setChecked(False)

        if "allow_buy" in settings:
            self.allow_buy_checkbox.setChecked(settings["allow_buy"] == "1")

        if "allow_sell" in settings:
            self.allow_sell_checkbox.setChecked(settings["allow_sell"] == "1")

        self.refresh_shares_filters_label()
        self._refresh_robot_summary()

    def save_current_state(self) -> None:
        settings = {
            "token": self.token_edit.text().strip(),
            "account_id": self.account_id_edit.text().strip(),
            "client_is_qualified": "1" if self.qualified_investor_checkbox.isChecked() else "0",
            "only_liquid_shares": "1" if self.only_liquid_shares_checkbox.isChecked() else "0",
            "growth_percent": self.growth_percent_edit.text().strip(),
            "growth_candle_interval": self.growth_candle_interval_combo.currentText(),
            "scan_interval_seconds": self.scan_interval_seconds_edit.text().strip(),
            "max_price_age_seconds": self.max_price_age_seconds_edit.text().strip(),
            "take_profit_percent": self.take_profit_percent_edit.text().strip(),
            "stop_loss_percent": self.stop_loss_percent_edit.text().strip(),
            "bot_money_limit_rub": self.bot_money_limit_edit.text().strip(),
            "auto_buy_amount_rub": self.auto_buy_amount_edit.text().strip(),
            "bot_money_limit_usd": self.bot_money_limit_usd_edit.text().strip(),
            "auto_buy_amount_usd": self.auto_buy_amount_usd_edit.text().strip(),
            "bot_money_limit_eur": self.bot_money_limit_eur_edit.text().strip(),
            "auto_buy_amount_eur": self.auto_buy_amount_eur_edit.text().strip(),
            "manual_mode": "0",
            "auto_trading_enabled": "1" if self.auto_trading_enabled_checkbox.isChecked() else "0",
            "allow_buy": "1" if self.allow_buy_checkbox.isChecked() else "0",
            "allow_sell": "1" if self.allow_sell_checkbox.isChecked() else "0",
            "manual_instrument_id": self.manual_instrument_id_edit.text().strip(),
            "manual_buy_amount": self.manual_buy_amount_edit.text().strip(),
            "manual_sell_lots": self.manual_sell_lots_edit.text().strip(),
            "manual_limit_offset": self.manual_limit_offset_edit.text().strip(),
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
        self.only_liquid_shares_checkbox.setChecked(True)
        self.manual_mode_checkbox.setChecked(False)
        self.auto_trading_enabled_checkbox.setChecked(False)

        self.allow_buy_checkbox.setChecked(True)
        self.allow_sell_checkbox.setChecked(True)

        self.growth_percent_edit.setText("1.00")
        self.growth_candle_interval_combo.setCurrentText("1 минута")
        self.scan_interval_seconds_edit.setText("10")
        self.max_price_age_seconds_edit.setText("30")

        self.take_profit_percent_edit.setText("1.00")
        self.stop_loss_percent_edit.setText("1.00")
        self.bot_money_limit_edit.setText("0")
        self.auto_buy_amount_edit.setText("0")
        self.bot_money_limit_usd_edit.setText("0")
        self.auto_buy_amount_usd_edit.setText("0")
        self.bot_money_limit_eur_edit.setText("0")
        self.auto_buy_amount_eur_edit.setText("0")

        self.manual_instrument_id_edit.setText("SBER_TQBR")
        self.manual_buy_amount_edit.setText("10000.00")
        self.manual_sell_lots_edit.setText("1")
        self.manual_limit_offset_edit.setText("0")

        self.robot_is_running = False
        self.robot_status_label.setText("Робот: выключен")
        self._set_robot_visual_state("stopped")
        self._set_robot_inputs_locked(False)

        self.selected_shares_by_uid.clear()
        self.refresh_selected_shares_table()
        self.refresh_available_shares_table()
        self.refresh_shares_filters_label()
        self._refresh_robot_summary()
        self._refresh_manual_trade_buttons_state()

        self._log("Настройки сброшены.")
        self._log("Рабочий список акций очищен.")
        self._log("Покупки разрешены: да")
        self._log("Продажи разрешены: да")

    def _share_passes_fixed_filters(
        self,
        share: TBankShare,
        client_is_qualified: bool,
        only_liquid_shares: bool,
    ) -> bool:
        if share.real_exchange != MOEX_REAL_EXCHANGE:
            return False

        if share.currency.upper() != MOEX_SHARE_CURRENCY:
            return False

        if not share.api_trade_available_flag:
            return False

        if not share.buy_available_flag:
            return False

        if not share.sell_available_flag:
            return False

        if share.blocked_tca_flag:
            return False

        if share.for_qual_investor_flag and not client_is_qualified:
            return False

        if only_liquid_shares and not share.liquidity_flag:
            return False

        return True

    def _get_shares_filter_text(
        self,
        client_is_qualified: bool,
        only_liquid_shares: bool,
    ) -> str:
        qualified_text = (
            "инструменты для квалов допускаются"
            if client_is_qualified
            else "инструменты только для квалов исключены"
        )
        liquidity_text = (
            "только ликвидные"
            if only_liquid_shares
            else "ликвидность не ограничивается"
        )

        return (
            "Жёсткие фильтры: Московская биржа; RUB; "
            "торговля через API; покупка и продажа доступны; "
            "инструмент не заблокирован. "
            f"Дополнительно: {liquidity_text}; {qualified_text}."
        )

    def refresh_shares_filters_label(self) -> None:
        self.shares_filters_label.setText(
            self._get_shares_filter_text(
                self.qualified_investor_checkbox.isChecked(),
                self.only_liquid_shares_checkbox.isChecked(),
            )
        )

    def refresh_shares_after_filter_change(self) -> None:
        self.refresh_shares_filters_label()

        if not self.all_shares:
            return

        client_is_qualified = self.qualified_investor_checkbox.isChecked()
        only_liquid_shares = self.only_liquid_shares_checkbox.isChecked()

        self.available_shares = self._filter_available_shares(
            shares=self.all_shares,
            client_is_qualified=client_is_qualified,
            only_liquid_shares=only_liquid_shares,
        )

        self.refresh_available_shares_table()
        self._sync_selected_shares_with_available()

        self._log(
            f"Рабочих акций после изменения фильтров: {len(self.available_shares)}"
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
            "Купить при росте, %",
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
            raise ValueError(
                "Интервал проверки должен быть целым числом секунд."
            ) from error

        if scan_interval_seconds <= 0:
            raise ValueError("Интервал проверки должен быть больше 0.")

        max_price_age_seconds_raw = self.max_price_age_seconds_edit.text().strip()

        try:
            max_price_age_seconds = int(max_price_age_seconds_raw)
        except ValueError as error:
            raise ValueError(
                "Максимальный возраст цены должен быть целым числом секунд."
            ) from error

        if max_price_age_seconds <= 0:
            raise ValueError("Максимальный возраст цены должен быть больше 0.")

        take_profit_percent = self._parse_decimal_field(
            self.take_profit_percent_edit,
            "Продать при прибыли, %",
        )
        stop_loss_percent = self._parse_decimal_field(
            self.stop_loss_percent_edit,
            "Продать при убытке, %",
        )
        bot_money_limit_rub = self._parse_decimal_field(
            self.bot_money_limit_edit,
            "Лимит денег для бота RUB",
        )
        auto_buy_amount_rub = self._parse_decimal_field(
            self.auto_buy_amount_edit,
            "Сумма автопокупки RUB",
        )
        bot_money_limit_usd = self._parse_decimal_field(
            self.bot_money_limit_usd_edit,
            "Лимит денег для бота USD",
        )
        auto_buy_amount_usd = self._parse_decimal_field(
            self.auto_buy_amount_usd_edit,
            "Сумма автопокупки USD",
        )
        bot_money_limit_eur = self._parse_decimal_field(
            self.bot_money_limit_eur_edit,
            "Лимит денег для бота EUR",
        )
        auto_buy_amount_eur = self._parse_decimal_field(
            self.auto_buy_amount_eur_edit,
            "Сумма автопокупки EUR",
        )

        positive_values = {
            "Рост для покупки": growth_percent,
            "Процент прибыли": take_profit_percent,
            "Процент убытка": stop_loss_percent,
        }

        for label, value in positive_values.items():
            if value <= 0:
                raise ValueError(f"{label} должен быть больше 0.")

        non_negative_values = {
            "Лимит RUB": bot_money_limit_rub,
            "Автопокупка RUB": auto_buy_amount_rub,
            "Лимит USD": bot_money_limit_usd,
            "Автопокупка USD": auto_buy_amount_usd,
            "Лимит EUR": bot_money_limit_eur,
            "Автопокупка EUR": auto_buy_amount_eur,
        }

        for label, value in non_negative_values.items():
            if value < 0:
                raise ValueError(f"{label} не может быть меньше 0.")

        return {
            "growth_percent": growth_percent,
            "growth_candle_interval": growth_candle_interval,
            "growth_candle_interval_value": GROWTH_CANDLE_INTERVALS[
                growth_candle_interval
            ],
            "scan_interval_seconds": scan_interval_seconds,
            "max_price_age_seconds": max_price_age_seconds,
            "take_profit_percent": take_profit_percent,
            "stop_loss_percent": stop_loss_percent,
            "bot_money_limit_rub": bot_money_limit_rub,
            "auto_buy_amount_rub": auto_buy_amount_rub,
            "bot_money_limit_usd": bot_money_limit_usd,
            "auto_buy_amount_usd": auto_buy_amount_usd,
            "bot_money_limit_eur": bot_money_limit_eur,
            "auto_buy_amount_eur": auto_buy_amount_eur,
            "auto_trading_enabled": self.auto_trading_enabled_checkbox.isChecked(),
        }

    def _set_robot_inputs_locked(self, locked: bool) -> None:
        enabled = not locked

        widgets = [
            self.token_edit,
            self.account_id_edit,
            self.qualified_investor_checkbox,
            self.only_liquid_shares_checkbox,
            self.growth_percent_edit,
            self.growth_candle_interval_combo,
            self.scan_interval_seconds_edit,
            self.max_price_age_seconds_edit,
            self.database_maintenance_button,
            self.take_profit_percent_edit,
            self.stop_loss_percent_edit,
            self.bot_money_limit_edit,
            self.auto_buy_amount_edit,
            self.bot_money_limit_usd_edit,
            self.auto_buy_amount_usd_edit,
            self.bot_money_limit_eur_edit,
            self.auto_buy_amount_eur_edit,
            self.manual_mode_checkbox,
            self.auto_trading_enabled_checkbox,
            self.allow_buy_checkbox,
            self.allow_sell_checkbox,
            self.manual_instrument_id_edit,
            self.manual_buy_amount_edit,
            self.manual_sell_lots_edit,
            self.manual_limit_offset_edit,
            self.manual_last_price_button,
            self.accounts_button,
            self.shares_button,
            self.sell_all_robot_positions_button,
            self.apply_checked_shares_button,
            self.clear_selected_shares_button,
            self.save_state_button,
            self.reset_state_button,
            self.manual_market_buy_button,
            self.manual_limit_buy_button,
            self.manual_market_sell_button,
            self.manual_limit_sell_button,
            self.shares_table,
            self.selected_shares_table,
        ]

        for widget in widgets:
            widget.setEnabled(enabled)

        self._refresh_manual_trade_buttons_state()

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
        self.pending_robot_start_settings = None
        self.pending_robot_start_account = None
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

        if settings["auto_trading_enabled"]:
            answer = QMessageBox.question(
                self,
                "Реальная автоторговля",
                (
                    "Вы включаете реальную автоторговлю. "
                    "Робот будет отправлять реальные рыночные заявки.\n\n"
                    "Продолжить?"
                ),
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )

            if answer != QMessageBox.StandardButton.Yes:
                self._log("Запуск реальной автоторговли отменён пользователем.")
                self._set_robot_visual_state("stopped")
                return

        selected_shares = list(self.selected_shares_by_uid.values())

        if not selected_shares:
            self._reject_robot_start(
                "Рабочий список акций пуст. "
                "Сначала нажмите 'Загрузить акции', выберите рабочие акции "
                "и сохраните/обновите рабочий список."
            )
            return

        self._set_robot_inputs_locked(True)
        self._set_robot_visual_state("starting")
        self._log("Проверяю token/account_id через T-Invest API.")
        self._log(
            "Использую сохранённый рабочий список акций. "
            "Полный справочник акций на старте робота не загружается."
        )
        self._log("Синхронизирую позиции робота с брокером.")

        async def call_api_with_timeout(coro, timeout_seconds: float, stage: str):
            try:
                return await asyncio.wait_for(coro, timeout=timeout_seconds)
            except TimeoutError as error:
                raise TimeoutError(
                    f"API не ответил за {timeout_seconds:.0f} сек. Этап: {stage}."
                ) from error

        async def task():
            async with AsyncClient(token) as client:
                accounts = await call_api_with_timeout(
                    get_accounts(client),
                    timeout_seconds=20,
                    stage="проверка аккаунтов",
                )
                account_exists = any(
                    account.account_id == account_id
                    for account in accounts
                )

                if not account_exists:
                    return accounts, selected_shares, []

                positions = await call_api_with_timeout(
                    get_portfolio_positions(client, account_id),
                    timeout_seconds=20,
                    stage="загрузка портфеля / синхронизация позиций",
                )

                return accounts, selected_shares, positions

        def on_success(result: tuple[list[TBankAccount], list[TBankShare], list[TBankPortfolioPosition]]) -> None:
            self._handle_robot_start_validation_success(
                result=result,
                account_id=account_id,
                settings=settings,
                client_is_qualified=self.qualified_investor_checkbox.isChecked(),
                only_liquid_shares=self.only_liquid_shares_checkbox.isChecked(),
            )

        self._run_async_task(
            "robot_start_validation",
            task,
            on_success,
            self._handle_robot_start_validation_error,
        )

    def _handle_robot_start_validation_success(
        self,
        result: tuple[list[TBankAccount], list[TBankShare], list[TBankPortfolioPosition]],
        account_id: str,
        settings: dict[str, object],
        client_is_qualified: bool,
        only_liquid_shares: bool,
    ) -> None:
        accounts, selected_shares, broker_positions = result

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

        if not selected_shares:
            self._reject_robot_start(
                "Рабочий список акций пуст. "
                "Сначала нажмите 'Загрузить акции', выберите рабочие акции "
                "и сохраните/обновите рабочий список."
            )
            return

        self.selected_shares_by_uid = {
            share.uid: share
            for share in selected_shares
        }

        self.refresh_selected_shares_table()

        sync_report = sync_robot_positions_with_broker(
            account_id=account_id,
            broker_positions=broker_positions,
            shares=selected_shares,
        )
        self.pending_robot_start_settings = settings
        self.pending_robot_start_account = account
        self.refresh_robot_positions_table()
        self.monitoring_tabs.setCurrentWidget(self.robot_positions_tab_widget)
        self.tabs.setCurrentWidget(self.monitoring_tabs)
        self._set_robot_visual_state("positions_review")
        self._log(
            "Позиции робота синхронизированы перед запуском: "
            f"проверено={sync_report.checked_count}, "
            f"уменьшено={sync_report.reduced_count}, "
            f"обнулено={sync_report.zeroed_count}."
        )
        self._log(
            "Проверьте колонку 'Лотов у робота' и нажмите "
            "'Обновить позиции и запустить робота'."
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

        self.pending_robot_start_settings = None
        self.pending_robot_start_account = None
        self.robot_session_started_at_utc = datetime.now(timezone.utc)
        self.robot_session_finished_at_utc = None

        self.robot_is_running = True
        self._set_robot_inputs_locked(True)
        self._set_robot_visual_state("running")

        self._log("Проверка token/account_id через API пройдена.")
        self._log(f"Account ID подтверждён: {account.account_id} / {account.name}")
        if settings["auto_trading_enabled"]:
            self._log(
                "Робот включён. Режим: РЕАЛЬНАЯ АВТОТОРГОВЛЯ рыночными заявками."
            )
        else:
            self._log("Робот включён. Режим: тестирование без отправки заявок.")
        self._log(f"Рабочих акций: {len(self.selected_shares_by_uid)}")
        self._log(f"Рост для покупки: {settings['growth_percent']}%")
        self._log(
            f"Интервал расчёта роста: {settings['growth_candle_interval']}"
        )
        self._log(f"Интервал проверки: {settings['scan_interval_seconds']} сек.")
        self._log(f"Макс. возраст цены: {settings['max_price_age_seconds']} сек.")
        self._log(
            "Лимиты бота: "
            f"RUB={settings['bot_money_limit_rub']}, "
            f"USD={settings['bot_money_limit_usd']}, "
            f"EUR={settings['bot_money_limit_eur']}."
        )
        self._log(
            "Суммы автопокупки: "
            f"RUB={settings['auto_buy_amount_rub']}, "
            f"USD={settings['auto_buy_amount_usd']}, "
            f"EUR={settings['auto_buy_amount_eur']}."
        )
        if settings["auto_trading_enabled"]:
            self._log(
                "Реальная автоторговля включена: входы и выходы выполняются "
                "рыночными заявками."
            )
        else:
            self._log(
                "Реальные торговые заявки не отправляются. "
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
        self.refresh_runtime_balance()

    def stop_robot_placeholder(self) -> None:
        if self.growth_monitor_worker is None:
            self.robot_is_running = False
            self.pending_robot_start_settings = None
            self.pending_robot_start_account = None
            self._set_robot_inputs_locked(False)
            self._set_robot_visual_state("stopped")
            self._log("Мониторинг не был запущен.")
            return

        self._set_robot_visual_state("stopping")
        self._log("Остановка мониторинга запрошена.")
        self.growth_monitor_worker.stop()

    def _handle_growth_monitor_log_message(self, message: str) -> None:
        self._log(message)

        if message == "Портфель робота изменён: обновляю свободные средства.":
            self.refresh_runtime_balance()

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
        self.refresh_robot_orders_table()
        self.refresh_robot_positions_table()
        self.refresh_growth_cycles_table()
        self._refresh_robot_summary()

    def refresh_growth_current_table(self) -> None:
        fill_growth_current_table(self.growth_current_table)

    def refresh_growth_signals_table(self) -> None:
        fill_growth_signals_table(self.growth_signals_table)

    def refresh_buy_intents_table(self) -> None:
        fill_buy_intents_table(self.buy_intents_table)

    def refresh_robot_orders_table(self) -> None:
        fill_robot_orders_table(self.robot_orders_table)

    def refresh_robot_positions_table(self) -> None:
        fill_robot_positions_table(self.robot_positions_table)

    def refresh_growth_cycles_table(self) -> None:
        fill_growth_cycles_table(self.growth_cycles_table)

    def _on_growth_monitor_finished(self) -> None:
        self._finish_robot_session()
        self.robot_is_running = False
        self.pending_robot_start_settings = None
        self.pending_robot_start_account = None
        self._set_robot_inputs_locked(False)
        self._set_robot_visual_state("stopped")
        self._refresh_robot_summary()
        self._log("Мониторинг остановлен.")

    def _on_growth_monitor_failed(self, error_text: str) -> None:
        self._finish_robot_session()
        self.robot_is_running = False
        self.pending_robot_start_settings = None
        self.pending_robot_start_account = None
        self._set_robot_inputs_locked(False)
        self._set_robot_visual_state("error")
        self._refresh_robot_summary()
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

        if hasattr(self, "update_robot_positions_button"):
            self.update_robot_positions_button.setEnabled(False)

        if hasattr(self, "cancel_robot_start_button"):
            self.cancel_robot_start_button.setEnabled(False)

        if state == "starting":
            self.robot_is_running = False
            self.robot_status_label.setText("Робот: синхронизация позиций")
            self.robot_status_label.setStyleSheet(
                "font-weight: bold; color: #8a5a00;"
            )
            self.robot_toggle_button.setChecked(True)
            self.robot_toggle_button.setEnabled(False)
            self.robot_toggle_button.setText("Синхронизация позиций...")
            self.robot_toggle_button.setToolTip(
                "Проверяем token, account_id, рабочие акции и синхронизируем позиции."
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

        elif state == "positions_review":
            self.robot_is_running = False
            self.robot_status_label.setText("Робот: проверьте позиции")
            self.robot_status_label.setStyleSheet(
                "font-weight: bold; color: #8a5a00;"
            )
            self.robot_toggle_button.setChecked(False)
            self.robot_toggle_button.setEnabled(False)
            self.robot_toggle_button.setText("Позиции синхронизированы")
            self.robot_toggle_button.setToolTip(
                "Проверьте позиции робота и нажмите 'Обновить позиции и запустить робота'."
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

            if hasattr(self, "update_robot_positions_button"):
                self.update_robot_positions_button.setEnabled(True)

            if hasattr(self, "cancel_robot_start_button"):
                self.cancel_robot_start_button.setEnabled(True)

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
            self.robot_toggle_button.setText("Ошибка. Включить и синхронизировать")
            self.robot_toggle_button.setToolTip(
                "Робот остановлен из-за ошибки. Нажмите, чтобы повторить синхронизацию и запуск."
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
            self.robot_toggle_button.setText("Включить и синхронизировать")
            self.robot_toggle_button.setToolTip(
                "Робот выключен. Нажмите, чтобы синхронизировать позиции перед запуском."
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

        self._refresh_robot_summary()
        self._refresh_manual_trade_buttons_state()

    def _cleanup_growth_monitor_worker(self) -> None:
        self.growth_monitor_thread = None
        self.growth_monitor_worker = None

    def _get_manual_instrument_id(self) -> str:
        instrument_id = self.manual_instrument_id_edit.text().strip()

        if not instrument_id:
            raise ValueError("Ручной инструмент не может быть пустым.")

        return instrument_id

    def _matches_manual_instrument_id(
        self,
        share: TBankShare,
        instrument_id: str,
    ) -> bool:
        normalized_instrument_id = instrument_id.strip().upper()

        return (
            share.uid == instrument_id
            or share.figi.upper() == normalized_instrument_id
            or f"{share.ticker}_{share.class_code}".upper() == normalized_instrument_id
            or (
                share.class_code == "TQBR"
                and share.ticker.upper() == normalized_instrument_id
            )
        )

    def _find_share_by_manual_instrument_id(
        self,
        shares: list[TBankShare],
        instrument_id: str,
    ) -> TBankShare:
        for share in shares:
            if self._matches_manual_instrument_id(share, instrument_id):
                return share

        raise ValueError(f"Акция не найдена: {instrument_id}")

    def _validate_manual_trade_share(
        self,
        share: TBankShare,
        client_is_qualified: bool,
        side: str | None = None,
    ) -> None:
        if share.real_exchange != MOEX_REAL_EXCHANGE:
            raise ValueError(
                "Робот работает только с акциями Московской биржи."
            )

        if share.currency.upper() != MOEX_SHARE_CURRENCY:
            raise ValueError(
                "Робот работает только с акциями, торгуемыми в рублях."
            )

        if not share.api_trade_available_flag:
            raise ValueError("По инструменту недоступна торговля через API.")

        if share.blocked_tca_flag:
            raise ValueError("Инструмент заблокирован для торговли.")

        if share.for_qual_investor_flag and not client_is_qualified:
            raise ValueError(
                "Инструмент доступен только квалифицированному инвестору."
            )

        if side == "BUY" and not share.buy_available_flag:
            raise ValueError("Покупка по этому инструменту недоступна.")

        if side == "SELL" and not share.sell_available_flag:
            raise ValueError("Продажа по этому инструменту недоступна.")

    def _get_current_manual_trade_share(self, side: str) -> TBankShare:
        instrument_id = self._get_manual_instrument_id()
        shares = list(self.all_shares)

        if not shares:
            shares = list(self.selected_shares_by_uid.values())

        if not shares:
            raise ValueError(
                "Список акций пуст. Сначала нажмите 'Получить акции' "
                "или добавьте инструмент в рабочие акции."
            )

        share = self._find_share_by_manual_instrument_id(
            shares=shares,
            instrument_id=instrument_id,
        )
        self._validate_manual_trade_share(
            share=share,
            client_is_qualified=self.qualified_investor_checkbox.isChecked(),
            side=side,
        )

        return share

    def _get_available_money(
        self,
        balance: PortfolioBalance,
        currency: str,
    ) -> Decimal:
        clean_currency = currency.strip().upper()

        for money in balance.money:
            if money.currency.upper() == clean_currency:
                return money.available

        return Decimal("0")

    def _parse_manual_limit_offset(self) -> Decimal:
        offset_steps = self._parse_decimal_field(
            self.manual_limit_offset_edit,
            "Отступ лимитной заявки, шагов цены",
        )

        if offset_steps < 0:
            raise ValueError("Отступ лимитной заявки не может быть меньше 0.")

        return offset_steps

    def _round_price_to_increment(
        self,
        price: Decimal,
        increment: Decimal,
        side: str,
    ) -> Decimal:
        if increment <= 0:
            return price

        rounding = ROUND_FLOOR if side == "BUY" else ROUND_CEILING
        steps = (price / increment).to_integral_value(rounding=rounding)

        return steps * increment

    def _calculate_manual_limit_price(
        self,
        side: str,
        best_bid: Decimal,
        best_ask: Decimal,
        offset: Decimal,
        share: TBankShare,
    ) -> Decimal:
        offset_price = offset * share.min_price_increment

        if side == "BUY":
            raw_price = best_ask - offset_price
        elif side == "SELL":
            raw_price = best_bid + offset_price
        else:
            raise ValueError(f"Неизвестная сторона заявки: {side}")

        if raw_price <= 0:
            raise ValueError(
                "Лимитная цена после применения отступа стала меньше или равна 0."
            )

        return self._round_price_to_increment(
            price=raw_price,
            increment=share.min_price_increment,
            side=side,
        )

    def _confirm_manual_order(
        self,
        title: str,
        text: str,
    ) -> bool:
        answer = QMessageBox.question(
            self,
            title,
            text,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )

        return answer == QMessageBox.StandardButton.Yes

    def _get_current_manual_quote_share(self) -> TBankShare:
        instrument_id = self._get_manual_instrument_id()
        shares = list(self.all_shares)

        if not shares:
            shares = list(self.selected_shares_by_uid.values())

        if not shares:
            raise ValueError(
                "Список акций пуст. Сначала нажмите 'Получить акции' "
                "или добавьте инструмент в рабочие акции."
            )

        share = self._find_share_by_manual_instrument_id(
            shares=shares,
            instrument_id=instrument_id,
        )
        self._validate_manual_trade_share(
            share=share,
            client_is_qualified=self.qualified_investor_checkbox.isChecked(),
            side=None,
        )

        return share

    def refresh_manual_last_price(self) -> None:
        try:
            token = self._get_token()
            share = self._get_current_manual_quote_share()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка последней цены", str(error))
            return

        async def task():
            async with AsyncClient(token) as client:
                last_price = await get_last_price(client, share.uid)

                return share, last_price

        self._run_async_task(
            "manual_last_price",
            task,
            self.show_manual_last_price,
        )

    def show_manual_last_price(self, result: tuple) -> None:
        share, last_price = result
        price_time = last_price.time.replace(tzinfo=None).isoformat(
            sep=" ",
            timespec="seconds",
        )
        text = f"{last_price.price} {share.currency} | {price_time} UTC"

        self.manual_last_price_label.setText(text)
        self._log(f"Последняя цена получена: {text}")

    def manual_market_buy(self) -> None:
        self._submit_manual_order(side="BUY", order_type="MARKET")

    def manual_limit_buy(self) -> None:
        self._submit_manual_order(side="BUY", order_type="LIMIT")

    def manual_market_sell(self) -> None:
        self._submit_manual_order(side="SELL", order_type="MARKET")

    def manual_limit_sell(self) -> None:
        self._submit_manual_order(side="SELL", order_type="LIMIT")

    def manual_buy_placeholder(self) -> None:
        self.manual_limit_buy()

    def manual_sell_placeholder(self) -> None:
        self.manual_limit_sell()

    def _submit_manual_order(
        self,
        side: str,
        order_type: str,
    ) -> None:
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

        if side == "BUY" and not self.allow_buy_checkbox.isChecked():
            QMessageBox.warning(
                self,
                "Ошибка ручной покупки",
                "Покупки запрещены в настройках.",
            )
            return

        if side == "SELL" and not self.allow_sell_checkbox.isChecked():
            QMessageBox.warning(
                self,
                "Ошибка ручной продажи",
                "Продажи запрещены в настройках.",
            )
            return

        try:
            token = self._get_token()
            account_id = self._get_account_id()
            share = self._get_current_manual_trade_share(side=side)
            limit_offset = self._parse_manual_limit_offset()

            if side == "BUY":
                buy_amount = self._parse_decimal_field(
                    self.manual_buy_amount_edit,
                    "Сумма ручной покупки",
                )

                if buy_amount <= 0:
                    raise ValueError("Сумма ручной покупки должна быть больше 0.")

                sell_lots = 0
            else:
                sell_lots_raw = self.manual_sell_lots_edit.text().strip()
                sell_lots = int(sell_lots_raw)

                if sell_lots <= 0:
                    raise ValueError("Объём продажи должен быть больше 0.")

                buy_amount = Decimal("0")
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка ручной сделки", str(error))
            return

        if side == "SELL":
            robot_position = get_robot_position(
                account_id=account_id,
                instrument_uid=share.uid,
            )

            if robot_position is None or robot_position.robot_lots <= 0:
                QMessageBox.warning(
                    self,
                    "Ошибка ручной продажи",
                    "У робота нет позиции по этому инструменту.",
                )
                return

            if sell_lots > robot_position.robot_lots:
                QMessageBox.warning(
                    self,
                    "Ошибка ручной продажи",
                    (
                        "Нельзя продать больше лотов, чем закреплено за роботом: "
                        f"продажа={sell_lots}, у робота={robot_position.robot_lots}."
                    ),
                )
                return

        async def task():
            async with AsyncClient(token) as client:
                best_prices = await get_best_order_book_prices(
                    client=client,
                    instrument_id=share.uid,
                    depth=1,
                )
                price_for_estimate = (
                    best_prices.best_ask
                    if side == "BUY"
                    else best_prices.best_bid
                )
                limit_price = Decimal("0")

                if order_type == "LIMIT":
                    limit_price = self._calculate_manual_limit_price(
                        side=side,
                        best_bid=best_prices.best_bid,
                        best_ask=best_prices.best_ask,
                        offset=limit_offset,
                        share=share,
                    )
                    price_for_estimate = limit_price

                if side == "BUY":
                    one_lot_amount = price_for_estimate * Decimal(share.lot)
                    quantity_lots = int(buy_amount // one_lot_amount)

                    if quantity_lots <= 0:
                        raise ValueError(
                            "Суммы ручной покупки не хватает даже на 1 лот: "
                            f"лот стоит примерно {one_lot_amount:.2f} {share.currency}."
                        )

                    requested_amount = one_lot_amount * Decimal(quantity_lots)
                    balance = await get_balance(client, account_id)
                    available_money = self._get_available_money(
                        balance=balance,
                        currency=share.currency,
                    )

                    if requested_amount > available_money:
                        raise ValueError(
                            "Недостаточно свободных денег для покупки: "
                            f"нужно {requested_amount:.2f} {share.currency}, "
                            f"доступно {available_money:.2f} {share.currency}."
                        )
                else:
                    quantity_lots = sell_lots
                    requested_amount = price_for_estimate * Decimal(
                        share.lot * quantity_lots
                    )

                order_request_id = uuid4().hex
                robot_order_id = create_robot_order(
                    account_id=account_id,
                    order_request_id=order_request_id,
                    side=side,
                    order_type=order_type,
                    instrument_uid=share.uid,
                    ticker=share.ticker,
                    class_code=share.class_code,
                    name=share.name,
                    quantity_lots=quantity_lots,
                    quantity_shares=quantity_lots * share.lot,
                    limit_price=limit_price,
                    requested_amount=requested_amount,
                    source="MANUAL_BUTTON",
                )

                try:
                    if order_type == "LIMIT":
                        result = await post_limit_order(
                            client=client,
                            account_id=account_id,
                            order_request_id=order_request_id,
                            share=share,
                            side=side,
                            quantity_lots=quantity_lots,
                            limit_price=limit_price,
                        )
                    else:
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
                        source="MANUAL_BUTTON",
                    )

                return share, result, order_type, quantity_lots, limit_price

        side_text = "покупку" if side == "BUY" else "продажу"
        type_text = "лимитная" if order_type == "LIMIT" else "рыночная"

        if side == "BUY":
            size_text = f"Сумма: {buy_amount} {share.currency}"
        else:
            size_text = f"Лотов: {sell_lots}"

        if order_type == "LIMIT":
            confirm_text = (
                f"{type_text.capitalize()} заявка на {side_text}\n\n"
                f"{share.name} ({share.ticker}_{share.class_code})\n"
                f"{size_text}\n"
                f"Отступ от best price: {limit_offset} шаг(ов) цены"
            )
        else:
            confirm_text = (
                f"{type_text.capitalize()} заявка на {side_text}\n\n"
                f"{share.name} ({share.ticker}_{share.class_code})\n"
                f"{size_text}"
            )

        if not self._confirm_manual_order("Подтверждение ручной сделки", confirm_text):
            self._log("Ручная сделка отменена пользователем.")
            return

        self._run_async_task(
            f"manual_{side.lower()}_{order_type.lower()}_order",
            task,
            lambda result: self.show_manual_order_result(side, result),
        )

    def show_manual_order_result(
        self,
        side: str,
        result: tuple[TBankShare, TBankPostOrderResult, str, int, Decimal],
    ) -> None:
        share, order_result, order_type, quantity_lots, limit_price = result
        self.refresh_robot_orders_table()
        self.refresh_robot_positions_table()
        self._refresh_robot_summary()

        price_text = (
            f"limit_price={limit_price}, "
            if order_type == "LIMIT"
            else ""
        )
        self._log(
            "Ручная заявка отправлена: "
            f"{order_type} {side} {share.ticker}_{share.class_code}, "
            f"лотов={quantity_lots}, "
            f"{price_text}"
            f"broker_order_id={order_result.broker_order_id}, "
            f"status={order_result.execution_report_status}, "
            f"исполнено лотов={order_result.lots_executed}."
        )

        if order_result.lots_executed > 0:
            self.refresh_runtime_balance()

        if order_type == "LIMIT":
            self._log("Обновляю активные заявки после лимитной заявки.")
            self.load_active_orders()
        else:
            self._log("Обновляю активные позиции после рыночной заявки.")
            self.load_positions()

    def start_sell_all_robot_positions(self) -> None:
        if self.robot_is_running or self.growth_monitor_worker is not None:
            QMessageBox.warning(
                self,
                "Робот работает",
                "Сначала выключите робота, затем продавайте все позиции.",
            )
            return

        if (
            self.pending_robot_start_settings is not None
            or self.pending_robot_start_account is not None
        ):
            QMessageBox.warning(
                self,
                "Запуск робота не завершён",
                "Завершите или отмените синхронизацию перед массовой продажей.",
            )
            return

        if self.workers:
            QMessageBox.warning(
                self,
                "Есть активная задача",
                "Дождитесь завершения текущей операции и повторите продажу.",
            )
            return

        try:
            token = self._get_token()
            account_id = self._get_account_id()
        except ValueError as error:
            QMessageBox.warning(
                self,
                "Продажа всех позиций",
                str(error),
            )
            return

        positions = [
            position
            for position in list_robot_positions(account_id=account_id)
            if position.robot_lots > 0
        ]

        if not positions:
            QMessageBox.information(
                self,
                "Позиции робота",
                "У робота нет открытых позиций для продажи.",
            )
            self._refresh_robot_summary()
            return

        preview_lines = [
            (
                f"{position.ticker}_{position.class_code} — "
                f"{position.robot_lots} лот(ов), "
                f"{position.currency}"
            )
            for position in positions[:20]
        ]

        if len(positions) > 20:
            preview_lines.append(
                f"... ещё {len(positions) - 20} позиций"
            )

        preview_text = "\n".join(preview_lines)
        answer = QMessageBox.warning(
            self,
            "Продать все позиции робота",
            (
                "Будут отправлены РЕАЛЬНЫЕ рыночные заявки на продажу "
                "всех позиций, закреплённых за роботом.\n\n"
                "Внешние позиции клиента не затрагиваются.\n\n"
                f"{preview_text}\n\n"
                "Продолжить?"
            ),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )

        if answer != QMessageBox.StandardButton.Yes:
            self._log("Продажа всех позиций робота отменена пользователем.")
            return

        tracked_uids = {
            position.instrument_uid
            for position in positions
        }

        self._set_robot_inputs_locked(True)
        self.robot_toggle_button.setEnabled(False)
        self.sell_all_robot_positions_button.setText(
            "Продажа позиций..."
        )
        self.sell_all_robot_positions_button.setEnabled(False)
        self._log(
            "Запущена массовая продажа позиций робота: "
            f"позиций={len(positions)}."
        )

        async def task():
            async with AsyncClient(token) as client:
                shares = await asyncio.wait_for(
                    get_shares(client),
                    timeout=40,
                )
                broker_positions = await asyncio.wait_for(
                    get_portfolio_positions(client, account_id),
                    timeout=20,
                )
                tracked_shares = [
                    share
                    for share in shares
                    if share.uid in tracked_uids
                ]
                sync_report = sync_robot_positions_with_broker(
                    account_id=account_id,
                    broker_positions=broker_positions,
                    shares=tracked_shares,
                )
                sell_report = await sell_all_robot_positions(
                    client=client,
                    account_id=account_id,
                    shares=tracked_shares,
                )

                return sync_report, sell_report

        self._run_async_task(
            "sell_all_robot_positions",
            task,
            self.show_sell_all_robot_positions_result,
            self._handle_sell_all_robot_positions_error,
        )

    def _finish_sell_all_robot_positions_ui(self) -> None:
        self._set_robot_inputs_locked(False)
        self._set_robot_visual_state("stopped")
        self.sell_all_robot_positions_button.setText(
            "Продать все позиции робота"
        )
        self.sell_all_robot_positions_button.setEnabled(True)

    def show_sell_all_robot_positions_result(
        self,
        result: tuple[object, BulkRobotSellReport],
    ) -> None:
        sync_report, sell_report = result
        self._finish_sell_all_robot_positions_ui()

        self._log(
            "Позиции синхронизированы перед массовой продажей: "
            f"проверено={sync_report.checked_count}, "
            f"уменьшено={sync_report.reduced_count}, "
            f"обнулено={sync_report.zeroed_count}."
        )

        for item in sell_report.items:
            instrument = f"{item.ticker}_{item.class_code}"

            if item.error_text:
                self._log(
                    "Массовая продажа: НЕ ПРОДАНО: "
                    f"{instrument}, "
                    f"запрошено={item.requested_lots}, "
                    f"исполнено={item.executed_lots}, "
                    f"ошибка={item.error_text}"
                )
            elif item.executed_lots < item.requested_lots:
                self._log(
                    "Массовая продажа: ЧАСТИЧНО: "
                    f"{instrument}, "
                    f"запрошено={item.requested_lots}, "
                    f"исполнено={item.executed_lots}, "
                    f"status={item.execution_report_status}, "
                    f"broker_order_id={item.broker_order_id}."
                )
            else:
                self._log(
                    "Массовая продажа: ПРОДАНО: "
                    f"{instrument}, "
                    f"лотов={item.executed_lots}, "
                    f"status={item.execution_report_status}, "
                    f"broker_order_id={item.broker_order_id}."
                )

        self.refresh_robot_orders_table()
        self.refresh_robot_positions_table()
        self._refresh_robot_summary()
        self.refresh_runtime_balance()

        summary_text = (
            f"Полностью продано: {sell_report.fully_sold_count}\n"
            f"Частично продано: {sell_report.partially_sold_count}\n"
            f"Не продано: {sell_report.failed_count}\n"
            f"Всего исполнено лотов: "
            f"{sell_report.total_executed_lots}"
        )

        self._log(
            "Массовая продажа завершена: "
            f"полностью={sell_report.fully_sold_count}, "
            f"частично={sell_report.partially_sold_count}, "
            f"не продано={sell_report.failed_count}, "
            f"исполнено лотов={sell_report.total_executed_lots}."
        )

        if sell_report.total_positions_count == 0:
            QMessageBox.information(
                self,
                "Продажа всех позиций",
                (
                    "После синхронизации с брокером открытых "
                    "позиций робота не осталось."
                ),
            )
        elif (
            sell_report.failed_count > 0
            or sell_report.partially_sold_count > 0
        ):
            QMessageBox.warning(
                self,
                "Продажа завершена не полностью",
                summary_text,
            )
        else:
            QMessageBox.information(
                self,
                "Все позиции проданы",
                summary_text,
            )

    def _handle_sell_all_robot_positions_error(
        self,
        error_text: str,
    ) -> None:
        self._finish_sell_all_robot_positions_ui()
        clean_error_text = error_text.strip()

        if not clean_error_text:
            clean_error_text = (
                "Неизвестная ошибка массовой продажи позиций."
            )

        self._log(
            "Ошибка массовой продажи позиций робота: "
            f"{clean_error_text}"
        )
        QMessageBox.critical(
            self,
            "Ошибка продажи всех позиций",
            clean_error_text,
        )

    def start_database_maintenance(self) -> None:
        if self.robot_is_running or self.growth_monitor_worker is not None:
            QMessageBox.warning(
                self,
                "Робот работает",
                "Сначала выключите робота, затем запустите обслуживание БД.",
            )
            return

        if (
            self.pending_robot_start_settings is not None
            or self.pending_robot_start_account is not None
        ):
            QMessageBox.warning(
                self,
                "Запуск робота не завершён",
                "Завершите или отмените синхронизацию позиций перед обслуживанием БД.",
            )
            return

        if self.workers:
            QMessageBox.warning(
                self,
                "Есть активная задача",
                "Дождитесь завершения текущих операций и повторите обслуживание БД.",
            )
            return

        before_bytes = get_database_total_size_bytes()
        answer = QMessageBox.question(
            self,
            "Обслуживание БД",
            (
                "Будут выполнены проверка целостности, очистка WAL и VACUUM.\n\n"
                f"Текущий размер файлов БД: {format_database_size(before_bytes)}.\n"
                "Операция может занять несколько минут. "
                "Во время неё приложение не закрывайте.\n\n"
                "Продолжить?"
            ),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )

        if answer != QMessageBox.StandardButton.Yes:
            self._log("Обслуживание БД отменено пользователем.")
            return

        self._set_robot_inputs_locked(True)
        self.robot_toggle_button.setEnabled(False)
        self.database_maintenance_button.setEnabled(False)
        self._log(
            "Обслуживание БД запущено: quick_check, WAL checkpoint, VACUUM, optimize."
        )
        self._log(
            f"Размер файлов БД до обслуживания: {format_database_size(before_bytes)}."
        )

        async def task():
            return run_database_maintenance()

        self._run_async_task(
            "database_maintenance",
            task,
            self.show_database_maintenance_result,
            self._handle_database_maintenance_error,
        )

    def _finish_database_maintenance_ui(self) -> None:
        self._set_robot_inputs_locked(False)
        self._set_robot_visual_state("stopped")
        self.database_maintenance_button.setEnabled(True)

    def show_database_maintenance_result(
        self,
        result: DatabaseMaintenanceResult,
    ) -> None:
        self._finish_database_maintenance_ui()
        self._log(
            "Обслуживание БД завершено: "
            f"до={format_database_size(result.before_bytes)}, "
            f"после={format_database_size(result.after_bytes)}, "
            f"освобождено={format_database_size(result.reclaimed_bytes)}, "
            f"quick_check={result.quick_check_result}."
        )
        QMessageBox.information(
            self,
            "Обслуживание БД завершено",
            (
                f"Размер до: {format_database_size(result.before_bytes)}\n"
                f"Размер после: {format_database_size(result.after_bytes)}\n"
                f"Освобождено: {format_database_size(result.reclaimed_bytes)}"
            ),
        )

    def _handle_database_maintenance_error(self, error_text: str) -> None:
        self._finish_database_maintenance_ui()
        clean_error_text = error_text.strip()

        if not clean_error_text:
            clean_error_text = "Неизвестная ошибка обслуживания базы данных."

        self._log(f"Ошибка обслуживания БД: {clean_error_text}")
        QMessageBox.critical(
            self,
            "Ошибка обслуживания БД",
            clean_error_text,
        )

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

    def clear_log_view(self) -> None:
        self.log_edit.clear()

    def _log(self, message: str) -> None:
        now = datetime.now().strftime("%H:%M:%S")
        self.log_edit.appendPlainText(f"[{now}] {message}")

    def _hide_info_tables(self) -> None:
        self.info_title_label.setVisible(False)
        self.startup_hint_label.setVisible(True)

        for table in (
            self.accounts_table,
            self.money_table,
            self.positions_table,
            self.orders_table,
        ):
            table.setVisible(False)

        self.active_orders_actions_widget.setVisible(False)

    def _show_info_table(self, title: str, table: QTableWidget) -> None:
        self.info_title_label.setText(title)
        self.info_title_label.setVisible(True)
        self.startup_hint_label.setVisible(False)

        for current_table in (
            self.accounts_table,
            self.money_table,
            self.positions_table,
            self.orders_table,
        ):
            current_table.setVisible(current_table is table)

        self.active_orders_actions_widget.setVisible(table is self.orders_table)
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
        self._refresh_robot_summary()

    def cancel_robot_start_after_sync(self) -> None:
        self.pending_robot_start_settings = None
        self.pending_robot_start_account = None
        self._set_robot_inputs_locked(False)
        self._set_robot_visual_state("stopped")
        self._log("Запуск робота отменён после синхронизации позиций.")

    def update_robot_positions_from_table(self) -> None:
        if self.robot_is_running:
            QMessageBox.warning(
                self,
                "Робот включён",
                "Лоты робота нельзя редактировать во время работы робота.",
            )
            return

        changed_count = 0

        for row in range(self.robot_positions_table.rowCount()):
            instrument_item = self.robot_positions_table.item(row, 0)
            name_item = self.robot_positions_table.item(row, 1)
            robot_lots_item = self.robot_positions_table.item(row, 3)
            broker_lots_item = self.robot_positions_table.item(row, 4)
            account_id_item = self.robot_positions_table.item(row, 8)
            instrument_uid_item = self.robot_positions_table.item(row, 9)

            if (
                instrument_item is None
                or name_item is None
                or robot_lots_item is None
                or broker_lots_item is None
                or account_id_item is None
                or instrument_uid_item is None
            ):
                continue

            raw_robot_lots = robot_lots_item.text().strip()
            raw_broker_lots = broker_lots_item.text().strip()
            row_label = (
                f"{instrument_item.text()} / {name_item.text()}"
            )

            try:
                robot_lots = int(raw_robot_lots)
                broker_lots = int(raw_broker_lots)
            except ValueError:
                QMessageBox.warning(
                    self,
                    "Ошибка",
                    (
                        "Количество лотов у робота должно быть целым числом. "
                        f"Строка: {row_label}"
                    ),
                )
                return

            if robot_lots < 0:
                QMessageBox.warning(
                    self,
                    "Ошибка",
                    (
                        "Количество лотов у робота не может быть меньше 0. "
                        f"Строка: {row_label}"
                    ),
                )
                return

            if robot_lots > broker_lots:
                QMessageBox.warning(
                    self,
                    "Ошибка",
                    (
                        "Лотов у робота не может быть больше, чем лотов у брокера. "
                        f"Строка: {row_label}, "
                        f"робот={robot_lots}, брокер={broker_lots}"
                    ),
                )
                return

            changed = set_robot_position_lots(
                account_id=account_id_item.text(),
                instrument_uid=instrument_uid_item.text(),
                robot_lots=robot_lots,
                reason=(
                    "Ручная корректировка через таблицу "
                    "Позиции робота."
                ),
            )

            if changed:
                changed_count += 1

        self.refresh_robot_positions_table()
        self._log(
            "Лоты робота обновлены из таблицы. "
            f"Изменено строк: {changed_count}"
        )

        if (
            self.pending_robot_start_settings is None
            or self.pending_robot_start_account is None
        ):
            QMessageBox.warning(
                self,
                "Запуск не подготовлен",
                "Сначала нажмите 'Включить и синхронизировать'.",
            )
            return

        settings = self.pending_robot_start_settings
        account = self.pending_robot_start_account
        self.pending_robot_start_settings = None
        self.pending_robot_start_account = None

        self._start_robot_after_validation(
            settings=settings,
            account=account,
        )
        self.monitoring_tabs.setCurrentWidget(
            self.growth_current_table
        )
        self.tabs.setCurrentWidget(self.monitoring_tabs)

    def sync_robot_positions(self) -> None:
        if self.robot_is_running:
            QMessageBox.warning(
                self,
                "Робот включён",
                "Позиции робота нельзя синхронизировать во время работы робота.",
            )
            return

        try:
            token = self._get_token()
            account_id = self._get_account_id()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка", str(error))
            return

        async def task():
            async with AsyncClient(token) as client:
                positions = await get_portfolio_positions(client, account_id)
                shares = await get_shares(client)

                return positions, shares

        self._run_async_task(
            "robot_position_sync",
            task,
            lambda result, current_account_id=account_id: self.show_robot_position_sync(
                current_account_id,
                result,
            ),
        )

    def show_robot_position_sync(
        self,
        account_id: str,
        result: tuple[list[TBankPortfolioPosition], list[TBankShare]],
    ) -> None:
        positions, shares = result
        sync_report = sync_robot_positions_with_broker(
            account_id=account_id,
            broker_positions=positions,
            shares=shares,
        )
        self.refresh_robot_positions_table()
        self.monitoring_tabs.setCurrentWidget(self.robot_positions_tab_widget)
        self.tabs.setCurrentWidget(self.monitoring_tabs)

        self._log(
            "Позиции робота синхронизированы: "
            f"проверено={sync_report.checked_count}, "
            f"уменьшено={sync_report.reduced_count}, "
            f"обнулено={sync_report.zeroed_count}."
        )

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
            self._refresh_robot_summary()

        self._log(f"Получено аккаунтов: {len(accounts)}")
        self._show_info_table("Аккаунты", self.accounts_table)

    async def _resolve_account_id_for_request(
        self,
        client,
        requested_account_id: str,
    ) -> tuple[str, bool]:
        accounts = await get_accounts(client)
        account_ids = {
            account.account_id
            for account in accounts
        }

        if requested_account_id in account_ids:
            return requested_account_id, False

        if len(accounts) == 1:
            return accounts[0].account_id, True

        if not accounts:
            raise ValueError(
                "T-Invest API не вернул ни одного доступного брокерского счёта "
                "для текущего токена."
            )

        available_accounts = ", ".join(
            f"{account.name}: {account.account_id}"
            for account in accounts
        )
        raise ValueError(
            "Указанный Account ID не найден среди счетов, доступных "
            "текущему токену. Нажмите «Проверить подключение» и выберите "
            f"нужный счёт. Доступные счета: {available_accounts}"
        )

    def _apply_resolved_account_id(
        self,
        resolved_account_id: str,
        was_auto_selected: bool,
    ) -> None:
        current_account_id = self.account_id_edit.text().strip()

        if resolved_account_id == current_account_id:
            return

        self.account_id_edit.setText(resolved_account_id)
        save_app_settings(
            {
                "account_id": resolved_account_id,
            }
        )

        if was_auto_selected:
            self._log(
                "Account ID исправлен автоматически: "
                f"{resolved_account_id}. Для токена доступен один счёт."
            )
        else:
            self._log(
                f"Account ID обновлён: {resolved_account_id}."
            )

    def load_balance(self) -> None:
        try:
            token = self._get_token()
            requested_account_id = self._get_account_id()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка", str(error))
            return

        async def task():
            async with AsyncClient(token) as client:
                resolved_account_id, was_auto_selected = (
                    await self._resolve_account_id_for_request(
                        client=client,
                        requested_account_id=requested_account_id,
                    )
                )
                balance = await get_balance(
                    client,
                    resolved_account_id,
                )

                return (
                    resolved_account_id,
                    was_auto_selected,
                    balance,
                )

        self._run_async_task(
            "balance",
            task,
            self.show_balance_for_account,
        )

    def show_balance_for_account(
        self,
        result: tuple[str, bool, PortfolioBalance],
    ) -> None:
        resolved_account_id, was_auto_selected, balance = result
        self._apply_resolved_account_id(
            resolved_account_id=resolved_account_id,
            was_auto_selected=was_auto_selected,
        )
        self.show_balance(balance)

    def show_balance(self, balance: PortfolioBalance) -> None:
        self._apply_runtime_balance(balance)

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
            requested_account_id = self._get_account_id()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка", str(error))
            return

        async def task():
            async with AsyncClient(token) as client:
                resolved_account_id, was_auto_selected = (
                    await self._resolve_account_id_for_request(
                        client=client,
                        requested_account_id=requested_account_id,
                    )
                )
                positions = await get_portfolio_positions(
                    client,
                    resolved_account_id,
                )

                return (
                    resolved_account_id,
                    was_auto_selected,
                    positions,
                )

        self._run_async_task(
            "positions",
            task,
            self.show_positions_for_account,
        )

    def show_positions_for_account(
        self,
        result: tuple[
            str,
            bool,
            list[TBankPortfolioPosition],
        ],
    ) -> None:
        resolved_account_id, was_auto_selected, positions = result
        self._apply_resolved_account_id(
            resolved_account_id=resolved_account_id,
            was_auto_selected=was_auto_selected,
        )
        self.show_positions(positions)

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

    def _collect_active_order_rows(
        self,
        only_checked: bool,
        only_limit_orders: bool,
    ) -> list[dict[str, str]]:
        result: list[dict[str, str]] = []

        for row in range(self.orders_table.rowCount()):
            checkbox_item = self.orders_table.item(row, 0)
            order_id_item = self.orders_table.item(row, 1)
            order_type_item = self.orders_table.item(row, 4)
            ticker_item = self.orders_table.item(row, 5)
            class_code_item = self.orders_table.item(row, 6)

            if order_id_item is None or order_type_item is None:
                continue

            if only_checked:
                if checkbox_item is None:
                    continue

                if checkbox_item.checkState() != Qt.CheckState.Checked:
                    continue

            order_type = order_type_item.text().strip()

            if only_limit_orders and order_type != "ORDER_TYPE_LIMIT":
                continue

            broker_order_id = order_id_item.text().strip()

            if not broker_order_id:
                continue

            ticker = ticker_item.text().strip() if ticker_item is not None else ""
            class_code = class_code_item.text().strip() if class_code_item is not None else ""

            result.append(
                {
                    "broker_order_id": broker_order_id,
                    "order_type": order_type,
                    "ticker": ticker,
                    "class_code": class_code,
                }
            )

        return result

    def cancel_selected_active_order(self) -> None:
        orders = self._collect_active_order_rows(
            only_checked=True,
            only_limit_orders=False,
        )

        if not orders:
            QMessageBox.warning(
                self,
                "Заявки не выбраны",
                "Отметьте одну или несколько активных заявок слева в таблице.",
            )
            return

        self._confirm_and_cancel_active_orders(
            orders=orders,
            title="Отмена отмеченных заявок",
            message=f"Отменить отмеченные активные заявки: {len(orders)}?",
        )

    def cancel_all_active_limit_orders(self) -> None:
        orders = self._collect_active_order_rows(
            only_checked=False,
            only_limit_orders=True,
        )

        if not orders:
            QMessageBox.information(
                self,
                "Лимитных заявок нет",
                "В таблице нет активных лимитных заявок.",
            )
            return

        self._confirm_and_cancel_active_orders(
            orders=orders,
            title="Отмена всех лимитных заявок",
            message=f"Отменить все активные лимитные заявки: {len(orders)}?",
        )

    def _confirm_and_cancel_active_orders(
        self,
        orders: list[dict[str, str]],
        title: str,
        message: str,
    ) -> None:
        preview_rows = []

        for order in orders[:10]:
            preview_rows.append(
                f"{order['ticker']}_{order['class_code']} | {order['order_type']} | {order['broker_order_id']}"
            )

        preview_text = "\n".join(preview_rows)

        if len(orders) > 10:
            preview_text += f"\n... ещё {len(orders) - 10}"

        confirm_text = f"{message}\n\n{preview_text}"

        answer = QMessageBox.question(
            self,
            title,
            confirm_text,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )

        if answer != QMessageBox.StandardButton.Yes:
            self._log("Отмена активных заявок отменена пользователем.")
            return

        try:
            token = self._get_token()
            account_id = self._get_account_id()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка", str(error))
            return

        broker_order_ids = [
            order["broker_order_id"]
            for order in orders
        ]

        async def task():
            cancelled_order_ids = []

            async with AsyncClient(token) as client:
                for broker_order_id in broker_order_ids:
                    await cancel_order(
                        client=client,
                        account_id=account_id,
                        broker_order_id=broker_order_id,
                    )
                    cancelled_order_ids.append(broker_order_id)

            return cancelled_order_ids

        self._run_async_task(
            "cancel_active_orders",
            task,
            self.show_cancel_active_orders_result,
        )

    def show_cancel_active_orders_result(self, broker_order_ids: list[str]) -> None:
        robot_orders_count = 0

        for broker_order_id in broker_order_ids:
            was_robot_order = mark_robot_order_cancelled_by_broker_order(
                broker_order_id=broker_order_id,
            )

            if was_robot_order:
                robot_orders_count += 1

        self.refresh_robot_orders_table()
        self._log(
            "Активные заявки отменены: "
            f"всего={len(broker_order_ids)}, заявок робота={robot_orders_count}."
        )
        self.load_active_orders()

    def load_active_orders(self) -> None:
        try:
            token = self._get_token()
            requested_account_id = self._get_account_id()
        except ValueError as error:
            QMessageBox.warning(self, "Ошибка", str(error))
            return

        async def task():
            async with AsyncClient(token) as client:
                resolved_account_id, was_auto_selected = (
                    await self._resolve_account_id_for_request(
                        client=client,
                        requested_account_id=requested_account_id,
                    )
                )
                orders = await get_active_orders(
                    client,
                    resolved_account_id,
                )

                return (
                    resolved_account_id,
                    was_auto_selected,
                    orders,
                )

        self._run_async_task(
            "active_orders",
            task,
            self.show_active_orders_for_account,
        )

    def show_active_orders_for_account(
        self,
        result: tuple[
            str,
            bool,
            list[TBankActiveOrder],
        ],
    ) -> None:
        resolved_account_id, was_auto_selected, orders = result
        self._apply_resolved_account_id(
            resolved_account_id=resolved_account_id,
            was_auto_selected=was_auto_selected,
        )
        self.show_active_orders(orders)

    def show_active_orders(self, orders: list[TBankActiveOrder]) -> None:
        headers = [
            "✓",
            "order_id",
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
        ]

        self.orders_table.clear()
        self.orders_table.setColumnCount(len(headers))
        self.orders_table.setRowCount(len(orders))
        self.orders_table.setHorizontalHeaderLabels(headers)

        for row_index, order in enumerate(orders):
            checkbox_item = QTableWidgetItem()
            checkbox_item.setFlags(
                Qt.ItemFlag.ItemIsEnabled
                | Qt.ItemFlag.ItemIsSelectable
                | Qt.ItemFlag.ItemIsUserCheckable
            )
            checkbox_item.setCheckState(Qt.CheckState.Unchecked)
            self.orders_table.setItem(row_index, 0, checkbox_item)

            direction = order.direction.removeprefix("ORDER_DIRECTION_")

            row = [
                order.order_id,
                order.execution_report_status,
                direction,
                order.order_type,
                order.ticker,
                order.class_code,
                order.lots_requested,
                order.lots_executed,
                order.initial_security_price,
                order.total_order_amount,
                order.order_date,
            ]

            for column_index, value in enumerate(row, start=1):
                self.orders_table.setItem(
                    row_index,
                    column_index,
                    self._make_read_only_item(value),
                )

        self.orders_table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents
        )
        self.orders_table.verticalHeader().setVisible(False)
        self.orders_table.setColumnHidden(8, True)  # lots_exec сейчас не отображаем

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
        only_liquid_shares = self.only_liquid_shares_checkbox.isChecked()
        self.available_share_prices_by_uid = {}
        self.refresh_shares_filters_label()

        async def task():
            async with AsyncClient(token) as client:
                shares = await asyncio.wait_for(
                    get_shares(client),
                    timeout=40,
                )
                price_candidate_uids = [
                    share.uid
                    for share in shares
                    if (
                        share.currency.upper()
                        == MOEX_SHARE_CURRENCY
                        and share.real_exchange
                        == MOEX_REAL_EXCHANGE
                        and share.api_trade_available_flag
                        and share.buy_available_flag
                        and share.sell_available_flag
                        and not share.blocked_tca_flag
                    )
                ]

                if not price_candidate_uids:
                    return shares, [], ""

                try:
                    prices = await asyncio.wait_for(
                        get_last_prices_batched(
                            client=client,
                            instrument_ids=price_candidate_uids,
                            batch_size=100,
                        ),
                        timeout=60,
                    )
                    price_error_text = ""
                except Exception as error:
                    prices = []
                    clean_error_text = str(error).strip() or repr(error)
                    price_error_text = (
                        f"{type(error).__name__}: {clean_error_text}"
                    )

                return shares, prices, price_error_text

        self._run_async_task(
            "shares",
            task,
            lambda result, qualified=client_is_qualified, liquid=only_liquid_shares: self.show_shares(
                result,
                qualified,
                liquid,
            ),
        )

    def _filter_available_shares(
        self,
        shares: list[TBankShare],
        client_is_qualified: bool,
        only_liquid_shares: bool,
    ) -> list[TBankShare]:
        return [
            share
            for share in shares
            if self._share_passes_fixed_filters(
                share=share,
                client_is_qualified=client_is_qualified,
                only_liquid_shares=only_liquid_shares,
            )
        ]

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
        result: tuple[
            list[TBankShare],
            list[TBankLastPrice],
            str,
        ],
        client_is_qualified: bool,
        only_liquid_shares: bool,
    ) -> None:
        shares, prices, price_error_text = result
        self.all_shares = shares
        self.available_share_prices_by_uid = (
            map_last_prices_by_instrument_uid(prices)
        )
        self.available_shares = self._filter_available_shares(
            shares=shares,
            client_is_qualified=client_is_qualified,
            only_liquid_shares=only_liquid_shares,
        )

        self.refresh_available_shares_table()

        qualified_count = sum(
            1
            for share in shares
            if share.for_qual_investor_flag
        )
        available_prices_count = sum(
            1
            for share in self.available_shares
            if share.uid in self.available_share_prices_by_uid
        )

        self._log(f"Клиент квал: {'да' if client_is_qualified else 'нет'}")
        self._log(
            f"Только ликвидные акции: "
            f"{'да' if only_liquid_shares else 'нет'}"
        )
        self._log(
            "Фильтры акций: "
            f"{self._get_shares_filter_text(client_is_qualified, only_liquid_shares)}"
        )
        self._log(f"Всего акций из API: {len(shares)}")
        self._log(
            f"Акций с признаком для квалов в общем списке: {qualified_count}"
        )
        self._log(
            f"Рабочих акций после фильтра: {len(self.available_shares)}"
        )
        self._log(
            "Последние цены получены: "
            f"{available_prices_count} из "
            f"{len(self.available_shares)} доступных акций."
        )

        if price_error_text:
            self._log(
                "Справочник акций загружен, но последние цены "
                f"получить не удалось: {price_error_text}"
            )

        self._sync_selected_shares_with_available()
        self.tabs.setCurrentWidget(self.shares_tab_widget)

    def _count_checked_available_shares(self) -> int:
        checked_count = 0

        for row in range(self.shares_table.rowCount()):
            checkbox_item = self.shares_table.item(row, 0)

            if checkbox_item is None:
                continue

            if checkbox_item.checkState() == Qt.CheckState.Checked:
                checked_count += 1

        return checked_count

    def apply_shares_search_filter(self) -> None:
        query = self.shares_search_edit.text().strip().casefold()
        visible_count = 0
        total_count = self.shares_table.rowCount()
        checked_count = self._count_checked_available_shares()

        for row in range(total_count):
            searchable_values = []

            for column in (2, 3, 7, 11):
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
                f"Найдено: {visible_count} из {total_count}; "
                f"отмечено: {checked_count}"
            )
        else:
            self.shares_search_status_label.setText(
                f"Всего доступных акций: {total_count}; "
                f"отмечено: {checked_count}"
            )

    def _format_share_price_value(
        self,
        value: Decimal,
    ) -> str:
        text = format(value, "f")

        if "." in text:
            text = text.rstrip("0").rstrip(".")

        return text or "0"

    def refresh_available_shares_table(self) -> None:
        self.shares_table.setSortingEnabled(False)

        headers = [
            "✓",
            "#",
            "ticker",
            "name",
            "текущая цена",
            "стоимость лота",
            "currency",
            "class_code",
            "акций в 1 лоте",
            "шаг цены",
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
            checkbox_item = CheckableTableWidgetItem()
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

            last_price = self.available_share_prices_by_uid.get(
                share.uid
            )

            if last_price is None or last_price.price <= 0:
                current_price_text = "—"
                lot_cost_text = "—"
                price_tooltip = (
                    "Последняя цена по инструменту не получена."
                )
            else:
                current_price_text = self._format_share_price_value(
                    last_price.price
                )
                lot_cost_text = self._format_share_price_value(
                    last_price.price * Decimal(share.lot)
                )
                price_time_text = (
                    last_price.time.replace(tzinfo=None).isoformat(
                        sep=" ",
                        timespec="seconds",
                    )
                )
                price_tooltip = (
                    f"Цена на {price_time_text} UTC; "
                    f"тип={last_price.last_price_type}"
                )

            row_values = [
                row_index + 1,
                share.ticker,
                share.name,
                current_price_text,
                lot_cost_text,
                share.currency,
                share.class_code,
                share.lot,
                share.min_price_increment,
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

            for column_index, value in enumerate(
                row_values,
                start=1,
            ):
                item = self._make_read_only_item(value)

                if column_index in (4, 5):
                    item.setToolTip(price_tooltip)

                self.shares_table.setItem(
                    row_index,
                    column_index,
                    item,
                )

        self.shares_table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents
        )
        self.shares_table.verticalHeader().setVisible(False)
        self.shares_table.setColumnHidden(
            1,
            True,
        )
        self.shares_table.setColumnHidden(
            7,
            True,
        )
        self.shares_table.setColumnHidden(
            10,
            True,
        )
        self.shares_table.setColumnHidden(
            11,
            True,
        )
        self.shares_table.setColumnHidden(
            12,
            True,
        )
        self.shares_table.setColumnHidden(
            13,
            True,
        )
        self.shares_table.setColumnHidden(
            14,
            True,
        )
        self.shares_table.setColumnHidden(
            18,
            True,
        )
        self.shares_table.setSortingEnabled(True)
        self.shares_table.sortItems(
            0,
            Qt.SortOrder.DescendingOrder,
        )
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
            uid_item = self.shares_table.item(row, 11)

            if checkbox_item is None or uid_item is None:
                continue

            if (
                checkbox_item.checkState()
                != Qt.CheckState.Checked
            ):
                continue

            share = self._find_available_share_by_uid(
                uid_item.text()
            )

            if share is None:
                continue

            selected_shares[share.uid] = share

        self.selected_shares_by_uid = selected_shares
        self.refresh_selected_shares_table()

        self._log(
            "Рабочий список акций полностью обновлён. "
            f"Выбрано: {len(self.selected_shares_by_uid)}"
        )

        self.tabs.setCurrentWidget(
            self.selected_shares_tab_widget
        )

    def _find_available_share_by_uid(self, uid: str) -> TBankShare | None:
        for share in self.available_shares:
            if share.uid == uid:
                return share

        return None

    def add_selected_share_from_available_table(
        self,
        row: int,
        column: int,
    ) -> None:
        uid_item = self.shares_table.item(row, 11)

        if uid_item is None:
            return

        uid = uid_item.text()
        share = self._find_available_share_by_uid(uid)

        if share is None:
            self._log(f"Акция не найдена в доступном списке: {uid}")
            return

        if uid in self.selected_shares_by_uid:
            self._log(
                f"Акция уже есть в рабочем списке: {share.ticker}"
            )
            return

        self.selected_shares_by_uid[uid] = share
        self.refresh_selected_shares_table()

        self._log(
            "Акция добавлена в рабочий список: "
            f"{share.ticker} / {share.name}"
        )
        self._log(
            "Всего рабочих акций выбрано: "
            f"{len(self.selected_shares_by_uid)}"
        )

        self.tabs.setCurrentWidget(
            self.selected_shares_tab_widget
        )

    def remove_selected_share_from_table(self, row: int, column: int) -> None:
        uid_item = self.selected_shares_table.item(row, 8)

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
                share.min_price_increment,
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
                "акций в 1 лоте",
                "шаг цены",
                "currency",
                "real_exchange",
                "uid",
                "qual",
                "liquidity",
            ],
            rows,
        )
        self.selected_shares_table.setColumnHidden(3, True)  # class_code нужен коду, но не нужен клиенту
        self.selected_shares_table.setColumnHidden(6, True)  # currency нужен коду, но не нужен клиенту
        self.selected_shares_table.setColumnHidden(7, True)  # real_exchange нужен коду, но не нужен клиенту
        self.selected_shares_table.setColumnHidden(8, True)  # uid: нужен коду, но не нужен клиенту
        self._refresh_robot_summary()

