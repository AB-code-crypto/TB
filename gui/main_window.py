import os
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from dotenv import load_dotenv
from PySide6.QtCore import Qt, QThread
from PySide6.QtWidgets import (
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
from tbank.accounts import TBankAccount, get_accounts
from tbank.active_orders import TBankActiveOrder, get_active_orders
from tbank.balance import PortfolioBalance, get_balance
from tbank.candles import TBankCandle, get_candles
from tbank.last_prices import TBankLastPrice, get_last_prices_batched
from tbank.positions import TBankPortfolioPosition, get_portfolio_positions


CANDLE_INTERVALS: dict[str, int] = {
    "1 минута": marketdata_pb2.CANDLE_INTERVAL_1_MIN,
    "5 минут": marketdata_pb2.CANDLE_INTERVAL_5_MIN,
    "15 минут": marketdata_pb2.CANDLE_INTERVAL_15_MIN,
    "1 час": marketdata_pb2.CANDLE_INTERVAL_HOUR,
    "1 день": marketdata_pb2.CANDLE_INTERVAL_DAY,
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

        self.threads: list[QThread] = []
        self.workers: list[AsyncTaskWorker] = []

        self.setWindowTitle("TBank Robot — GUI v0.1")
        self.resize(1300, 800)

        self.token_edit = QLineEdit(initial_token)
        self.token_edit.setEchoMode(QLineEdit.EchoMode.Password)

        self.account_id_edit = QLineEdit(initial_account_id)
        self.instrument_ids_edit = QLineEdit("SBER_TQBR, GAZP_TQBR, LKOH_TQBR")
        self.candle_instrument_edit = QLineEdit("SBER_TQBR")
        self.candle_days_edit = QLineEdit("1")
        self.candle_limit_edit = QLineEdit("50")

        self.candle_interval_combo = QComboBox()
        self.candle_interval_combo.addItems(list(CANDLE_INTERVALS.keys()))

        self.accounts_table = QTableWidget()
        self.money_table = QTableWidget()
        self.positions_table = QTableWidget()
        self.orders_table = QTableWidget()
        self.prices_table = QTableWidget()
        self.candles_table = QTableWidget()

        self.log_edit = QPlainTextEdit()
        self.log_edit.setReadOnly(True)

        self._build_ui()

        self._log("GUI v0.1 запущен.")
        self._log(
            f"Поле токена: {'заполнено' if self.token_edit.text().strip() else 'пустое'}"
        )
        self._log(
            f"Account ID: {self.account_id_edit.text().strip() if self.account_id_edit.text().strip() else 'не задан'}"
        )

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

        controls_layout.addWidget(QLabel("Instrument IDs:"), 3, 0)
        controls_layout.addWidget(self.instrument_ids_edit, 3, 1, 1, 2)

        last_prices_button = QPushButton("Получить last prices")
        last_prices_button.clicked.connect(self.load_last_prices)
        controls_layout.addWidget(last_prices_button, 3, 3)

        controls_layout.addWidget(QLabel("Свечи инструмент:"), 4, 0)
        controls_layout.addWidget(self.candle_instrument_edit, 4, 1)

        controls_layout.addWidget(QLabel("Интервал:"), 4, 2)
        controls_layout.addWidget(self.candle_interval_combo, 4, 3)

        controls_layout.addWidget(QLabel("Дней назад:"), 5, 0)
        controls_layout.addWidget(self.candle_days_edit, 5, 1)

        controls_layout.addWidget(QLabel("Лимит свечей:"), 5, 2)
        controls_layout.addWidget(self.candle_limit_edit, 5, 3)

        candles_button = QPushButton("Получить свечи")
        candles_button.clicked.connect(self.load_candles)
        controls_layout.addWidget(candles_button, 6, 0, 1, 4)

        self.tabs = QTabWidget()
        self.tabs.addTab(self.accounts_table, "Аккаунты")
        self.tabs.addTab(self.money_table, "Баланс")
        self.tabs.addTab(self.positions_table, "Позиции")
        self.tabs.addTab(self.orders_table, "Активные заявки")
        self.tabs.addTab(self.prices_table, "Last prices")
        self.tabs.addTab(self.candles_table, "Свечи")
        self.tabs.addTab(self.log_edit, "Лог")

        root_layout.addWidget(controls)
        root_layout.addWidget(self.tabs)

        self.setCentralWidget(root)

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
        task_factory,
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
                price.ticker,
                price.class_code,
                price.price,
                price.time,
                price.instrument_uid,
                price.last_price_type,
            ]
            for price in prices
        ]

        self._fill_table(
            self.prices_table,
            ["ticker", "class_code", "price", "time_utc", "uid", "type"],
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
                candle.time,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.is_complete,
            ]
            for candle in candles
        ]

        self._fill_table(
            self.candles_table,
            ["time_utc", "open", "high", "low", "close", "volume", "complete"],
            rows,
        )

        self._log(f"Получено свечей: {len(candles)}")
        self.tabs.setCurrentWidget(self.candles_table)
