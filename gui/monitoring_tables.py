from datetime import datetime

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QHeaderView, QTableWidget, QTableWidgetItem

from bd.growth_current_state import list_growth_current_states
from bd.growth_scan_cycle import list_recent_growth_scan_cycles
from bd.growth_signal import list_recent_growth_signals
from bd.buy_intent import list_recent_buy_intents
from bd.robot_order import list_recent_robot_orders
from bd.robot_position import list_robot_positions


def _format_table_value(value: object) -> str:
    if value is None:
        return ""

    if isinstance(value, datetime):
        return value.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")

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


def _set_table_value(
    table: QTableWidget,
    row: int,
    column: int,
    value: object,
) -> None:
    item = QTableWidgetItem(_format_table_value(value))
    item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
    table.setItem(row, column, item)


def _set_editable_table_value(
    table: QTableWidget,
    row: int,
    column: int,
    value: object,
) -> None:
    item = QTableWidgetItem(_format_table_value(value))
    item.setFlags(item.flags() | Qt.ItemFlag.ItemIsEditable)
    table.setItem(row, column, item)


def _fit_table_columns(table: QTableWidget) -> None:
    table.horizontalHeader().setSectionResizeMode(
        QHeaderView.ResizeMode.ResizeToContents
    )
    table.verticalHeader().setVisible(False)


def fill_robot_orders_table(table: QTableWidget) -> None:
    orders = list_recent_robot_orders(limit=100)

    headers = [
        "ID",
        "UTC",
        "Сторона",
        "Статус",
        "Статус брокера",
        "Инструмент",
        "Название",
        "Лотов",
        "Лимитная цена",
        "Исполнено",
        "Цена исполнения",
        "Сумма",
        "Ошибка",
    ]

    table.setColumnCount(len(headers))
    table.setHorizontalHeaderLabels(headers)
    table.setRowCount(len(orders))

    for row_index, order in enumerate(orders):
        _set_table_value(table, row_index, 0, order.id)
        _set_table_value(table, row_index, 1, order.created_at_utc)
        _set_table_value(table, row_index, 2, order.side)
        _set_table_value(table, row_index, 3, order.status)
        _set_table_value(table, row_index, 4, order.execution_report_status)
        _set_table_value(
            table,
            row_index,
            5,
            f"{order.ticker}_{order.class_code}",
        )
        _set_table_value(table, row_index, 6, order.name)
        _set_table_value(table, row_index, 7, order.quantity_lots)
        _set_table_value(table, row_index, 8, order.limit_price)
        _set_table_value(table, row_index, 9, order.lots_executed)
        _set_table_value(
            table,
            row_index,
            10,
            order.executed_order_price,
        )
        _set_table_value(table, row_index, 11, order.total_order_amount)
        _set_table_value(table, row_index, 12, order.error_text)

    _fit_table_columns(table)


def fill_robot_positions_table(table: QTableWidget) -> None:
    positions = list_robot_positions()

    headers = [
        "Инструмент",
        "Название",
        "Средняя цена",
        "Лотов у робота",
        "Лотов у брокера",
        "Внешних лотов клиента",
        "Комментарий",
        "Синхронизация UTC",
        "account_id",
        "instrument_uid",
    ]

    table.setColumnCount(len(headers))
    table.setHorizontalHeaderLabels(headers)
    table.setRowCount(len(positions))

    for row_index, position in enumerate(positions):
        _set_table_value(
            table,
            row_index,
            0,
            f"{position.ticker}_{position.class_code}",
        )
        _set_table_value(table, row_index, 1, position.name)
        _set_table_value(table, row_index, 2, position.avg_price)
        _set_editable_table_value(
            table,
            row_index,
            3,
            position.robot_lots,
        )
        _set_table_value(
            table,
            row_index,
            4,
            position.last_broker_lots,
        )
        _set_table_value(
            table,
            row_index,
            5,
            position.external_lots,
        )
        _set_table_value(table, row_index, 6, position.sync_note)
        _set_table_value(
            table,
            row_index,
            7,
            position.last_sync_at_utc,
        )
        _set_table_value(table, row_index, 8, position.account_id)
        _set_table_value(
            table,
            row_index,
            9,
            position.instrument_uid,
        )

    table.setColumnHidden(8, True)
    table.setColumnHidden(9, True)

    _fit_table_columns(table)


def fill_growth_current_table(table: QTableWidget) -> None:
    states = list_growth_current_states(limit=500)

    headers = [
        "Инструмент",
        "Название",
        "Рост",
        "Сигнал",
        "Текущая цена",
        "Open свечи",
        "Свеча UTC",
        "Цена UTC",
        "Цикл",
    ]

    table.setColumnCount(len(headers))
    table.setHorizontalHeaderLabels(headers)
    table.setRowCount(len(states))

    for row_index, state in enumerate(states):
        _set_table_value(
            table,
            row_index,
            0,
            f"{state.ticker}_{state.class_code}",
        )
        _set_table_value(table, row_index, 1, state.name)
        _set_table_value(
            table,
            row_index,
            2,
            f"{state.growth_percent:.4f}%",
        )
        _set_table_value(
            table,
            row_index,
            3,
            "ДА" if state.is_signal else "",
        )
        _set_table_value(table, row_index, 4, state.current_price)
        _set_table_value(
            table,
            row_index,
            5,
            state.candle_open_price,
        )
        _set_table_value(
            table,
            row_index,
            6,
            state.candle_time_utc,
        )
        _set_table_value(
            table,
            row_index,
            7,
            state.last_price_time_utc,
        )
        _set_table_value(
            table,
            row_index,
            8,
            state.scan_cycle_id,
        )

    _fit_table_columns(table)




def fill_growth_signals_table(table: QTableWidget) -> None:
    signals = list_recent_growth_signals(limit=100)

    headers = [
        "ID",
        "Обнаружен UTC",
        "Инструмент",
        "Название",
        "Свеча UTC",
        "Рост",
        "Порог",
        "Текущая цена",
        "Open свечи",
        "Статус",
    ]

    table.setColumnCount(len(headers))
    table.setHorizontalHeaderLabels(headers)
    table.setRowCount(len(signals))

    for row_index, signal in enumerate(signals):
        _set_table_value(table, row_index, 0, signal.id)
        _set_table_value(
            table,
            row_index,
            1,
            signal.detected_at_utc,
        )
        _set_table_value(
            table,
            row_index,
            2,
            f"{signal.ticker}_{signal.class_code}",
        )
        _set_table_value(
            table,
            row_index,
            3,
            signal.name,
        )
        _set_table_value(
            table,
            row_index,
            4,
            signal.candle_time_utc,
        )
        _set_table_value(
            table,
            row_index,
            5,
            f"{signal.growth_percent:.4f}%",
        )
        _set_table_value(
            table,
            row_index,
            6,
            f"{signal.threshold_percent:.4f}%",
        )
        _set_table_value(
            table,
            row_index,
            7,
            signal.current_price,
        )
        _set_table_value(
            table,
            row_index,
            8,
            signal.candle_open_price,
        )
        _set_table_value(
            table,
            row_index,
            9,
            signal.status,
        )

    _fit_table_columns(table)




def fill_buy_intents_table(table: QTableWidget) -> None:
    intents = list_recent_buy_intents(limit=100)

    headers = [
        "ID",
        "UTC",
        "Инструмент",
        "Статус",
        "Причина",
        "Цена",
        "Рост",
        "Сумма покупки",
        "Лот",
        "Лотов",
        "Акций",
        "Плановая сумма",
        "Сигнал",
    ]

    table.setColumnCount(len(headers))
    table.setHorizontalHeaderLabels(headers)
    table.setRowCount(len(intents))

    for row_index, intent in enumerate(intents):
        _set_table_value(table, row_index, 0, intent.id)
        _set_table_value(table, row_index, 1, intent.created_at_utc)
        _set_table_value(table, row_index, 2, f"{intent.ticker}_{intent.class_code}")
        _set_table_value(table, row_index, 3, intent.status)
        _set_table_value(table, row_index, 4, intent.reason)
        _set_table_value(table, row_index, 5, intent.current_price)
        _set_table_value(table, row_index, 6, f"{intent.growth_percent:.4f}%")
        _set_table_value(table, row_index, 7, intent.requested_amount)
        _set_table_value(table, row_index, 8, intent.lot)
        _set_table_value(table, row_index, 9, intent.quantity_lots)
        _set_table_value(table, row_index, 10, intent.quantity_shares)
        _set_table_value(table, row_index, 11, intent.estimated_order_amount)
        _set_table_value(table, row_index, 12, intent.growth_signal_id)

    _fit_table_columns(table)


def fill_growth_cycles_table(table: QTableWidget) -> None:
    cycles = list_recent_growth_scan_cycles(limit=100)

    headers = [
        "ID",
        "Статус",
        "Старт UTC",
        "Длительность",
        "Интервал",
        "Цен",
        "Расчётов",
        "Сигналов",
        "Новых",
        "Дублей",
        "Пропущено",
        "Cache",
        "API",
        "Ошибка",
    ]

    table.setColumnCount(len(headers))
    table.setHorizontalHeaderLabels(headers)
    table.setRowCount(len(cycles))

    for row_index, cycle in enumerate(cycles):
        _set_table_value(table, row_index, 0, cycle.id)
        _set_table_value(table, row_index, 1, cycle.status)
        _set_table_value(table, row_index, 2, cycle.started_at_utc)
        _set_table_value(table, row_index, 3, f"{cycle.duration_seconds:.2f} сек.")
        _set_table_value(table, row_index, 4, cycle.interval_label)
        _set_table_value(table, row_index, 5, cycle.prices_received_count)
        _set_table_value(table, row_index, 6, cycle.results_count)
        _set_table_value(table, row_index, 7, cycle.signals_count)
        _set_table_value(table, row_index, 8, cycle.new_signals_count)
        _set_table_value(table, row_index, 9, cycle.duplicate_signals_count)
        _set_table_value(table, row_index, 10, cycle.skipped_count)
        _set_table_value(table, row_index, 11, cycle.candle_cache_hits)
        _set_table_value(table, row_index, 12, cycle.candle_api_requests)
        _set_table_value(table, row_index, 13, cycle.error_text)

    _fit_table_columns(table)
