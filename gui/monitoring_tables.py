from PySide6.QtWidgets import QHeaderView, QTableWidget, QTableWidgetItem

from bd.growth_current_state import list_growth_current_states
from bd.growth_scan_cycle import list_recent_growth_scan_cycles
from bd.growth_signal import list_recent_growth_signals


def _set_table_value(
    table: QTableWidget,
    row: int,
    column: int,
    value: object,
) -> None:
    if value is None:
        text = ""
    else:
        text = str(value)

    table.setItem(row, column, QTableWidgetItem(text))


def _fit_table_columns(table: QTableWidget) -> None:
    table.horizontalHeader().setSectionResizeMode(
        QHeaderView.ResizeMode.ResizeToContents
    )
    table.verticalHeader().setVisible(False)


def fill_growth_current_table(table: QTableWidget) -> None:
    states = list_growth_current_states(limit=500)

    headers = [
        "Инструмент",
        "Название",
        "Рост",
        "Сигнал",
        "Текущая цена",
        "Open свечи",
        "Интервал",
        "Свеча UTC",
        "Цена UTC",
        "Источник",
        "Цикл",
        "Обновлено UTC",
    ]

    table.setColumnCount(len(headers))
    table.setHorizontalHeaderLabels(headers)
    table.setRowCount(len(states))

    for row_index, state in enumerate(states):
        _set_table_value(table, row_index, 0, f"{state.ticker}_{state.class_code}")
        _set_table_value(table, row_index, 1, state.name)
        _set_table_value(table, row_index, 2, f"{state.growth_percent:.4f}%")
        _set_table_value(table, row_index, 3, "ДА" if state.is_signal else "")
        _set_table_value(table, row_index, 4, state.current_price)
        _set_table_value(table, row_index, 5, state.candle_open_price)
        _set_table_value(table, row_index, 6, state.interval_label)
        _set_table_value(table, row_index, 7, state.candle_time_utc)
        _set_table_value(table, row_index, 8, state.last_price_time_utc)
        _set_table_value(table, row_index, 9, state.base_source)
        _set_table_value(table, row_index, 10, state.scan_cycle_id)
        _set_table_value(table, row_index, 11, state.calculated_at_utc)

    _fit_table_columns(table)


def fill_growth_signals_table(table: QTableWidget) -> None:
    signals = list_recent_growth_signals(limit=100)

    headers = [
        "ID",
        "Обнаружен UTC",
        "Инструмент",
        "Интервал",
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
        _set_table_value(table, row_index, 1, signal.detected_at_utc)
        _set_table_value(table, row_index, 2, f"{signal.ticker}_{signal.class_code}")
        _set_table_value(table, row_index, 3, signal.interval_label)
        _set_table_value(table, row_index, 4, signal.candle_time_utc)
        _set_table_value(table, row_index, 5, f"{signal.growth_percent:.4f}%")
        _set_table_value(table, row_index, 6, f"{signal.threshold_percent:.4f}%")
        _set_table_value(table, row_index, 7, signal.current_price)
        _set_table_value(table, row_index, 8, signal.candle_open_price)
        _set_table_value(table, row_index, 9, signal.status)

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
