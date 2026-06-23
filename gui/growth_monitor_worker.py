import asyncio
from threading import Event

from PySide6.QtCore import QObject, Signal, Slot

from bot.growth_monitor_service import run_growth_monitor_service


class GrowthMonitorWorker(QObject):
    log_message = Signal(str)
    finished = Signal()
    failed = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self._stop_event = Event()

    @Slot()
    def run(self) -> None:
        try:
            asyncio.run(
                run_growth_monitor_service(
                    should_stop=self._stop_event.is_set,
                    on_log=self.log_message.emit,
                )
            )
        except Exception as error:
            self.failed.emit(f"{type(error).__name__}: {error}")
            return

        self.finished.emit()

    @Slot()
    def stop(self) -> None:
        self._stop_event.set()
        self.log_message.emit("Запрошена остановка мониторинга.")
