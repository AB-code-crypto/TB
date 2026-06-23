import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

from PySide6.QtCore import QObject, Signal, Slot


class AsyncTaskWorker(QObject):
    finished = Signal(object)
    failed = Signal(str)

    def __init__(self, task_factory: Callable[[], Coroutine[Any, Any, object]]) -> None:
        super().__init__()
        self.task_factory = task_factory

    @Slot()
    def run(self) -> None:
        try:
            result = asyncio.run(self.task_factory())
        except Exception as error:
            self.failed.emit(f"{type(error).__name__}: {error}")
            return

        self.finished.emit(result)