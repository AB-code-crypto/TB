from dataclasses import dataclass
from pathlib import Path
import shutil
import sqlite3

from bd.database import DB_PATH


MEGABYTE = 1024 * 1024
MAINTENANCE_FREE_SPACE_RESERVE_BYTES = 256 * MEGABYTE


@dataclass(frozen=True)
class DatabaseMaintenanceResult:
    database_path: Path
    before_bytes: int
    after_bytes: int
    reclaimed_bytes: int
    quick_check_result: str


def _related_database_paths() -> tuple[Path, Path, Path]:
    return (
        DB_PATH,
        Path(str(DB_PATH) + "-wal"),
        Path(str(DB_PATH) + "-shm"),
    )


def get_database_total_size_bytes() -> int:
    return sum(
        path.stat().st_size
        for path in _related_database_paths()
        if path.exists()
    )


def format_database_size(bytes_count: int) -> str:
    units = ("Б", "КБ", "МБ", "ГБ", "ТБ")
    value = float(bytes_count)

    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.2f} {unit}"

        value /= 1024

    return f"{bytes_count} Б"


def _validate_free_space() -> None:
    database_size = DB_PATH.stat().st_size
    free_bytes = shutil.disk_usage(DB_PATH.parent).free
    required_free_bytes = database_size + MAINTENANCE_FREE_SPACE_RESERVE_BYTES

    if free_bytes < required_free_bytes:
        raise RuntimeError(
            "Недостаточно свободного места для VACUUM: "
            f"нужно минимум {format_database_size(required_free_bytes)}, "
            f"доступно {format_database_size(free_bytes)}."
        )


def _run_checkpoint_truncate(connection: sqlite3.Connection) -> None:
    row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()

    if row is None:
        raise RuntimeError("SQLite не вернул результат wal_checkpoint(TRUNCATE).")

    busy = int(row[0])

    if busy != 0:
        raise RuntimeError(
            "Не удалось обрезать WAL: база занята другим соединением. "
            "Закройте фоновые задачи и повторите обслуживание."
        )


def run_database_maintenance() -> DatabaseMaintenanceResult:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Файл базы данных не найден: {DB_PATH}")

    before_bytes = get_database_total_size_bytes()
    _validate_free_space()

    connection = sqlite3.connect(
        DB_PATH,
        timeout=60,
        isolation_level=None,
    )

    try:
        connection.execute("PRAGMA busy_timeout = 60000")

        quick_check_row = connection.execute("PRAGMA quick_check").fetchone()

        if quick_check_row is None:
            raise RuntimeError("SQLite не вернул результат PRAGMA quick_check.")

        quick_check_result = str(quick_check_row[0])

        if quick_check_result.lower() != "ok":
            raise RuntimeError(
                "Проверка целостности базы не пройдена: "
                f"{quick_check_result}"
            )

        _run_checkpoint_truncate(connection)
        connection.execute("VACUUM")
        connection.execute("PRAGMA optimize")
        _run_checkpoint_truncate(connection)
    finally:
        connection.close()

    after_bytes = get_database_total_size_bytes()

    return DatabaseMaintenanceResult(
        database_path=DB_PATH,
        before_bytes=before_bytes,
        after_bytes=after_bytes,
        reclaimed_bytes=max(0, before_bytes - after_bytes),
        quick_check_result=quick_check_result,
    )
