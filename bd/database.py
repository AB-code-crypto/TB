from pathlib import Path
import sqlite3
import sys


def get_project_root() -> Path:
    """
    В режиме исходников база лежит в корне проекта/data.
    В собранном .exe база лежит рядом с .exe в папке data.
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent

    return Path(__file__).resolve().parents[1]


PROJECT_ROOT = get_project_root()
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "tbank_robot.sqlite3"


def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(exist_ok=True)

    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row

    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")

    return connection
