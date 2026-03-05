from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from ui.config import get_config


class DatabaseManager:
    def __init__(self) -> None:
        self._cfg = get_config()

    def _db_path(self, key: str) -> Path:
        path = self._cfg.db_paths.get(key)
        if path is None:
            raise KeyError(f"Unknown database key: {key}")
        return path

    @contextmanager
    def connect(self, key: str, *, readonly: bool = True) -> Iterator[sqlite3.Connection]:
        db_path = self._db_path(key)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        if readonly:
            if not db_path.exists():
                conn = sqlite3.connect(":memory:")
                conn.row_factory = sqlite3.Row
                try:
                    yield conn
                finally:
                    conn.close()
                return
            uri = f"file:{db_path.as_posix()}?mode=ro"
            conn = sqlite3.connect(uri, uri=True)
        else:
            conn = sqlite3.connect(db_path)

        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()


db_manager = DatabaseManager()
