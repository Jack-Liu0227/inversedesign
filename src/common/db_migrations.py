from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from src.common.db_paths import MATERIAL_DISCOVERY_WORKFLOW_DB, ROOT


def _ensure_run_status_column(db_path: Path) -> bool:
    if not db_path.exists():
        return False

    changed = False
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='agno_approvals'")
        if cur.fetchone() is None:
            return False

        cur.execute("PRAGMA table_info(agno_approvals)")
        columns = {row[1] for row in cur.fetchall()}
        if "run_status" not in columns:
            # Keep migration minimal and backwards-compatible for existing rows.
            cur.execute("ALTER TABLE agno_approvals ADD COLUMN run_status VARCHAR")
            conn.commit()
            changed = True
    finally:
        conn.close()
    return changed


def run_local_db_migrations() -> list[Path]:
    updated: list[Path] = []
    candidates: Iterable[Path] = (
        MATERIAL_DISCOVERY_WORKFLOW_DB,
        ROOT / "src" / "workflow_material_discovery.db",  # legacy location used by older revisions
    )
    for db_path in candidates:
        if _ensure_run_status_column(db_path):
            updated.append(db_path)
    return updated

