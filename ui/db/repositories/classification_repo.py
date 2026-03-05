from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from ui.db.connection import db_manager
from ui.services.timezone_service import normalize_row_datetimes


class ClassificationRepository:
    DB_KEY = "ui_classifications"

    def run_migrations(self) -> None:
        migration_path = Path(__file__).resolve().parents[2] / "migrations" / "001_classification.sql"
        sql = migration_path.read_text(encoding="utf-8")
        with db_manager.connect(self.DB_KEY, readonly=False) as conn:
            conn.executescript(sql)
            conn.commit()

    def create_tag(self, *, name: str, color: str, group_name: str, description: str) -> dict[str, Any]:
        with db_manager.connect(self.DB_KEY, readonly=False) as conn:
            conn.execute(
                """
                INSERT INTO ui_tags(name, color, group_name, description)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    color = excluded.color,
                    group_name = excluded.group_name,
                    description = excluded.description,
                    updated_at = CURRENT_TIMESTAMP
                """,
                [name, color, group_name, description],
            )
            row = conn.execute("SELECT * FROM ui_tags WHERE name = ?", [name]).fetchone()
            conn.commit()
        return normalize_row_datetimes(dict(row))

    def list_tags(self) -> list[dict[str, Any]]:
        with db_manager.connect(self.DB_KEY, readonly=True) as conn:
            rows = conn.execute(
                "SELECT * FROM ui_tags ORDER BY group_name ASC, name ASC"
            ).fetchall()
        return [normalize_row_datetimes(dict(r)) for r in rows]

    def upsert_annotation(
        self,
        *,
        source_db: str,
        source_table: str,
        source_pk: str,
        status: str,
        priority: str,
        note: str,
    ) -> int:
        with db_manager.connect(self.DB_KEY, readonly=False) as conn:
            conn.execute(
                """
                INSERT INTO ui_record_annotations(source_db, source_table, source_pk, status, priority, note)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_db, source_table, source_pk) DO UPDATE SET
                    status = excluded.status,
                    priority = excluded.priority,
                    note = excluded.note,
                    updated_at = CURRENT_TIMESTAMP
                """,
                [source_db, source_table, source_pk, status, priority, note],
            )
            row = conn.execute(
                """
                SELECT id FROM ui_record_annotations
                WHERE source_db = ? AND source_table = ? AND source_pk = ?
                """,
                [source_db, source_table, source_pk],
            ).fetchone()
            conn.commit()
        return int(row["id"])

    def assign_tags(self, *, annotation_id: int, tag_names: list[str]) -> None:
        with db_manager.connect(self.DB_KEY, readonly=False) as conn:
            for tag_name in tag_names:
                conn.execute(
                    """
                    INSERT INTO ui_tags(name) VALUES(?)
                    ON CONFLICT(name) DO NOTHING
                    """,
                    [tag_name],
                )
                tag_row = conn.execute("SELECT id FROM ui_tags WHERE name = ?", [tag_name]).fetchone()
                conn.execute(
                    """
                    INSERT INTO ui_record_tag_map(annotation_id, tag_id)
                    VALUES(?, ?)
                    ON CONFLICT(annotation_id, tag_id) DO NOTHING
                    """,
                    [annotation_id, int(tag_row["id"])],
                )
            conn.commit()

    def get_annotations(self) -> list[dict[str, Any]]:
        with db_manager.connect(self.DB_KEY, readonly=True) as conn:
            rows = conn.execute(
                """
                SELECT a.*, GROUP_CONCAT(t.name, ',') AS tags
                FROM ui_record_annotations a
                LEFT JOIN ui_record_tag_map m ON m.annotation_id = a.id
                LEFT JOIN ui_tags t ON t.id = m.tag_id
                GROUP BY a.id
                ORDER BY a.updated_at DESC
                """
            ).fetchall()
        return [normalize_row_datetimes(dict(r)) for r in rows]


classification_repo = ClassificationRepository()
