from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path
from typing import Any

try:
    import numpy as np
except Exception:  # pragma: no cover - optional dependency
    np = None

try:
    from sentence_transformers import SentenceTransformer
except Exception:  # pragma: no cover - optional dependency
    SentenceTransformer = None

from .db_paths import MATERIAL_AGENT_SHARED_DB

_EMBEDDER: Any = None
_HAS_FTS = True
_MAX_CHUNK_CHARS = 900
_OVERLAP_CHARS = 140
_RRF_K = 60.0


def _connect() -> sqlite3.Connection:
    db_path: Path = MATERIAL_AGENT_SHARED_DB
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _split_into_segments(text: str, max_chars: int = _MAX_CHUNK_CHARS) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    blocks = [b.strip() for b in re.split(r"\n\s*\n", raw) if b.strip()]
    if not blocks:
        blocks = [raw]

    segments: list[str] = []
    cur = ""
    for block in blocks:
        candidate = f"{cur}\n\n{block}".strip() if cur else block
        if len(candidate) <= max_chars:
            cur = candidate
            continue
        if cur:
            segments.append(cur)
            tail = cur[-_OVERLAP_CHARS:] if len(cur) > _OVERLAP_CHARS else cur
            cur = f"{tail}\n{block}".strip()
            if len(cur) <= max_chars:
                continue
        # Hard split very long blocks.
        start = 0
        while start < len(block):
            end = min(len(block), start + max_chars)
            piece = block[start:end].strip()
            if piece:
                segments.append(piece)
            if end >= len(block):
                break
            start = max(0, end - _OVERLAP_CHARS)
        cur = ""
    if cur:
        segments.append(cur.strip())
    return segments


def _ensure_schema(conn: sqlite3.Connection) -> None:
    global _HAS_FTS
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS material_doc_segments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_row_id INTEGER NOT NULL,
            material_type TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_kind TEXT NOT NULL DEFAULT 'bootstrap',
            workflow_run_id TEXT NOT NULL DEFAULT '',
            run_id TEXT NOT NULL DEFAULT '',
            session_id TEXT NOT NULL DEFAULT '',
            round_index INTEGER NOT NULL DEFAULT 0,
            title TEXT NOT NULL,
            segment_index INTEGER NOT NULL,
            segment_text TEXT NOT NULL,
            segment_hash TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(doc_row_id, segment_index, segment_hash)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_doc_segments_type_round ON material_doc_segments(material_type, round_index DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_doc_segments_run_round ON material_doc_segments(workflow_run_id, round_index DESC)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS material_doc_retrieval_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    seg_cols = {str(r[1]).strip().lower() for r in conn.execute("PRAGMA table_info(material_doc_segments)").fetchall() if isinstance(r, tuple) and len(r) > 1}
    if "run_id" not in seg_cols:
        conn.execute("ALTER TABLE material_doc_segments ADD COLUMN run_id TEXT NOT NULL DEFAULT ''")
    conn.execute("UPDATE material_doc_segments SET run_id = workflow_run_id WHERE run_id = ''")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_doc_segments_run_id_round ON material_doc_segments(run_id, round_index DESC)"
    )
    try:
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS material_doc_segments_fts
            USING fts5(title, segment_text, content='material_doc_segments', content_rowid='id')
            """
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS material_doc_segments_ai AFTER INSERT ON material_doc_segments
            BEGIN
                INSERT INTO material_doc_segments_fts(rowid, title, segment_text)
                VALUES (new.id, new.title, new.segment_text);
            END
            """
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS material_doc_segments_ad AFTER DELETE ON material_doc_segments
            BEGIN
                INSERT INTO material_doc_segments_fts(material_doc_segments_fts, rowid, title, segment_text)
                VALUES('delete', old.id, old.title, old.segment_text);
            END
            """
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS material_doc_segments_au AFTER UPDATE ON material_doc_segments
            BEGIN
                INSERT INTO material_doc_segments_fts(material_doc_segments_fts, rowid, title, segment_text)
                VALUES('delete', old.id, old.title, old.segment_text);
                INSERT INTO material_doc_segments_fts(rowid, title, segment_text)
                VALUES (new.id, new.title, new.segment_text);
            END
            """
        )
        _HAS_FTS = True
    except sqlite3.OperationalError:
        _HAS_FTS = False
    conn.commit()


def ensure_material_doc_segment_index() -> None:
    conn = _connect()
    try:
        _ensure_schema(conn)
    finally:
        conn.close()


def _meta_key_for_last_id(material_type: str) -> str:
    return f"segments_last_doc_id::{material_type.strip().lower()}"


def _last_synced_doc_id(conn: sqlite3.Connection, material_type: str) -> int:
    row = conn.execute(
        "SELECT value FROM material_doc_retrieval_meta WHERE key = ?",
        (_meta_key_for_last_id(material_type),),
    ).fetchone()
    if not row:
        return 0
    try:
        return max(0, int(row[0]))
    except (TypeError, ValueError):
        return 0


def _set_last_synced_doc_id(conn: sqlite3.Connection, material_type: str, last_id: int) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO material_doc_retrieval_meta(key, value) VALUES(?, ?)",
        (_meta_key_for_last_id(material_type), str(int(max(0, last_id)))),
    )


def sync_material_doc_segments(*, material_type: str) -> int:
    mtype = str(material_type or "").strip().lower()
    if not mtype:
        return 0
    conn = _connect()
    try:
        _ensure_schema(conn)
        last_id = _last_synced_doc_id(conn, mtype)
        rows = conn.execute(
            """
            SELECT id, material_type, source_name, source_kind, workflow_run_id, run_id, session_id, round_index, title, content
            FROM material_doc_knowledge
            WHERE material_type = ? AND id > ?
            ORDER BY id ASC
            """,
            (mtype, int(last_id)),
        ).fetchall()
        inserted = 0
        max_id = last_id
        for row in rows:
            doc_id = int(row[0] or 0)
            max_id = max(max_id, doc_id)
            title = _normalize_text(row[8])
            content = str(row[9] or "")
            segments = _split_into_segments(content, max_chars=_MAX_CHUNK_CHARS)
            if not segments:
                continue
            for idx, seg in enumerate(segments):
                seg_text = _normalize_text(seg)
                seg_hash = str(abs(hash(seg_text)))
                conn.execute(
                    """
                    INSERT OR IGNORE INTO material_doc_segments (
                        doc_row_id, material_type, source_name, source_kind, workflow_run_id,
                        run_id, session_id, round_index, title, segment_index, segment_text, segment_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        doc_id,
                        str(row[1] or ""),
                        str(row[2] or ""),
                        str(row[3] or ""),
                        str(row[4] or ""),
                        str(row[5] or row[4] or ""),
                        str(row[6] or ""),
                        int(row[7] or 0),
                        title,
                        idx,
                        seg_text,
                        seg_hash,
                    ),
                )
                inserted += 1
        _set_last_synced_doc_id(conn, mtype, max_id)
        conn.commit()
        return inserted
    finally:
        conn.close()


def _to_match_query(query_text: str) -> str:
    tokens = [t for t in re.split(r"[^0-9A-Za-z\u4e00-\u9fff_]+", str(query_text or "").strip()) if t]
    if not tokens:
        return ""
    return " OR ".join(tokens[:24])


def _get_embedder() -> Any:
    global _EMBEDDER
    if _EMBEDDER is not None:
        return _EMBEDDER
    if SentenceTransformer is None:
        return None
    model_name = os.getenv("DOC_RAG_EMBED_MODEL", "all-MiniLM-L6-v2").strip() or "all-MiniLM-L6-v2"
    try:
        _EMBEDDER = SentenceTransformer(model_name)
    except Exception:
        _EMBEDDER = None
    return _EMBEDDER


def _semantic_ranks(query_text: str, rows: list[dict[str, Any]]) -> dict[int, int]:
    if not rows:
        return {}
    embedder = _get_embedder()
    if embedder is None or np is None:
        return {}
    try:
        qv = np.asarray(embedder.encode([query_text], show_progress_bar=False))[0]
        docs = np.asarray(
            embedder.encode([f"{r.get('title', '')}\n{r.get('segment_text', '')}" for r in rows], show_progress_bar=False)
        )
        denom = (np.linalg.norm(docs, axis=1) * np.linalg.norm(qv)) + 1e-9
        sims = (docs @ qv) / denom
        order = np.argsort(-sims)
        return {int(rows[idx]["id"]): int(rank + 1) for rank, idx in enumerate(order)}
    except Exception:
        return {}


def retrieve_material_doc_segments(
    *,
    material_type: str,
    query_text: str,
    workflow_run_id: str = "",
    before_round_index: int | None = None,
    top_k: int = 8,
    fetch_k: int = 30,
) -> list[dict[str, Any]]:
    mtype = str(material_type or "").strip().lower()
    if not mtype:
        return []
    ensure_material_doc_segment_index()
    sync_material_doc_segments(material_type=mtype)
    conn = _connect()
    try:
        _ensure_schema(conn)
        params: list[Any] = [mtype]
        where = ["s.material_type = ?"]
        run_id = str(workflow_run_id or "").strip()
        if run_id:
            where.append("(s.source_kind = 'bootstrap' OR (s.source_kind='iteration_feedback' AND s.workflow_run_id = ? AND s.round_index < ?))")
            params.append(run_id)
            params.append(int(before_round_index) if before_round_index is not None else 10**9)
        match_q = _to_match_query(query_text)
        rows: list[sqlite3.Row] = []
        if _HAS_FTS and match_q:
            sql = f"""
                SELECT s.id, s.source_name, s.source_kind, s.workflow_run_id, s.session_id, s.round_index,
                       s.title, s.segment_text, bm25(material_doc_segments_fts) AS bm25_score
                FROM material_doc_segments_fts f
                JOIN material_doc_segments s ON s.id = f.rowid
                WHERE material_doc_segments_fts MATCH ? AND {' AND '.join(where)}
                ORDER BY bm25_score ASC
                LIMIT ?
            """
            rows = conn.execute(sql, [match_q, *params, int(max(1, fetch_k))]).fetchall()
        if not rows:
            like = f"%{_normalize_text(query_text)}%"
            sql = f"""
                SELECT s.id, s.source_name, s.source_kind, s.workflow_run_id, s.session_id, s.round_index,
                       s.title, s.segment_text, 9999.0 AS bm25_score
                FROM material_doc_segments s
                WHERE {' AND '.join(where)} AND (s.title LIKE ? OR s.segment_text LIKE ?)
                ORDER BY s.round_index DESC, s.id DESC
                LIMIT ?
            """
            rows = conn.execute(sql, [*params, like, like, int(max(1, fetch_k))]).fetchall()
        if not rows:
            # Hard fallback: provide recent bootstrap segments to avoid empty context on cold starts.
            sql = f"""
                SELECT s.id, s.source_name, s.source_kind, s.workflow_run_id, s.session_id, s.round_index,
                       s.title, s.segment_text, 9999.0 AS bm25_score
                FROM material_doc_segments s
                WHERE {' AND '.join(where)} AND s.source_kind = 'bootstrap'
                ORDER BY s.id DESC
                LIMIT ?
            """
            rows = conn.execute(sql, [*params, int(max(1, fetch_k))]).fetchall()

        candidates: list[dict[str, Any]] = [
            {
                "id": int(r[0]),
                "source_name": str(r[1] or ""),
                "source_kind": str(r[2] or ""),
                "workflow_run_id": str(r[3] or ""),
                "session_id": str(r[4] or ""),
                "round_index": int(r[5] or 0),
                "title": str(r[6] or ""),
                "segment_text": str(r[7] or ""),
                "bm25_score": float(r[8] or 0.0),
            }
            for r in rows
        ]
        if not candidates:
            return []

        sparse_rank = {item["id"]: idx + 1 for idx, item in enumerate(candidates)}
        dense_rank = _semantic_ranks(query_text, candidates)
        for item in candidates:
            rid = int(item["id"])
            r_sparse = sparse_rank.get(rid, 10**9)
            score = 1.0 / (_RRF_K + float(r_sparse))
            if dense_rank:
                score += 1.0 / (_RRF_K + float(dense_rank.get(rid, 10**9)))
            item["retrieval_score"] = score
            item["retrieval_method"] = "hybrid_rrf" if dense_rank else "bm25_only"

        candidates.sort(key=lambda x: float(x.get("retrieval_score", 0.0)), reverse=True)
        output: list[dict[str, Any]] = []
        for item in candidates[: max(1, int(top_k))]:
            output.append(
                {
                    "source": "doc_segment",
                    "source_name": item["source_name"],
                    "source_kind": item["source_kind"],
                    "workflow_run_id": item["workflow_run_id"],
                    "session_id": item["session_id"],
                    "round_index": item["round_index"],
                    "title": item["title"],
                    "content": item["segment_text"],
                    "retrieval_score": float(item["retrieval_score"]),
                    "retrieval_method": item["retrieval_method"],
                }
            )
        return output
    finally:
        conn.close()
