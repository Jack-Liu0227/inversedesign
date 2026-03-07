from __future__ import annotations
import json, sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "db" / "material_agent_shared.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
if callable(load_dotenv):
    load_dotenv(dotenv_path=ROOT / ".env", override=False)

_KNOWN_TYPES = {"ti", "steel", "al", "hea", "hea_pitting"}
_TYPE_ALIAS = {"titanium": "ti", "ti_alloy": "ti", "aluminum": "al", "aluminium": "al", "stainless": "steel", "high_entropy": "hea"}
_THEORY_FILE = "{material_type}.theory_evolution.md"
_SUMMARY_FILE = "{run_id}.round{round_index}.summary.md"
_CANDIDATES_FILE = "{run_id}.round{round_index}.candidates.md"
_UPSERT_SQL = (
    "INSERT OR REPLACE INTO material_doc_knowledge (material_type, source_name, chunk_index, source_kind, workflow_run_id, "
    "run_id, session_id, round_index, title, content, tags_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)
_CREATE_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS material_doc_knowledge ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, material_type TEXT NOT NULL, source_name TEXT NOT NULL, chunk_index INTEGER NOT NULL, "
    "source_kind TEXT NOT NULL DEFAULT 'bootstrap', workflow_run_id TEXT NOT NULL DEFAULT '', session_id TEXT NOT NULL DEFAULT '', "
    "run_id TEXT NOT NULL DEFAULT '', "
    "round_index INTEGER NOT NULL DEFAULT 0, title TEXT NOT NULL, content TEXT NOT NULL, tags_json TEXT NOT NULL DEFAULT '[]', "
    "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, "
    "UNIQUE(material_type, source_name, chunk_index, source_kind, workflow_run_id, round_index))"
)
_ALTERS = {
    "source_kind": "ALTER TABLE material_doc_knowledge ADD COLUMN source_kind TEXT NOT NULL DEFAULT 'bootstrap'",
    "workflow_run_id": "ALTER TABLE material_doc_knowledge ADD COLUMN workflow_run_id TEXT NOT NULL DEFAULT ''",
    "run_id": "ALTER TABLE material_doc_knowledge ADD COLUMN run_id TEXT NOT NULL DEFAULT ''",
    "session_id": "ALTER TABLE material_doc_knowledge ADD COLUMN session_id TEXT NOT NULL DEFAULT ''",
    "round_index": "ALTER TABLE material_doc_knowledge ADD COLUMN round_index INTEGER NOT NULL DEFAULT 0",
}


@dataclass
class MaterialDocChunk:
    material_type: str
    source_name: str
    chunk_index: int
    title: str
    content: str
    tags: list[str]
    source_kind: str = "bootstrap"
    workflow_run_id: str = ""
    run_id: str = ""
    session_id: str = ""
    round_index: int = 0


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_TABLE_SQL)
    cols = {str(r[1]).strip().lower() for r in conn.execute("PRAGMA table_info(material_doc_knowledge)").fetchall() if isinstance(r, tuple) and len(r) > 1}
    for col, sql in _ALTERS.items():
        if col not in cols:
            conn.execute(sql)
    if "run_id" in {str(r[1]).strip().lower() for r in conn.execute("PRAGMA table_info(material_doc_knowledge)").fetchall() if isinstance(r, tuple) and len(r) > 1}:
        conn.execute("UPDATE material_doc_knowledge SET run_id = workflow_run_id WHERE run_id = ''")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_material_doc_type ON material_doc_knowledge(material_type, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_material_doc_run_round ON material_doc_knowledge(workflow_run_id, round_index DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_material_doc_run_id_round ON material_doc_knowledge(run_id, round_index DESC)")


def _chunk_to_params(c: MaterialDocChunk, *, default_source_kind: str) -> tuple[Any, ...]:
    workflow_run_id = str(c.workflow_run_id or "").strip()
    run_id = str(c.run_id or workflow_run_id).strip()
    return (c.material_type, c.source_name, int(c.chunk_index), str(c.source_kind or default_source_kind), workflow_run_id, run_id, str(c.session_id or ""), int(c.round_index or 0), c.title, c.content, json.dumps(c.tags, ensure_ascii=False))


def _upsert_chunks(conn: sqlite3.Connection, chunks: list[MaterialDocChunk], *, default_source_kind: str) -> None:
    conn.executemany(_UPSERT_SQL, [_chunk_to_params(c, default_source_kind=default_source_kind) for c in chunks])


def _parse_tags_json(raw: Any) -> list[Any]:
    try:
        tags = json.loads(raw)
    except Exception:
        return []
    return tags if isinstance(tags, list) else []


def _material_type_from_path(path: Path) -> str:
    stem = str(path.stem).strip().lower()
    return stem if stem in _KNOWN_TYPES else _TYPE_ALIAS.get(stem, "")


def _split_markdown_chunks(text: str) -> list[tuple[str, str]]:
    out, title, cur = [], "Overview", []
    for line in str(text or "").splitlines():
        line = str(line).rstrip()
        if line.startswith("#"):
            content = "\n".join(cur).strip()
            if content:
                out.append((title, content))
            title, cur = line.lstrip("#").strip() or "Overview", []
            continue
        cur.append(line)
    content = "\n".join(cur).strip()
    if content:
        out.append((title, content))
    return out


def _extract_risk_snippets(candidate_rows: list[tuple[Any, Any]], *, max_len: int = 260) -> list[str]:
    out: list[str] = []
    for title, content in candidate_rows:
        text = f"{str(title or '').strip()}\n{str(content or '').strip()}".strip()
        lower = text.lower()
        idx = lower.find("risk tags:")
        if idx < 0:
            idx = lower.find("risk_tags:")
        if idx >= 0:
            snippet = text[idx : idx + max_len]
            if snippet and snippet not in out:
                out.append(snippet)
    return out


def _extract_json_obj(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        start, end = raw.find("{"), raw.rfind("}")
        if start >= 0 and end > start:
            try:
                parsed = json.loads(raw[start : end + 1])
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                return {}
    return {}


def _top_items(values: list[str], *, limit: int = 3) -> list[str]:
    seen: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.append(text)
        if len(seen) >= max(1, int(limit)):
            break
    return seen


def _risk_tag_principle(tag: str) -> str:
    mapping = {
        "unrealistic_properties": "Do not carry forward property targets that sit outside the practical strength-ductility envelope of conventional titanium alloy routes.",
        "self_contradictory_predictions": "Reject combinations where strength and ductility both improve sharply unless a clear mechanism change supports the shift.",
        "overestimated_strength": "Treat large strength jumps as non-credible unless chemistry and aging route both support a stronger precipitation or phase-balance argument.",
        "strength_elongation_mismatch": "Use strength gains cautiously when the accompanying ductility trend is inconsistent with the proposed chemistry and heat treatment.",
        "controlled_cooling": "Extra cooling complexity should be kept only when it directly supports a necessary microstructural control mechanism.",
        "non_standard_alloy": "Non-standard alloying additions need mechanism-based justification before they are used as primary design levers.",
        "minor_impurity": "Minor impurity additions should not be treated as main strengthening strategies in next-round design moves.",
        "moderate_confidence": "Medium-confidence predictions should be converted into conservative constraints rather than taken as proof of target reachability.",
    }
    return mapping.get(str(tag or "").strip().lower(), "")


def _generic_promising_directions(*, valid_count: int, total_count: int, risk_tags: list[str]) -> list[str]:
    directions: list[str] = []
    if valid_count > 0:
        directions.append("Preserve the chemistry and heat-treatment families that remained internally consistent, then iterate with small directional moves instead of large jumps.")
    if "controlled_cooling" in {str(x).strip().lower() for x in risk_tags}:
        directions.append("Prefer simpler solution-plus-aging routes until a more complex cooling path has a clear microstructural justification.")
    directions.append("Use next-round exploration to tighten mechanism credibility first, then push property targets only within that plausible process window.")
    if valid_count <= 0 and total_count > 0:
        directions.append("Reset around the most conventional alpha-plus-beta processing window before adding novelty in chemistry or staging.")
    return _top_items(directions, limit=2)


def _round_assessment_text(*, valid_count: int, total_count: int, risk_tags: list[str]) -> str:
    if total_count <= 0:
        return "No round data was available, so the update should be treated as a placeholder summary."
    if valid_count <= 0:
        return "No candidate in this round was judged fully credible; the round mainly refined constraints and failure boundaries rather than producing a trustworthy forward path."
    if valid_count < total_count:
        return "A small set of candidates remained internally credible, but the round still behaved more like constraint discovery than objective closure."
    if risk_tags:
        return "Most candidates were internally credible, but remaining risk signals suggest the design window is still narrow and should be advanced conservatively."
    return "This round produced internally credible patterns that can be used as a stable base for the next directional iteration."


def _derive_theory_backfill_with_llm(
    *,
    material_type: str,
    goal_text: str,
    risk_texts: list[str],
    reason_texts: list[str],
    workflow_run_id: str = "",
    session_id: str = "",
    round_index: int = 0,
) -> list[str]:
    fallback_lines = [
        "Keep Ti as the balance element and adjust alloying additions in small, directional steps.",
        "Control total beta stabilizers to improve strength while limiting ductility loss.",
        "Use a complete route sentence for heat treatment: solution treatment + quench + aging.",
        "Prioritize changes that reduce previously observed risk tags and repeated invalid reasons.",
    ]
    try:
        from src.agents.material_doc_manager_agent import material_doc_manager_agent
        from src.common.material_doc_retrieval import retrieve_material_doc_segments
        from src.common.prompt_formatting import format_theory_retrieved_segments
        from src.common.prompt_llmresponse_store import log_prompt_llm_response
    except Exception:
        return fallback_lines[:4]

    retrieval_query = (
        f"goal={goal_text}\n"
        f"risk_signals={json.dumps(risk_texts[:12], ensure_ascii=False)}\n"
        f"reason_signals={json.dumps(reason_texts[:12], ensure_ascii=False)}"
    )
    retrieved_doc_segments = retrieve_material_doc_segments(
        material_type=str(material_type or "").strip().lower(),
        query_text=retrieval_query,
        workflow_run_id=str(workflow_run_id or "").strip(),
        before_round_index=int(round_index) if int(round_index or 0) > 0 else None,
        top_k=8,
        fetch_k=30,
    )
    material_type_text = str(material_type or "").strip().lower()
    goal_text_clean = str(goal_text or "").strip()
    risk_lines = [str(x or "").strip() for x in risk_texts if str(x or "").strip()][:12]
    reason_lines = [str(x or "").strip() for x in reason_texts if str(x or "").strip()][:12]
    evidence_block = format_theory_retrieved_segments(retrieved_doc_segments, max_items=8, max_snippet_chars=300)
    prompt = (
        "Generate 4-6 concise theory lines for next-round design.\n"
        "Rules: avoid repeating raw historical records; extract mechanisms/constraints/process principles.\n"
        'Return strict JSON only: {"theory_lines": ["...", "..."]}\n'
        f"Material type: {material_type_text}\n"
        f"Design objective: {goal_text_clean or 'N/A'}\n"
        "Risk signals:\n"
        + ("\n".join([f"- {x}" for x in risk_lines]) if risk_lines else "- N/A")
        + "\nReason signals:\n"
        + ("\n".join([f"- {x}" for x in reason_lines]) if reason_lines else "- N/A")
        + "\nRetrieved evidence:\n"
        + evidence_block
    )
    call_session_id = str(session_id or workflow_run_id or f"theory-{material_type}").strip()
    call_run_id = str(workflow_run_id or "").strip() or None
    start = time.perf_counter()
    try:
        response = material_doc_manager_agent.run(prompt, session_id=call_session_id)
        latency_ms = int((time.perf_counter() - start) * 1000)
        content = str(getattr(response, "content", "") or "").strip()
        if not content:
            raise RuntimeError("Theory update failed: empty content returned by LLM.")
        parsed = _extract_json_obj(content)
        log_prompt_llm_response(
            workflow_name="material_discovery_workflow",
            trace_id=call_session_id,
            session_id=call_session_id,
            run_id=call_run_id,
            step_name="Theory Doc Manager",
            agent_name="doc_manager",
            model_id=None,
            prompt_text=prompt,
            llm_response_text=content,
            response_json=parsed if isinstance(parsed, dict) else {},
            success=True,
            error_text=None,
            latency_ms=latency_ms,
        )
        lines = [str(x).strip() for x in parsed.get("theory_lines", [])] if isinstance(parsed, dict) else []
        lines = [x for x in lines if x]
        if not lines:
            return fallback_lines[:4]
        return lines[:6]
    except Exception as exc:
        latency_ms = int((time.perf_counter() - start) * 1000)
        log_prompt_llm_response(
            workflow_name="material_discovery_workflow",
            trace_id=call_session_id,
            session_id=call_session_id,
            run_id=call_run_id,
            step_name="Theory Doc Manager",
            agent_name="doc_manager",
            model_id=None,
            prompt_text=prompt,
            llm_response_text="",
            response_json={},
            success=False,
            error_text=str(exc),
            latency_ms=latency_ms,
        )
        return fallback_lines[:4]


def _extract_round_principles(
    *,
    material_type: str,
    workflow_run_id: str,
    session_id: str,
    round_index: int,
    goal: str,
    candidates: list[dict[str, Any]],
    evaluations: list[dict[str, Any]],
) -> list[str]:
    reasons, risks = [], []
    for item in evaluations:
        if not isinstance(item, dict):
            continue
        for reason in item.get("reasons", []) or []:
            text = str(reason).strip()
            if text and text not in reasons:
                reasons.append(text)
        for risk in item.get("risk_tags", []) or []:
            text = str(risk).strip()
            if text and text not in risks:
                risks.append(text)
        if len(reasons) >= 8 and len(risks) >= 8:
            break
    hints: list[str] = []
    for cand in candidates[:6]:
        if isinstance(cand, dict):
            hint = str(cand.get("reason", "") or "").strip()
            if hint:
                hints.append(hint)
        if len(hints) >= 3:
            break
    return _derive_theory_backfill_with_llm(
        material_type=material_type,
        goal_text=goal,
        risk_texts=risks,
        reason_texts=reasons + hints,
        workflow_run_id=workflow_run_id,
        session_id=session_id,
        round_index=round_index,
    )


def _load_bootstrap_material_doc(*, material_type: str) -> str:
    conn = _connect()
    try:
        _ensure_schema(conn)
        rows = conn.execute("SELECT title, content FROM material_doc_knowledge WHERE material_type = ? AND source_kind = 'bootstrap' ORDER BY source_name ASC, chunk_index ASC, id ASC", (str(material_type or "").strip().lower(),)).fetchall()
    finally:
        conn.close()
    parts: list[str] = []
    for title, content in rows:
        t, c = str(title or "").strip(), str(content or "").strip()
        if t:
            parts.append(f"## {t}")
        if c:
            parts.append(c)
    return "\n\n".join(parts).strip()


def _load_previous_theory_snapshot(*, material_type: str, workflow_run_id: str, round_index: int) -> str:
    prev_round = int(round_index) - 1
    if prev_round <= 0:
        return ""
    conn = _connect()
    try:
        _ensure_schema(conn)
        row = conn.execute(
            "SELECT content FROM material_doc_knowledge WHERE material_type = ? AND source_kind = 'iteration_feedback' AND workflow_run_id = ? AND source_name = ? AND round_index = ? ORDER BY id DESC LIMIT 1",
            (str(material_type or "").strip().lower(), str(workflow_run_id or "").strip(), _THEORY_FILE.format(material_type=str(material_type or "").strip().lower()), prev_round),
        ).fetchone()
    finally:
        conn.close()
    return str(row[0] or "").strip() if row else ""


def _build_round_theory_snapshot(
    *,
    material_type: str,
    workflow_run_id: str,
    session_id: str,
    round_index: int,
    goal: str,
    candidates: list[dict[str, Any]],
    evaluations: list[dict[str, Any]],
    principle_lines: list[str] | None = None,
) -> str:
    baseline = _load_previous_theory_snapshot(material_type=material_type, workflow_run_id=workflow_run_id, round_index=round_index) or _load_bootstrap_material_doc(material_type=material_type)
    if not baseline:
        baseline = f"# {str(material_type).upper()} Knowledge Baseline\n\nNo bootstrap content found."
    lines = [str(x).strip() for x in (principle_lines or []) if str(x).strip()] or _extract_round_principles(
        material_type=material_type,
        workflow_run_id=workflow_run_id,
        session_id=session_id,
        round_index=round_index,
        goal=goal,
        candidates=candidates,
        evaluations=evaluations,
    )
    bullets = "\n".join([f"- {x}" for x in lines]) if lines else "- Keep baseline theoretical guidance."
    section = f"\n\n---\n\n## Round {int(round_index)} Theory Update\n\nRound objective remained: {goal}\n\nRecovered theoretical guidance for next design move:\n\n{bullets}\n"
    return baseline.strip() + section


def _build_round_summary_content(
    *,
    goal: str,
    principle_lines: list[str],
    evaluations: list[dict[str, Any]],
) -> str:
    total_count = len([x for x in evaluations if isinstance(x, dict)])
    valid_count = len([x for x in evaluations if isinstance(x, dict) and bool(x.get("is_valid", False))])
    risk_tags = _top_items(
        [str(tag).strip() for item in evaluations if isinstance(item, dict) for tag in (item.get("risk_tags", []) or []) if str(tag).strip()],
        limit=4,
    )
    invalid_reasons = _top_items(
        [
            str(reason).strip()
            for item in evaluations
            if isinstance(item, dict) and not bool(item.get("is_valid", False))
            for reason in (item.get("reasons", []) or [])
            if str(reason).strip()
        ],
        limit=3,
    )
    avoid_lines = _top_items([_risk_tag_principle(tag) for tag in risk_tags if _risk_tag_principle(tag)], limit=3)
    if not avoid_lines and invalid_reasons:
        avoid_lines = [
            "Avoid repeating failure patterns that were already judged internally inconsistent in the previous round."
        ]
    promising_lines = _generic_promising_directions(valid_count=valid_count, total_count=total_count, risk_tags=risk_tags)
    principle_block = "\n".join([f"- {x}" for x in _top_items(principle_lines, limit=5)]) or "- Keep updates focused on mechanism-level constraints."
    avoid_block = "\n".join([f"- {x}" for x in avoid_lines]) or "- Avoid carrying forward claims that exceed the plausible process-property envelope."
    direction_block = "\n".join([f"- {x}" for x in promising_lines]) or "- Iterate conservatively from the most credible chemistry and process window."
    invalid_reason_block = "\n".join([f"- {x}" for x in invalid_reasons]) or "- None prominent."
    risk_tag_block = "\n".join([f"- {x}" for x in risk_tags]) or "- None prominent."
    return (
        f"Goal: {goal}\n\n"
        "Round assessment:\n"
        f"{_round_assessment_text(valid_count=valid_count, total_count=total_count, risk_tags=risk_tags)}\n\n"
        "Principles extracted:\n"
        f"{principle_block}\n\n"
        "Avoid next round:\n"
        f"{avoid_block}\n\n"
        "Promising direction:\n"
        f"{direction_block}\n\n"
        "Round evidence summary:\n"
        f"- Candidate count: {total_count}\n"
        f"- Valid count: {valid_count}\n"
        f"- Invalid count: {max(0, total_count - valid_count)}\n\n"
        "Top invalid reasons:\n"
        f"{invalid_reason_block}\n\n"
        "Top risk tags:\n"
        f"{risk_tag_block}\n"
    )


def ensure_bootstrap_material_docs(*, docs_dir: str | Path | None = None) -> int:
    conn = _connect()
    try:
        _ensure_schema(conn)
        row = conn.execute("SELECT COUNT(*) FROM material_doc_knowledge WHERE source_kind = 'bootstrap'").fetchone()
    finally:
        conn.close()
    return 0 if (int(row[0] or 0) > 0 if row else False) else upsert_material_docs_from_dir(docs_dir=docs_dir)


def upsert_material_docs_from_dir(docs_dir: str | Path | None = None) -> int:
    from src.common.material_doc_retrieval import sync_material_doc_segments

    base = Path(docs_dir) if docs_dir else ROOT / "knowledge" / "material_bootstrap"
    files = sorted([p for p in base.glob("*.md") if p.is_file()]) if base.exists() else []
    if not files:
        return 0
    rows: list[MaterialDocChunk] = []
    for path in files:
        mtype = _material_type_from_path(path)
        if not mtype:
            continue
        for idx, (title, content) in enumerate(_split_markdown_chunks(path.read_text(encoding="utf-8", errors="ignore"))):
            rows.append(MaterialDocChunk(material_type=mtype, source_name=path.name, chunk_index=idx, title=title, content=content, tags=["bootstrap", mtype]))
    if not rows:
        return 0
    conn = _connect()
    try:
        _ensure_schema(conn)
        _upsert_chunks(conn, rows, default_source_kind="bootstrap")
        conn.commit()
        for mtype in sorted({str(r.material_type or "").strip().lower() for r in rows if str(r.material_type or "").strip()}):
            sync_material_doc_segments(material_type=mtype)
        return len(rows)
    finally:
        conn.close()


def ensure_iteration_theory_snapshots(*, max_rounds: int = 200) -> int:
    conn = _connect()
    try:
        _ensure_schema(conn)
        rounds = conn.execute(
            "SELECT DISTINCT material_type, workflow_run_id, session_id, round_index FROM material_doc_knowledge WHERE source_kind = 'iteration_feedback' AND workflow_run_id <> '' AND round_index > 0 ORDER BY workflow_run_id DESC, round_index ASC LIMIT ?",
            (max(1, int(max_rounds)),),
        ).fetchall()
        inserted: list[MaterialDocChunk] = []
        for material_type, workflow_run_id, session_id, round_index in rounds:
            mtype, run_id, sess, rdx = str(material_type or "").strip().lower(), str(workflow_run_id or "").strip(), str(session_id or "").strip(), int(round_index or 0)
            if not mtype or not run_id or rdx <= 0:
                continue
            theory_file = _THEORY_FILE.format(material_type=mtype)
            exists = conn.execute(
                "SELECT 1 FROM material_doc_knowledge WHERE material_type = ? AND source_kind = 'iteration_feedback' AND workflow_run_id = ? AND round_index = ? AND source_name = ? LIMIT 1",
                (mtype, run_id, rdx, theory_file),
            ).fetchone()
            if exists:
                continue
            summary = conn.execute(
                "SELECT content FROM material_doc_knowledge WHERE source_kind='iteration_feedback' AND workflow_run_id=? AND round_index=? AND source_name=? ORDER BY id DESC LIMIT 1",
                (run_id, rdx, _SUMMARY_FILE.format(run_id=run_id, round_index=rdx)),
            ).fetchone()
            candidates = conn.execute(
                "SELECT title, content FROM material_doc_knowledge WHERE source_kind='iteration_feedback' AND workflow_run_id=? AND round_index=? AND source_name=? ORDER BY chunk_index ASC, id ASC",
                (run_id, rdx, _CANDIDATES_FILE.format(run_id=run_id, round_index=rdx)),
            ).fetchall()
            baseline = _load_previous_theory_snapshot(material_type=mtype, workflow_run_id=run_id, round_index=rdx) or _load_bootstrap_material_doc(material_type=mtype)
            if not baseline:
                baseline = f"# {mtype.upper()} Knowledge Baseline\n\nNo bootstrap content found."
            summary_text = str(summary[0] or "").strip() if summary else ""
            lines = _derive_theory_backfill_with_llm(
                material_type=mtype,
                goal_text=summary_text,
                risk_texts=_extract_risk_snippets(candidates),
                reason_texts=[summary_text],
                workflow_run_id=run_id,
                session_id=sess,
                round_index=rdx,
            )
            bullets = "\n".join([f"- {x}" for x in lines]) if lines else "- Keep baseline theoretical guidance."
            content = baseline.strip() + f"\n\n---\n\n## Round {rdx} Theory Update (Backfilled)\n\nBackfilled theory notes for this round:\n\n{bullets}\n"
            inserted.append(MaterialDocChunk(material_type=mtype, source_name=theory_file, chunk_index=0, source_kind="iteration_feedback", workflow_run_id=run_id, run_id=run_id, session_id=sess, round_index=rdx, title=f"Round {rdx} Theory Snapshot", content=content, tags=["iteration_feedback", "theory_snapshot", mtype, f"round_{rdx}", "backfilled"]))
        if not inserted:
            return 0
        _upsert_chunks(conn, inserted, default_source_kind="iteration_feedback")
        conn.commit()
        return len(inserted)
    finally:
        conn.close()


def backfill_iteration_candidate_docs(*, max_rounds: int = 500) -> int:
    conn = _connect()
    try:
        _ensure_schema(conn)
        rounds = conn.execute(
            """
            SELECT workflow_run_id, run_id, session_id, material_type, round_index, goal
            FROM material_samples
            WHERE workflow_run_id <> '' AND round_index > 0
            GROUP BY workflow_run_id, run_id, session_id, material_type, round_index, goal
            ORDER BY workflow_run_id DESC, round_index ASC
            LIMIT ?
            """,
            (max(1, int(max_rounds)),),
        ).fetchall()
        rows_to_upsert: list[MaterialDocChunk] = []
        touched_material_types: set[str] = set()
        for workflow_run_id, run_id, session_id, material_type, round_index, goal in rounds:
            workflow_run_id = str(workflow_run_id or "").strip()
            run_id = str(run_id or workflow_run_id).strip()
            session_id = str(session_id or "").strip()
            material_type = str(material_type or "").strip().lower()
            round_index = int(round_index or 0)
            goal = str(goal or "").strip()
            if not workflow_run_id or not material_type or round_index <= 0:
                continue
            sample_rows = conn.execute(
                """
                SELECT candidate_index, composition_json, processing_json, predicted_values_json, confidence,
                       prediction_error, is_valid, judge_score, judge_reasons_json, risk_tags_json, recommended_action
                FROM material_samples
                WHERE workflow_run_id = ? AND material_type = ? AND round_index = ?
                ORDER BY candidate_index ASC, id ASC
                """,
                (workflow_run_id, material_type, round_index),
            ).fetchall()
            if not sample_rows:
                continue
            touched_material_types.add(material_type)
            synthetic_evaluations: list[dict[str, Any]] = []
            for sample in sample_rows:
                _, _, _, _, _, _, is_valid, judge_score, reasons_json, risk_json, recommended_action = sample
                try:
                    reasons = json.loads(reasons_json or "[]")
                except Exception:
                    reasons = []
                try:
                    risk_tags = json.loads(risk_json or "[]")
                except Exception:
                    risk_tags = []
                synthetic_evaluations.append(
                    {
                        "is_valid": bool(is_valid),
                        "validity_score": float(judge_score or 0.0),
                        "reasons": reasons if isinstance(reasons, list) else [],
                        "risk_tags": risk_tags if isinstance(risk_tags, list) else [],
                        "recommended_action": str(recommended_action or "drop").strip().lower() or "drop",
                    }
                )
            principle_lines = _derive_theory_backfill_with_llm(
                material_type=material_type,
                goal_text=goal,
                risk_texts=_top_items(
                    [str(tag).strip() for item in synthetic_evaluations for tag in (item.get("risk_tags", []) or []) if str(tag).strip()],
                    limit=8,
                ),
                reason_texts=_top_items(
                    [str(reason).strip() for item in synthetic_evaluations for reason in (item.get("reasons", []) or []) if str(reason).strip()],
                    limit=8,
                ),
                workflow_run_id=workflow_run_id,
                session_id=session_id,
                round_index=round_index,
            )
            rows_to_upsert.append(
                MaterialDocChunk(
                    material_type=material_type,
                    source_name=_SUMMARY_FILE.format(run_id=run_id, round_index=round_index),
                    chunk_index=0,
                    source_kind="iteration_feedback",
                    workflow_run_id=workflow_run_id,
                    run_id=run_id,
                    session_id=session_id,
                    round_index=round_index,
                    title=f"Round {round_index} Summary",
                    content=_build_round_summary_content(goal=goal, principle_lines=principle_lines, evaluations=synthetic_evaluations),
                    tags=["iteration_feedback", material_type, f"round_{round_index}"],
                )
            )
            for sample in sample_rows:
                candidate_index, comp_json, proc_json, pred_json, confidence, prediction_error, is_valid, judge_score, reasons_json, risk_json, recommended_action = sample
                try:
                    composition = json.loads(comp_json or "{}")
                except Exception:
                    composition = {}
                try:
                    processing = json.loads(proc_json or "{}")
                except Exception:
                    processing = {}
                try:
                    predicted_values = json.loads(pred_json or "{}")
                except Exception:
                    predicted_values = {}
                try:
                    reasons = json.loads(reasons_json or "[]")
                except Exception:
                    reasons = []
                try:
                    risk_tags = json.loads(risk_json or "[]")
                except Exception:
                    risk_tags = []
                action = str(recommended_action or "drop").strip().lower() or "drop"
                content = (
                    f"Candidate index: {int(candidate_index or 0)}\n"
                    f"Composition: {json.dumps(composition if isinstance(composition, dict) else {}, ensure_ascii=False)}\n"
                    f"Processing: {json.dumps(processing if isinstance(processing, dict) else {}, ensure_ascii=False)}\n"
                    f"Predicted values: {json.dumps(predicted_values if isinstance(predicted_values, dict) else {}, ensure_ascii=False)}\n"
                    f"Confidence: {str(confidence or '').strip() or 'N/A'}\n"
                    f"Prediction error: {str(prediction_error or '').strip() or 'N/A'}\n"
                    "Reason: \n"
                    "Expected tradeoff: \n"
                    f"Is valid: {bool(is_valid)}\n"
                    f"Validity score: {float(judge_score or 0.0)}\n"
                    f"Recommended action: {action}\n"
                    f"Reasons: {[str(x).strip() for x in reasons if str(x).strip()]}\n"
                    f"Risk tags: {[str(x).strip() for x in risk_tags if str(x).strip()]}\n"
                )
                rows_to_upsert.append(
                    MaterialDocChunk(
                        material_type=material_type,
                        source_name=_CANDIDATES_FILE.format(run_id=run_id, round_index=round_index),
                        chunk_index=int(candidate_index or 0) + 1,
                        source_kind="iteration_feedback",
                        workflow_run_id=workflow_run_id,
                        run_id=run_id,
                        session_id=session_id,
                        round_index=round_index,
                        title=f"Round {round_index} Candidate {int(candidate_index or 0)}",
                        content=content,
                        tags=["iteration_feedback", material_type, f"round_{round_index}", action],
                    )
                )
        if not rows_to_upsert:
            return 0
        _upsert_chunks(conn, rows_to_upsert, default_source_kind="iteration_feedback")
        conn.commit()
        try:
            from src.common.material_doc_retrieval import sync_material_doc_segments
        except Exception:
            sync_material_doc_segments = None
        if callable(sync_material_doc_segments):
            for material_type in sorted(touched_material_types):
                sync_material_doc_segments(material_type=material_type)
        return len(rows_to_upsert)
    finally:
        conn.close()


def upsert_iteration_doc_context(*, material_type: str, workflow_run_id: str, session_id: str, round_index: int, goal: str, candidates: list[dict[str, Any]], predictions: list[dict[str, Any]], evaluations: list[dict[str, Any]], limit: int = 10) -> int:
    from src.common.material_doc_retrieval import sync_material_doc_segments

    ensure_bootstrap_material_docs()
    mtype, run_id, sess, rdx = str(material_type or "").strip().lower(), str(workflow_run_id or "").strip(), str(session_id or "").strip(), max(1, int(round_index))
    if not mtype or not run_id:
        return 0
    eval_map = {item["candidate_index"]: item for item in evaluations if isinstance(item, dict) and isinstance(item.get("candidate_index"), int)}
    pred_map = {item["candidate_index"]: item for item in predictions if isinstance(item, dict) and isinstance(item.get("candidate_index"), int)}
    principle_lines = _extract_round_principles(
        material_type=mtype,
        workflow_run_id=run_id,
        session_id=sess,
        round_index=rdx,
        goal=goal,
        candidates=candidates,
        evaluations=evaluations,
    )
    rows = [MaterialDocChunk(material_type=mtype, source_name=_SUMMARY_FILE.format(run_id=run_id, round_index=rdx), chunk_index=0, source_kind="iteration_feedback", workflow_run_id=run_id, run_id=run_id, session_id=sess, round_index=rdx, title=f"Round {rdx} Summary", content=_build_round_summary_content(goal=goal, principle_lines=principle_lines, evaluations=evaluations), tags=["iteration_feedback", mtype, f"round_{rdx}", "principle_summary"])]
    for idx, candidate in enumerate(candidates[: max(1, int(limit))]):
        cand, ev = candidate if isinstance(candidate, dict) else {}, eval_map.get(idx, {})
        pred = pred_map.get(idx, {})
        reasons = [str(x).strip() for x in ev.get("reasons", []) if str(x).strip()] if isinstance(ev, dict) else []
        risk_tags = [str(x).strip() for x in ev.get("risk_tags", []) if str(x).strip()] if isinstance(ev, dict) else []
        action = str(ev.get("recommended_action", "drop") if isinstance(ev, dict) else "drop").strip().lower()
        predicted_values = pred.get("predicted_values", {}) if isinstance(pred.get("predicted_values"), dict) else {}
        confidence = str(pred.get("confidence", "") or "").strip() if isinstance(pred, dict) else ""
        prediction_error = str(pred.get("prediction_error", "") or pred.get("error", "") or "").strip() if isinstance(pred, dict) else ""
        content = (
            f"Candidate index: {idx}\nComposition: {json.dumps(cand.get('composition', {}), ensure_ascii=False)}\n"
            f"Processing: {json.dumps(cand.get('processing', {}), ensure_ascii=False)}\n"
            f"Predicted values: {json.dumps(predicted_values, ensure_ascii=False)}\n"
            f"Confidence: {confidence or 'N/A'}\n"
            f"Prediction error: {prediction_error or 'N/A'}\n"
            f"Reason: {str(cand.get('reason', '') or '').strip()}\nExpected tradeoff: {str(cand.get('expected_tradeoff', '') or '').strip()}\n"
            f"Is valid: {bool(ev.get('is_valid', False)) if isinstance(ev, dict) else False}\nValidity score: {float(ev.get('validity_score', 0.0) or 0.0) if isinstance(ev, dict) else 0.0}\n"
            f"Recommended action: {action}\nReasons: {reasons}\nRisk tags: {risk_tags}\n"
        )
        rows.append(MaterialDocChunk(material_type=mtype, source_name=_CANDIDATES_FILE.format(run_id=run_id, round_index=rdx), chunk_index=idx + 1, source_kind="iteration_feedback", workflow_run_id=run_id, run_id=run_id, session_id=sess, round_index=rdx, title=f"Round {rdx} Candidate {idx}", content=content, tags=["iteration_feedback", mtype, f"round_{rdx}", action or "drop"]))
    rows.append(
        MaterialDocChunk(
            material_type=mtype,
            source_name=_THEORY_FILE.format(material_type=mtype),
            chunk_index=0,
            source_kind="iteration_feedback",
            workflow_run_id=run_id,
            run_id=run_id,
            session_id=sess,
            round_index=rdx,
            title=f"Round {rdx} Theory Snapshot",
            content=_build_round_theory_snapshot(
                material_type=mtype,
                workflow_run_id=run_id,
                session_id=sess,
                round_index=rdx,
                goal=goal,
                candidates=candidates,
                evaluations=evaluations,
                principle_lines=principle_lines,
            ),
            tags=["iteration_feedback", "theory_snapshot", mtype, f"round_{rdx}"],
        )
    )
    conn = _connect()
    try:
        _ensure_schema(conn)
        _upsert_chunks(conn, rows, default_source_kind="iteration_feedback")
        conn.commit()
        sync_material_doc_segments(material_type=mtype)
        return len(rows)
    finally:
        conn.close()


def fetch_material_doc_context(material_type: str, limit: int = 8, *, workflow_run_id: str = "", before_round_index: int | None = None) -> list[dict[str, Any]]:
    ensure_bootstrap_material_docs()
    mtype, run_id, capped = str(material_type or "").strip().lower(), str(workflow_run_id or "").strip(), max(1, int(limit))
    conn = _connect()
    try:
        _ensure_schema(conn)
        if run_id:
            rows = conn.execute(
                "SELECT source_name, chunk_index, title, content, tags_json, source_kind, workflow_run_id, run_id, session_id, round_index "
                "FROM material_doc_knowledge WHERE material_type = ? AND (source_kind = 'bootstrap' OR (source_kind = 'iteration_feedback' AND workflow_run_id = ? AND round_index < ? AND source_name = ?)) "
                "ORDER BY source_kind ASC, round_index DESC, id DESC LIMIT ?",
                (mtype, run_id, int(before_round_index) if before_round_index is not None else 10**9, _THEORY_FILE.format(material_type=mtype), capped),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT source_name, chunk_index, title, content, tags_json, source_kind, workflow_run_id, run_id, session_id, round_index "
                "FROM material_doc_knowledge WHERE material_type = ? AND (source_kind = 'bootstrap' OR (source_kind = 'iteration_feedback' AND source_name = ?)) "
                "ORDER BY source_kind ASC, round_index DESC, id DESC LIMIT ?",
                (mtype, _THEORY_FILE.format(material_type=mtype), capped),
            ).fetchall()
    finally:
        conn.close()
    out: list[dict[str, Any]] = []
    for source_name, chunk_index, title, content, tags_json, source_kind, row_workflow_run_id, row_run_id, session_id, round_index in rows:
        out.append({"source_name": str(source_name or ""), "chunk_index": int(chunk_index or 0), "title": str(title or ""), "content": str(content or ""), "tags": _parse_tags_json(tags_json), "source_kind": str(source_kind or ""), "workflow_run_id": str(row_workflow_run_id or ""), "run_id": str(row_run_id or row_workflow_run_id or ""), "session_id": str(session_id or ""), "round_index": int(round_index or 0)})
    return out
