from __future__ import annotations
import json, os, sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import litellm
except Exception:
    litellm = None
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "db" / "material_agent_shared.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
MODEL_CONFIG_DIR = ROOT / "src" / "model_config"
PROVIDERS_CONFIG_PATH = MODEL_CONFIG_DIR / "providers.json"
AGENT_MODELS_CONFIG_PATH = MODEL_CONFIG_DIR / "agent_models.json"
if callable(load_dotenv):
    load_dotenv(dotenv_path=ROOT / ".env", override=False)

_KNOWN_TYPES = {"ti", "steel", "al", "hea", "hea_pitting"}
_TYPE_ALIAS = {"titanium": "ti", "ti_alloy": "ti", "aluminum": "al", "aluminium": "al", "stainless": "steel", "high_entropy": "hea"}
_THEORY_FILE = "{material_type}.theory_evolution.md"
_SUMMARY_FILE = "{run_id}.round{round_index}.summary.md"
_CANDIDATES_FILE = "{run_id}.round{round_index}.candidates.md"
_UPSERT_SQL = (
    "INSERT OR REPLACE INTO material_doc_knowledge (material_type, source_name, chunk_index, source_kind, workflow_run_id, "
    "session_id, round_index, title, content, tags_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)
_CREATE_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS material_doc_knowledge ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, material_type TEXT NOT NULL, source_name TEXT NOT NULL, chunk_index INTEGER NOT NULL, "
    "source_kind TEXT NOT NULL DEFAULT 'bootstrap', workflow_run_id TEXT NOT NULL DEFAULT '', session_id TEXT NOT NULL DEFAULT '', "
    "round_index INTEGER NOT NULL DEFAULT 0, title TEXT NOT NULL, content TEXT NOT NULL, tags_json TEXT NOT NULL DEFAULT '[]', "
    "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, "
    "UNIQUE(material_type, source_name, chunk_index, source_kind, workflow_run_id, round_index))"
)
_ALTERS = {
    "source_kind": "ALTER TABLE material_doc_knowledge ADD COLUMN source_kind TEXT NOT NULL DEFAULT 'bootstrap'",
    "workflow_run_id": "ALTER TABLE material_doc_knowledge ADD COLUMN workflow_run_id TEXT NOT NULL DEFAULT ''",
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
    session_id: str = ""
    round_index: int = 0


def _pick_first(value: str) -> str:
    return str(value or "").split(",")[0].strip()


def _env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _resolve_theory_llm_config() -> dict[str, str]:
    providers_cfg, models_cfg = _read_json_file(PROVIDERS_CONFIG_PATH), _read_json_file(AGENT_MODELS_CONFIG_PATH)
    if not providers_cfg or not models_cfg:
        return {"model": _env_str("MODEL_ID") or _env_str("DEEPSEEK_MODEL"), "api_key": _pick_first(_env_str("DEEPSEEK_API_KEY")), "base_url": _env_str("DEEPSEEK_BASE_URLS", "https://api.deepseek.com/v1")}
    default_binding = models_cfg.get("default", {}) if isinstance(models_cfg.get("default"), dict) else {}
    provider_name = str(default_binding.get("provider", providers_cfg.get("default_provider", "")) or "").strip().lower()
    provider_map = providers_cfg.get("providers", {}) if isinstance(providers_cfg.get("providers"), dict) else {}
    spec = provider_map.get(provider_name, {}) if isinstance(provider_map.get(provider_name), dict) else {}
    model_id_env = str(default_binding.get("model_id_env", "MODEL_ID")).strip()
    default_model_env = str(spec.get("default_model_env", "")).strip()
    api_key_env = str(spec.get("api_key_env", "")).strip()
    base_url_env = str(spec.get("base_url_env", "")).strip()
    return {
        "model": _env_str(model_id_env) or _env_str(default_model_env) or str(spec.get("default_model", "")).strip(),
        "api_key": _pick_first(_env_str(api_key_env) or str(spec.get("api_key", "")).strip()),
        "base_url": _env_str(base_url_env) or str(spec.get("base_url", "")).strip(),
    }


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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_material_doc_type ON material_doc_knowledge(material_type, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_material_doc_run_round ON material_doc_knowledge(workflow_run_id, round_index DESC)")


def _chunk_to_params(c: MaterialDocChunk, *, default_source_kind: str) -> tuple[Any, ...]:
    return (c.material_type, c.source_name, int(c.chunk_index), str(c.source_kind or default_source_kind), str(c.workflow_run_id or ""), str(c.session_id or ""), int(c.round_index or 0), c.title, c.content, json.dumps(c.tags, ensure_ascii=False))


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


def _derive_theory_backfill_with_llm(*, material_type: str, goal_text: str, risk_texts: list[str], reason_texts: list[str]) -> list[str]:
    if litellm is None:
        raise RuntimeError("Theory update failed: litellm is not available.")
    cfg = _resolve_theory_llm_config()
    model, api_key, base_url = str(cfg.get("model") or "").strip(), str(cfg.get("api_key") or "").strip(), str(cfg.get("base_url") or "").strip()
    if not model or not base_url:
        raise RuntimeError("Theory update failed: missing model/base_url for LLM config.")
    if not api_key and not (("localhost" in base_url) or ("127.0.0.1" in base_url)):
        raise RuntimeError("Theory update failed: missing API key for remote LLM provider.")
    payload = {
        "material_type": str(material_type or "").strip().lower(),
        "goal": str(goal_text or "").strip(),
        "risk_signals": [str(x or "").strip() for x in risk_texts if str(x or "").strip()][:12],
        "reason_signals": [str(x or "").strip() for x in reason_texts if str(x or "").strip()][:12],
    }
    system_prompt = "You are a metallurgy knowledge abstraction assistant. Given round signals, output transferable theory guidance only (no candidate history, no metrics, no counts). Return strict JSON: {\"theory_lines\": [\"...\", \"...\"]}. Each line should be actionable and generalizable."
    user_prompt = "Generate 4-6 concise theory lines for next-round design.\nRules: avoid repeating raw historical records; extract mechanisms/constraints/process principles.\nInput JSON:\n" + json.dumps(payload, ensure_ascii=False)
    try:
        resp = litellm.completion(model=model, messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}], temperature=0.2, api_key=api_key or None, base_url=base_url, timeout=45)
        content = str(resp.choices[0].message.content or "").strip() if resp and getattr(resp, "choices", None) else ""
        if not content:
            raise RuntimeError("Theory update failed: empty content returned by LLM.")
        parsed = _extract_json_obj(content)
        lines = [str(x).strip() for x in parsed.get("theory_lines", [])] if isinstance(parsed, dict) else []
        lines = [x for x in lines if x]
        if not lines:
            raise RuntimeError("Theory update failed: LLM response missing theory_lines.")
        return lines[:6]
    except Exception as exc:
        raise RuntimeError(f"Theory update failed: LLM call error: {exc}") from exc


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


def _build_round_theory_snapshot(*, material_type: str, workflow_run_id: str, round_index: int, goal: str, candidates: list[dict[str, Any]], evaluations: list[dict[str, Any]]) -> str:
    baseline = _load_previous_theory_snapshot(material_type=material_type, workflow_run_id=workflow_run_id, round_index=round_index) or _load_bootstrap_material_doc(material_type=material_type)
    if not baseline:
        baseline = f"# {str(material_type).upper()} Knowledge Baseline\n\nNo bootstrap content found."
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
    lines = _derive_theory_backfill_with_llm(material_type=material_type, goal_text=goal, risk_texts=risks, reason_texts=reasons + hints)
    bullets = "\n".join([f"- {x}" for x in lines]) if lines else "- Keep baseline theoretical guidance."
    section = f"\n\n---\n\n## Round {int(round_index)} Theory Update\n\nRound objective remained: {goal}\n\nRecovered theoretical guidance for next design move:\n\n{bullets}\n"
    return baseline.strip() + section


def ensure_bootstrap_material_docs(*, docs_dir: str | Path | None = None) -> int:
    conn = _connect()
    try:
        _ensure_schema(conn)
        row = conn.execute("SELECT COUNT(*) FROM material_doc_knowledge WHERE source_kind = 'bootstrap'").fetchone()
    finally:
        conn.close()
    return 0 if (int(row[0] or 0) > 0 if row else False) else upsert_material_docs_from_dir(docs_dir=docs_dir)


def upsert_material_docs_from_dir(docs_dir: str | Path | None = None) -> int:
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
            lines = _derive_theory_backfill_with_llm(material_type=mtype, goal_text=summary_text, risk_texts=_extract_risk_snippets(candidates), reason_texts=[summary_text])
            bullets = "\n".join([f"- {x}" for x in lines]) if lines else "- Keep baseline theoretical guidance."
            content = baseline.strip() + f"\n\n---\n\n## Round {rdx} Theory Update (Backfilled)\n\nBackfilled theory notes for this round:\n\n{bullets}\n"
            inserted.append(MaterialDocChunk(material_type=mtype, source_name=theory_file, chunk_index=0, source_kind="iteration_feedback", workflow_run_id=run_id, session_id=sess, round_index=rdx, title=f"Round {rdx} Theory Snapshot", content=content, tags=["iteration_feedback", "theory_snapshot", mtype, f"round_{rdx}", "backfilled"]))
        if not inserted:
            return 0
        _upsert_chunks(conn, inserted, default_source_kind="iteration_feedback")
        conn.commit()
        return len(inserted)
    finally:
        conn.close()


def upsert_iteration_doc_context(*, material_type: str, workflow_run_id: str, session_id: str, round_index: int, goal: str, candidates: list[dict[str, Any]], evaluations: list[dict[str, Any]], limit: int = 10) -> int:
    ensure_bootstrap_material_docs()
    mtype, run_id, sess, rdx = str(material_type or "").strip().lower(), str(workflow_run_id or "").strip(), str(session_id or "").strip(), max(1, int(round_index))
    if not mtype or not run_id:
        return 0
    eval_map = {item["candidate_index"]: item for item in evaluations if isinstance(item, dict) and isinstance(item.get("candidate_index"), int)}
    rows = [MaterialDocChunk(material_type=mtype, source_name=_SUMMARY_FILE.format(run_id=run_id, round_index=rdx), chunk_index=0, source_kind="iteration_feedback", workflow_run_id=run_id, session_id=sess, round_index=rdx, title=f"Round {rdx} Summary", content=f"Goal: {goal}", tags=["iteration_feedback", mtype, f"round_{rdx}"])]
    for idx, candidate in enumerate(candidates[: max(1, int(limit))]):
        cand, ev = candidate if isinstance(candidate, dict) else {}, eval_map.get(idx, {})
        reasons = [str(x).strip() for x in ev.get("reasons", []) if str(x).strip()] if isinstance(ev, dict) else []
        risk_tags = [str(x).strip() for x in ev.get("risk_tags", []) if str(x).strip()] if isinstance(ev, dict) else []
        action = str(ev.get("recommended_action", "drop") if isinstance(ev, dict) else "drop").strip().lower()
        content = (
            f"Candidate index: {idx}\nComposition: {json.dumps(cand.get('composition', {}), ensure_ascii=False)}\n"
            f"Processing: {json.dumps(cand.get('processing', {}), ensure_ascii=False)}\n"
            f"Reason: {str(cand.get('reason', '') or '').strip()}\nExpected tradeoff: {str(cand.get('expected_tradeoff', '') or '').strip()}\n"
            f"Is valid: {bool(ev.get('is_valid', False)) if isinstance(ev, dict) else False}\nValidity score: {float(ev.get('validity_score', 0.0) or 0.0) if isinstance(ev, dict) else 0.0}\n"
            f"Recommended action: {action}\nReasons: {reasons}\nRisk tags: {risk_tags}\n"
        )
        rows.append(MaterialDocChunk(material_type=mtype, source_name=_CANDIDATES_FILE.format(run_id=run_id, round_index=rdx), chunk_index=idx + 1, source_kind="iteration_feedback", workflow_run_id=run_id, session_id=sess, round_index=rdx, title=f"Round {rdx} Candidate {idx}", content=content, tags=["iteration_feedback", mtype, f"round_{rdx}", action or "drop"]))
    rows.append(
        MaterialDocChunk(
            material_type=mtype,
            source_name=_THEORY_FILE.format(material_type=mtype),
            chunk_index=0,
            source_kind="iteration_feedback",
            workflow_run_id=run_id,
            session_id=sess,
            round_index=rdx,
            title=f"Round {rdx} Theory Snapshot",
            content=_build_round_theory_snapshot(material_type=mtype, workflow_run_id=run_id, round_index=rdx, goal=goal, candidates=candidates, evaluations=evaluations),
            tags=["iteration_feedback", "theory_snapshot", mtype, f"round_{rdx}"],
        )
    )
    conn = _connect()
    try:
        _ensure_schema(conn)
        _upsert_chunks(conn, rows, default_source_kind="iteration_feedback")
        conn.commit()
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
                "SELECT source_name, chunk_index, title, content, tags_json, source_kind, workflow_run_id, session_id, round_index "
                "FROM material_doc_knowledge WHERE material_type = ? AND (source_kind = 'bootstrap' OR (source_kind = 'iteration_feedback' AND workflow_run_id = ? AND round_index < ? AND source_name = ?)) "
                "ORDER BY source_kind ASC, round_index DESC, id DESC LIMIT ?",
                (mtype, run_id, int(before_round_index) if before_round_index is not None else 10**9, _THEORY_FILE.format(material_type=mtype), capped),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT source_name, chunk_index, title, content, tags_json, source_kind, workflow_run_id, session_id, round_index "
                "FROM material_doc_knowledge WHERE material_type = ? AND (source_kind = 'bootstrap' OR (source_kind = 'iteration_feedback' AND source_name = ?)) "
                "ORDER BY source_kind ASC, round_index DESC, id DESC LIMIT ?",
                (mtype, _THEORY_FILE.format(material_type=mtype), capped),
            ).fetchall()
    finally:
        conn.close()
    out: list[dict[str, Any]] = []
    for source_name, chunk_index, title, content, tags_json, source_kind, row_run_id, session_id, round_index in rows:
        out.append({"source_name": str(source_name or ""), "chunk_index": int(chunk_index or 0), "title": str(title or ""), "content": str(content or ""), "tags": _parse_tags_json(tags_json), "source_kind": str(source_kind or ""), "workflow_run_id": str(row_run_id or ""), "session_id": str(session_id or ""), "round_index": int(round_index or 0)})
    return out
