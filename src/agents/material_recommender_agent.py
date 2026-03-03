from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd
from agno.agent import Agent
from agno.db.sqlite import SqliteDb
from agno.tools import tool

from src.common import build_model, MATERIAL_RECOMMENDER_AGENT_DB
from src.fewshot import resolve_dataset, resolve_material_type_input


def _extract_composition_cols(df: pd.DataFrame) -> List[str]:
    return [c for c in df.columns if "wt%" in c.lower() or "at%" in c.lower()]


def _extract_processing_cols(
    df: pd.DataFrame,
    composition_cols: List[str],
    target_cols: List[str],
) -> List[str]:
    excluded = set(composition_cols) | set(target_cols)
    candidates = [col for col in df.columns if col not in excluded]
    text_like = []
    for col in candidates:
        col_lower = col.lower()
        if any(
            token in col_lower
            for token in ("processing_description", "heat treatment method", "process", "description", "method", "route")
        ):
            text_like.append(col)

    if text_like:
        return text_like
    return candidates


def _serialize_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    return value


def _score_row(goal: str, row: pd.Series, target_cols: List[str]) -> float:
    goal_lower = goal.lower()
    score = 0.0
    for col in target_cols:
        val = row.get(col)
        if pd.isna(val):
            continue
        try:
            numeric = float(val)
        except (TypeError, ValueError):
            continue
        col_lower = col.lower()
        if "uts" in col_lower or "ys" in col_lower:
            score += numeric if "strength" in goal_lower or "high" in goal_lower else numeric * 0.2
        elif "el" in col_lower:
            score += numeric if "ductility" in goal_lower or "elongation" in goal_lower else numeric * 0.2
        elif "ep" in col_lower:
            score += numeric if "corrosion" in goal_lower or "pitting" in goal_lower else numeric * 0.2
    return score


def _top_candidates(df: pd.DataFrame, goal: str, target_cols: List[str], k: int = 3) -> List[Tuple[int, float]]:
    scored = []
    for i, row in df.iterrows():
        scored.append((i, _score_row(goal, row, target_cols)))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:k]


@tool
def recommend_new_material(
    goal: str,
    material_type: str = "",
    top_n: int = 3,
) -> Dict:
    return recommend_new_material_impl(
        goal=goal,
        material_type=material_type,
        top_n=top_n,
    )


def recommend_new_material_impl(
    goal: str,
    material_type: str = "",
    top_n: int = 3,
) -> Dict:
    resolved_material_type, route_reason = resolve_material_type_input(
        goal=goal,
        material_type=material_type,
    )
    spec = resolve_dataset(resolved_material_type)
    df = pd.read_csv(spec.dataset_path)
    comp_cols = _extract_composition_cols(df)
    if not comp_cols:
        raise ValueError(f"No composition columns found in dataset: {spec.dataset_path}")
    processing_cols = _extract_processing_cols(df, comp_cols, spec.target_cols)

    top_rows = _top_candidates(df, goal=goal, target_cols=spec.target_cols, k=top_n)
    candidates = []
    for idx, score in top_rows:
        row = df.iloc[idx]
        comp = {}
        for col in comp_cols:
            val = row.get(col)
            if pd.notna(val):
                element = col.split("(")[0].strip()
                comp[element] = float(val)
        processing: Dict[str, Any] = {}
        for col in processing_cols:
            val = _serialize_value(row.get(col))
            if val is None:
                continue
            if isinstance(val, str) and not val.strip():
                continue
            processing[col] = val
        candidates.append(
            {
                "composition": comp,
                "processing": processing,
                "score": float(score),
                "reason": f"Derived from high-scoring historical sample row={idx}",
            }
        )

    return {
        "material_type": resolved_material_type,
        "material_type_input": material_type,
        "material_type_route_reason": route_reason,
        "goal": goal,
        "candidates": candidates,
        "processing_fields_used": processing_cols,
    }


_recommender_model = build_model("material_recommender/agent")


material_recommender_agent = Agent(
    name="Material Recommender Agent",
    model=_recommender_model,
    db=SqliteDb(db_file=str(MATERIAL_RECOMMENDER_AGENT_DB)),
    instructions=[
        "You recommend new material candidates from historical datasets.",
        "First normalize material type to one of supported dataset keys.",
        "Always call recommend_new_material tool to produce candidate composition and processing suggestions.",
        "Return concise rationale for each candidate.",
    ],
    tools=[recommend_new_material],
    markdown=True,
)
