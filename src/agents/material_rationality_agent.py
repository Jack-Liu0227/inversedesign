from __future__ import annotations

from agno.agent import Agent
from agno.db.sqlite import SqliteDb

from src.common import MATERIAL_AGENT_SHARED_DB_ID, MATERIAL_REVIEW_AGENT_DB, build_model

material_rationality_agent = Agent(
    name="Material Rationality Judge Agent",
    model=build_model("material_rationality/agent"),
    db=SqliteDb(db_file=str(MATERIAL_REVIEW_AGENT_DB), id=MATERIAL_AGENT_SHARED_DB_ID),
    instructions=[
        "Judge whether each candidate and its predicted properties are physically and engineering-wise reasonable.",
        "Input includes goal, material_type, candidates, and candidate_predictions.",
        "Return only valid JSON: {\"evaluations\": [...]}",
        "Each item in evaluations must include: candidate_index, is_valid, validity_score, reasons, risk_tags, recommended_action, cleaned_candidate.",
        "validity_score is in [0, 1].",
        "Use recommended_action keep/revise/drop.",
        "Set is_valid=false for unrealistic chemistry, impossible process constraints, or self-contradictory predictions.",
        "cleaned_candidate may be null; if provided, keep same schema as recommender candidate.",
        "If cleaned_candidate.processing is present, it must contain exactly one key: 'heat treatment method'.",
        "Do not output thermo_mechanical, microstructure_target, or other processing sub-keys.",
    ],
    markdown=True,
)
