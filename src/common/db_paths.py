from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DB_DIR = ROOT / "db"
DB_DIR.mkdir(parents=True, exist_ok=True)

MATERIAL_ROUTER_AGENT_DB = DB_DIR / "material_router_agent.db"
MATERIAL_RECOMMENDER_AGENT_DB = DB_DIR / "material_recommender_agent.db"
MATERIAL_PREDICTOR_AGENT_DB = DB_DIR / "material_predictor_agent.db"
MATERIAL_REVIEW_AGENT_DB = DB_DIR / "material_review_agent.db"
MATERIAL_DISCOVERY_WORKFLOW_DB = DB_DIR / "material_discovery_workflow.db"
PREDICTION_PROMPT_LOG_DB = DB_DIR / "prediction_prompt_logs.db"
