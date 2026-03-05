from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DB_DIR = ROOT / "db"
DB_DIR.mkdir(parents=True, exist_ok=True)

# Shared Agent DB so all workflow-driven agent/tool calls can be queried under one session store.
MATERIAL_AGENT_SHARED_DB = DB_DIR / "material_agent_shared.db"
MATERIAL_AGENT_SHARED_DB_ID = "material-agent-shared-db"
MATERIAL_ROUTER_AGENT_DB = MATERIAL_AGENT_SHARED_DB
MATERIAL_RECOMMENDER_AGENT_DB = MATERIAL_AGENT_SHARED_DB
MATERIAL_PREDICTOR_AGENT_DB = MATERIAL_AGENT_SHARED_DB
MATERIAL_REVIEW_AGENT_DB = MATERIAL_AGENT_SHARED_DB
MATERIAL_DISCOVERY_WORKFLOW_DB = MATERIAL_AGENT_SHARED_DB
MATERIAL_DISCOVERY_WORKFLOW_DB_ID = "material-discovery-workflow-db"
PREDICTION_PROMPT_LOG_DB = DB_DIR / "prediction_prompt_logs.db"
WORKFLOW_AUDIT_LOG_DB = DB_DIR / "workflow_audit.db"
