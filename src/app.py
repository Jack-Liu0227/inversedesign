from agno.os import AgentOS

from src.agents import (
    material_rationality_agent,
    material_predictor_agent,
    material_recommender_agent,
    material_router_agent,
)
from src.common import cleanup_workflow_logs, configure_app_logging, run_local_db_migrations, should_force_tracing


def _bootstrap_runtime() -> None:
    configure_app_logging()
    run_local_db_migrations()
    cleanup_workflow_logs()


_bootstrap_runtime()
from src.workflows import workflow

agent_os = AgentOS(
    agents=[
        material_router_agent,
        material_recommender_agent,
        material_predictor_agent,
        material_rationality_agent,
    ],
    workflows=[workflow],
    tracing=should_force_tracing(),
)
app = agent_os.get_app()

# Start AgentOS API service:
# uvicorn src.app:app --host 0.0.0.0 --port 8000 --reload
