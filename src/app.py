from agno.os import AgentOS

from src.agents import (
    material_doc_manager_agent,
    material_rationality_agent,
    material_predictor_agent,
    material_recommender_agent,
    material_router_agent,
)
from src.common import (
    backfill_iteration_candidate_docs,
    cleanup_workflow_logs,
    configure_app_logging,
    ensure_bootstrap_material_docs,
    ensure_material_doc_segment_index,
    ensure_iteration_theory_snapshots,
    run_local_db_migrations,
    should_force_tracing,
)


def _bootstrap_runtime() -> None:
    configure_app_logging()
    run_local_db_migrations()
    ensure_bootstrap_material_docs()
    ensure_material_doc_segment_index()
    backfill_iteration_candidate_docs()
    ensure_iteration_theory_snapshots()
    cleanup_workflow_logs()


_bootstrap_runtime()
from src.workflows import workflow

agent_os = AgentOS(
    agents=[
        material_router_agent,
        material_recommender_agent,
        material_predictor_agent,
        material_rationality_agent,
        material_doc_manager_agent,
    ],
    workflows=[workflow],
    tracing=should_force_tracing(),
)
app = agent_os.get_app()

# Start AgentOS API service:
# python src/run_agent_os.py --host 0.0.0.0 --port 8000 --reload
