from agno.os import AgentOS

from src.agents import (
    material_predictor_agent,
    material_recommender_agent,
    material_review_agent,
    material_router_agent,
)
from src.workflows import workflow

agent_os = AgentOS(
    agents=[
        material_router_agent,
        material_recommender_agent,
        material_predictor_agent,
        material_review_agent,
    ],
    workflows=[workflow],
    tracing=False,
)
app = agent_os.get_app()

# Start AgentOS API service:
# uvicorn src.app:app --host 0.0.0.0 --port 8000 --reload
