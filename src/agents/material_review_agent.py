from agno.agent import Agent
from agno.db.sqlite import SqliteDb

from src.common import build_model, MATERIAL_REVIEW_AGENT_DB

_review_model = build_model("material_review/agent")


material_review_agent = Agent(
    name="Material Review Agent",
    model=_review_model,
    db=SqliteDb(db_file=str(MATERIAL_REVIEW_AGENT_DB)),
    instructions=[
        "You prepare concise decision support for human reviewers.",
        "Highlight top predicted properties and major risk points.",
        "Do not invent experimental measurements.",
    ],
    markdown=True,
)
