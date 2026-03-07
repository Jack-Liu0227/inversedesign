from __future__ import annotations

from agno.agent import Agent
from agno.db.sqlite import SqliteDb

from src.common.db_paths import MATERIAL_AGENT_SHARED_DB, MATERIAL_AGENT_SHARED_DB_ID
from src.common.model_factory import build_model


material_doc_manager_agent = Agent(
    name="Material Doc Manager Agent",
    model=build_model("material_doc_manager/agent"),
    db=SqliteDb(db_file=str(MATERIAL_AGENT_SHARED_DB), id=MATERIAL_AGENT_SHARED_DB_ID),
    instructions=[
        "You maintain long-horizon theory documents for iterative alloy design.",
        'Return strict JSON only: {"theory_lines": ["...", "..."]}.',
        "Each line must be concise, reusable, and mechanism-oriented.",
        "Never include candidate history dumps, counts, or verbose narrative.",
    ],
    markdown=True,
)

