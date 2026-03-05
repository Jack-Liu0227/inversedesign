from __future__ import annotations

from agno.db.sqlite import SqliteDb
from agno.workflow import Loop, Workflow

from src.common import MATERIAL_DISCOVERY_WORKFLOW_DB, MATERIAL_DISCOVERY_WORKFLOW_DB_ID
from src.schemas import LoopMode, WorkflowInput

from .decision_steps import end_when_satisfied
from .workflow_factory import steps_for_mode


def build_material_discovery_workflow(mode: LoopMode = "ai_only") -> Workflow:
    return Workflow(
        name="material_discovery_workflow",
        db=SqliteDb(db_file=str(MATERIAL_DISCOVERY_WORKFLOW_DB), id=MATERIAL_DISCOVERY_WORKFLOW_DB_ID),
        input_schema=WorkflowInput,
        steps=[
            Loop(
                name="Material Discovery Loop",
                max_iterations=50,
                end_condition=end_when_satisfied,
                steps=steps_for_mode(mode),
            ),
        ],
        stream_events=False,
    )


workflow = build_material_discovery_workflow()
