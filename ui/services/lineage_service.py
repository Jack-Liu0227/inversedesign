from __future__ import annotations

from typing import Any

from ui.db.repositories.session_repo import SessionRepository, session_repo
from ui.db.repositories.workflow_repo import WorkflowRepository, workflow_repo


class LineageService:
    def __init__(
        self,
        workflow_repository: WorkflowRepository | None = None,
        session_repository: SessionRepository | None = None,
    ) -> None:
        self._workflow_repository = workflow_repository or workflow_repo
        self._session_repository = session_repository or session_repo

    def build_lineage(self, trace_or_session_id: str) -> dict[str, Any]:
        events = self._workflow_repository.find_lineage_events(trace_or_session_id)
        audits = self._workflow_repository.find_run_audits(trace_or_session_id)

        linked_session_ids = {trace_or_session_id}
        for event in events:
            if event.get("session_id"):
                linked_session_ids.add(event["session_id"])
        for audit in audits:
            if audit.get("session_id"):
                linked_session_ids.add(audit["session_id"])

        sessions: list[dict[str, Any]] = []
        for sid in linked_session_ids:
            sessions.extend(self._session_repository.find_sessions(sid))

        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, str]] = []
        timeline: list[dict[str, Any]] = []

        for event in events:
            node_id = f"event:{event['id']}"
            nodes.append(
                {
                    "id": node_id,
                    "kind": "event",
                    "label": f"{event.get('step_name') or 'Event'} / {event.get('event_type') or '-'}",
                    "created_at": event.get("created_at"),
                    "data": event,
                }
            )
            session_id = event.get("session_id")
            if session_id:
                edges.append({"source": f"session:{session_id}", "target": node_id, "relation": "emits"})
            timeline.append(
                {
                    "created_at": event.get("created_at"),
                    "kind": "event",
                    "title": nodes[-1]["label"],
                    "data": event,
                }
            )

        for audit in audits:
            node_id = f"run:{audit['id']}"
            nodes.append(
                {
                    "id": node_id,
                    "kind": "run_audit",
                    "label": f"Run decision: {audit.get('decision')}",
                    "created_at": audit.get("created_at"),
                    "data": audit,
                }
            )
            session_id = audit.get("session_id")
            if session_id:
                edges.append({"source": f"session:{session_id}", "target": node_id, "relation": "summarizes"})
            timeline.append(
                {
                    "created_at": audit.get("created_at"),
                    "kind": "run_audit",
                    "title": nodes[-1]["label"],
                    "data": audit,
                }
            )

        for session in sessions:
            sid = session.get("session_id")
            nodes.append(
                {
                    "id": f"session:{sid}",
                    "kind": "session",
                    "label": f"Session {sid}",
                    "created_at": str(session.get("created_at")) if session.get("created_at") is not None else None,
                    "data": session,
                }
            )

        timeline.sort(key=lambda x: x.get("created_at") or "")

        return {
            "query": trace_or_session_id,
            "nodes": nodes,
            "edges": edges,
            "timeline": timeline,
        }


lineage_service = LineageService()
