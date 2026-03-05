from __future__ import annotations

from typing import Any

from ui.db.repositories.session_repo import session_repo
from ui.db.repositories.workflow_repo import workflow_repo


class LineageService:
    def build_lineage(self, trace_or_session_id: str) -> dict[str, Any]:
        events = workflow_repo.find_lineage_events(trace_or_session_id)
        audits = workflow_repo.find_run_audits(trace_or_session_id)

        linked_session_ids = {trace_or_session_id}
        for event in events:
            if event.get("session_id"):
                linked_session_ids.add(event["session_id"])
        for audit in audits:
            if audit.get("session_id"):
                linked_session_ids.add(audit["session_id"])

        sessions: list[dict[str, Any]] = []
        for sid in linked_session_ids:
            sessions.extend(session_repo.find_sessions(sid))

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
