from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass

_CURRENT_RUN_ID: ContextVar[str] = ContextVar("workflow_current_run_id", default="")
_CURRENT_SESSION_ID: ContextVar[str] = ContextVar("workflow_current_session_id", default="")
_CURRENT_TRACE_ID: ContextVar[str] = ContextVar("workflow_current_trace_id", default="")


@dataclass
class WorkflowRunContextToken:
    run_id_token: Token
    session_id_token: Token
    trace_id_token: Token


def set_workflow_run_context(*, run_id: str, session_id: str, trace_id: str) -> WorkflowRunContextToken:
    return WorkflowRunContextToken(
        run_id_token=_CURRENT_RUN_ID.set(str(run_id or "").strip()),
        session_id_token=_CURRENT_SESSION_ID.set(str(session_id or "").strip()),
        trace_id_token=_CURRENT_TRACE_ID.set(str(trace_id or "").strip()),
    )


def reset_workflow_run_context(token: WorkflowRunContextToken) -> None:
    _CURRENT_RUN_ID.reset(token.run_id_token)
    _CURRENT_SESSION_ID.reset(token.session_id_token)
    _CURRENT_TRACE_ID.reset(token.trace_id_token)


def get_current_run_id() -> str:
    return str(_CURRENT_RUN_ID.get() or "").strip()


def get_current_session_id() -> str:
    return str(_CURRENT_SESSION_ID.get() or "").strip()


def get_current_trace_id() -> str:
    return str(_CURRENT_TRACE_ID.get() or "").strip()
