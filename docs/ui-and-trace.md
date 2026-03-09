# UI And Trace Guide

## Purpose

This document explains how to use the FastAPI UI to inspect workflow outputs, debug runs, and track process evolution.

## Application Entry

Start the UI with:

```bash
uvicorn ui.app:app --reload --port 8010
```

Entry file: [`ui/app.py`](../ui/app.py)

The app mounts static assets, registers error handlers, runs classification migrations on startup, and exposes page routers plus `/api/*` endpoints.

## Main Pages

Route definitions are in [`ui/routers/pages.py`](../ui/routers/pages.py).

### Dashboard

Route: `/`

Use it for:

- total prediction counts
- workflow event totals
- recent anomaly triage
- low-confidence monitoring
- pending annotation checks

### Explorer

Route: `/explorer`

Best when you already know which database and table you want to inspect. It supports:

- dynamic database and table selection
- keyword search and identifier lookup
- date filtering
- workflow-related column filters such as `trace_id`, `session_id`, `workflow_run_id`, `step_name`, `agent_name`, and `tool_name`
- sorting and pagination

### Viewer

Route: `/viewer`

Best when you want to inspect record details across workflow outputs.

Highlights:

- card-style result browsing
- trace/run/session/step/agent/tool filters
- special aggregation mode for `material_doc_knowledge`
- detail side panel for the selected record

For `material_doc_knowledge`, the UI can switch between:

- `chunk` mode: inspect raw chunks
- `full` mode: aggregate all chunks from the same document/run/round into a single rendered document

### Tool Trace

Route: `/tool-trace`

This page is designed for debugging tool execution chains.

It supports:

- filtering by `session_id`, `workflow_run_id`, `step_name`, `tool_name`, and success state
- grouped presentation by workflow step
- detail view for tool input and tool output payloads
- fallback to step-log rows when no explicit tool call rows are available

This makes it useful even when a step did not invoke a named tool but still produced logged input/output data.

### Material Data

Route: `/material-data`

Use it to inspect material results with filters such as:

- `material_type`
- `source`
- `workflow_run_id`
- `run_note`
- creation time window
- valid-only toggle

### Doc Evolution

Route: `/doc-evolution`

This page shows how document knowledge evolves across workflow runs and rounds. It is useful for checking whether the context accumulation pipeline is producing the intended history.

### Record Cleanup And Recycle Bin

Routes:

- `/record-cleanup`
- `/recycle-bin`

These pages support careful cleanup and post-delete inspection rather than direct blind deletion.

## API Surface

API aggregation is defined in [`ui/routers/api.py`](../ui/routers/api.py).

The UI mounts the following API groups:

- logs
- lineage
- classifications
- records
- tool trace
- viewer
- material data

## Operational Debugging Workflow

A typical debugging sequence is:

1. Open `Dashboard` to confirm whether errors or anomalies are increasing.
2. Go to `Tool Trace` and filter by `workflow_run_id` or `session_id`.
3. Inspect failing or empty tool outputs.
4. Cross-check affected records in `Viewer`.
5. Drop to `Explorer` only when raw table verification is needed.

## Trace Keys To Remember

The most useful correlation fields across pages are:

- `session_id`
- `trace_id`
- `workflow_run_id`
- `step_name`
- `agent_name`
- `tool_name`

If you standardize on `workflow_run_id` first, the rest of the UI becomes much easier to navigate.
