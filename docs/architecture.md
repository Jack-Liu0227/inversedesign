# Architecture Overview

## Scope

This document describes how the repository is organized and how data flows between the workflow runtime and the monitoring UI.

## Layers

### Workflow runtime

The workflow runtime lives under [`src/`](../src) and is responsible for:

- validating workflow input
- selecting models and providers
- loading prior run context and material knowledge
- generating candidate materials
- predicting target properties
- writing structured records to SQLite
- deciding whether to continue or stop

Relevant modules:

- [`src/workflows/material_discovery/builder.py`](../src/workflows/material_discovery/builder.py)
- [`src/workflows/material_discovery/workflow_factory.py`](../src/workflows/material_discovery/workflow_factory.py)
- [`src/workflows/material_discovery/agent_steps.py`](../src/workflows/material_discovery/agent_steps.py)
- [`src/workflows/material_discovery/judge_steps.py`](../src/workflows/material_discovery/judge_steps.py)
- [`src/schemas/workflow_input.py`](../src/schemas/workflow_input.py)

### Shared persistence

The project uses SQLite as the main persistence layer.

Key path definitions:

- [`src/common/db_paths.py`](../src/common/db_paths.py)
- [`ui/config.py`](../ui/config.py)

The storage model separates shared workflow state, per-agent session state, prompt logs, and workflow audit records so that the UI can inspect them independently.

### Monitoring UI

The UI lives under [`ui/`](../ui). It is a FastAPI application with Jinja templates and repository-style database access.

Main responsibilities:

- render dashboard metrics and anomaly lists
- browse arbitrary workflow-related tables
- inspect workflow outputs at record level
- trace tool invocations grouped by step
- compare document evolution across workflow rounds
- support classification, cleanup, and recycle flows

Relevant modules:

- [`ui/app.py`](../ui/app.py)
- [`ui/routers/pages.py`](../ui/routers/pages.py)
- [`ui/routers/api.py`](../ui/routers/api.py)
- [`ui/db/repositories/`](../ui/db/repositories)

## Execution Flow

```text
User goal / feedback
        |
        v
WorkflowInput validation
        |
        v
Material discovery workflow loop
        |
        +--> candidate recommendation
        |
        +--> few-shot prediction
        |
        +--> judge / stop decision
        |
        +--> audit + prompt logging
        |
        v
SQLite databases
        |
        v
FastAPI UI + API inspection endpoints
```

## Workflow Loop Model

The workflow is built in [`src/workflows/material_discovery/builder.py`](../src/workflows/material_discovery/builder.py) using an Agno `Workflow` with a single `Loop`.

Important runtime characteristics:

- workflow name: `material_discovery_workflow`
- database-backed run persistence via `SqliteDb`
- configurable loop mode through `steps_for_mode(...)`
- stop condition delegated to `end_when_satisfied`
- max loop iterations in the workflow graph set to `50`
- user-facing iteration count controlled by validated input such as `max_iterations`

## Inputs And Continuation

The workflow schema supports both fresh runs and resumed runs.

Operationally important fields:

- `resume_run_id`: continue a previous run
- `mounted_workflow_run_ids`: mount earlier runs as context
- `experiment_feedback`: inject measured results from experiments
- `preference_feedback`: steer the next proposal round
- `run_note`: label runs for later UI filtering
- `debug`, `debug_level`, `include_debug`: enrich trace visibility

## UI Information Model

The UI exposes two complementary inspection modes:

1. Table-first exploration via `Explorer`
2. Record-first investigation via `Viewer` and `Tool Trace`

This split is deliberate:

- `Explorer` is useful when operators know which database/table they need.
- `Viewer` is better when the task is to inspect workflow outcomes by trace, run, step, agent, or tool.
- `Tool Trace` is optimized for debugging tool invocation sequences and step-level fallbacks.

## Design Intent

This repository is structured to support an iterative materials design loop with strong observability:

- model and provider choices are configurable
- workflow runs are resumable and auditable
- UI pages are directly backed by workflow persistence
- trace artifacts remain queryable after the run ends
