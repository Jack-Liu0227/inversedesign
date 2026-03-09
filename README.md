# InverseDesign

InverseDesign is a materials discovery workspace built around Agno workflows, few-shot property prediction, and a FastAPI-based observability UI. It combines candidate recommendation, property estimation, human or AI feedback loops, and multi-database trace inspection in one project.

## What This Repository Provides

- A material discovery workflow that iterates over recommendation, prediction, judgement, and stopping decisions.
- Retrieval-augmented few-shot prediction utilities for material property estimation.
- Workflow persistence backed by SQLite for run state, prompts, responses, and audit logs.
- A FastAPI + Jinja UI for dashboarding, data browsing, lineage viewing, document evolution, and tool trace analysis.
- Configurable model/provider routing through JSON files plus `.env` overrides.

## System Overview

The repository is split into two major layers:

1. Workflow and agent runtime under [`src/`](./src), which handles model selection, dataset routing, recommendation, prediction, and iterative workflow execution.
2. Monitoring and data exploration UI under [`ui/`](./ui), which exposes database-backed inspection pages and API endpoints over the workflow outputs.

Core runtime flow:

1. Build workflow input from a design goal and optional feedback.
2. Select or mount historical material/document context.
3. Generate candidate compositions or reuse prior run artifacts.
4. Predict properties with the configured model stack and few-shot data support.
5. Evaluate results, record audit data, and decide whether to continue.
6. Surface traces, run history, and derived records in the UI.

## Main Features

### 1. Automated material discovery workflow

- Entry point: [`src/run_workflow.py`](./src/run_workflow.py)
- Workflow factory: [`src/workflows/material_discovery/builder.py`](./src/workflows/material_discovery/builder.py)
- Input schema: [`src/schemas/workflow_input.py`](./src/schemas/workflow_input.py)

Supported input capabilities include:

- `goal`: required optimization target
- `max_iterations`: loop cap for iterative search
- `top_k`: retrieval/prediction candidate count
- `experiment_feedback`: structured measured results for subsequent rounds
- `preference_feedback`: human preference signal for the next proposal round
- `resume_run_id`: continue a previous workflow run
- `mounted_workflow_run_ids`: mount prior runs as reusable context
- `run_note`: annotate a run for later filtering in the UI
- `debug` / `include_debug`: enable richer workflow trace payloads

### 2. Provider-aware model selection

Model instantiation is centralized in [`src/common/model_factory.py`](./src/common/model_factory.py).

- Provider definitions live in [`src/model_config/providers.json`](./src/model_config/providers.json)
- Agent-to-model bindings live in [`src/model_config/agent_models.json`](./src/model_config/agent_models.json)
- `.env` can still override model IDs and provider credentials

This allows individual agents or steps to switch providers without changing application code.

### 3. Workflow traceability and persistent storage

SQLite databases are created under [`db/`](./db) through constants defined in [`src/common/db_paths.py`](./src/common/db_paths.py) and [`ui/config.py`](./ui/config.py).

Important databases:

- `db/material_agent_shared.db`: workflow-level shared data
- `db/material_agent_sessions.db`: per-agent session storage
- `db/prediction_prompt_logs.db`: prompt logging for prediction calls
- `db/prompt_llmresponse.db`: stored LLM responses
- `db/workflow_audit.db`: workflow event and step-level audit logs
- `db/ui_classifications.db`: UI-side annotations and classifications

### 4. FastAPI UI for inspection and process tracking

App entry point: [`ui/app.py`](./ui/app.py)

Primary pages exposed by [`ui/routers/pages.py`](./ui/routers/pages.py):

- `/`: dashboard KPIs and anomaly triage
- `/explorer`: generic database explorer
- `/viewer`: record-centric result viewer with workflow filters
- `/tool-trace`: step/tool call inspection page
- `/material-data`: material result preview with run filters
- `/doc-evolution`: document evolution matrix across runs
- `/recycle-bin`: deleted record inspection
- `/record-cleanup`: cleanup preview and controlled deletion tooling

## Quick Start

### Requirements

- Python 3.10+
- SQLite (bundled with Python)
- Access to at least one configured LLM provider, or a local `ollama` endpoint

### Install dependencies

```bash
pip install -r requirements.txt
```

If you need the local Agno package from this repository checkout:

```bash
pip install -e ./agno/libs/agno
```

### Configure environment

Create a local `.env` with provider credentials and logging settings. Do not commit real keys.

Minimal example:

```dotenv
RICARDO_API_KEY=your-key
RICARDO_BASE_URLS=https://api.example.com/v1
RICARDO_API_MODEL=glm-4.5

MODEL_ID=
PREDICT_PROMPT_LOG_ENABLED=true
PREDICT_PROMPT_LOG_DB=db/prediction_prompt_logs.db
```

For full configuration notes, see [docs/configuration.md](./docs/configuration.md).

### Run the workflow from CLI

```bash
python src/run_workflow.py
```

The CLI loop supports paused workflow continuation, user confirmation steps, and JSON-formatted measured values.

### Run the UI

```bash
uvicorn ui.app:app --reload --port 8010
```

After startup, open [http://127.0.0.1:8010](http://127.0.0.1:8010).

## UI Pages At A Glance

| Page | Purpose |
| --- | --- |
| Dashboard | KPI summary, trend buckets, anomaly triage |
| Explorer | Table-oriented browsing across multiple SQLite databases |
| Viewer | Record cards with workflow/session/run filters and detail panels |
| Tool Trace | Grouped tool calls by workflow step, including fallback step-log views |
| Material Data | Material results filtered by source, run ID, note, date, validity |
| Doc Evolution | Cross-run matrix showing how material documents change over time |

Detailed operational notes are in [docs/ui-and-trace.md](./docs/ui-and-trace.md).

## Repository Layout

```text
inversedesign/
|-- agno/
|-- datasets/
|-- db/
|-- examples/
|-- knowledge/
|-- src/
|   |-- agents/
|   |-- common/
|   |-- fewshot/
|   |-- model_config/
|   |-- schemas/
|   `-- workflows/
|-- ui/
|   |-- db/
|   |-- routers/
|   |-- services/
|   |-- static/
|   `-- templates/
|-- .env
|-- README.md
`-- requirements.txt
```

## Recommended Reading Order

1. [docs/architecture.md](./docs/architecture.md)
2. [docs/configuration.md](./docs/configuration.md)
3. [docs/ui-and-trace.md](./docs/ui-and-trace.md)

## Current Focus Of This Version

This revision documents the initialized automated material discovery workflow, the upgraded UI surface, and the end-to-end process tracing path across workflow execution, prompt logs, audit logs, and UI inspection tools.
