# Configuration Guide

## Dependency Baseline

Pinned runtime dependencies are listed in [`requirements.txt`](../requirements.txt):

- `agno==2.5.6`
- `fastapi==0.135.1`
- `sqlalchemy==2.0.48`
- `python-dotenv`
- `pandas`
- `scikit-learn`
- `pydantic`
- `jinja2`
- `uvicorn`

Install with:

```bash
pip install -r requirements.txt
```

## Environment Variables

The project loads `.env` from the repository root in [`src/common/model_factory.py`](../src/common/model_factory.py).

### Provider configuration

Supported provider groups in the current code:

- `ricardo`
- `deepseek`
- `echoflow`
- `openrouter`
- `ollama`

Typical variables follow this pattern:

```dotenv
RICARDO_API_KEY=your-key
RICARDO_BASE_URLS=https://api.example.com/v1
RICARDO_API_MODEL=glm-4.5
```

Other providers use the same shape:

- `<PROVIDER>_API_KEY`
- `<PROVIDER>_BASE_URLS`
- `<PROVIDER>_MODEL`

### Model override

`MODEL_ID` is an optional override. When left empty, the project falls back to the provider default or the agent binding configuration.

### Prompt logging

```dotenv
PREDICT_PROMPT_LOG_ENABLED=true
PREDICT_PROMPT_LOG_DB=db/prediction_prompt_logs.db
```

## JSON Configuration Files

### Provider registry

File: [`src/model_config/providers.json`](../src/model_config/providers.json)

Purpose:

- define available providers
- map provider fields to environment variable names
- specify default provider and default model metadata

### Agent model bindings

File: [`src/model_config/agent_models.json`](../src/model_config/agent_models.json)

Purpose:

- bind each agent or log tag to a provider
- optionally bind a fixed model ID
- optionally point to a model ID environment variable

This separates deployment-time configuration from code.

## Workflow Runtime Inputs

Schema source: [`src/schemas/workflow_input.py`](../src/schemas/workflow_input.py)

Important fields:

| Field | Meaning |
| --- | --- |
| `goal` | Required optimization objective |
| `human_loop` | Enable human interaction mode |
| `max_iterations` | User-facing loop cap |
| `top_k` | Candidate/prediction retrieval size |
| `recommend_count_policy` | Recommendation strategy hint |
| `experiment_feedback` | Structured measured experiment feedback |
| `preference_feedback` | Human preference signal for the next round |
| `debug` | Enable richer trace logging |
| `debug_level` | Minimum debug verbosity level |
| `include_debug` | Include debug payload in outputs |
| `resume_run_id` | Continue a prior workflow run |
| `mounted_workflow_run_ids` | Mount one or more prior runs as context |
| `run_note` | Free-text label shown in UI filtering |

## Database Outputs

Database paths are centralized in:

- [`src/common/db_paths.py`](../src/common/db_paths.py)
- [`ui/config.py`](../ui/config.py)

Databases produced or consumed by the system:

| Database | Purpose |
| --- | --- |
| `material_agent_shared.db` | shared workflow and material document records |
| `material_agent_sessions.db` | agent session storage |
| `prediction_prompt_logs.db` | prediction prompt persistence |
| `prompt_llmresponse.db` | response storage |
| `workflow_audit.db` | workflow event and step logs |
| `ui_classifications.db` | UI-side tags and review metadata |

## Startup Commands

### CLI workflow

```bash
python src/run_workflow.py
```

### Web UI

```bash
uvicorn ui.app:app --reload --port 8010
```

### Health check

The UI exposes:

```text
GET /healthz
```

Expected response:

```json
{"status":"ok"}
```
