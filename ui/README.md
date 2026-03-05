# Material DB WebUI

## Run

```bash
pip install -r requirements.txt
uvicorn ui.app:app --reload --port 8010
```

Open: `http://127.0.0.1:8010`

## Pages

- `/` dashboard overview
- `/viewer` dedicated result viewer (DB/table/code/keyword filters + side-by-side details)
- `/explorer` complete database/table browser with row-level tagging
- `/recycle-bin` persistent recycle bin page for restore operations
- `/tool-trace` step-grouped agent tool-call analysis page (Tool Input/Output)

## Viewer extra actions

- Batch delete selected records from current table
- Auto backup deleted rows to recycle bin
- Restore selected rows from recycle bin

## APIs

- `GET /api/logs/predictions`
- `GET /api/logs/workflow-events`
- `GET /api/lineage/{trace_or_session_id}`
- `POST /api/classifications/tags`
- `POST /api/classifications/assign`
- `POST /api/classifications/state`

## Classification storage

Classification tables are created in `db/ui_classifications.db` on app startup.
Migration SQL: `ui/migrations/001_classification.sql`.
