from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from ui.config import get_config
from ui.db.repositories.classification_repo import classification_repo
from ui.routers import api_classifications, api_lineage, api_logs, api_material_data, api_records, api_tool_trace, api_viewer, pages

cfg = get_config()
app = FastAPI(title="Material DB UI", version="1.0.0")

app.mount("/static", StaticFiles(directory=str(cfg.static_dir)), name="static")

app.include_router(pages.router)
app.include_router(api_logs.router)
app.include_router(api_lineage.router)
app.include_router(api_classifications.router)
app.include_router(api_records.router)
app.include_router(api_tool_trace.router)
app.include_router(api_viewer.router)
app.include_router(api_material_data.router)


@app.on_event("startup")
def _startup() -> None:
    classification_repo.run_migrations()


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


# Run: uvicorn ui.app:app --reload --port 8010
