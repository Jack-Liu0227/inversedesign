from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from ui.config import get_config
from ui.db.repositories.classification_repo import classification_repo
from ui.error_handlers import register_error_handlers
from ui.routers import api, pages

cfg = get_config()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    classification_repo.run_migrations()
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="Material DB UI", version="1.0.0", lifespan=lifespan)
    register_error_handlers(app)
    app.mount("/static", StaticFiles(directory=str(cfg.static_dir)), name="static")

    app.include_router(pages.router)
    app.include_router(api.api_router)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()


# Run: uvicorn ui.app:app --reload --port 8010
