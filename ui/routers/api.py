from __future__ import annotations

from fastapi import APIRouter

from ui.routers import (
    api_classifications,
    api_lineage,
    api_logs,
    api_material_data,
    api_records,
    api_tool_trace,
    api_viewer,
)

api_router = APIRouter(prefix="/api")
api_router.include_router(api_logs.router)
api_router.include_router(api_lineage.router)
api_router.include_router(api_classifications.router)
api_router.include_router(api_records.router)
api_router.include_router(api_tool_trace.router)
api_router.include_router(api_viewer.router)
api_router.include_router(api_material_data.router)
