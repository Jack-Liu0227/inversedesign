from __future__ import annotations

import sqlite3

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ValueError)
    async def _handle_value_error(_request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(status_code=400, content={"detail": str(exc) or "Invalid request"})

    @app.exception_handler(KeyError)
    async def _handle_key_error(_request: Request, exc: KeyError) -> JSONResponse:
        detail = str(exc).strip("'") or "Invalid request"
        return JSONResponse(status_code=400, content={"detail": detail})

    @app.exception_handler(sqlite3.Error)
    async def _handle_sqlite_error(_request: Request, _exc: sqlite3.Error) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": "Database operation failed"})
