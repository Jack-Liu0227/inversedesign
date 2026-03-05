from __future__ import annotations

from pydantic import BaseModel
from fastapi import APIRouter

from ui.db.repositories.explorer_repo import explorer_repo


router = APIRouter(prefix="/api/records", tags=["records"])


class BatchDeleteRequest(BaseModel):
    source_db: str
    source_table: str
    key_col: str
    key_values: list[str]


class RestoreRequest(BaseModel):
    recycle_ids: list[int]


@router.get("/recycle-bin")
def recycle_bin(limit: int = 200):
    return {"items": explorer_repo.list_recycle_bin(limit=max(1, min(limit, 500)))}


@router.post("/batch-delete")
def batch_delete(req: BatchDeleteRequest):
    return explorer_repo.delete_rows_to_recycle_bin(
        db_key=req.source_db,
        table=req.source_table,
        key_col=req.key_col,
        key_values=[str(v) for v in req.key_values],
    )


@router.post("/restore")
def restore(req: RestoreRequest):
    return explorer_repo.restore_from_recycle_bin(recycle_ids=req.recycle_ids)
