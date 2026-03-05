from __future__ import annotations

from pydantic import BaseModel
from fastapi import APIRouter, Depends
from starlette.concurrency import run_in_threadpool

from ui.db.repositories.explorer_repo import ExplorerRepository
from ui.dependencies import get_explorer_repository


router = APIRouter(prefix="/records", tags=["records"])


class BatchDeleteRequest(BaseModel):
    source_db: str
    source_table: str
    key_col: str
    key_values: list[str]


class RestoreRequest(BaseModel):
    recycle_ids: list[int]


class PurgeRequest(BaseModel):
    recycle_ids: list[int] = []


@router.get("/recycle-bin")
async def recycle_bin(
    limit: int = 200,
    explorer_repository: ExplorerRepository = Depends(get_explorer_repository),
):
    items = await run_in_threadpool(explorer_repository.list_recycle_bin, limit=max(1, min(limit, 500)))
    return {"items": items}


@router.post("/batch-delete")
async def batch_delete(
    req: BatchDeleteRequest,
    explorer_repository: ExplorerRepository = Depends(get_explorer_repository),
):
    return await run_in_threadpool(
        explorer_repository.delete_rows_to_recycle_bin,
        db_key=req.source_db,
        table=req.source_table,
        key_col=req.key_col,
        key_values=[str(v) for v in req.key_values],
    )


@router.post("/restore")
async def restore(
    req: RestoreRequest,
    explorer_repository: ExplorerRepository = Depends(get_explorer_repository),
):
    return await run_in_threadpool(explorer_repository.restore_from_recycle_bin, recycle_ids=req.recycle_ids)


@router.post("/purge")
async def purge(
    req: PurgeRequest,
    explorer_repository: ExplorerRepository = Depends(get_explorer_repository),
):
    return await run_in_threadpool(
        explorer_repository.purge_recycle_bin,
        recycle_ids=req.recycle_ids,
        all_active=False,
    )


@router.post("/purge-all")
async def purge_all(
    explorer_repository: ExplorerRepository = Depends(get_explorer_repository),
):
    return await run_in_threadpool(
        explorer_repository.purge_recycle_bin,
        recycle_ids=[],
        all_active=True,
    )
