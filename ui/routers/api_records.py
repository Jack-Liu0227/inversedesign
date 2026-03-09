from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from ui.db.repositories.explorer_repo import ExplorerRepository
from ui.dependencies import get_explorer_repository
from ui.services.record_cleanup_service import record_cleanup_service


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


class CrossDatabaseActionRequest(BaseModel):
    filter_col: str
    filter_values: list[str] = Field(default_factory=list)


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


@router.get("/cross-db-suggestions")
async def cross_db_suggestions(
    filter_col: str,
    q: str = "",
    limit: int = 80,
    explorer_repository: ExplorerRepository = Depends(get_explorer_repository),
):
    return await run_in_threadpool(
        record_cleanup_service.suggestions,
        explorer_repository=explorer_repository,
        filter_col=filter_col,
        query=q,
        limit=max(1, min(int(limit), 200)),
    )


@router.post("/cross-db-preview-batch")
async def cross_db_preview_batch(
    req: CrossDatabaseActionRequest,
    explorer_repository: ExplorerRepository = Depends(get_explorer_repository),
):
    return await run_in_threadpool(
        record_cleanup_service.preview_many,
        explorer_repository=explorer_repository,
        filter_col=req.filter_col,
        filter_values=req.filter_values,
        sample_limit=8,
    )


@router.post("/delete-across-databases")
async def delete_across_databases(
    req: CrossDatabaseActionRequest,
    explorer_repository: ExplorerRepository = Depends(get_explorer_repository),
):
    return await run_in_threadpool(
        record_cleanup_service.delete_many,
        explorer_repository=explorer_repository,
        filter_col=req.filter_col,
        filter_values=req.filter_values,
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
