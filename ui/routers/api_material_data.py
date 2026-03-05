from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException

from src.common import import_csv_datasets_to_db
from ui.db.repositories.explorer_repo import explorer_repo
from ui.db.repositories.material_data_repo import material_data_repo


router = APIRouter(prefix="/api/material-data", tags=["material-data"])


@router.post("/import-csv")
def import_csv_to_db() -> dict[str, object]:
    result = import_csv_datasets_to_db()
    return {"ok": True, **result}


@router.get("/rows")
def list_rows(
    page: int = 1,
    page_size: int = 50,
    material_type: str = "",
    source: str = "",
    q: str = "",
    created_from: str = "",
    created_to: str = "",
    valid_only: bool = False,
    sort_by: str = "id",
    sort_order: str = "desc",
) -> dict[str, object]:
    rows, total = material_data_repo.list_rows(
        page=page,
        page_size=page_size,
        material_type=material_type,
        source=source,
        q=q,
        created_from=created_from,
        created_to=created_to,
        valid_only=valid_only,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    return {
        "rows": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "sort_by": sort_by,
        "sort_order": "asc" if str(sort_order).lower() == "asc" else "desc",
    }


@router.post("/batch-delete")
def batch_delete_rows(payload: dict[str, object] | None = Body(None)) -> dict[str, object]:
    body = payload or {}
    ids_raw = body.get("ids", [])
    if not isinstance(ids_raw, list):
        raise HTTPException(status_code=400, detail="ids must be a list")
    key_values = [str(v) for v in ids_raw]
    result = explorer_repo.delete_rows_to_recycle_bin(
        db_key="material_agent_shared",
        table="material_dataset_rows",
        key_col="id",
        key_values=key_values,
    )
    return {"ok": True, **result}


@router.post("/normalize-processing")
def normalize_processing_rows() -> dict[str, object]:
    updated = material_data_repo.normalize_processing_rows()
    return {"ok": True, "updated": updated}
