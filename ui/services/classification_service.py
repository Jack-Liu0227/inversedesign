from __future__ import annotations

from typing import Any

from ui.db.repositories.classification_repo import ClassificationRepository, classification_repo


class ClassificationService:
    def __init__(self, repository: ClassificationRepository | None = None) -> None:
        self._repository = repository or classification_repo

    def create_tag(self, *, name: str, color: str, group_name: str, description: str) -> dict[str, Any]:
        return self._repository.create_tag(
            name=name.strip(), color=color.strip(), group_name=group_name.strip(), description=description.strip()
        )

    def assign_tags(self, *, source_db: str, source_table: str, source_pk: str, tag_names: list[str]) -> dict[str, Any]:
        annotation_id = self._repository.upsert_annotation(
            source_db=source_db,
            source_table=source_table,
            source_pk=source_pk,
            status="new",
            priority="P2",
            note="",
        )
        cleaned_tags = [t.strip() for t in tag_names if t.strip()]
        self._repository.assign_tags(annotation_id=annotation_id, tag_names=cleaned_tags)
        return {"annotation_id": annotation_id, "tag_names": cleaned_tags}

    def update_state(
        self,
        *,
        source_db: str,
        source_table: str,
        source_pk: str,
        status: str,
        priority: str,
        note: str,
    ) -> dict[str, Any]:
        annotation_id = self._repository.upsert_annotation(
            source_db=source_db,
            source_table=source_table,
            source_pk=source_pk,
            status=status,
            priority=priority,
            note=note,
        )
        return {"annotation_id": annotation_id, "status": status, "priority": priority}

    def list_tags(self) -> list[dict[str, Any]]:
        return self._repository.list_tags()

    def list_annotations(self) -> list[dict[str, Any]]:
        return self._repository.get_annotations()


classification_service = ClassificationService()
