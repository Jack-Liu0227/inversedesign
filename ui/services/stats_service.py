from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from ui.db.repositories.classification_repo import classification_repo
from ui.db.repositories.prediction_repo import prediction_repo
from ui.db.repositories.workflow_repo import workflow_repo


class StatsService:
    _BEIJING = ZoneInfo("Asia/Shanghai")

    def dashboard(self) -> dict[str, Any]:
        pred_rows, pred_total = prediction_repo.list_predictions(page=1, page_size=200)
        wf_rows, wf_total = workflow_repo.list_workflow_events(page=1, page_size=500)
        annotations = classification_repo.get_annotations()

        now_local = datetime.now(self._BEIJING)
        last_24h = now_local - timedelta(hours=24)

        recent_count = 0
        low_confidence = 0
        for row in pred_rows:
            created = self._parse_dt(row.get("created_at"))
            if created and created >= last_24h:
                recent_count += 1
            if (row.get("confidence") or "").lower() in {"low", "very_low"}:
                low_confidence += 1

        errors = sum(1 for r in wf_rows if r.get("success") == 0 or r.get("error_text"))

        bucket = Counter()
        for row in wf_rows:
            dt = self._parse_dt(row.get("created_at"))
            if dt:
                bucket[dt.strftime("%Y-%m-%d %H:00")] += 1

        anomalies = [r for r in wf_rows if r.get("error_text")][:5]
        pending = [a for a in annotations if a.get("status") in {"new", "reviewed"}][:5]

        return {
            "kpis": {
                "total_predictions": pred_total,
                "total_events": wf_total,
                "predictions_24h": recent_count,
                "errors": errors,
                "low_confidence": low_confidence,
            },
            "trend": sorted(bucket.items()),
            "anomalies": anomalies,
            "pending_annotations": pending,
        }

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            try:
                dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=StatsService._BEIJING)
        return dt.astimezone(StatsService._BEIJING)


stats_service = StatsService()
