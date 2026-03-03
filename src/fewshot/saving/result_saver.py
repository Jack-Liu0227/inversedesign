from __future__ import annotations

from pathlib import Path
from typing import Dict, List
import threading
import json
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


class ResultSaver:
    def __init__(self, output_dir: str) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def save_predictions(
        self,
        rows: List[Dict[str, float]],
        target_cols: List[str],
    ) -> Path:
        df = pd.DataFrame(rows)
        path = self.output_dir / "predictions.csv"
        with self._lock:
            if path.exists():
                existing = pd.read_csv(path)
                df = self._merge_predictions(existing, df, target_cols)
            df.to_csv(path, index=False)
        return path

    def save_metrics(
        self,
        rows: List[Dict[str, float]],
        target_cols: List[str],
    ) -> Path:
        df = pd.DataFrame(rows)
        metrics: Dict[str, Dict[str, float]] = {}
        for col in target_cols:
            true_col = f"{col}_true"
            pred_col = f"{col}_predicted"
            if true_col not in df.columns or pred_col not in df.columns:
                continue
            y_true = df[true_col]
            y_pred = df[pred_col]
            valid_mask = ~(y_true.isna() | y_pred.isna())
            if valid_mask.sum() == 0:
                continue
            y_true_valid = y_true[valid_mask]
            y_pred_valid = y_pred[valid_mask]
            metrics[col] = {
                "mae": float(mean_absolute_error(y_true_valid, y_pred_valid)),
                "rmse": float(mean_squared_error(y_true_valid, y_pred_valid) ** 0.5),
                "r2": float(r2_score(y_true_valid, y_pred_valid))
                if len(y_true_valid) > 1
                else None,
            }
        path = self.output_dir / "metrics.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False)
        return path

    def save_details(self, details: List[Dict[str, object]]) -> Path:
        path = self.output_dir / "prediction_details.json"
        with self._lock:
            merged = {}
            if path.exists():
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        existing = json.load(f)
                except Exception:
                    existing = []
                for item in existing:
                    idx = item.get("sample_index")
                    if idx is not None:
                        merged[str(idx)] = item
            for item in details:
                idx = item.get("sample_index")
                if idx is not None:
                    key = str(idx)
                    if key in merged:
                        old_item = merged[key]
                        old_error = self._is_failed_detail(old_item)
                        new_error = self._is_failed_detail(item)
                        if (not old_error) and new_error:
                            continue
                    merged[key] = item
            merged_list = [merged[k] for k in sorted(merged.keys(), key=lambda v: int(v))]
            with open(path, "w", encoding="utf-8") as f:
                json.dump(merged_list, f, indent=2, ensure_ascii=False)
        return path

    @staticmethod
    def _merge_predictions(
        existing: pd.DataFrame,
        new_df: pd.DataFrame,
        target_cols: List[str],
    ) -> pd.DataFrame:
        if "sample_index" not in existing.columns or "sample_index" not in new_df.columns:
            return pd.concat([existing, new_df], ignore_index=True)

        existing = existing.copy()
        new_df = new_df.copy()
        existing["sample_index"] = existing["sample_index"].astype(int)
        new_df["sample_index"] = new_df["sample_index"].astype(int)

        merged = {int(row["sample_index"]): row for _, row in existing.iterrows()}
        for _, row in new_df.iterrows():
            idx = int(row["sample_index"])
            if idx not in merged:
                merged[idx] = row
                continue

            old = merged[idx]
            old_better = False
            new_better = False
            for col in target_cols:
                pred_col = f"{col}_predicted"
                old_val = old.get(pred_col)
                new_val = row.get(pred_col)
                old_bad = pd.isna(old_val) or old_val == 0
                new_bad = pd.isna(new_val) or new_val == 0
                if old_bad and not new_bad:
                    new_better = True
                if not old_bad and new_bad:
                    old_better = True

            if new_better and not old_better:
                merged[idx] = row

        merged_df = pd.DataFrame(list(merged.values()))
        merged_df = merged_df.sort_values("sample_index").reset_index(drop=True)
        return merged_df

    @staticmethod
    def _is_failed_detail(item: Dict[str, object]) -> bool:
        if item.get("error"):
            return True
        llm_response = item.get("llm_response")
        if isinstance(llm_response, str) and llm_response.strip().startswith("ERROR:"):
            return True
        return False
