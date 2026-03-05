from __future__ import annotations

from typing import Dict, List, Optional
import json
import re
from pathlib import Path
import pandas as pd


class ResultParser:
    def __init__(self, target_cols: List[str]) -> None:
        self.target_cols = target_cols

    def parse(self, response_text: str) -> Dict[str, Optional[float]]:
        for data in self._iter_json_objects(response_text):
            predictions = self._extract_predictions(data)
            if predictions:
                return predictions
        fallback = self._extract_predictions_from_text(response_text)
        if fallback:
            return fallback
        return {col: None for col in self.target_cols}

    def extract_confidence(self, response_text: str) -> Optional[str]:
        for data in self._iter_json_objects(response_text):
            confidence = data.get("confidence")
            if isinstance(confidence, str):
                value = confidence.lower().strip()
                if value in {"high", "medium", "low"}:
                    return value
        match = re.search(r"\bconfidence\b\s*[:=]\s*['\"]?(high|medium|low)", response_text, re.I)
        if match:
            return match.group(1).lower()
        return None

    def extract_reasoning(self, response_text: str) -> Optional[str]:
        for data in self._iter_json_objects(response_text):
            reasoning = data.get("reasoning")
            if isinstance(reasoning, str):
                text = reasoning.strip()
                if text:
                    return text
        return None

    def _extract_predictions(self, data: dict) -> Dict[str, Optional[float]]:
        result: Dict[str, Optional[float]] = {}
        pred_block = data.get("predictions", data.get("prediction", data.get("pred", {})))
        if not isinstance(pred_block, dict):
            if isinstance(pred_block, list):
                pred_block = {item.get("name"): item for item in pred_block if isinstance(item, dict)}
            else:
                return result
        normalized_keys = {self._normalize_key(k): k for k in pred_block.keys()}
        for col in self.target_cols:
            source_key = None
            if col in pred_block:
                source_key = col
            else:
                source_key = normalized_keys.get(self._normalize_key(col))
            if source_key is None:
                continue
            value = pred_block[source_key]
            if isinstance(value, dict):
                if "value" in value:
                    result[col] = self._coerce_float(value["value"])
                elif "predicted" in value:
                    result[col] = self._coerce_float(value["predicted"])
            elif isinstance(value, (int, float, str)):
                result[col] = self._coerce_float(value)
        return result

    @staticmethod
    def _iter_json_objects(text: str):
        for candidate in ResultParser._extract_json_candidates(text):
            try:
                parsed = json.loads(candidate)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                yield parsed

    @staticmethod
    def _extract_json_candidates(text: str) -> List[str]:
        code_block_pattern = r"```(?:json)?\s*(\{.*?\})\s*```"
        matches = re.findall(code_block_pattern, text, re.DOTALL)
        if matches:
            return matches

        candidates = []
        stack = []
        start = None
        for i, ch in enumerate(text):
            if ch == "{":
                if not stack:
                    start = i
                stack.append("{")
            elif ch == "}":
                if stack:
                    stack.pop()
                    if not stack and start is not None:
                        candidates.append(text[start : i + 1])
                        start = None
        return candidates

    def _extract_predictions_from_text(self, text: str) -> Dict[str, Optional[float]]:
        result: Dict[str, Optional[float]] = {}
        for col in self.target_cols:
            pattern = rf"{re.escape(col)}\s*[:=]\s*([+-]?\d+(?:\.\d+)?)"
            match = re.search(pattern, text)
            if match:
                result[col] = self._coerce_float(match.group(1))
        return result

    @staticmethod
    def _normalize_key(key: str) -> str:
        return re.sub(r"[\s_]+", "", key).lower()

    @staticmethod
    def _coerce_float(value: object) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            match = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", value)
            if match:
                return float(match.group(0))
        return None

    @staticmethod
    def parse_outputs_to_csv(
        outputs_dir: str | Path,
        target_cols: List[str],
        output_csv: str | Path,
    ) -> Path:
        parser = ResultParser(target_cols)
        outputs_path = Path(outputs_dir)
        rows: List[Dict[str, object]] = []

        for path in sorted(outputs_path.glob("sample_*.txt")):
            match = re.search(r"sample_(\d+)\.txt$", path.name)
            if not match:
                continue
            sample_index = int(match.group(1))
            text = path.read_text(encoding="utf-8", errors="ignore")
            parsed = parser.parse(text)
            confidence = parser.extract_confidence(text)
            row: Dict[str, object] = {"sample_index": sample_index}
            for col in target_cols:
                row[f"{col}_predicted"] = parsed.get(col)
            if confidence:
                row["confidence"] = confidence
            rows.append(row)

        df_new = pd.DataFrame(rows)
        output_path = Path(output_csv)
        if output_path.exists():
            existing = pd.read_csv(output_path)
            if "sample_index" in existing.columns:
                existing["sample_index"] = existing["sample_index"].astype(int)
                df_new["sample_index"] = df_new["sample_index"].astype(int)
                merged = existing.set_index("sample_index")
                updates = df_new.set_index("sample_index")
                for col in updates.columns:
                    if col not in merged.columns:
                        merged[col] = None
                    new_vals = updates[col]
                    merged[col] = merged[col].where(~new_vals.notna(), new_vals)
                merged.reset_index().to_csv(output_path, index=False)
                return output_path
        df_new.to_csv(output_path, index=False)
        return output_path
