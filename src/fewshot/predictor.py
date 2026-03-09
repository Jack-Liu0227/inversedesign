from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import json
import re

import pandas as pd

from .dataset_registry import resolve_dataset
from .material_dataset_pool import load_prediction_pool
from .model import ModelCaller
from .parsing import ResultParser
from .prompting import PromptBuilder
from .retrieval import SampleRetriever


@dataclass
class FewshotPrediction:
    material_type: str
    predicted_values: Dict[str, Optional[float]]
    confidence: str
    similar_samples: List[Dict[str, Any]]
    prompt: str
    llm_response: str


class FewshotPredictor:
    def __init__(
        self,
        model_name: str,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        temperature: float = 0.0,
        embedding_model: str = "EmbeddingModel/all-MiniLM-L6-v2",
    ) -> None:
        self.model_name = model_name
        self.api_key = api_key
        self.base_url = base_url
        self.temperature = temperature
        self.embedding_model = embedding_model

    def predict(
        self,
        material_type: str,
        composition: Dict[str, Any],
        processing: Optional[Any] = None,
        features: Optional[Any] = None,
        top_k: Optional[int] = None,
        mounted_workflow_run_ids: Optional[list[str]] = None,
        current_workflow_run_id: str = "",
    ) -> FewshotPrediction:
        spec = resolve_dataset(material_type)
        pool_rows = load_prediction_pool(
            material_type=material_type,
            mounted_workflow_run_ids=mounted_workflow_run_ids or [],
            current_workflow_run_id=current_workflow_run_id,
        )
        if not pool_rows:
            raise ValueError(f"No prediction pool rows available for material_type='{material_type}'")

        train_texts: List[str] = []
        train_targets: List[Dict[str, Any]] = []
        for row in pool_rows:
            row_comp = self._format_input_composition(self._compact_non_empty_dict(dict(row.get("composition") or {})))
            row_processing = self._compact_non_empty_dict(dict(row.get("processing") or {}))
            row_features = self._compact_non_empty_dict(dict(row.get("features") or {}))
            row_text = self._build_sample_text(
                composition=row_comp,
                processing=row_processing,
                features=row_features,
            )
            if not row_text:
                continue
            train_texts.append(row_text)
            row_targets = self._compact_non_empty_dict(dict(row.get("target_values") or {}))
            train_targets.append({col: row_targets.get(col) for col in spec.target_cols})
        if not train_texts:
            raise ValueError(f"No valid prediction pool rows available for material_type='{material_type}'")

        test_comp = self._format_input_composition(composition)
        normalized_processing = self._normalize_context_payload(processing, label="processing")
        normalized_features = self._normalize_context_payload(features, label="features")
        test_text = self._build_sample_text(
            composition=test_comp,
            processing=normalized_processing,
            features=normalized_features,
        )
        if not test_text:
            test_text = self._build_target_fallback_text(
                composition=composition,
                processing=normalized_processing,
                features=normalized_features,
            )

        k = top_k or spec.default_top_k
        retriever = SampleRetriever(embedding_model=self.embedding_model, top_k=k)
        retriever.fit(train_texts)
        retrieved = retriever.retrieve(test_text, k)

        prompt_builder = PromptBuilder(str(spec.template_path))
        reference_samples: List[Dict[str, Any]] = []
        similar_samples: List[Dict[str, Any]] = []

        for idx, score in retrieved:
            sample_text = train_texts[idx]
            props = train_targets[idx]
            reference_samples.append(
                {
                    "sample_text": sample_text,
                    "similarity": score,
                    "properties": props,
                }
            )
            out = {"sample_text": sample_text, "similarity": score}
            for col in spec.target_cols:
                if pd.notna(props.get(col)):
                    out[col] = float(props[col])
            similar_samples.append(out)

        prompt = prompt_builder.build_prompt(
            target_properties=spec.target_cols,
            test_sample=test_text,
            reference_samples=reference_samples,
        )
        caller = ModelCaller(
            model_name=self.model_name,
            temperature=self.temperature,
            api_key=self.api_key,
            base_url=self.base_url,
        )
        llm_response = caller.call(prompt)
        parser = ResultParser(spec.target_cols)
        predicted = parser.parse(llm_response)
        confidence = parser.extract_confidence(llm_response) or "low"

        return FewshotPrediction(
            material_type=material_type,
            predicted_values=predicted,
            confidence=confidence,
            similar_samples=similar_samples,
            prompt=prompt,
            llm_response=llm_response,
        )

    @staticmethod
    def _compact_non_empty_dict(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            k: v
            for k, v in payload.items()
            if v is not None and (not isinstance(v, str) or v.strip())
        }

    @staticmethod
    def _context_label(label: str) -> str:
        return "heat treatment method" if label == "processing" else f"{label}_text"

    @staticmethod
    def _format_input_composition(composition: Dict[str, Any]) -> str:
        parts = []
        for key, value in composition.items():
            if value is None:
                continue
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    continue
                try:
                    numeric = float(text)
                    if numeric != 0:
                        parts.append(f"{key} {numeric}")
                    continue
                except (TypeError, ValueError):
                    match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", text)
                    if match:
                        try:
                            numeric = float(match.group(0))
                            if numeric != 0:
                                parts.append(f"{key} {numeric}")
                            continue
                        except (TypeError, ValueError):
                            pass
                    parts.append(f"{key} {text}")
                    continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                parts.append(f"{key} {value}")
                continue
            if numeric != 0:
                parts.append(f"{key} {numeric}")
        return ", ".join(parts)

    @staticmethod
    def _build_target_fallback_text(
        composition: Dict[str, Any],
        processing: Dict[str, Any],
        features: Dict[str, Any],
    ) -> str:
        lines = ["Target descriptors were empty after normalization."]
        if composition:
            lines.append(f"Raw composition: {json.dumps(composition, ensure_ascii=False)}")
        if processing:
            lines.append(f"Raw processing: {json.dumps(processing, ensure_ascii=False)}")
        if features:
            lines.append(f"Raw features: {json.dumps(features, ensure_ascii=False)}")
        return "\n".join(lines)

    @staticmethod
    def _build_sample_text(
        composition: str,
        processing: Dict[str, Any],
        features: Dict[str, Any],
    ) -> str:
        lines = []
        if composition:
            lines.append(f"Composition: {composition}")
        for key, value in FewshotPredictor._prepare_processing_for_display(processing).items():
            lines.append(f"{FewshotPredictor._display_key_name(key)}: {value}")
        for key, value in features.items():
            lines.append(f"{FewshotPredictor._display_key_name(key)}: {value}")
        return "\n".join(lines).strip()

    @staticmethod
    def _extract_non_empty_fields(row: pd.Series, cols: List[str]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for col in cols:
            value = row.get(col)
            if pd.isna(value):
                continue
            if isinstance(value, str) and not value.strip():
                continue
            if hasattr(value, "item"):
                try:
                    value = value.item()
                except Exception:
                    pass
            out[col] = value
        return out

    @staticmethod
    def _normalize_context_payload(value: Optional[Any], label: str) -> Dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return FewshotPredictor._compact_non_empty_dict(value)

        normalized_label = FewshotPredictor._context_label(label)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return {}
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    return FewshotPredictor._compact_non_empty_dict(parsed)
            except Exception:
                pass
            return {normalized_label: FewshotPredictor._table_or_text_to_text(text)}
        if isinstance(value, list):
            return {normalized_label: FewshotPredictor._table_or_text_to_text(value)}
        return {normalized_label: str(value)}

    @staticmethod
    def _table_or_text_to_text(raw: Any) -> str:
        if isinstance(raw, list):
            lines: List[str] = []
            for idx, item in enumerate(raw, start=1):
                if isinstance(item, dict):
                    cells = ", ".join(f"{k}={v}" for k, v in item.items())
                    lines.append(f"Row {idx}: {cells}")
                else:
                    lines.append(f"Row {idx}: {item}")
            return "; ".join(lines)

        text = str(raw).strip()
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        table_lines = [ln for ln in lines if "|" in ln]
        if len(table_lines) < 2:
            return text

        headers = [h.strip() for h in table_lines[0].strip("|").split("|")]
        rows = []
        for ln in table_lines[1:]:
            cells = [c.strip() for c in ln.strip("|").split("|")]
            if len(cells) != len(headers):
                continue
            if all(set(c) <= {"-", ":"} for c in cells):
                continue
            row_text = ", ".join(f"{h}={v}" for h, v in zip(headers, cells))
            rows.append(row_text)
        if not rows:
            return text
        return "; ".join(f"Row {i + 1}: {r}" for i, r in enumerate(rows))

    @staticmethod
    def _prepare_processing_for_display(processing: Dict[str, Any]) -> Dict[str, Any]:
        if not processing:
            return {}

        priority_keys = {
            "processing_description",
            "heat treatment method",
            "processing description",
            "process_description",
            "process description",
        }
        for key, value in processing.items():
            normalized = str(key).strip().lower().replace("_", " ")
            if normalized in priority_keys and value is not None and str(value).strip():
                return {"heat treatment method": value}

        if "heat treatment method" in processing and str(processing["heat treatment method"]).strip():
            return {"heat treatment method": processing["heat treatment method"]}

        route_text = FewshotPredictor._to_ordered_processing_route(processing)
        if route_text:
            return {"heat treatment method": route_text}

        return processing

    @staticmethod
    def _to_ordered_processing_route(processing: Dict[str, Any]) -> str:
        if not isinstance(processing, dict) or not processing:
            return ""

        def _stage_rank(raw_key: str) -> int:
            key = str(raw_key).strip().lower().replace("_", " ")
            if "cast" in key or "熔" in key or "铸" in key:
                return 10
            if "forge" in key or "thermo" in key or "rolling" in key or "轧" in key or "锻" in key:
                return 20
            if "solution" in key or "固溶" in key:
                return 30
            if "quench" in key or "淬" in key:
                return 40
            if "age" in key or "时效" in key:
                return 50
            if "temper" in key or "回火" in key:
                return 60
            if "anneal" in key or "退火" in key:
                return 70
            if "cool" in key or "冷却" in key:
                return 80
            return 90

        ordered_items = sorted(
            [(str(k), str(v).strip()) for k, v in processing.items() if v is not None and str(v).strip()],
            key=lambda kv: (_stage_rank(kv[0]), kv[0].lower()),
        )
        if not ordered_items:
            return ""

        parts: List[str] = []
        for key, value in ordered_items:
            normalized_key = key.strip().lower().replace("_", " ")
            if normalized_key in {"heat treatment method", "processing description", "process description"}:
                parts.append(value)
            else:
                parts.append(value)
        return " -> ".join(parts)

    @staticmethod
    def _display_key_name(key: str) -> str:
        key_str = str(key).strip()
        lowered = key_str.lower().replace("_", " ")
        if lowered in {"processing description", "process description"}:
            return "heat treatment method"
        if lowered == "ph":
            return "PH"
        words = [w for w in key_str.replace("_", " ").split(" ") if w]
        if not words:
            return key_str
        return " ".join(w if any(ch.isupper() for ch in w) else w.capitalize() for w in words)
