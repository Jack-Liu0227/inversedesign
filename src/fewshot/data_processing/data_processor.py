from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import pandas as pd


@dataclass
class ColumnInfo:
    element_cols: List[str]
    processing_cols: List[str]
    feature_cols: List[str]
    target_cols: List[str]


class DataProcessor:
    def __init__(
        self,
        input_file: str,
        target_cols: Optional[List[str]] = None,
        element_cols: Optional[List[str]] = None,
        processing_cols: Optional[List[str]] = None,
        feature_cols: Optional[List[str]] = None,
    ) -> None:
        self.input_file = input_file
        self.target_cols = target_cols
        self.element_cols = element_cols
        self.processing_cols = processing_cols
        self.feature_cols = feature_cols

    def load_data(self) -> pd.DataFrame:
        return pd.read_csv(self.input_file)

    def identify_columns(self, df: pd.DataFrame) -> ColumnInfo:
        cols = df.columns.tolist()

        element_cols = self.element_cols or []
        if not element_cols:
            element_cols = [
                c
                for c in cols
                if "wt%" in c.lower() or "at%" in c.lower()
            ]

        default_targets = [c for c in cols if c in ("UTS(MPa)", "El(%)")]
        target_cols = self.target_cols or default_targets

        provided_processing = list(self.processing_cols or [])
        provided_features = list(self.feature_cols or [])
        if provided_processing or provided_features:
            processing_cols = provided_processing
            feature_cols = provided_features
        else:
            excluded = set(element_cols) | set(target_cols)
            processing_cols = []
            for c in cols:
                if c in excluded:
                    continue
                col_lower = c.lower()
                is_text_series = str(df[c].dtype) in {"object", "string"}
                looks_like_process_text = any(
                    token in col_lower
                    for token in ("processing", "process", "description", "method", "route", "treatment")
                )
                if is_text_series or looks_like_process_text:
                    processing_cols.append(c)
            feature_cols = []

        return ColumnInfo(
            element_cols=element_cols,
            processing_cols=processing_cols,
            feature_cols=feature_cols,
            target_cols=target_cols,
        )

    @staticmethod
    def format_composition(row: pd.Series, element_cols: List[str]) -> str:
        parts = []
        for col in element_cols:
            value = row.get(col)
            if pd.notna(value) and value != 0:
                element = col.split("(")[0].strip()
                parts.append(f"{element} {value}")
        return ", ".join(parts)
