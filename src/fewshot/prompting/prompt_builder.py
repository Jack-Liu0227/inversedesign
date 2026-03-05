from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Any
import json


class PromptBuilder:
    def __init__(self, template_path: str) -> None:
        self.template_path = template_path
        self.template = self._load_template(template_path)

    def _load_template(self, template_path: str) -> Dict[str, Any]:
        with open(template_path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _build_predictions_json_template(target_properties: List[str]) -> str:
        lines = []
        for prop in target_properties:
            unit = PromptBuilder._get_unit(prop)
            lines.append(f"\"{prop}\": {{\"value\": <number>, \"unit\": \"{unit}\"}}")
        return ",\n    ".join(lines)

    @staticmethod
    def _get_unit(target_property: str) -> str:
        if "(" in target_property and target_property.endswith(")"):
            return target_property[target_property.rfind("(") + 1 : -1].strip()
        lower = target_property.lower()
        if "uts" in lower or "tensile" in lower or "ys" in lower or "yield" in lower:
            return "MPa"
        if "el" in lower or "elongation" in lower:
            return "%"
        return ""

    @staticmethod
    def _safe_format(text: str, vars_dict: Dict[str, Any]) -> str:
        if not text:
            return text

        temp_open = "<<<BRACE_OPEN>>>"
        temp_close = "<<<BRACE_CLOSE>>>"
        protected = text.replace("{{", temp_open).replace("}}", temp_close)

        class SafeDict(dict):
            def __missing__(self, key: str) -> str:
                return "{" + key + "}"

        try:
            formatted = protected.format_map(SafeDict(**vars_dict))
            return formatted.replace(temp_open, "{").replace(temp_close, "}")
        except Exception:
            # Fallback: direct placeholder replacement for unescaped braces.
            result = text
            for key in sorted(vars_dict.keys(), key=len, reverse=True):
                placeholder = "{" + key + "}"
                if placeholder in result:
                    result = result.replace(placeholder, str(vars_dict[key]))
            return result

    def _apply_column_mapping(self, text: str) -> str:
        mapping = self.template.get("column_name_mapping", {}) or {}
        for old, new in mapping.items():
            text = text.replace(f"{old}:", f"{new}:")
        return text

    def _build_reference_section(
        self,
        reference_samples: List[Dict[str, Any]],
        target_properties: List[str],
    ) -> str:
        lines: List[str] = []
        for sample in reference_samples:
            sample_text = self._apply_column_mapping(sample["sample_text"])
            for line in sample_text.splitlines():
                if line.strip():
                    lines.append(line)
            lines.append("Properties:")
            for prop in target_properties:
                value = sample["properties"].get(prop)
                if value is not None:
                    unit = self._get_unit(prop)
                    lines.append(f"  - {prop}: {value} {unit}")
            lines.append("")
        return "\n".join(lines).strip()

    @staticmethod
    def _append_section(parts: List[str], title: str, content: str) -> None:
        if content:
            parts.append(f"### {title}\n{content}\n")

    @staticmethod
    def _append_plain(parts: List[str], content: str) -> None:
        if content.strip():
            parts.append(content)

    def build_prompt(
        self,
        target_properties: List[str],
        test_sample: str,
        reference_samples: List[Dict[str, Any]],
    ) -> str:
        apply_mapping = bool(self.template.get("apply_mapping_to_target", True))
        mapped_test = self._apply_column_mapping(test_sample) if apply_mapping else test_sample
        reference_text = self._build_reference_section(reference_samples, target_properties)
        template_vars = {
            "test_sample": mapped_test,
            "reference_samples": reference_text,
            "target_properties_list": ", ".join(target_properties),
            "predictions_json_template": self._build_predictions_json_template(target_properties),
        }

        parts: List[str] = []
        self._append_section(parts, "System Role", str(self.template.get("system_role", "")))

        task_desc = self._safe_format(self.template.get("task_description", ""), template_vars)
        self._append_section(parts, "Task", task_desc)

        ref_format = self._safe_format(self.template.get("reference_format", ""), template_vars)
        self._append_plain(parts, ref_format)

        input_format = self._safe_format(self.template.get("input_format", ""), template_vars)
        self._append_plain(parts, input_format)

        if self.template.get("analysis_protocol"):
            parts.append(self.template["analysis_protocol"])

        output_format = self._safe_format(self.template.get("output_format", ""), template_vars)
        self._append_plain(parts, output_format)

        return "\n\n".join(parts).strip()
