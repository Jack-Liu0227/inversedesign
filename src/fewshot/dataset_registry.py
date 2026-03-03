from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class DatasetSpec:
    key: str
    name: str
    dataset_path: Path
    target_cols: List[str]
    template_path: Path
    default_top_k: int = 3


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _skills_dir() -> Path:
    return Path(__file__).resolve().parent / "skills"


def _load_json(path: Path) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid JSON object: {path}")
    return data


def _dataset_config() -> Dict:
    path = _skills_dir() / "dataset_registry.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing dataset registry config: {path}")
    return _load_json(path)


def _routing_config() -> Dict:
    path = _skills_dir() / "routing_rules.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing routing rules config: {path}")
    return _load_json(path)


def _normalize_token(value: str) -> str:
    return "".join(ch for ch in (value or "").strip().lower() if ch.isalnum())


def _route_alias_lookup(registry: Dict[str, DatasetSpec], routes: Dict) -> Dict[str, str]:
    alias_map: Dict[str, str] = {}
    for key, rules in routes.items():
        if key not in registry or not isinstance(rules, dict):
            continue
        alias_map[_normalize_token(key)] = key
        aliases = [str(v).strip().lower() for v in rules.get("aliases", [])]
        for alias in aliases:
            if alias:
                alias_map[_normalize_token(alias)] = key
    return alias_map


def get_dataset_registry() -> Dict[str, DatasetSpec]:
    root = _repo_root()
    cfg = _dataset_config()
    datasets = cfg.get("datasets", {})
    if not isinstance(datasets, dict) or not datasets:
        raise ValueError("dataset_registry.json must contain a non-empty 'datasets' object")

    registry: Dict[str, DatasetSpec] = {}
    for key, spec in datasets.items():
        if not isinstance(spec, dict):
            continue
        dataset_path = root / str(spec.get("dataset_path", "")).strip()
        template_path = root / str(spec.get("template_path", "")).strip()
        registry[key] = DatasetSpec(
            key=key,
            name=str(spec.get("name", key)),
            dataset_path=dataset_path,
            target_cols=list(spec.get("target_cols", [])),
            template_path=template_path,
            default_top_k=int(spec.get("default_top_k", 3)),
        )
    return registry


def resolve_dataset(material_type: str) -> DatasetSpec:
    registry = get_dataset_registry()
    key = material_type.strip().lower()
    if key not in registry:
        supported = ", ".join(sorted(registry.keys()))
        raise ValueError(f"Unsupported material_type '{material_type}'. Use one of: {supported}")
    spec = registry[key]
    if not spec.dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found for '{material_type}': {spec.dataset_path}")
    if not spec.template_path.exists():
        raise FileNotFoundError(f"Prompt template not found: {spec.template_path}")
    if not spec.target_cols:
        raise ValueError(f"No target_cols configured for material_type '{material_type}'")
    return spec


def route_material_type(goal: str = "", material_type: str = "") -> str:
    registry = get_dataset_registry()
    routing = _routing_config()
    routes = routing.get("routes", {})
    default_key = str(routing.get("default_material_type", "ti")).strip().lower()

    explicit = material_type.strip().lower()
    alias_map = _route_alias_lookup(registry, routes)
    if explicit:
        normalized = _normalize_token(explicit)
        if explicit in registry:
            return explicit
        if normalized in alias_map:
            return alias_map[normalized]

    goal_lower = (goal or "").lower()
    for key, rules in routes.items():
        if key not in registry:
            continue
        keywords = [str(v).strip().lower() for v in rules.get("keywords", [])]
        if any(kw and kw in goal_lower for kw in keywords):
            return key

    if default_key not in registry:
        raise ValueError(f"default_material_type '{default_key}' is not defined in datasets")
    return default_key


def resolve_material_type_input(goal: str = "", material_type: str = "") -> Tuple[str, str]:
    registry = get_dataset_registry()
    routing = _routing_config()
    routes = routing.get("routes", {})
    alias_map = _route_alias_lookup(registry, routes)
    explicit_raw = (material_type or "").strip()
    explicit = explicit_raw.lower()

    if explicit:
        if explicit in registry:
            return explicit, "exact_dataset_key"
        normalized = _normalize_token(explicit)
        if normalized in alias_map:
            return alias_map[normalized], "alias_match"

    resolved = route_material_type(goal=goal, material_type=material_type)
    if explicit_raw:
        return resolved, "goal_or_default_fallback"
    return resolved, "goal_or_default"


def supported_material_type_hint() -> str:
    registry = get_dataset_registry()
    routing = _routing_config()
    routes = routing.get("routes", {})
    keys = ", ".join(sorted(registry.keys()))
    alias_examples: List[str] = []
    for key in sorted(registry.keys()):
        aliases = routes.get(key, {}).get("aliases", [])
        if aliases:
            alias_examples.append(f"{key}: {aliases[:2]}")
    alias_text = "; ".join(alias_examples[:5])
    if alias_text:
        return f"Use one of dataset keys: {keys}. Alias examples: {alias_text}"
    return f"Use one of dataset keys: {keys}"
