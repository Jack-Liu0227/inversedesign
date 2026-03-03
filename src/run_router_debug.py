from __future__ import annotations

import argparse
from pathlib import Path
import sys

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.fewshot import get_dataset_registry, resolve_dataset, route_material_type


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Debug material routing rules and dataset binding."
    )
    parser.add_argument(
        "--goal",
        type=str,
        default="Design high strength and good ductility alloy",
        help="Optimization goal used by routing rules.",
    )
    parser.add_argument(
        "--material-type",
        type=str,
        default="",
        help="Explicit material type or alias. Leave empty for auto-routing by goal.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all configured datasets and exit.",
    )
    return parser


def print_registry() -> None:
    registry = get_dataset_registry()
    print("Configured datasets:")
    for key in sorted(registry.keys()):
        spec = registry[key]
        print(f"- {key}: {spec.name}")
        print(f"  dataset: {spec.dataset_path}")
        print(f"  targets: {spec.target_cols}")
        print(f"  template: {spec.template_path}")
        print(f"  default_top_k: {spec.default_top_k}")


def main() -> None:
    args = build_parser().parse_args()
    if args.list:
        print_registry()
        return

    resolved_key = route_material_type(goal=args.goal, material_type=args.material_type)
    spec = resolve_dataset(resolved_key)

    print("Routing debug result")
    print("====================")
    print(f"input.goal: {args.goal}")
    print(f"input.material_type: {args.material_type or '<empty>'}")
    print(f"resolved.material_type: {resolved_key}")
    print(f"dataset.name: {spec.name}")
    print(f"dataset.path: {spec.dataset_path}")
    print(f"dataset.exists: {Path(spec.dataset_path).exists()}")
    print(f"target_cols: {spec.target_cols}")
    print(f"template.path: {spec.template_path}")
    print(f"template.exists: {Path(spec.template_path).exists()}")
    print(f"default_top_k: {spec.default_top_k}")


if __name__ == "__main__":
    main()
