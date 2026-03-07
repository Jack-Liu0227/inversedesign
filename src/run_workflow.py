from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Dict

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.workflows import workflow


def _is_paused(run_output) -> bool:
    paused = bool(getattr(run_output, "is_paused", False))
    if paused:
        return True
    status = getattr(run_output, "status", None)
    return "paused" in str(status).lower() if status is not None else False


def _validate_measured_values_json(user_val: str) -> None:
    if not user_val:
        return
    parsed = json.loads(user_val)
    if not isinstance(parsed, dict):
        raise ValueError("measured_values_json must decode to a JSON object")


def _collect_user_inputs(run_output) -> None:
    for req in (getattr(run_output, "steps_requiring_user_input", None) or []):
        values: Dict[str, object] = {}
        print(f"\nUser input required at step: {req.step_name}")
        print(req.user_input_message)
        for field in req.user_input_schema:
            required = bool(getattr(field, "required", getattr(field, "is_required", False)))
            field_name = getattr(field, "name", "value")
            field_type = getattr(field, "field_type", getattr(field, "type", "str"))
            marker = "*" if required else ""
            if field_name == "measured_values_json":
                print('  hint: {"UTS(MPa)": 980, "EL(%)": 12.4}')
            user_val = input(f"{field_name}{marker} ({field_type}): ").strip()
            if required and not user_val:
                raise ValueError(f"Missing required field: {field_name}")
            if field_name == "measured_values_json" and user_val:
                _validate_measured_values_json(user_val)
            values[field_name] = user_val
        req.set_user_input(**values)


def _collect_confirmations(run_output) -> None:
    for req in (getattr(run_output, "steps_requiring_confirmation", None) or []):
        print(f"\nConfirmation required at step: {req.step_name}")
        print(req.confirmation_message)
        yn = input("Confirm? (y/n): ").strip().lower()
        if yn == "y":
            req.confirm()
        else:
            req.reject()


def main() -> None:
    initial_input = {
        "goal": "Design titanium alloy with UTS>=2000 and EL>=15",
        "human_loop": False,
        "top_k": 3,
        "max_iterations": 3,
        # Set this to continue a previous run's round index, e.g. "a5a5a84c-...".
        "resume_run_id": None,
        "include_debug": True,
    }

    run_output = workflow.run(initial_input)
    while True:
        if not _is_paused(run_output):
            break
        if getattr(run_output, "steps_requiring_confirmation", None):
            _collect_confirmations(run_output)
        if getattr(run_output, "steps_requiring_user_input", None):
            _collect_user_inputs(run_output)
        if not hasattr(workflow, "continue_run"):
            break
        run_output = workflow.continue_run(run_output)

    print("\nWorkflow completed.")
    print(json.dumps(run_output.content, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
