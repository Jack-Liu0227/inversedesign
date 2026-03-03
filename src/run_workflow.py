from __future__ import annotations

from typing import Dict
from pathlib import Path
import sys
import json

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.workflows import workflow


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
            user_val = input(f"{field_name}{marker} ({field_type}): ").strip()
            if required and not user_val:
                raise ValueError(f"Missing required field: {field_name}")
            values[field_name] = user_val
        req.set_user_input(**values)


def _print_step_outputs(run_output) -> None:
    content = getattr(run_output, "content", None)
    if not isinstance(content, dict):
        return
    step_outputs = content.get("step_outputs", {})
    if not isinstance(step_outputs, dict) or not step_outputs:
        return
    print("\nStep outputs:")
    for step_name, payload in step_outputs.items():
        print(f"\n[{step_name}]")
        print(json.dumps(payload, ensure_ascii=False, indent=2))


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
        "material_type": "ti",
        "goal": "Design high strength and good ductility alloy",
        "composition": {"Ti": 88.0, "Al": 6.0, "V": 4.0, "Mo": 2.0},
        "processing": {"Processing_Description": "Solution treated and aged"},
        "features": {},
        "top_k": 3,
        "max_iterations": 3,
    }

    run_output = workflow.run(initial_input)
    while True:
        paused = bool(getattr(run_output, "is_paused", False))
        if not paused:
            status = getattr(run_output, "status", None)
            paused = "paused" in str(status).lower() if status is not None else False
        if not paused:
            break

        if getattr(run_output, "steps_requiring_confirmation", None):
            _collect_confirmations(run_output)
        if getattr(run_output, "steps_requiring_user_input", None):
            _collect_user_inputs(run_output)

        if not hasattr(workflow, "continue_run"):
            break
        run_output = workflow.continue_run(run_output)

    print("\nWorkflow completed.")
    print(run_output.content)
    _print_step_outputs(run_output)


if __name__ == "__main__":
    main()

# Run interactive local workflow loop (CLI):
# python src/run_workflow.py
# python -m src.run_workflow
