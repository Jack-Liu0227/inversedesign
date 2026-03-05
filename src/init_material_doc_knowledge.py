from __future__ import annotations

import json
import importlib.util
import sys
from pathlib import Path

# Support both:
# 1) python -m src.init_material_doc_knowledge
# 2) python src/init_material_doc_knowledge.py
if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

def _load_upsert_function():
    module_path = Path(__file__).resolve().parent / "common" / "material_doc_store.py"
    spec = importlib.util.spec_from_file_location("material_doc_store_local", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module spec: {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    fn = getattr(module, "upsert_material_docs_from_dir", None)
    if not callable(fn):
        raise RuntimeError("upsert_material_docs_from_dir not found")
    return fn


def main() -> None:
    upsert_material_docs_from_dir = _load_upsert_function()
    written = upsert_material_docs_from_dir()
    print(json.dumps({"ok": True, "rows_written": written}, ensure_ascii=False))


if __name__ == "__main__":
    main()
