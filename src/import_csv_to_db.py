from __future__ import annotations

import json
import sys
from pathlib import Path

# Support both:
# 1) python -m src.import_csv_to_db
# 2) python src/import_csv_to_db.py
if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from src.common.dataset_store import import_csv_datasets_to_db


def main() -> None:
    result = import_csv_datasets_to_db()
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
