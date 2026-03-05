from __future__ import annotations

import json

from src.common import import_csv_datasets_to_db


def main() -> None:
    result = import_csv_datasets_to_db()
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
