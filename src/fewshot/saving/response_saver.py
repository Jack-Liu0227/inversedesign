from __future__ import annotations

from pathlib import Path
import threading


class ResponseSaver:
    def __init__(self, outputs_dir: str) -> None:
        self.outputs_dir = Path(outputs_dir)
        self.outputs_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def save(self, sample_id: int, response_text: str) -> Path:
        path = self.outputs_dir / f"sample_{sample_id}.txt"
        text = response_text if isinstance(response_text, str) else str(response_text)
        with self._lock:
            if path.exists():
                existing = path.read_text(encoding="utf-8", errors="ignore")
                if self._is_error_text(text) and not self._is_error_text(existing):
                    return path
            try:
                path.write_text(text, encoding="utf-8")
            except UnicodeEncodeError:
                # Some gateway responses may contain invalid surrogate chars.
                safe_text = text.encode("utf-8", errors="replace").decode("utf-8")
                path.write_text(safe_text, encoding="utf-8")
        return path

    @staticmethod
    def _is_error_text(text: str) -> bool:
        return text.strip().startswith("ERROR:")
