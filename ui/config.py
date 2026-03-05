from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    project_root: Path
    db_dir: Path
    templates_dir: Path
    static_dir: Path
    timezone: str = "Asia/Shanghai"
    default_page_size: int = 20
    max_page_size: int = 100

    @property
    def db_paths(self) -> dict[str, Path]:
        return {
            "material_agent_shared": self.db_dir / "material_agent_shared.db",
            "prediction_prompt_logs": self.db_dir / "prediction_prompt_logs.db",
            "prompt_llmresponse": self.db_dir / "prompt_llmresponse.db",
            "workflow_audit": self.db_dir / "workflow_audit.db",
            "ui_classifications": self.db_dir / "ui_classifications.db",
        }


def get_config() -> AppConfig:
    ui_root = Path(__file__).resolve().parent
    project_root = ui_root.parent
    return AppConfig(
        project_root=project_root,
        db_dir=project_root / "db",
        templates_dir=ui_root / "templates",
        static_dir=ui_root / "static",
    )
