from __future__ import annotations

from dataclasses import dataclass, asdict, field
from pathlib import Path
import json
from typing import List, Optional


@dataclass
class DataConfig:
    input_file: str
    output_dir: str
    template_path: str
    sample_size: int = 3
    target_cols: Optional[List[str]] = None
    element_cols: Optional[List[str]] = None
    processing_cols: Optional[List[str]] = None
    feature_cols: Optional[List[str]] = None


@dataclass
class RetrievalConfig:
    top_k: int = 3
    embedding_model: str = "EmbeddingModel/all-MiniLM-L6-v2"


@dataclass
class LLMConfig:
    model_name: str = "gemini-2.5-flash"
    temperature: float = 0.0
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    max_retries: int = 2
    allow_mock_on_failure: bool = False


@dataclass
class PipelineConfig:
    data: DataConfig
    retrieval: RetrievalConfig = field(default_factory=RetrievalConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)

    def to_json(self, path: str) -> None:
        payload = {
            "data": asdict(self.data),
            "retrieval": asdict(self.retrieval),
            "llm": asdict(self.llm),
        }
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

    @staticmethod
    def from_json(path: str) -> "PipelineConfig":
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return PipelineConfig(
            data=DataConfig(**payload["data"]),
            retrieval=RetrievalConfig(**payload.get("retrieval", {})),
            llm=LLMConfig(**payload.get("llm", {})),
        )


def create_default_config(
    input_file: str = "datasets/Ti_alloys/titanium.csv",
    output_dir: str = "output/titanium_refactored_demo",
    template_path: str = "prompt_templates/default_unified.json",
    target_cols: Optional[List[str]] = None,
    top_k: int = 3,
    sample_size: int = 3,
    model_name: str = "gemini-2.5-flash",
    temperature: float = 0.0,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    max_retries: int = 2,
    allow_mock_on_failure: bool = False,
) -> PipelineConfig:
    if target_cols is None:
        target_cols = ["UTS(MPa)", "El(%)"]
    data = DataConfig(
        input_file=input_file,
        output_dir=output_dir,
        template_path=template_path,
        target_cols=target_cols,
        sample_size=sample_size,
    )
    retrieval = RetrievalConfig(top_k=top_k)
    llm = LLMConfig(
        model_name=model_name,
        temperature=temperature,
        api_key=api_key,
        base_url=base_url,
        max_retries=max_retries,
        allow_mock_on_failure=allow_mock_on_failure,
    )
    return PipelineConfig(data=data, retrieval=retrieval, llm=llm)
