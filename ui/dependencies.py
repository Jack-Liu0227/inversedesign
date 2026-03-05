from __future__ import annotations

from functools import lru_cache

from ui.config import AppConfig, get_config
from ui.db.repositories.classification_repo import ClassificationRepository, classification_repo
from ui.db.repositories.doc_evolution_repo import DocEvolutionRepository, doc_evolution_repo
from ui.db.repositories.explorer_repo import ExplorerRepository, explorer_repo
from ui.db.repositories.material_data_repo import MaterialDataRepository, material_data_repo
from ui.db.repositories.prediction_repo import PredictionRepository, prediction_repo
from ui.db.repositories.tool_trace_repo import ToolTraceRepository, tool_trace_repo
from ui.db.repositories.workflow_repo import WorkflowRepository, workflow_repo
from ui.services.classification_service import ClassificationService
from ui.services.lineage_service import LineageService
from ui.services.stats_service import StatsService


@lru_cache
def get_app_config() -> AppConfig:
    return get_config()


def get_classification_repository() -> ClassificationRepository:
    return classification_repo


def get_explorer_repository() -> ExplorerRepository:
    return explorer_repo


def get_doc_evolution_repository() -> DocEvolutionRepository:
    return doc_evolution_repo


def get_material_data_repository() -> MaterialDataRepository:
    return material_data_repo


def get_prediction_repository() -> PredictionRepository:
    return prediction_repo


def get_tool_trace_repository() -> ToolTraceRepository:
    return tool_trace_repo


def get_workflow_repository() -> WorkflowRepository:
    return workflow_repo


@lru_cache
def get_classification_service() -> ClassificationService:
    return ClassificationService(repository=get_classification_repository())


@lru_cache
def get_lineage_service() -> LineageService:
    return LineageService(workflow_repository=get_workflow_repository())


@lru_cache
def get_stats_service() -> StatsService:
    return StatsService(
        prediction_repository=get_prediction_repository(),
        workflow_repository=get_workflow_repository(),
        classification_repository=get_classification_repository(),
    )
