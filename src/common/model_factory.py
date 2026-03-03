import json
import os
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

from agno.models.openai import OpenAIChat

SRC_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = SRC_DIR.parent
DOTENV_PATH = PROJECT_ROOT / ".env"
MODEL_CONFIG_DIR = SRC_DIR / "model_config"
PROVIDERS_CONFIG_PATH = MODEL_CONFIG_DIR / "providers.json"
AGENT_MODELS_CONFIG_PATH = MODEL_CONFIG_DIR / "agent_models.json"
load_dotenv(dotenv_path=DOTENV_PATH, override=True)


def _pick_first(value: str) -> str:
    return value.split(",")[0].strip()


def _read_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Config file must be a JSON object: {path}")
    return data


def _resolve_provider_from_env() -> Dict[str, Dict[str, Any]]:
    return {
        "ollama": {
            "api_key": _pick_first(os.getenv("OLLAMA_API_KEY", "ollama")),
            "base_url": os.getenv("OLLAMA_BASE_URLS", "http://localhost:11434/v1").strip(),
            "default_model": os.getenv("OLLAMA_MODEL", "gpt-oss:20b").strip(),
            "requires_api_key": False,
        },
        "echoflow": {
            "api_key": _pick_first(os.getenv("ECHOFLOW_API_KEY", "")),
            "base_url": os.getenv("ECHOFLOW_BASE_URLS", "").strip(),
            "default_model": os.getenv("ECHOFLOW_MODEL", "").strip(),
            "requires_api_key": True,
        },
        "ricardo": {
            "api_key": _pick_first(os.getenv("RICARDO_API_KEY", "")),
            "base_url": os.getenv("RICARDO_BASE_URLS", "").strip(),
            "default_model": os.getenv("RICARDO_API_MODEL", "").strip(),
            "requires_api_key": True,
        },
        "openrouter": {
            "api_key": _pick_first(os.getenv("OPENROUTER_API_KEY", "")),
            "base_url": os.getenv("OPENROUTER_BASE_URLS", "https://openrouter.ai/api/v1").strip(),
            "default_model": os.getenv("OPENROUTER_MODEL", "openai/gpt-oss-20b:free").strip(),
            "requires_api_key": True,
        },
        "deepseek": {
            "api_key": _pick_first(os.getenv("DEEPSEEK_API_KEY", "")),
            "base_url": os.getenv("DEEPSEEK_BASE_URLS", "https://api.deepseek.com/v1").strip(),
            "default_model": os.getenv("DEEPSEEK_MODEL", "").strip(),
            "requires_api_key": True,
        },
    }


def _resolve_provider_from_file(
    provider_name: str,
    provider_map: Dict[str, Any],
) -> Dict[str, Any]:
    if provider_name not in provider_map:
        valid = ", ".join(sorted(provider_map.keys()))
        raise ValueError(f"Unsupported provider '{provider_name}'. Use one of: {valid}")
    spec = provider_map[provider_name]
    if not isinstance(spec, dict):
        raise ValueError(f"Provider spec must be object: {provider_name}")
    api_key_env = str(spec.get("api_key_env", "")).strip()
    base_url_env = str(spec.get("base_url_env", "")).strip()
    default_model_env = str(spec.get("default_model_env", "")).strip()
    api_key = _pick_first(os.getenv(api_key_env, "")) if api_key_env else str(spec.get("api_key", "")).strip()
    base_url = os.getenv(base_url_env, "").strip() if base_url_env else str(spec.get("base_url", "")).strip()
    default_model = (
        os.getenv(default_model_env, "").strip() if default_model_env else str(spec.get("default_model", "")).strip()
    )
    requires_api_key = bool(spec.get("requires_api_key", True))
    return {
        "api_key": api_key,
        "base_url": base_url,
        "default_model": default_model,
        "requires_api_key": requires_api_key,
    }


def _resolve_agent_binding(log_tag: str, models_cfg: Dict[str, Any]) -> Dict[str, Any]:
    default_binding = models_cfg.get("default", {}) if isinstance(models_cfg.get("default", {}), dict) else {}
    agents_binding = models_cfg.get("agents", {}) if isinstance(models_cfg.get("agents", {}), dict) else {}
    binding = agents_binding.get(log_tag, default_binding)
    if not isinstance(binding, dict):
        raise ValueError(f"Agent binding must be object for log tag '{log_tag}'")
    return binding


def build_model(log_tag: str = "model_factory") -> OpenAIChat:
    providers_cfg = _read_json_file(PROVIDERS_CONFIG_PATH)
    models_cfg = _read_json_file(AGENT_MODELS_CONFIG_PATH)

    if providers_cfg and models_cfg:
        binding = _resolve_agent_binding(log_tag, models_cfg)
        provider_name = str(binding.get("provider", "")).strip().lower()
        if not provider_name:
            provider_name = str(providers_cfg.get("default_provider", "ricardo")).strip().lower()
        provider_map = providers_cfg.get("providers", {})
        if not isinstance(provider_map, dict) or not provider_map:
            raise ValueError("providers.json must contain a non-empty 'providers' object")
        provider_config = _resolve_provider_from_file(provider_name, provider_map)
        model_id = os.getenv(
            str(binding.get("model_id_env", "MODEL_ID")).strip(),
            "",
        ).strip()
        model_id = model_id or str(binding.get("model_id", "")).strip()
        resolved_model_id = model_id or provider_config["default_model"]
    else:
        provider_name = os.getenv("MODEL_PROVIDER", "ricardo").strip().lower()
        providers = _resolve_provider_from_env()
        if provider_name not in providers:
            valid_providers = ", ".join(sorted(providers.keys()))
            raise ValueError(f"Unsupported MODEL_PROVIDER='{provider_name}'. Use one of: {valid_providers}")
        provider_config = providers[provider_name]
        model_id = os.getenv("MODEL_ID", "").strip()
        resolved_model_id = model_id or provider_config["default_model"]

    if provider_config.get("requires_api_key", True) and not provider_config["api_key"]:
        raise ValueError(f"Missing API key for provider '{provider_name}'. Check .env or providers.json")
    if not provider_config["base_url"]:
        raise ValueError(f"Missing base URL for provider '{provider_name}'. Check .env or providers.json")
    if not resolved_model_id:
        raise ValueError(f"Missing model id for provider '{provider_name}'. Check agent_models.json/.env")

    effective_api_key = provider_config["api_key"] or ("ollama" if provider_name == "ollama" else "")
    print(f"[{log_tag}] provider={provider_name} model={resolved_model_id}")
    compatible_role_map = {
        "system": "system",
        "user": "user",
        "assistant": "assistant",
        "tool": "tool",
        "model": "assistant",
    }
    return OpenAIChat(
        id=resolved_model_id,
        api_key=effective_api_key,
        base_url=provider_config["base_url"],
        role_map=compatible_role_map,
    )
