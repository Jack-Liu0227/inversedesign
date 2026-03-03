from __future__ import annotations

from . import ProviderConfig

PROVIDER = ProviderConfig(
    name="ollama",
    api_key_env=("OLLAMA_API_KEY",),
    base_urls_env="OLLAMA_BASE_URLS",
    default_base_urls=(
        "http://127.0.0.1:11434",
    ),
    models=(
        "gpt-oss:20b",
    ),
    litellm_provider="ollama",
)
