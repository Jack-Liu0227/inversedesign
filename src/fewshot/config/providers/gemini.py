from __future__ import annotations

from . import ProviderConfig

PROVIDER = ProviderConfig(
    name="gemini",
    api_key_env=("GEMINI_API_KEY", "GOOGLE_API_KEY"),
    base_urls_env="GEMINI_BASE_URLS",
    default_base_urls=None,
    models=(),
    litellm_provider="gemini",
)
