from __future__ import annotations

from . import ProviderConfig

PROVIDER = ProviderConfig(
    name="deepseek",
    api_key_env=("DEEPSEEK_API_KEY",),
    base_urls_env="DEEPSEEK_BASE_URLS",
    default_base_urls=None,
    models=(),
)
