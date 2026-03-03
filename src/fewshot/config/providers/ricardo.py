from __future__ import annotations

from . import ProviderConfig

PROVIDER = ProviderConfig(
    name="ricardo",
    api_key_env=("RICARDO_API_KEY",),
    base_urls_env="RICARDO_BASE_URLS",
    default_base_urls=(
        "https://api.ricardochat.cn/v1",
        "https://123444567.xyz/v1",
        "https://123444321.xyz/v1",
        "https://api.ricardochat.xyz/v1",
    ),
    models=(),
    litellm_provider="openai",
)
