from __future__ import annotations

from . import ProviderConfig

PROVIDER = ProviderConfig(
    name="theapi",
    api_key_env=("THEAPI_API_KEY",),
    base_urls_env="THEAPI_BASE_URLS",
    default_base_urls=(
        "https://svip.theapi.top/v1",
    ),
    models=(),
    litellm_provider="openai",
)
