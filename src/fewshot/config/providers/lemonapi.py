from __future__ import annotations

from . import ProviderConfig

PROVIDER = ProviderConfig(
    name="lemonapi",
    api_key_env=("LEMONAPI_API_KEY",),
    base_urls_env="LEMONAPI_BASE_URLS",
    default_base_urls=(
        "https://new.lemonapi.site/v1",
    ),
    models=(
        "[L]gemini-2.5-pro",
        "[L]gemini-2.5-pro-maxthinking",
        "[L]gemini-2.5-pro-search",
        "[L]gemini-2.5-flash",
        "[L]gemini-2.5-flash-maxthinking",
        "[L]gemini-2.5-flash-search",
        "[L]gemini-3-flash-preview",
        "[L]gemini-3-flash-preview-search",
        "[L]gemini-3-pro-preview",
        "[L]gemini-3-pro-preview-search",
        "[L]gemini-3.1-pro-preview",
    ),
    litellm_provider="openai",
)
