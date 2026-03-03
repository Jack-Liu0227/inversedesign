from __future__ import annotations

from . import ProviderConfig

PROVIDER = ProviderConfig(
    name="openrouter",
    api_key_env=("OPENROUTER_API_KEY",),
    base_urls_env="OPENROUTER_BASE_URLS",
    default_base_urls=(
        "https://openrouter.ai/api/v1",
    ),
    models=(
        "gpt-oss-120b:free",
        "google/gemini-3-flash-preview",
        "google/gemini-2.5-pro",
        "google/gemini-3.1-pro-preview",
        "google/gemini-3-pro-preview",
        "google/gemini-2.5-flash",
    ),
    # Use OpenAI-compatible routing against OpenRouter base_url.
    # Configured model "google/gemini-3-flash-preview" resolves to
    # "openai/google/gemini-3-flash-preview".
    litellm_provider="openai",
)
