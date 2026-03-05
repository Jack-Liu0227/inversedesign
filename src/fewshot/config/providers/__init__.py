from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Optional, Sequence


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    api_key_env: Sequence[str]
    base_urls_env: Optional[str]
    default_base_urls: Optional[Sequence[str]]
    models: Sequence[str]
    litellm_provider: Optional[str] = None

    def supports_model(self, model_name: str) -> bool:
        # Empty model list means provider does not enforce a model whitelist.
        if not self.models:
            return True
        normalized = self._strip_provider_prefix(model_name)
        return normalized in self.models

    def resolve_api_key(self, env: Mapping[str, str]) -> Optional[str]:
        for key in self.api_key_env:
            value = env.get(key)
            if value:
                return value
        return None

    def resolve_base_urls(self, env: Mapping[str, str]) -> Optional[str]:
        if self.base_urls_env:
            value = env.get(self.base_urls_env)
            if value:
                return value
        if self.default_base_urls:
            return ",".join(self.default_base_urls)
        return None

    def resolve_model(self, model_name: str) -> str:
        if not self.litellm_provider:
            return model_name
        prefix = f"{self.litellm_provider}/"
        if model_name.startswith(prefix):
            return model_name
        return f"{prefix}{model_name}"

    def _strip_provider_prefix(self, model_name: str) -> str:
        if not self.litellm_provider:
            return model_name
        prefix = f"{self.litellm_provider}/"
        if model_name.startswith(prefix):
            return model_name[len(prefix):]
        return model_name


def _load_providers() -> Iterable[ProviderConfig]:
    from .ricardo import PROVIDER as ricardo_provider
    from .deepseek import PROVIDER as deepseek_provider
    from .gemini import PROVIDER as gemini_provider
    from .theapi import PROVIDER as theapi_provider
    from .lemonapi import PROVIDER as lemonapi_provider
    from .openrouter import PROVIDER as openrouter_provider
    from .ollama import PROVIDER as ollama_provider

    return (
        ricardo_provider,
        deepseek_provider,
        gemini_provider,
        theapi_provider,
        lemonapi_provider,
        openrouter_provider,
        ollama_provider,
    )


PROVIDERS = {provider.name: provider for provider in _load_providers()}


def get_provider(name: str) -> ProviderConfig:
    if name not in PROVIDERS:
        raise KeyError(f"Unknown LLM provider: {name}")
    return PROVIDERS[name]


__all__ = ["ProviderConfig", "PROVIDERS", "get_provider"]
