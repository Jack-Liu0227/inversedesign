from __future__ import annotations

from typing import Dict, Optional
import logging
import json
import re
import time
import os
import random
import threading
from urllib import error as urlerror
from urllib import request as urlrequest

try:
    import litellm
    _HAS_LITELLM = True
except Exception:
    _HAS_LITELLM = False


class ModelCaller:
    def __init__(
        self,
        model_name: str,
        temperature: float = 0.0,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        max_retries: int = 2,
        allow_mock_on_failure: bool = False,
    ) -> None:
        self.model_name = model_name
        self.temperature = temperature
        self.api_key = api_key
        self.base_url = base_url
        self.max_retries = max_retries
        self.allow_mock_on_failure = allow_mock_on_failure
        self.max_concurrent = int(os.getenv("LLM_MAX_CONCURRENT", "0"))
        self.min_interval = float(os.getenv("LLM_MIN_INTERVAL", "0"))
        self.failure_threshold = int(os.getenv("LLM_FAILURE_THRESHOLD", "0"))
        self.cooldown_seconds = float(os.getenv("LLM_COOLDOWN_SECONDS", "30"))
        self.backoff_base = float(os.getenv("LLM_BACKOFF_BASE", "1"))
        self.backoff_max = float(os.getenv("LLM_BACKOFF_MAX", "20"))
        self._semaphore = (
            threading.Semaphore(self.max_concurrent) if self.max_concurrent > 0 else None
        )
        self._rate_lock = threading.Lock()
        self._last_call_time = 0.0
        self._key_lock = threading.Lock()
        self._key_index = 0
        self._breaker_lock = threading.Lock()
        self._consecutive_failures = 0
        self._circuit_open_until = 0.0

    def call(self, prompt: str, fallback_predictions: Dict[str, float]) -> str:
        is_ollama_model = self.model_name.startswith("ollama/")
        if not is_ollama_model and (not _HAS_LITELLM or not self.api_key):
            if self.allow_mock_on_failure:
                return self._mock_response(fallback_predictions)
            raise RuntimeError("LLM call unavailable: missing litellm or API key.")

        api_keys = self._expand_api_keys(self.api_key)
        base_urls = self._expand_base_urls(self.base_url)
        if is_ollama_model and (not base_urls or base_urls == [None]):
            if self.allow_mock_on_failure:
                return self._mock_response(
                    fallback_predictions, error=RuntimeError("Missing Ollama base_url.")
                )
            raise RuntimeError(
                "LLM call unavailable: missing Ollama base_url. "
                "Set OLLAMA_BASE_URLS or LLM_BASE_URLS."
            )
        logger = logging.getLogger(__name__)
        last_error: Optional[Exception] = None
        if self._semaphore:
            self._semaphore.acquire()
        try:
            self._ensure_circuit_open()
            ordered_keys = self._rotate_keys(api_keys) if api_keys else [""]
            for key_idx, api_key in enumerate(ordered_keys, start=1):
                for base_url in base_urls:
                    for attempt in range(self.max_retries):
                        try:
                            self._wait_rate_limit()
                            logger.info(
                                "LLM call start model=%s base_url=%s key=%s attempt=%s",
                                self.model_name,
                                base_url,
                                f"{key_idx}/{len(ordered_keys)}",
                                attempt + 1,
                            )
                            if is_ollama_model:
                                content = self._call_ollama_direct(
                                    base_url=base_url,
                                    prompt=prompt,
                                )
                            else:
                                response = litellm.completion(
                                    model=self.model_name,
                                    messages=[{"role": "user", "content": prompt}],
                                    temperature=self.temperature,
                                    api_key=api_key,
                                    base_url=base_url,
                                )
                                content = response.choices[0].message.content
                            logger.info(
                                "LLM call success model=%s base_url=%s key=%s",
                                self.model_name,
                                base_url,
                                f"{key_idx}/{len(ordered_keys)}",
                            )
                            self._reset_breaker()
                            return content
                        except Exception as exc:
                            last_error = exc
                            self._record_failure()
                            is_retryable, is_rate_limit, suggested_delay = (
                                self._classify_error(exc)
                            )
                            logger.warning(
                                "LLM call failed model=%s base_url=%s key=%s attempt=%s "
                                "error_type=%s error=%s error_repr=%r",
                                self.model_name,
                                base_url,
                                f"{key_idx}/{len(ordered_keys)}",
                                attempt + 1,
                                type(exc).__name__,
                                str(exc),
                                exc,
                            )
                            if is_retryable and attempt < self.max_retries - 1:
                                wait_time = self._retry_wait_seconds(
                                    attempt, is_rate_limit, suggested_delay
                                )
                                time.sleep(wait_time)
                                continue
                            break
        finally:
            if self._semaphore:
                self._semaphore.release()

        if self.allow_mock_on_failure:
            return self._mock_response(fallback_predictions, error=last_error)
        raise RuntimeError(
            f"LLM call failed after retries: {self._format_error(last_error)}"
        ) from last_error

    @staticmethod
    def _expand_base_urls(base_url: Optional[str]) -> list[Optional[str]]:
        if not base_url:
            return [None]
        if "," not in base_url:
            return [base_url.strip()]
        return [url.strip() for url in base_url.split(",") if url.strip()]

    @staticmethod
    def _expand_api_keys(api_key: str) -> list[str]:
        if "," not in api_key:
            return [api_key.strip()]
        return [key.strip() for key in api_key.split(",") if key.strip()]

    def _rotate_keys(self, api_keys: list[str]) -> list[str]:
        if not api_keys:
            return []
        with self._key_lock:
            start = self._key_index % len(api_keys)
            self._key_index = (self._key_index + 1) % len(api_keys)
        return api_keys[start:] + api_keys[:start]

    def _wait_rate_limit(self) -> None:
        if self.min_interval <= 0:
            return
        with self._rate_lock:
            now = time.monotonic()
            wait_time = (self._last_call_time + self.min_interval) - now
            if wait_time > 0:
                time.sleep(wait_time)
            self._last_call_time = time.monotonic()

    def _backoff_seconds(self, attempt: int) -> float:
        base = max(self.backoff_base, 0.1)
        delay = min(self.backoff_max, base * (2 ** attempt))
        jitter = random.uniform(0, delay * 0.2)
        return delay + jitter

    def _retry_wait_seconds(
        self,
        attempt: int,
        is_rate_limit: bool,
        suggested_delay: Optional[float],
    ) -> float:
        if is_rate_limit and suggested_delay:
            return min(suggested_delay, 30)
        return self._backoff_seconds(attempt)

    @staticmethod
    def _classify_error(exc: Exception) -> tuple[bool, bool, Optional[float]]:
        error_str = str(exc)
        error_type = type(exc).__name__
        error_lower = error_str.lower()
        is_rate_limit = (
            "RateLimitError" in error_type
            or "429" in error_str
            or "rate limit" in error_lower
            or "quota" in error_lower
        )
        suggested_delay = None
        if is_rate_limit:
            match = re.search(r"retry in (\d+(?:\.\d+)?)s", error_str, re.IGNORECASE)
            if match:
                suggested_delay = float(match.group(1))
        is_connection_error = (
            "APIConnectionError" in error_type
            or "ConnectionError" in error_type
            or "Timeout" in error_type
            or "connection" in error_lower
            or "network" in error_lower
            or "timeout" in error_lower
        )
        is_retryable = (
            is_rate_limit
            or is_connection_error
            or "InternalServerError" in error_str
            or "Internal server error" in error_str
            or "500" in error_str
            or "http_error" in error_str
        )
        return is_retryable, is_rate_limit, suggested_delay

    def _call_ollama_direct(self, base_url: Optional[str], prompt: str) -> str:
        if not base_url:
            raise RuntimeError("Empty Ollama base_url.")
        normalized_base = base_url.strip().rstrip("/")
        if normalized_base.endswith("/v1"):
            normalized_base = normalized_base[:-3]
        if normalized_base.endswith("/api"):
            normalized_base = normalized_base[:-4]
        if not normalized_base:
            raise RuntimeError(f"Invalid Ollama base_url: {base_url!r}")

        raw_model = self.model_name.split("/", 1)[1] if "/" in self.model_name else self.model_name
        payload = {
            "model": raw_model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "options": {
                "temperature": self.temperature,
            },
        }
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urlrequest.Request(
            url=f"{normalized_base}/api/chat",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        # Force local direct path and avoid accidental proxy interception.
        opener = urlrequest.build_opener(urlrequest.ProxyHandler({}))
        timeout_sec = float(os.getenv("OLLAMA_TIMEOUT_SECONDS", "180"))
        try:
            with opener.open(req, timeout=timeout_sec) as resp:
                body = resp.read().decode("utf-8", errors="replace")
        except urlerror.HTTPError as exc:
            err_body = ""
            try:
                err_body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = ""
            details = f"HTTP {exc.code} {exc.reason}"
            if err_body:
                details = f"{details}: {err_body[:500]}"
            raise RuntimeError(f"Ollama HTTP error at /api/chat: {details}") from exc
        except urlerror.URLError as exc:
            raise RuntimeError(f"Ollama connection error at /api/chat: {exc}") from exc

        try:
            parsed = json.loads(body)
        except Exception as exc:
            raise RuntimeError(f"Ollama returned non-JSON response: {body[:500]}") from exc

        message = parsed.get("message") if isinstance(parsed, dict) else None
        content = message.get("content") if isinstance(message, dict) else None
        if content is None:
            if isinstance(parsed, dict) and parsed.get("error"):
                raise RuntimeError(f"Ollama API error: {parsed.get('error')}")
            raise RuntimeError(f"Ollama response missing message.content: {parsed}")
        return str(content)

    @staticmethod
    def _format_error(exc: Optional[Exception]) -> str:
        if exc is None:
            return "<no exception captured>"
        error_str = str(exc).strip()
        if error_str:
            return error_str
        return f"{type(exc).__name__}: {repr(exc)}"

    def _ensure_circuit_open(self) -> None:
        if self.failure_threshold <= 0:
            return
        with self._breaker_lock:
            now = time.monotonic()
            if self._circuit_open_until > now:
                raise RuntimeError("Circuit breaker open; skipping LLM call.")

    def _record_failure(self) -> None:
        if self.failure_threshold <= 0:
            return
        with self._breaker_lock:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.failure_threshold:
                self._circuit_open_until = time.monotonic() + self.cooldown_seconds
                self._consecutive_failures = 0

    def _reset_breaker(self) -> None:
        if self.failure_threshold <= 0:
            return
        with self._breaker_lock:
            self._consecutive_failures = 0
            self._circuit_open_until = 0.0

    @staticmethod
    def _mock_response(
        predictions: Dict[str, float], error: Optional[Exception] = None
    ) -> str:
        payload = {
            "predictions": {
                key: {"value": float(value), "unit": ""} for key, value in predictions.items()
            },
            "confidence": "low",
            "reasoning": "Mock response used because LLM call was unavailable.",
        }
        if error:
            payload["reasoning"] = f"Mock response used because LLM call failed: {error}"
        return json.dumps(payload, indent=2, ensure_ascii=True)
