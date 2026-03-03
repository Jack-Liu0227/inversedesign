import argparse
import re
import time
from typing import Callable, Iterable, Optional


class KeyRotator:
    def __init__(self, keys: Iterable[str]) -> None:
        self.keys = [k.strip() for k in keys if k and k.strip()]
        self.index = 0

    def get_next_key(self) -> Optional[str]:
        if not self.keys:
            return None
        key = self.keys[self.index]
        self.index = (self.index + 1) % len(self.keys)
        return key

    def __len__(self) -> int:
        return len(self.keys)


def _compute_max_retries(num_keys: int) -> int:
    if num_keys <= 10:
        return max(num_keys * 2, 6)
    return 6


def smart_poll(
    call_fn: Callable[[Optional[str]], str],
    keys: Iterable[str],
    max_retries: Optional[int] = None,
    base_delay: float = 2.0,
) -> str:
    rotator = KeyRotator(keys)
    num_keys = len(rotator)
    max_retries = max_retries or _compute_max_retries(num_keys)
    used_keys = set()
    last_error = None

    for attempt in range(max_retries):
        api_key = rotator.get_next_key()
        retry_count = 0
        max_attempts = max(num_keys, 1)
        while api_key in used_keys and retry_count < max_attempts:
            api_key = rotator.get_next_key()
            retry_count += 1
        if api_key:
            used_keys.add(api_key)

        try:
            return call_fn(api_key)
        except Exception as exc:
            last_error = exc
            error_str = str(exc)
            error_type = type(exc).__name__

            is_rate_limit = (
                "RateLimitError" in error_type
                or "429" in error_str
                or "rate limit" in error_str.lower()
                or "quota" in error_str.lower()
            )
            suggested_delay = None
            if is_rate_limit:
                match = re.search(r"retry in (\d+(?:\.\d+)?)s", error_str, re.IGNORECASE)
                if match:
                    suggested_delay = float(match.group(1))

            is_retryable = (
                is_rate_limit
                or "InternalServerError" in error_str
                or "Internal server error" in error_str
                or "500" in error_str
                or "http_error" in error_str
                or "timeout" in error_str.lower()
                or "connection" in error_str.lower()
            )

            if is_retryable and attempt < max_retries - 1:
                if suggested_delay and is_rate_limit:
                    wait_time = min(suggested_delay, 30)
                else:
                    wait_time = min(base_delay * (2 ** attempt), 16)
                time.sleep(wait_time)
                continue
            break

    raise RuntimeError(f"smart_poll failed after {max_retries} attempts: {last_error}")


class DemoCaller:
    def __init__(self, fail_times: int) -> None:
        self.remaining = fail_times

    def __call__(self, api_key: Optional[str]) -> str:
        if self.remaining > 0:
            self.remaining -= 1
            raise Exception("RateLimitError: retry in 1s")
        key_preview = (api_key[:6] + "...") if api_key else "no-key"
        return f"ok using {key_preview}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal smart polling demo.")
    parser.add_argument("--keys", default="k1,k2,k3", help="comma separated api keys")
    parser.add_argument("--fail-times", type=int, default=2, help="simulate failures")
    args = parser.parse_args()

    keys = [k.strip() for k in args.keys.split(",") if k.strip()]
    caller = DemoCaller(args.fail_times)
    result = smart_poll(caller, keys)
    print(result)


if __name__ == "__main__":
    main()
