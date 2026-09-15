"""HTTP drain: batch POST canonical events to phlo-observer.

- keepalive via a persistent ``httpx.Client``
- connect/read timeouts, bounded retries with exponential backoff + jitter
- retries only retryable status classes (network errors, 408, 429, 5xx)
- honors ``Retry-After`` when the value is sane
- gzip body above a configurable threshold
- splits batches on HTTP 413
- optional bearer token
"""

from __future__ import annotations

import gzip
import random
import threading
import time
from collections.abc import Sequence

import httpx

from observe_core.drains.base import CanonicalEvent, DrainFailure, PermanentDrainFailure

_RETRYABLE_STATUSES = frozenset({408, 429}) | frozenset(range(500, 600))
_MAX_RETRY_AFTER_S = 30.0


class HttpDrain:
    """POST canonical event batches to a phlo-observer ingestion endpoint."""

    name = "http"
    is_remote = True

    def __init__(
        self,
        endpoint: str,
        *,
        token: str | None = None,
        api_key: str | None = None,
        api_key_header: str = "X-API-Key",
        headers: dict[str, str] | None = None,
        connect_timeout_s: float = 5.0,
        read_timeout_s: float = 10.0,
        max_attempts: int = 5,
        backoff_base_ms: float = 250.0,
        backoff_cap_ms: float = 10_000.0,
        gzip_threshold_bytes: int = 64 * 1024,
        spool_on_failure: bool = True,
    ) -> None:
        self.endpoint = endpoint
        self.spool_on_failure = spool_on_failure
        self.max_attempts = max(1, max_attempts)
        self.backoff_base_s = backoff_base_ms / 1000.0
        self.backoff_cap_s = backoff_cap_ms / 1000.0
        self.gzip_threshold = gzip_threshold_bytes
        self._client = httpx.Client(
            timeout=httpx.Timeout(read_timeout_s, connect=connect_timeout_s),
            headers=self._headers(token, api_key, api_key_header, headers),
        )
        self._lock = threading.Lock()

    @staticmethod
    def _headers(
        token: str | None,
        api_key: str | None,
        api_key_header: str,
        extra: dict[str, str] | None,
    ) -> dict[str, str]:
        headers = {"Content-Type": "application/json", "User-Agent": "observe-core/1"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if api_key:
            headers[api_key_header] = api_key
        if extra:
            headers.update(extra)
        return headers

    def emit_batch(self, events: Sequence[CanonicalEvent]) -> None:
        """POST the batch; split on 413; raise DrainFailure after retries."""
        self._send_payloads([item.payload for item in events])

    def emit_raw(self, payloads: Sequence[bytes]) -> None:
        """POST pre-serialized payloads (spool replay)."""
        self._send_payloads(list(payloads))

    def _send_payloads(self, payloads: list[bytes]) -> None:
        body = b"[" + b",".join(payloads) + b"]"
        headers: dict[str, str] = {}
        if len(body) >= self.gzip_threshold:
            body = gzip.compress(body)
            headers["Content-Encoding"] = "gzip"

        last_error: Exception | None = None
        for attempt in range(self.max_attempts):
            try:
                with self._lock:
                    response = self._client.post(self.endpoint, content=body, headers=headers)
            except httpx.HTTPError as exc:
                last_error = exc
                self._sleep(attempt, retry_after=None)
                continue
            if response.status_code == 413 and len(payloads) > 1:
                mid = len(payloads) // 2
                self._send_payloads(payloads[:mid])
                self._send_payloads(payloads[mid:])
                return
            if response.status_code in _RETRYABLE_STATUSES:
                last_error = DrainFailure(
                    f"observer returned retryable status {response.status_code}"
                )
                self._sleep(attempt, retry_after=_retry_after(response))
                continue
            if response.status_code >= 400:
                # Non-retryable 4xx: the payload itself was rejected; replaying
                # it later would fail identically, so mark it permanent.
                raise PermanentDrainFailure(
                    f"observer rejected batch: {response.status_code} {response.text[:200]}"
                )
            return
        raise DrainFailure(f"observer unreachable after {self.max_attempts} attempts: {last_error}")

    def _sleep(self, attempt: int, retry_after: float | None) -> None:
        if attempt + 1 >= self.max_attempts:
            return
        if retry_after is not None:
            delay = retry_after
        else:
            delay = min(self.backoff_base_s * (2**attempt), self.backoff_cap_s)
            delay = delay * (0.5 + random.random())  # noqa: S311 - jitter, not crypto
        time.sleep(min(delay, self.backoff_cap_s))

    def flush(self) -> None:
        """No-op: requests are sent synchronously per batch."""

    def close(self) -> None:
        """Close the HTTP client and release keepalive connections."""
        with self._lock:
            self._client.close()


def _retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        seconds = float(raw)
    except ValueError:
        return None
    if 0 <= seconds <= _MAX_RETRY_AFTER_S:
        return seconds
    return None
