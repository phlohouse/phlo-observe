"""Secret redaction.

Runs after normalization and before any drain sees an event, so secrets never
reach console output, files, HTTP requests or spool segments.

Three independent mechanisms:

- **key rules** — exact names, regexes, and dotted-path rules
- **value patterns** — regexes that redact a string value no matter the key
- **URL sanitization** — :func:`sanitize_url` strips credentials from URLs
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

REDACTED = "[REDACTED]"

DEFAULT_SECRET_KEYS = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "authorization",
        "cookie",
        "set-cookie",
        "client_secret",
        "access_key",
        "private_key",
    }
)
"""Default case-insensitive key names whose values are always redacted."""

DEFAULT_VALUE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"Bearer\s+[A-Za-z0-9\-._~+/]{8,}={0,2}", re.IGNORECASE),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}"),
)
"""Regexes redacting whole string values: bearer tokens, PEM keys, AWS keys, JWTs."""


class Redactor:
    """Recursive redactor over normalized event dictionaries."""

    def __init__(
        self,
        *,
        extra_keys: list[str] | None = None,
        key_patterns: list[str] | None = None,
        path_rules: list[str] | None = None,
        value_patterns: list[str] | None = None,
        max_depth: int = 8,
        enabled: bool = True,
    ) -> None:
        self.enabled = enabled
        self._keys = DEFAULT_SECRET_KEYS | {k.lower() for k in (extra_keys or [])}
        self._key_res = [re.compile(p, re.IGNORECASE) for p in (key_patterns or [])]
        self._paths = [tuple(p.split(".")) for p in (path_rules or [])]
        self._value_res = list(DEFAULT_VALUE_PATTERNS) + [
            re.compile(p) for p in (value_patterns or [])
        ]
        self._max_depth = max_depth

    def redact_event(self, data: dict[str, Any]) -> dict[str, Any]:
        """Redact a canonical event dict in place and return it."""
        if not self.enabled:
            return data
        self._redact(data, path=(), depth=0)
        return data

    def _key_hit(self, key: str, path: tuple[str, ...]) -> bool:
        lowered = key.lower()
        if lowered in self._keys:
            return True
        if any(rx.search(key) for rx in self._key_res):
            return True
        dotted = ".".join((*path, key))
        return any(
            dotted == ".".join(rule) or dotted.endswith("." + ".".join(rule))
            for rule in self._paths
        )

    def _redact(self, node: Any, *, path: tuple[str, ...], depth: int) -> None:
        if depth > self._max_depth:
            return
        if isinstance(node, dict):
            for key, value in node.items():
                key_str = str(key)
                if self._key_hit(key_str, path) or (
                    isinstance(value, str) and self._value_hit(value)
                ):
                    node[key] = REDACTED
                else:
                    self._redact(value, path=(*path, key_str), depth=depth + 1)
        elif isinstance(node, list):
            for i, item in enumerate(node):
                if isinstance(item, str) and self._value_hit(item):
                    node[i] = REDACTED
                else:
                    self._redact(item, path=path, depth=depth + 1)

    def _value_hit(self, value: str) -> bool:
        return any(rx.search(value) for rx in self._value_res)


def sanitize_url(url: str) -> str:
    """Strip credentials and secret query parameters from a URL.

    Applies to database, HTTP, S3 and similar connection strings.
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        return REDACTED
    netloc = parts.hostname or ""
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    if parts.username:
        netloc = f"{parts.username}:***@{netloc}"
    query = urlencode(
        [
            (k, REDACTED if k.lower() in DEFAULT_SECRET_KEYS else v)
            for k, v in parse_qsl(parts.query, keep_blank_values=True)
        ]
    )
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))
