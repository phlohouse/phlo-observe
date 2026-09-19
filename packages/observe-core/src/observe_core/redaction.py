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
        "credentials",
    }
)
"""Default case-insensitive secret key *patterns* (spec §16).

A key matches when a pattern occurs in the key at a segment boundary —
``_``, ``-``, ``.``, whitespace, or a lower/UPPER camel split — so real-world
names like ``access_token``, ``refresh-token``, ``secretKey``,
``x.api.key`` and ``aws_access_key_id`` are all covered without raw substring
matching (which would redact e.g. ``monkey`` or ``tokenizer``).
"""

_SEGMENT_SPLIT = re.compile(r"[_\-\.\s/]+|(?<=[a-z0-9])(?=[A-Z])")


def _key_segments(key: str) -> list[str]:
    """Split a key into lowercase segments on ``_ - . /`` and camel bounds."""
    return [s.lower() for s in _SEGMENT_SPLIT.split(key) if s]


def _segments_match(pattern: tuple[str, ...], segments: list[str]) -> bool:
    """True when ``pattern`` occurs as a consecutive run within ``segments``."""
    n = len(pattern)
    return any(segments[i : i + n] == list(pattern) for i in range(len(segments) - n + 1))


_DEFAULT_KEY_PATTERNS = [tuple(_key_segments(p)) for p in DEFAULT_SECRET_KEYS]
_SINGLE_SEGMENT_DEFAULTS = frozenset(p[0] for p in _DEFAULT_KEY_PATTERNS if len(p) == 1)
_MULTI_SEGMENT_DEFAULTS = [p for p in _DEFAULT_KEY_PATTERNS if len(p) > 1]


def _is_secret_key(key: str) -> bool:
    """Segment-aware check against the default secret key patterns."""
    lowered = key.lower()
    if lowered in DEFAULT_SECRET_KEYS:
        return True
    segments = _key_segments(key)
    seg_set = frozenset(segments)
    if seg_set & _SINGLE_SEGMENT_DEFAULTS:
        return True
    return any(p[0] in seg_set and _segments_match(p, segments) for p in _MULTI_SEGMENT_DEFAULTS)


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
        self._default_patterns = _DEFAULT_KEY_PATTERNS
        self._key_res = [re.compile(p, re.IGNORECASE) for p in (key_patterns or [])]
        self._paths = [tuple(p.split(".")) for p in (path_rules or [])]
        self._value_res = list(DEFAULT_VALUE_PATTERNS) + [
            re.compile(p) for p in (value_patterns or [])
        ]
        self._max_depth = max_depth
        # Verdict cache for key name / pattern / regex checks (everything that
        # does not depend on ``path``). Bounded against unbounded key sets.
        self._key_cache: dict[str, bool] = {}

    def _key_hit(
        self, key: str, path: tuple[str, ...], extra_paths: tuple[tuple[str, ...], ...] = ()
    ) -> bool:
        hit = self._key_cache.get(key)
        if hit is None:
            hit = self._key_hit_uncached(key)
            if len(self._key_cache) < 4096:
                self._key_cache[key] = hit
        if hit:
            return True
        paths = self._paths + list(extra_paths)
        if not paths:
            return False
        dotted = ".".join((*path, key))
        return any(
            dotted == ".".join(rule) or dotted.endswith("." + ".".join(rule)) for rule in paths
        )

    def _key_hit_uncached(self, key: str) -> bool:
        lowered = key.lower()
        if lowered in self._keys:
            return True
        segments = _key_segments(key)
        seg_set = frozenset(segments)
        if seg_set & _SINGLE_SEGMENT_DEFAULTS:
            return True
        if any(p[0] in seg_set and _segments_match(p, segments) for p in _MULTI_SEGMENT_DEFAULTS):
            return True
        return any(rx.search(key) for rx in self._key_res)

    def redact_event(
        self, data: dict[str, Any], *, extra_paths: tuple[tuple[str, ...], ...] = ()
    ) -> dict[str, Any]:
        """Redact a canonical event dict in place and return it.

        ``extra_paths`` adds dotted-path rules for this call only — used for
        contract-declared sensitive fields (spec §7.2/§34).
        """
        if not self.enabled:
            return data
        self._redact(data, path=(), depth=0, extra_paths=extra_paths)
        return data

    def redact_value(
        self,
        value: Any,
        *,
        path: tuple[str, ...] = (),
        extra_paths: tuple[tuple[str, ...], ...] = (),
    ) -> Any:
        """Redact one normalized value with a fresh traversal depth budget.

        Runtime normalization happens before values are placed in the
        envelope.  Redacting those values at their eventual envelope depth
        would spend part of the bounded traversal budget on the envelope
        fields themselves, allowing secrets near the normalization limit to
        survive.  Direct callers of :meth:`redact_event` retain the original
        envelope-wide depth bound.
        """
        if self.enabled:
            self._redact(value, path=path, depth=0, extra_paths=extra_paths)
        return value

    def _redact(
        self,
        node: Any,
        *,
        path: tuple[str, ...],
        depth: int,
        extra_paths: tuple[tuple[str, ...], ...] = (),
    ) -> None:
        if depth > self._max_depth:
            return
        if isinstance(node, dict):
            for key, value in node.items():
                key_str = str(key)
                if self._key_hit(key_str, path, extra_paths) or (
                    isinstance(value, str) and self._value_hit(value)
                ):
                    node[key] = REDACTED
                else:
                    self._redact(
                        value, path=(*path, key_str), depth=depth + 1, extra_paths=extra_paths
                    )
        elif isinstance(node, list):
            for i, item in enumerate(node):
                if isinstance(item, str) and self._value_hit(item):
                    node[i] = REDACTED
                else:
                    self._redact(item, path=path, depth=depth + 1, extra_paths=extra_paths)

    def _value_hit(self, value: str) -> bool:
        return any(rx.search(value) for rx in self._value_res)


def sanitize_url(url: str) -> str:
    """Strip credentials and secret query parameters from a URL.

    Applies to database, HTTP, S3 and similar connection strings.
    """
    try:
        parts = urlsplit(url)
        # ``.port`` raises ValueError on a malformed port (e.g. ``h:abc``).
        hostname, port = parts.hostname or "", parts.port
    except ValueError:
        return REDACTED
    netloc = hostname
    if port:
        netloc = f"{netloc}:{port}"
    if parts.username:
        netloc = f"{parts.username}:***@{netloc}"
    query = urlencode(
        [
            (k, REDACTED if _is_secret_key(k) else v)
            for k, v in parse_qsl(parts.query, keep_blank_values=True)
        ]
    )
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))
