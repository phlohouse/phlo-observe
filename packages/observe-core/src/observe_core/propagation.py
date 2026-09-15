"""Context propagation across process boundaries (spec §7.4).

V1 binds context via ``contextvars``, which cannot cross a subprocess, a
``multiprocessing`` fork worker, a task-queue hop, or a dbt shell-out. V2 adds
a compact *context envelope* that can be injected into a child process's
environment and re-bound on the other side::

    # parent
    env = propagation.child_env()          # os.environ + OBSERVE_CONTEXT=...
    subprocess.run(cmd, env=env)

    # child (any process importing observe_core)
    propagation.bind_from_env()            # reads OBSERVE_CONTEXT, binds it

The envelope deliberately carries correlation identifiers only — never
secrets, never large payloads. ``OBSERVE_CONTEXT`` is a base64url JSON object:

.. code-block:: json

    {"trace_id": "...", "run_id": "...", "asset_key": "...", "branch": "..."}

``--observe-context <value>`` offers the same handoff for CLIs that scrub the
environment but pass arguments through.
"""

from __future__ import annotations

import base64
import json
import os
from typing import Any

import orjson

from observe_core.context import get_context
from observe_core.models import CORRELATION_KEYS

CONTEXT_ENV_VAR = "OBSERVE_CONTEXT"
"""Environment variable carrying the serialized context envelope."""

CONTEXT_ARG = "--observe-context"
"""Command-line flag carrying the serialized context envelope."""

_PROPAGATED_SERVICE_KEYS = ("service_name", "service_version", "environment")
"""Service fields safe to inherit into a child process's ambient context."""


def encode_context(context: dict[str, Any] | None = None) -> str:
    """Serialize a context dict (default: current ambient context) to a token.

    Only canonical correlation keys plus service identity are propagated;
    ``correlation.extra`` and arbitrary ambient values are not — envelopes are
    a correlation handoff, not a data channel.
    """
    source = context if context is not None else get_context()
    envelope = {
        key: str(value)
        for key, value in source.items()
        if value is not None and (key in CORRELATION_KEYS or key in _PROPAGATED_SERVICE_KEYS)
    }
    return base64.urlsafe_b64encode(orjson.dumps(envelope)).decode()


def decode_context(token: str) -> dict[str, str]:
    """Parse a context token back into a dict; malformed input yields ``{}``."""
    try:
        raw = base64.urlsafe_b64decode(token.encode())
        data = json.loads(raw)
    except (ValueError, orjson.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {
        key: str(value)
        for key, value in data.items()
        if value is not None and (key in CORRELATION_KEYS or key in _PROPAGATED_SERVICE_KEYS)
    }


def child_env(env: dict[str, str] | None = None) -> dict[str, str]:
    """Return an environment for a child process carrying the current context.

    ``env`` defaults to ``os.environ``; the returned dict is a copy with
    ``OBSERVE_CONTEXT`` set — or removed when no context is bound, so a child
    never inherits a stale envelope.
    """
    merged = dict(os.environ if env is None else env)
    token = encode_context()
    if token and decode_context(token):
        merged[CONTEXT_ENV_VAR] = token
    else:
        merged.pop(CONTEXT_ENV_VAR, None)
    return merged


def bind_context_token(token: str) -> None:
    """Bind a decoded context token into the ambient context."""
    context = decode_context(token)
    if context:
        _bind_permanent(context)


def _bind_permanent(context: dict[str, str]) -> None:
    # Deliberately never reset: the envelope describes the whole process.
    from observe_core.context import bind_context_token as _bind  # noqa: PLC0415

    _bind(**context)


def bind_from_env(env: dict[str, str] | None = None) -> bool:
    """Bind context from ``OBSERVE_CONTEXT`` in ``env`` (default os.environ).

    Returns True when a context was found and bound. Intended to be called
    once near process start — in a CLI entry point, an ``__main__`` guard, or
    a worker bootstrap.
    """
    source = os.environ if env is None else env
    token = source.get(CONTEXT_ENV_VAR)
    if not token:
        return False
    context = decode_context(token)
    if not context:
        return False
    _bind_permanent(context)
    return True


def bind_from_argv(argv: list[str]) -> bool:
    """Bind context from a ``--observe-context <token>`` argument, if present.

    Lets a controlled child process receive context through its CLI even when
    the environment is scrubbed (e.g. ``env -i`` launches).
    """
    try:
        index = argv.index(CONTEXT_ARG)
    except ValueError:
        return False
    if index + 1 >= len(argv):
        return False
    context = decode_context(argv[index + 1])
    if not context:
        return False
    _bind_permanent(context)
    return True


def context_arg(argv: list[str] | None = None) -> str | None:
    """Build a ``--observe-context <token>`` argument for a child command.

    Returns None when no context is bound, so callers can append
    unconditionally::

        extra = context_arg()
        cmd = ["dbt", "run"] + ([CONTEXT_ARG, extra] if extra else [])
    """
    token = encode_context()
    return token if decode_context(token) else None
