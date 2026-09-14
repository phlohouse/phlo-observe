"""Context propagation built on :mod:`contextvars`.

Ambient context binds correlation identifiers (``run_id``, ``trace_id``, ...)
and service fields that automatically enrich every event emitted inside the
``with`` block or, for asyncio, inside the same task tree.

Precedence for each field, highest first:

1. explicit values set on the event (``evt.set_correlation(...)``)
2. the active operation context (the enclosing ``observe()`` block)
3. bound ambient context (``bind_context``)
4. configured service defaults

Thread-pool note: ``contextvars`` do not cross ``threading.Thread`` or
``concurrent.futures`` boundaries automatically. Capture
``contextvars.copy_context()`` inside the bound block and submit
``ctx.run(fn, ...)`` to propagate; see :func:`propagate` for a helper.
"""

from __future__ import annotations

import contextvars
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from observe_core.models import CORRELATION_KEYS

_ambient: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "observe_ambient", default=None
)
_operation: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "observe_operation", default=None
)

_SERVICE_KEYS = frozenset({"service_name", "service_version", "environment", "host", "instance_id"})


class BoundContext:
    """Token-based ambient binding for frameworks that cannot use ``with``.

    Call :meth:`reset` exactly once when the logical scope ends.
    """

    __slots__ = ("_token", "_used")

    def __init__(self, token: contextvars.Token[dict[str, Any] | None]) -> None:
        self._token = token
        self._used = False

    def reset(self) -> None:
        """Restore the context that existed before binding."""
        if not self._used:
            _ambient.reset(self._token)
            self._used = True

    def __enter__(self) -> BoundContext:
        """Return self so ``with bind_context_token(...)`` also works."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Reset the binding."""
        self.reset()


def bind_context_token(**values: Any) -> BoundContext:
    """Bind ambient values without a context manager. Returns a :class:`BoundContext`."""
    merged = {**(_ambient.get() or {}), **{k: v for k, v in values.items() if v is not None}}
    return BoundContext(_ambient.set(merged))


@contextmanager
def bind_context(**values: Any) -> Iterator[None]:
    """Bind ambient context values for the duration of the block.

    Example::

        with bind_context(run_id=run_id, service_name="phlo-dagster"):
            ...
    """
    bound = bind_context_token(**values)
    try:
        yield
    finally:
        bound.reset()


def get_context() -> dict[str, Any]:
    """Return a copy of the currently bound ambient context."""
    return dict(_ambient.get() or {})


def clear_context() -> None:
    """Remove all bound ambient values in this context."""
    _ambient.set(None)


def propagate() -> contextvars.Context:
    """Capture the current context for use in another thread.

    Usage::

        ctx = propagate()
        executor.submit(ctx.run, fn, *args)
    """
    return contextvars.copy_context()


def _operation_context() -> dict[str, Any] | None:
    return _operation.get()


def _push_operation(ctx: dict[str, Any]) -> contextvars.Token[dict[str, Any] | None]:
    return _operation.set(ctx)


def _pop_operation(token: contextvars.Token[dict[str, Any] | None]) -> None:
    _operation.reset(token)


def ambient_correlation() -> dict[str, str]:
    """Canonical correlation keys currently bound in ambient context."""
    return {
        k: str(v)
        for k, v in (_ambient.get() or {}).items()
        if k in CORRELATION_KEYS and v is not None
    }


def ambient_service() -> dict[str, str]:
    """Service fields currently bound in ambient context."""
    return {
        k: str(v) for k, v in (_ambient.get() or {}).items() if k in _SERVICE_KEYS and v is not None
    }


def ambient_extra() -> dict[str, Any]:
    """Non-canonical bound values, destined for ``correlation.extra``."""
    return {
        k: v
        for k, v in (_ambient.get() or {}).items()
        if k not in CORRELATION_KEYS and k not in _SERVICE_KEYS
    }
