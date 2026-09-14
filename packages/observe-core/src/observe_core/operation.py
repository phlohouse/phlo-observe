"""The ``observe()`` operation API.

Each ``with observe(...)`` / ``async with observe(...)`` block is one operation
and emits exactly one completion event when the block exits — normally or by
exception. Nested operations each emit their own event and are linked to their
parent through ``parent_span_id``/``trace_id``; correlation fields set on a
parent operation are inherited by nested ones.

The same object works as a decorator for sync and async functions.
"""

from __future__ import annotations

import contextvars
import functools
import inspect
import secrets
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, overload

from observe_core import context as _ctx
from observe_core.builder import EventBuilder
from observe_core.models import Category, Delivery, Severity
from observe_core.runtime import get_runtime
from observe_core.timestamps import monotonic_ms, utcnow

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

_P = ParamSpec("_P")
_R = TypeVar("_R")


def _new_span_id() -> str:
    return secrets.token_hex(8)


def _new_trace_id() -> str:
    return secrets.token_hex(16)


class observe:
    """Context manager, async context manager and decorator for operations.

    Usage::

        with observe("asset.materialize", category="data") as evt:
            evt.set(rows_out=len(frame))

        @observe("transform.execute")
        def transform(...): ...

        async with observe("query.execute") as evt: ...
    """

    def __init__(
        self,
        name: str,
        *,
        category: Category | str | None = None,
        delivery: Delivery | str | None = None,
        severity: Severity | str | None = None,
        attributes: dict[str, Any] | None = None,
        correlation: dict[str, Any] | None = None,
        capture_stacktrace: bool | None = None,
    ) -> None:
        self.name = name
        self.category = Category(category) if category else Category.OTHER
        self.delivery = Delivery(delivery) if delivery else Delivery.TELEMETRY
        self.severity = Severity(severity) if severity else None
        self.attributes = attributes
        self.correlation = correlation
        self.capture_stacktrace = capture_stacktrace
        self._builder: EventBuilder | None = None
        self._token: contextvars.Token[dict[str, Any] | None] | None = None
        self._monotonic_start = 0.0

    # -- enter/exit ---------------------------------------------------------

    def _enter(self) -> EventBuilder:
        runtime = get_runtime()
        builder = EventBuilder(
            self.name,
            category=self.category,
            delivery=self.delivery,
            severity=self.severity,
            attributes=self.attributes,
        )
        if self.correlation:
            builder.set_correlation(**self.correlation)

        parent = _ctx._operation_context()
        merged: dict[str, Any] = {**_ctx.ambient_correlation()}
        if parent is not None:
            merged.update(parent.get("correlation", {}))
        merged.update(builder.correlation)

        op_ctx = {
            "correlation": merged,
            "span_id": _new_span_id(),
            "trace_id": merged.get("trace_id") or _new_trace_id(),
            "parent_span_id": (parent or {}).get("span_id"),
        }
        merged["trace_id"] = op_ctx["trace_id"]
        merged["span_id"] = op_ctx["span_id"]
        merged["parent_span_id"] = op_ctx["parent_span_id"]
        builder.correlation = merged
        builder.started_at = utcnow()
        self._monotonic_start = monotonic_ms()
        self._token = _ctx._push_operation(op_ctx)
        self._builder = builder
        _ = runtime  # ensures runtime exists even if the block never exits
        return builder

    def _exit(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> bool:
        builder = self._builder
        if self._token is not None:
            _ctx._pop_operation(self._token)
            self._token = None
        if builder is None:
            return False
        builder.ended_at = utcnow()
        builder.duration_ms = monotonic_ms() - self._monotonic_start
        get_runtime().emit(builder, exc)
        return False  # never swallow application exceptions

    # -- sync context manager ------------------------------------------------

    def __enter__(self) -> EventBuilder:
        """Enter the operation."""
        return self._enter()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> bool:
        """Finalize and emit the completion event; re-raise exceptions."""
        return self._exit(exc_type, exc, tb)

    # -- async context manager ------------------------------------------------

    async def __aenter__(self) -> EventBuilder:
        """Enter the operation (async)."""
        return self._enter()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> bool:
        """Finalize and emit the completion event; re-raise exceptions."""
        return self._exit(exc_type, exc, tb)

    # -- decorator --------------------------------------------------------------

    @overload
    def __call__(
        self, fn: Callable[_P, Coroutine[Any, Any, _R]]
    ) -> Callable[_P, Coroutine[Any, Any, _R]]: ...

    @overload
    def __call__(self, fn: Callable[_P, _R]) -> Callable[_P, _R]: ...

    def __call__(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        """Decorate a sync or async function, preserving its metadata."""
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                async with self._fresh():
                    return await fn(*args, **kwargs)

            return async_wrapper

        @functools.wraps(fn)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            with self._fresh():
                return fn(*args, **kwargs)

        return sync_wrapper

    def _fresh(self) -> observe:
        """A new operation instance with the same parameters (for decoration)."""
        return observe(
            self.name,
            category=self.category,
            delivery=self.delivery,
            severity=self.severity,
            attributes=self.attributes,
            correlation=self.correlation,
            capture_stacktrace=self.capture_stacktrace,
        )
