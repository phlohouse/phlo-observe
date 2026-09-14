"""Structured errors.

``ObservedError`` is the application-facing failure type: it carries a
machine-readable ``code``, a human ``message``, a concrete ``why`` and an
optional ``fix``. It never serializes exception locals or arbitrary object
graphs.
"""

from __future__ import annotations

import traceback
from typing import Any

from observe_core.models import ErrorInfo


class ObservedError(Exception):
    """A structured application error intended for telemetry.

    All fields are accessible programmatically and serialize predictably.
    ``cause`` chaining works through normal ``raise ... from ...``; the cause
    is recorded as type+message only, never as a serialized object graph.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        why: str | None = None,
        fix: str | None = None,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.why = why
        self.fix = fix
        self.retryable = retryable
        self.details = dict(details) if details else {}

    def __str__(self) -> str:
        """Return the human-readable message."""
        return self.message

    def to_error_info(self, *, include_traceback: bool = False) -> ErrorInfo:
        """Convert to the canonical :class:`ErrorInfo` shape."""
        details = dict(self.details)
        if self.__cause__ is not None:
            details.setdefault(
                "cause",
                {
                    "exception_type": type(self.__cause__).__qualname__,
                    "message": str(self.__cause__),
                },
            )
        return ErrorInfo(
            code=self.code,
            message=self.message,
            why=self.why,
            fix=self.fix,
            exception_type=type(self).__qualname__,
            retryable=self.retryable,
            stacktrace=_format_traceback(self) if include_traceback else None,
            details=details,
        )


def _format_traceback(exc: BaseException) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))


def error_info_from_exception(exc: BaseException, *, include_traceback: bool = False) -> ErrorInfo:
    """Convert an arbitrary exception to canonical :class:`ErrorInfo`.

    Only the exception type, ``str(exc)``, an optional cause summary and the
    traceback are captured — never ``exc.__dict__`` or frame locals.
    """
    if isinstance(exc, ObservedError):
        return exc.to_error_info(include_traceback=include_traceback)
    details: dict[str, Any] = {}
    if exc.__cause__ is not None:
        details["cause"] = {
            "exception_type": type(exc.__cause__).__qualname__,
            "message": str(exc.__cause__),
        }
    return ErrorInfo(
        code=type(exc).__qualname__.upper(),
        message=str(exc) or type(exc).__qualname__,
        why=None,
        fix=None,
        exception_type=type(exc).__qualname__,
        retryable=False,
        stacktrace=_format_traceback(exc) if include_traceback else None,
        details=details,
    )
