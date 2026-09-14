"""Human-readable console drain for local development."""

from __future__ import annotations

import sys
from collections.abc import Sequence
from typing import TextIO

from observe_core.drains.base import CanonicalEvent

_COLORS = {
    "success": "\x1b[32m",
    "failure": "\x1b[31m",
    "cancelled": "\x1b[33m",
    "partial": "\x1b[35m",
    "unknown": "\x1b[37m",
}
_RESET = "\x1b[0m"


class ConsoleDrain:
    """One-line completion summaries with optional expanded error details."""

    name = "console"
    is_remote = False

    def __init__(
        self,
        *,
        stream: TextIO | None = None,
        color: str = "auto",
        show_error_details: bool = True,
    ) -> None:
        self._stream = stream or sys.stderr
        self._color = self._stream.isatty() if color == "auto" else color == "always"
        self._show_errors = show_error_details

    def emit_batch(self, events: Sequence[CanonicalEvent]) -> None:
        """Write one summary line per event."""
        for item in events:
            data = item.data
            duration_ms = data.get("duration_ms")
            duration = f" {duration_ms:.0f}ms" if isinstance(duration_ms, int | float) else ""
            line = (
                f"{data['observed_at']} "
                f"{str(data['severity']).upper():<8} "
                f"{data['event']:<32} "
                f"{data['outcome']}{duration}"
            )
            correlation = data.get("correlation") or {}
            service = data.get("service") or {}
            extras = []
            if correlation.get("run_id"):
                extras.append(f"run={correlation['run_id']}")
            if service.get("name") and service["name"] != "unknown":
                extras.append(f"service={service['name']}")
            if extras:
                line += " " + " ".join(extras)
            outcome = str(data["outcome"])
            self._write(line, outcome)
            error = data.get("error")
            if self._show_errors and error:
                code = error.get("code") or "-"
                self._write(f"    error[{code}]: {error.get('message')}", outcome)
                if error.get("why"):
                    self._write(f"    why: {error['why']}", outcome)
                if error.get("fix"):
                    self._write(f"    fix: {error['fix']}", outcome)

    def emit_raw(self, payloads: Sequence[bytes]) -> None:
        """Write raw canonical payloads (one JSON line each)."""
        for payload in payloads:
            self._stream.write(payload.decode("utf-8").rstrip("\n") + "\n")
        self._stream.flush()

    def flush(self) -> None:
        """Flush the underlying stream."""
        self._stream.flush()

    def close(self) -> None:
        """Flush; the stream itself is left open (it may be stderr/stdout)."""
        self._stream.flush()

    def _write(self, line: str, outcome: str) -> None:
        if self._color:
            color = _COLORS.get(outcome, "")
            self._stream.write(f"{color}{line}{_RESET}\n")
        else:
            self._stream.write(line + "\n")
