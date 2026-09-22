"""Typed configuration for observe-core.

``ObserveSettings`` is a ``pydantic-settings`` model: every field can be set in
code or through an ``OBSERVE_*`` environment variable. See
``docs/configuration.md`` for the full reference.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from pydantic_settings import (
    BaseSettings,
    EnvSettingsSource,
    NoDecode,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    SettingsError,
)


class ConsoleDrainConfig(BaseModel):
    """Human-readable local development output."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["console"] = "console"
    stream: Literal["stderr", "stdout"] = "stderr"
    color: Literal["auto", "always", "never"] = "auto"
    show_error_details: bool = True


class JsonlDrainConfig(BaseModel):
    """Append one canonical JSON object per line to a rotating file."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["jsonl"] = "jsonl"
    path: Path = Path("observe-events.jsonl")
    max_bytes: int = 64 * 1024 * 1024
    backup_count: int = 5
    fsync: bool = False


class HttpDrainConfig(BaseModel):
    """Batch POST canonical events to a phlo-observer endpoint."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["http"] = "http"
    endpoint: str
    token: str | None = None
    api_key: str | None = None
    api_key_header: str = "X-API-Key"
    headers: dict[str, str] = Field(default_factory=dict)
    connect_timeout_s: float = 5.0
    read_timeout_s: float = 10.0
    max_attempts: int = 5
    backoff_base_ms: float = 250.0
    backoff_cap_ms: float = 10_000.0
    gzip_threshold_bytes: int = 64 * 1024
    spool_on_failure: bool = True


class OtlpDrainConfig(BaseModel):
    """Export events to an OpenTelemetry collector (requires ``phlo-observe-core[otlp]``)."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["otlp"] = "otlp"
    endpoint: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)


class MemoryDrainConfig(BaseModel):
    """In-memory drain for tests and embedding."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["memory"] = "memory"


DrainConfig = Annotated[
    ConsoleDrainConfig | JsonlDrainConfig | HttpDrainConfig | OtlpDrainConfig | MemoryDrainConfig,
    Field(discriminator="type"),
]
"""Discriminated union of supported drain configurations."""


def _default_drains() -> list[DrainConfig]:
    return [ConsoleDrainConfig()]


def _csv_or_json_list(value: Any) -> Any:
    """Accept ``a,b`` or ``["a","b"]`` for ``list[str]`` settings.

    Fields marked ``NoDecode`` receive raw env strings here; JSON array syntax
    still works for operators who prefer it. Empty input means "no values".
    """
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            pass
        else:
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
    return [item.strip() for item in text.split(",") if item.strip()]


_SECRETISH_FIELD = re.compile(
    r"token|secret|password|credential|api_?key|private_?key|url", re.IGNORECASE
)


def format_settings_error(exc: BaseException) -> str:
    """Render a configuration failure for an operator (spec §73).

    Reports which ``OBSERVE_*`` variable is invalid, the received value when
    it is not secret-bearing, and where to find the allowed values.
    """
    if isinstance(exc, SettingsError):
        cause = exc.__cause__
        detail = f": {cause}" if cause is not None else ""
        return (
            f"{exc}{detail}\nCheck your OBSERVE_* environment variables; "
            "see docs/configuration.md for allowed values."
        )
    if not isinstance(exc, ValidationError):
        return str(exc)
    lines = ["invalid configuration:"]
    for err in exc.errors():
        field = ".".join(str(p) for p in err["loc"])
        env = f"OBSERVE_{field.upper()}" if field else "configuration"
        received = err.get("input")
        shown = (
            "<redacted>"
            if received not in (None, "") and _SECRETISH_FIELD.search(field)
            else repr(received)
        )
        lines.append(f"  {env}: {err['msg']} (received {shown})")
    lines.append("Fix or unset the offending variables; see docs/configuration.md.")
    return "\n".join(lines)


def default_spool_dir() -> Path:
    """Return the default spool directory (``$XDG_STATE_HOME`` aware)."""
    xdg = os.environ.get("XDG_STATE_HOME")
    base = Path(xdg) if xdg else Path.home() / ".local" / "state"
    return base / "phlo-observe" / "spool"


class _ShorthandEnvSource(EnvSettingsSource):
    """Expands ``OBSERVE_DRAINS=console,http`` into structured drain configs."""

    def prepare_field_value(
        self, field_name: str, field: Any, value: Any, value_is_complex: bool
    ) -> Any:
        if field_name == "drains" and isinstance(value, str) and not value.lstrip().startswith("["):
            names = [name.strip() for name in value.split(",") if name.strip()]
            return json.dumps([self._expand(name) for name in names])
        return super().prepare_field_value(field_name, field, value, value_is_complex)

    @staticmethod
    def _expand(name: str) -> dict[str, Any]:
        if name == "console":
            return {"type": "console"}
        if name == "jsonl":
            return {
                "type": "jsonl",
                "path": os.environ.get("OBSERVE_JSONL_PATH", "observe-events.jsonl"),
            }
        if name == "http":
            endpoint = os.environ.get("OBSERVE_HTTP_ENDPOINT")
            if not endpoint:
                raise ValueError(
                    "OBSERVE_DRAINS includes 'http' but OBSERVE_HTTP_ENDPOINT is not set. "
                    "Set OBSERVE_HTTP_ENDPOINT=http://phlo-observer:8080/v1/events or "
                    "remove 'http' from OBSERVE_DRAINS."
                )
            return {
                "type": "http",
                "endpoint": endpoint,
                "token": os.environ.get("OBSERVE_HTTP_TOKEN"),
                "api_key": os.environ.get("OBSERVE_HTTP_API_KEY"),
            }
        if name == "otlp":
            return {"type": "otlp", "endpoint": os.environ.get("OBSERVE_OTLP_ENDPOINT")}
        if name == "memory":
            return {"type": "memory"}
        raise ValueError(
            f"Unknown drain name {name!r} in OBSERVE_DRAINS. "
            "Allowed values: console, jsonl, http, otlp, memory."
        )


class ObserveSettings(BaseSettings):
    """Top-level observe-core configuration.

    All fields are settable via ``OBSERVE_``-prefixed environment variables or
    programmatically for tests and notebooks.
    """

    model_config = SettingsConfigDict(
        env_prefix="OBSERVE_",
        extra="ignore",
        validate_default=True,
    )

    enabled: bool = True
    """Master switch. When false, emission is a near-zero-cost no-op."""

    runtime_backend: Literal["worker", "sync", "capture"] = "worker"
    """Transport backend (spec §7.5): ``worker`` = bounded queue + background
    workers (production default), ``sync`` = deliver on the caller's thread,
    ``capture`` = in-memory capture for tests."""

    envelope_version: Literal["1.0", "2.0"] = "2.0"
    """Canonical envelope version emitted (spec §43). The observer accepts
    both; ``2.0`` adds ``entities``, ``tags`` and ``contract`` fields."""

    contract_validation: Literal["off", "warn", "strict"] = "warn"
    """Event-contract validation mode (spec §7.2). ``warn`` records violations
    on the event without failing application work; ``strict`` raises."""

    service_name: str = "unknown"
    service_version: str | None = None
    environment: str = "development"
    instance_id: str | None = None
    host: str | None = None

    queue_capacity: int = 10_000
    batch_size: int = 100
    flush_interval_ms: int = 1_000
    drop_policy: Literal["drop_newest", "drop_oldest"] = "drop_newest"
    """Queue-pressure policy for ``debug`` and ``telemetry`` events."""

    drains: list[DrainConfig] = Field(default_factory=_default_drains)
    """Drain configs. Env shorthand: ``OBSERVE_DRAINS=console,http``."""

    spool_enabled: bool = True
    spool_dir: Path | None = None
    """Spool directory; defaults to ``~/.local/state/phlo-observe/spool``."""
    spool_max_bytes: int = 1024 * 1024 * 1024
    spool_segment_max_bytes: int = 32 * 1024 * 1024
    spool_on_full: Literal["drop_oldest", "drop_newest"] = "drop_oldest"
    spool_replay_interval_s: float = 30.0
    """How often the worker attempts to replay the spool to remote drains."""

    capture_stacktrace: bool = True
    max_event_bytes: int = 256 * 1024
    max_depth: int = 8

    redact_keys: Annotated[list[str], NoDecode] = Field(default_factory=list)
    """Additional exact key names to redact (case-insensitive)."""
    redact_key_patterns: Annotated[list[str], NoDecode] = Field(default_factory=list)
    """Regexes matched against key names."""
    redact_paths: Annotated[list[str], NoDecode] = Field(default_factory=list)
    """Dotted paths to always redact, e.g. ``attributes.connection.password``."""
    redact_value_patterns: Annotated[list[str], NoDecode] = Field(default_factory=list)
    """Regexes matched against string values regardless of key."""
    redaction_enabled: bool = True

    sampling_debug_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    """Fraction of ``debug`` events kept. Default: 1.0 dev / 0.1 production."""
    sampling_telemetry_rate: float = Field(default=1.0, ge=0.0, le=1.0)
    """``critical`` events are never sampled."""
    sampling_policy: Annotated[list[dict[str, Any]], NoDecode] = Field(default_factory=list)
    """Ordered sampling rules (spec §7.6), evaluated before the base rates.
    Each entry may set matchers (``event``, ``severity``, ``outcome``,
    ``min_duration_ms``, ``environment``, ``service``) and either ``rate``
    (0.0-1.0, applied at run/trace level) or ``keep`` (true/false). Decisions
    are recorded on the event under ``attributes._observe.sampling``."""
    tail_sampling: bool = False
    """Retain per-run event buffers and emit reduced telemetry for uneventful
    runs (spec §7.7). Optional; bounded by ``tail_max_runs``."""
    tail_max_runs: int = 1_000
    """Maximum concurrent run buffers held by the tail sampler."""
    tail_min_duration_ms: float = 30_000.0
    """Runs longer than this keep full telemetry even when successful."""
    tail_terminal_events: Annotated[list[str], NoDecode] = Field(default_factory=list)
    """Event names that close a run buffer for a keep/drop decision
    (spec §7.7). Matched exactly; names ending ``.completed``/``.failed``/
    ``.cancelled`` are always terminal. Core ships no names of its own —
    applications register their run-boundary vocabulary (for Phlo,
    ``configure_phlo`` supplies ``pipeline.run`` etc.)."""

    telemetry_required: bool = False
    """Fail-closed mode: raise instead of dropping when export is impossible."""
    fail_fast: bool = False
    """Development aid: raise on event-construction errors instead of diagnosing."""

    worker_count: int = 1
    shutdown_timeout_s: float = 5.0

    @field_validator("drains", mode="before")
    @classmethod
    def _coerce_drains(cls, value: Any) -> Any:
        if isinstance(value, str):
            text = value.strip()
            if text.startswith("["):
                return json.loads(text)
            names = [n.strip() for n in text.split(",") if n]
            return [_ShorthandEnvSource._expand(n) for n in names]
        return value

    @field_validator(
        "redact_keys",
        "redact_key_patterns",
        "redact_paths",
        "redact_value_patterns",
        "tail_terminal_events",
        mode="before",
    )
    @classmethod
    def _coerce_str_list(cls, value: Any) -> Any:
        return _csv_or_json_list(value)

    @field_validator("sampling_policy", mode="before")
    @classmethod
    def _coerce_policy(cls, value: Any) -> Any:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            return json.loads(text)
        return value

    @field_validator(
        "batch_size", "queue_capacity", "max_event_bytes", "max_depth", "tail_max_runs"
    )
    @classmethod
    def _positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("must be a positive integer")
        return value

    def resolved_debug_rate(self) -> float:
        """Effective sampling rate for ``debug`` delivery events."""
        if self.sampling_debug_rate is not None:
            return self.sampling_debug_rate
        return 0.1 if self.environment == "production" else 1.0

    def resolved_spool_dir(self) -> Path:
        """Effective spool directory."""
        return self.spool_dir or default_spool_dir()

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Customize sources: env vars get the ``OBSERVE_DRAINS`` shorthand expansion."""
        return (
            init_settings,
            _ShorthandEnvSource(settings_cls),
            dotenv_settings,
            file_secret_settings,
        )
