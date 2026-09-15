"""Observer service configuration.

All fields are settable via ``PHLO_OBSERVER_*`` environment variables. Token
fields support ``_FILE`` variants pointing at a file containing the secret
(Docker/Kubernetes secret mounts).
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Annotated, Any

from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict, SettingsError


def _file_value(env_name: str) -> str | None:
    """Read a secret from ``<NAME>_FILE`` (Docker/Kubernetes secret mounts)."""
    file_path = os.environ.get(f"{env_name}_FILE")
    if not file_path:
        return None
    try:
        return Path(file_path).read_text().strip()
    except OSError as exc:
        raise ValueError(f"{env_name}_FILE={file_path!r} is not readable: {exc}") from exc


class ObserverSettings(BaseSettings):
    """phlo-observer service settings."""

    model_config = SettingsConfigDict(env_prefix="PHLO_OBSERVER_", extra="ignore")

    host: str = "0.0.0.0"  # noqa: S104 - service binds publicly by design
    port: int = 8080
    log_level: str = "INFO"

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/phlo_observer"
    db_pool_size: int = 10
    db_pool_max_overflow: int = 10

    # NoDecode keeps pydantic-settings from json.loads-ing the raw env value
    # before the before-validator can apply the documented comma-separated form.
    ingest_tokens: Annotated[list[str], NoDecode] = Field(default_factory=list)
    """Tokens allowed to write events (comma-separated). Empty = dev mode."""
    read_tokens: Annotated[list[str], NoDecode] = Field(default_factory=list)
    """Tokens allowed to query (comma-separated). Empty = dev mode."""
    admin_tokens: Annotated[list[str], NoDecode] = Field(default_factory=list)
    """Tokens allowed to administer (quarantine replay, archive, insight
    transitions). Never inherited from other token sets: once any token is
    configured, an unset admin list closes the admin surface to everyone
    rather than silently extending it to every reader."""

    alert_webhook_urls: Annotated[list[str], NoDecode] = Field(default_factory=list)
    """POST insight/incident notifications to these URLs (comma-separated).
    Delivery is fire-and-forget; failures never delay ingestion (§34)."""

    raw_retention_days: int = 14
    event_retention_days: int = 90
    run_retention_days: int = 365
    retention_interval_s: float = 3600.0
    """How often the in-process retention sweep runs."""

    max_body_bytes: int = 10 * 1024 * 1024
    max_batch_events: int = 1000
    max_raw_payload_bytes: int = 256 * 1024
    """Bodies larger than this are stored in raw_events as a SHA-256 digest
    only, so very large artifacts are not duplicated (spec §34.3)."""

    metrics_enabled: bool = True
    metrics_public: bool = False
    """When False, /metrics requires a read token (if read tokens are set)."""

    stream_notify: bool = True
    """Republish this instance's stream messages to other replicas via
    Postgres LISTEN/NOTIFY so SSE clients see every committed change
    regardless of which instance they connected to (spec §26/§38)."""

    docs_enabled: bool = True
    """Serve /docs and /openapi.json. Disable for hardened deployments."""

    self_observe_drains: str = "console"
    """Internal self-observation drains (comma list): console, jsonl, otlp,
    memory. The observer instruments itself with observe-core per spec §46;
    ``http`` is rejected here so internal events can never loop back into
    this service's own ingest endpoint."""

    otlp_endpoint: str | None = None
    """Optional OTLP HTTP endpoint for forwarding normalized events."""

    run_migrations: bool = False
    """When true, apply Alembic migrations to head during startup."""

    auth_optional_dev: bool = True
    """Documented dev escape hatch; set false to hard-fail without tokens."""

    @field_validator(
        "ingest_tokens", "read_tokens", "admin_tokens", "alert_webhook_urls", mode="before"
    )
    @classmethod
    def _split_tokens(cls, value: object) -> object:
        """Accept ``a,b`` or ``["a","b"]``; either arrives here as a raw string."""
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
                    return [str(t).strip() for t in parsed if str(t).strip()]
        return [t.strip() for t in text.split(",") if t.strip()]

    @field_validator("database_url", mode="before")
    @classmethod
    def _resolve_db_url(cls, value: object) -> object:
        # Only the _FILE variant is resolved here: the plain env var already
        # arrives as ``value`` through pydantic-settings' env source, and
        # constructor arguments must keep precedence over it. A mounted secret
        # file is deliberate operator config, so it wins over both.
        file_path = os.environ.get("PHLO_OBSERVER_DATABASE_URL_FILE")
        if file_path:
            try:
                return Path(file_path).read_text().strip()
            except OSError as exc:
                raise ValueError(
                    f"PHLO_OBSERVER_DATABASE_URL_FILE={file_path!r} is not readable: {exc}"
                ) from exc
        return value

    @property
    def ingest_token_set(self) -> frozenset[str]:
        """Ingest tokens, including any merged from ``_FILE`` variants."""
        return frozenset(self.ingest_tokens)

    @property
    def read_token_set(self) -> frozenset[str]:
        """Read tokens, including any merged from ``_FILE`` variants."""
        return frozenset(self.read_tokens)

    @property
    def admin_token_set(self) -> frozenset[str]:
        """Admin tokens only — no fallback to read or ingest credentials."""
        return frozenset(self.admin_tokens)

    def require_tokens(self) -> None:
        """Fail fast when auth is disabled outside a dev deployment.

        With ``auth_optional_dev=false`` every surface must be locked down:
        missing ingest tokens leave writes open, missing read tokens leave
        queries open, and missing admin tokens leave administration closed —
        a partially configured token set is not "hardened".
        """
        if self.auth_optional_dev:
            return
        missing = []
        if not self.ingest_tokens:
            missing.append("ingest tokens (PHLO_OBSERVER_INGEST_TOKENS)")
        if not self.read_tokens:
            missing.append("read tokens (PHLO_OBSERVER_READ_TOKENS)")
        if not self.admin_tokens:
            missing.append("admin tokens (PHLO_OBSERVER_ADMIN_TOKENS)")
        if missing:
            raise ValueError(
                f"PHLO_OBSERVER_AUTH_OPTIONAL_DEV=false but {' and '.join(missing)} "
                "are missing. Configure them (or *_FILE variants) before serving."
            )

    def self_observe_drain_configs(self) -> list[dict[str, Any]]:
        """Parse ``self_observe_drains`` into observe-core drain configs.

        Only non-HTTP drains are permitted: internal observer events must
        never be posted back into this service's own ingest API (spec §46
        recursion guard). An ``http`` entry is a configuration error.
        """
        allowed = {"console", "jsonl", "otlp", "memory"}
        configs: list[dict[str, Any]] = []
        for name in (d.strip() for d in self.self_observe_drains.split(",")):
            if not name:
                continue
            if name == "http":
                raise ValueError(
                    "PHLO_OBSERVER_SELF_OBSERVE_DRAINS may not include 'http': "
                    "internal events must not post back into this observer. "
                    "Use console, jsonl or otlp."
                )
            if name not in allowed:
                raise ValueError(
                    f"Unknown self-observation drain {name!r}. "
                    f"Allowed values: {', '.join(sorted(allowed))}."
                )
            cfg: dict[str, Any] = {"type": name}
            if name == "otlp" and self.otlp_endpoint:
                cfg["endpoint"] = self.otlp_endpoint
            if name == "jsonl":
                cfg["path"] = "phlo-observer-events.jsonl"
            configs.append(cfg)
        return configs


_SECRETISH_FIELD = re.compile(
    r"token|secret|password|credential|api_?key|private_?key|url", re.IGNORECASE
)


def format_settings_error(exc: BaseException) -> str:
    """Render a configuration failure for an operator (spec §73).

    Reports which ``PHLO_OBSERVER_*`` variable is invalid, the received value
    when it is not secret-bearing, and where to find the allowed values.
    """
    if isinstance(exc, SettingsError):
        cause = exc.__cause__
        detail = f": {cause}" if cause is not None else ""
        return (
            f"{exc}{detail}\nCheck your PHLO_OBSERVER_* environment variables; "
            "see docs/configuration.md for allowed values."
        )
    if not isinstance(exc, ValidationError):
        return str(exc)
    lines = ["invalid configuration:"]
    for err in exc.errors():
        field = ".".join(str(p) for p in err["loc"])
        env = f"PHLO_OBSERVER_{field.upper()}" if field else "configuration"
        received = err.get("input")
        shown = (
            "<redacted>"
            if received not in (None, "") and _SECRETISH_FIELD.search(field)
            else repr(received)
        )
        lines.append(f"  {env}: {err['msg']} (received {shown})")
    lines.append("Fix or unset the offending variables; see docs/configuration.md.")
    return "\n".join(lines)


def load_settings() -> ObserverSettings:
    """Build settings, resolving ``_FILE`` token variants into the token lists.

    Pydantic parse/validation failures are translated into an operator-facing
    ``ValueError`` (spec §73) so startup surfaces a readable message rather
    than a raw ``ValidationError`` traceback.
    """
    try:
        settings = ObserverSettings()
    except (ValidationError, SettingsError) as exc:
        raise ValueError(format_settings_error(exc)) from exc
    for env_name, attr in (
        ("PHLO_OBSERVER_INGEST_TOKENS", "ingest_tokens"),
        ("PHLO_OBSERVER_READ_TOKENS", "read_tokens"),
        ("PHLO_OBSERVER_ADMIN_TOKENS", "admin_tokens"),
    ):
        # Only the _FILE variant is merged here: the plain env var already
        # arrives via pydantic-settings' env source, so merging it again
        # would double-count every token.
        resolved = _file_value(env_name)
        if resolved:
            file_tokens = [t.strip() for t in resolved.split(",") if t.strip()]
            merged = [*getattr(settings, attr), *file_tokens]
            object.__setattr__(settings, attr, merged)
    return settings
