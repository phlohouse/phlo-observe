"""Observer service configuration.

All fields are settable via ``PHLO_OBSERVER_*`` environment variables. Token
fields support ``_FILE`` variants pointing at a file containing the secret
(Docker/Kubernetes secret mounts).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _file_or_value(env_name: str) -> str | None:
    """Read a secret from ``<NAME>_FILE`` or ``<NAME>``, file winning."""
    file_path = os.environ.get(f"{env_name}_FILE")
    if file_path:
        try:
            return Path(file_path).read_text().strip()
        except OSError as exc:
            raise ValueError(f"{env_name}_FILE={file_path!r} is not readable: {exc}") from exc
    return os.environ.get(env_name)


class ObserverSettings(BaseSettings):
    """phlo-observer service settings."""

    model_config = SettingsConfigDict(env_prefix="PHLO_OBSERVER_", extra="ignore")

    host: str = "0.0.0.0"  # noqa: S104 - service binds publicly by design
    port: int = 8080
    log_level: str = "INFO"

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/phlo_observer"
    db_pool_size: int = 10
    db_pool_max_overflow: int = 10

    ingest_tokens: list[str] = Field(default_factory=list)
    """Tokens allowed to write events. Empty = dev mode (no auth)."""
    read_tokens: list[str] = Field(default_factory=list)
    """Tokens allowed to query. Empty = dev mode (no auth)."""

    raw_retention_days: int = 14
    event_retention_days: int = 90
    run_retention_days: int = 365
    retention_interval_s: float = 3600.0
    """How often the in-process retention sweep runs."""

    max_body_bytes: int = 10 * 1024 * 1024
    max_batch_events: int = 1000

    metrics_enabled: bool = True
    metrics_public: bool = False
    """When False, /metrics requires a read token (if read tokens are set)."""

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

    @field_validator("ingest_tokens", "read_tokens", mode="before")
    @classmethod
    def _split_tokens(cls, value: object) -> object:
        if isinstance(value, str):
            return [t.strip() for t in value.split(",") if t.strip()]
        return value

    @field_validator("database_url", mode="before")
    @classmethod
    def _resolve_db_url(cls, value: object) -> object:
        resolved = _file_or_value("PHLO_OBSERVER_DATABASE_URL")
        return resolved if resolved is not None else value

    @property
    def ingest_token_set(self) -> frozenset[str]:
        """Ingest tokens, including any merged from ``_FILE`` variants."""
        return frozenset(self.ingest_tokens)

    @property
    def read_token_set(self) -> frozenset[str]:
        """Read tokens, including any merged from ``_FILE`` variants."""
        return frozenset(self.read_tokens)

    def require_tokens(self) -> None:
        """Fail fast when auth is disabled outside a dev deployment."""
        if not self.ingest_tokens and not self.auth_optional_dev:
            raise ValueError(
                "No ingest tokens configured and PHLO_OBSERVER_AUTH_OPTIONAL_DEV=false. "
                "Set PHLO_OBSERVER_INGEST_TOKENS (or *_FILE) before serving."
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


def load_settings() -> ObserverSettings:
    """Build settings, resolving ``_FILE`` token variants into the token lists."""
    settings = ObserverSettings()
    for env_name, attr in (
        ("PHLO_OBSERVER_INGEST_TOKENS", "ingest_tokens"),
        ("PHLO_OBSERVER_READ_TOKENS", "read_tokens"),
    ):
        resolved = _file_or_value(env_name)
        if resolved:
            file_tokens = [t.strip() for t in resolved.split(",") if t.strip()]
            merged = [*getattr(settings, attr), *file_tokens]
            object.__setattr__(settings, attr, merged)
    return settings
