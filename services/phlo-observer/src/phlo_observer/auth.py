"""Token authentication for ingestion and read APIs.

Tokens are compared with ``secrets.compare_digest`` and never logged. When no
tokens are configured at all the service runs in documented dev mode and
accepts everything — ``auth_optional_dev=false`` turns that into a startup
error via ``ObserverSettings.require_tokens()``.
"""

from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, Request, status

from phlo_observer.settings import ObserverSettings


def get_settings(request: Request) -> ObserverSettings:
    """FastAPI dependency: the settings instance bound at app creation."""
    return request.app.state.settings


def _extract_token(request: Request) -> str | None:
    auth = request.headers.get("authorization")
    if auth and auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return request.headers.get("x-api-key")


def _token_ok(token: str | None, allowed: frozenset[str]) -> bool:
    if not allowed:
        return True  # dev mode: no tokens configured
    if token is None:
        return False
    return any(secrets.compare_digest(token, allowed_token) for allowed_token in allowed)


async def require_ingest_token(
    request: Request, settings: ObserverSettings = Depends(get_settings)
) -> None:
    """Dependency gating write endpoints on the ingest token set."""
    if not _token_ok(_extract_token(request), settings.ingest_token_set):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing ingest token")


async def require_read_token(
    request: Request, settings: ObserverSettings = Depends(get_settings)
) -> None:
    """Dependency gating query endpoints on the read token set."""
    if not _token_ok(_extract_token(request), settings.read_token_set):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing read token")


async def require_admin_token(
    request: Request, settings: ObserverSettings = Depends(get_settings)
) -> None:
    """Dependency gating admin endpoints (quarantine replay, archive).

    Scoped authorization (spec §34): admin falls back to the read token set
    when no admin tokens are configured, keeping small deployments simple.
    """
    if not _token_ok(_extract_token(request), settings.admin_token_set):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing admin token")
