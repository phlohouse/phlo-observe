"""Token authentication for ingestion and read APIs.

Tokens are compared with ``secrets.compare_digest`` and never logged. When no
tokens are configured at all the service runs in documented dev mode and
accepts everything — ``auth_optional_dev=false`` turns that into a startup
error via ``ObserverSettings.require_tokens()``. The moment *any* token
surface is configured, every surface fails closed: partial configuration
must never leave a credential surface anonymously open.
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


def _any_tokens_configured(settings: ObserverSettings) -> bool:
    """True once any credential exists — dev mode is *all* surfaces unset."""
    return bool(settings.ingest_token_set or settings.read_token_set or settings.admin_token_set)


def _require(
    request: Request, allowed: frozenset[str], surface: str, settings: ObserverSettings
) -> None:
    """Deny unless the request carries a valid token for ``surface``.

    Dev mode is *all* token surfaces unset. Once any credential exists, an
    unset surface fails closed — configuring only ingest tokens must not
    leave reads anonymous, or vice versa.
    """
    if not _any_tokens_configured(settings):
        return  # dev mode: nothing configured at all
    token = _extract_token(request)
    if (
        token is None
        or not allowed
        or not any(secrets.compare_digest(token, candidate) for candidate in allowed)
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, f"invalid or missing {surface} token")


async def require_ingest_token(
    request: Request, settings: ObserverSettings = Depends(get_settings)
) -> None:
    """Dependency gating write endpoints on the ingest token set."""
    _require(request, settings.ingest_token_set, "ingest", settings)


async def require_read_token(
    request: Request, settings: ObserverSettings = Depends(get_settings)
) -> None:
    """Dependency gating query endpoints on the read token set."""
    _require(request, settings.read_token_set, "read", settings)


async def require_admin_token(
    request: Request, settings: ObserverSettings = Depends(get_settings)
) -> None:
    """Dependency gating admin endpoints (quarantine replay, archive).

    Scoped authorization (spec §34): admin credentials are never inherited
    from read tokens. Dev mode (no tokens configured on any surface) stays
    open; the moment any token exists, an unset admin list fails closed —
    administration is denied to everyone, never silently extended to readers.
    """
    _require(request, settings.admin_token_set, "admin", settings)
