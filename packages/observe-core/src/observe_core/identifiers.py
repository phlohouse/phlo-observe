"""Canonical entity identifiers (spec §9.2).

V2 standardizes how observed things are named. Identifiers are namespaced
URIs, deterministic, parseable, and stable across observer restarts::

    asset://silver/samples
    iceberg://catalog/schema/table
    branch://nessie/run-abc123
    run://dagster/01J...
    model://dbt/silver_samples
    source://dlt/labware_samples

The URI grammar is ``<namespace>://<path>`` where ``path`` may itself be
hierarchical. Parsing never raises on well-formed input and produces a
:class:`EntityId` whose :attr:`EntityId.namespace` selects the entity kind.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

URI_PATTERN = re.compile(r"^(?P<ns>[a-z][a-z0-9]*)://(?P<path>[^\s]+)$")


class EntityKind(StrEnum):
    """The namespaces V2 recognizes as first-class entity kinds."""

    ASSET = "asset"
    RUN = "run"
    TABLE = "table"
    ICEBERG = "iceberg"
    BRANCH = "branch"
    MODEL = "model"
    SOURCE = "source"
    SNAPSHOT = "snapshot"
    SERVICE = "service"
    DEPLOYMENT = "deployment"
    QUALITY_SUITE = "quality"
    INCIDENT = "incident"


@dataclass(frozen=True)
class EntityId:
    """A parsed canonical entity identifier.

    ``namespace`` is the URI scheme (``asset`` in ``asset://x/y``);
    ``path`` is everything after ``://`` and may carry its own hierarchy
    (``catalog/schema/table`` for Iceberg identifiers).
    """

    namespace: str
    path: str

    @property
    def kind(self) -> EntityKind | None:
        """The recognized :class:`EntityKind`, or None for custom namespaces."""
        try:
            return EntityKind(self.namespace)
        except ValueError:
            return None

    @property
    def parts(self) -> tuple[str, ...]:
        """Path segments, split on ``/``."""
        return tuple(part for part in self.path.split("/") if part)

    def __str__(self) -> str:
        """Render as the canonical ``<namespace>://<path>`` URI."""
        return f"{self.namespace}://{self.path}"


def entity_id(namespace: str, *parts: object) -> EntityId:
    """Build a canonical :class:`EntityId` from a namespace and path parts.

    ``None`` and empty parts are skipped, so optional components (a missing
    partition, for instance) compose safely::

        entity_id("asset", "silver", "samples")          -> asset://silver/samples
        entity_id("run", "dagster", run_id)              -> run://dagster/01J...
    """
    if not re.fullmatch(r"[a-z][a-z0-9]*", namespace):
        raise ValueError(
            f"invalid entity namespace {namespace!r}: lowercase letters and digits only"
        )
    clean = [str(part).strip("/") for part in parts if part is not None and str(part).strip("/")]
    if not clean:
        raise ValueError("entity identifier requires at least one non-empty path part")
    return EntityId(namespace=namespace, path="/".join(clean))


def parse_entity_id(value: str) -> EntityId:
    """Parse a ``<namespace>://<path>`` identifier.

    Raises ``ValueError`` for malformed input; callers handling untrusted
    values should treat the exception as a validation failure.
    """
    match = URI_PATTERN.match(value.strip())
    if not match:
        raise ValueError(f"not a canonical entity identifier: {value!r}")
    return EntityId(namespace=match.group("ns"), path=match.group("path"))


def is_entity_id(value: object) -> bool:
    """True when ``value`` parses as a canonical entity identifier."""
    return isinstance(value, str) and URI_PATTERN.match(value.strip()) is not None


def asset_id(*parts: object) -> EntityId:
    """``asset://<path>`` — a Phlo asset key such as ``silver.samples``."""
    return entity_id("asset", *parts)


def run_id_for(system: str, run: object) -> EntityId:
    """``run://<system>/<run>`` — a run owned by an orchestrator."""
    return entity_id("run", system, run)


def table_id(*parts: object) -> EntityId:
    """``table://<path>`` — a warehouse table."""
    return entity_id("table", *parts)


def iceberg_id(catalog: str, *schema_table: object) -> EntityId:
    """``iceberg://<catalog>/<schema>/<table>``."""
    return entity_id("iceberg", catalog, *schema_table)


def branch_id(system: str, branch: object) -> EntityId:
    """``branch://<system>/<branch>`` — a WAP/Nessie branch."""
    return entity_id("branch", system, branch)


def model_id(system: str, model: object) -> EntityId:
    """``model://<system>/<model>`` — a dbt (or other) model."""
    return entity_id("model", system, model)


def source_id(system: str, source: object) -> EntityId:
    """``source://<system>/<source>`` — an ingestion source."""
    return entity_id("source", system, source)


def snapshot_id_for(table: object, snapshot: object) -> EntityId:
    """``snapshot://<table>/<snapshot>`` — an Iceberg snapshot."""
    return entity_id("snapshot", table, snapshot)


def service_id(name: object) -> EntityId:
    """``service://<name>`` — a deployed service."""
    return entity_id("service", name)


def incident_id(incident: object) -> EntityId:
    """``incident://<id>``."""
    return entity_id("incident", incident)
