"""events.received_at index — retention deletes on the observer-side clock.

Retention now expires canonical events by ``received_at`` (untrusted producer
clocks can skew ``observed_at`` arbitrarily). The delete needs its own index
or every sweep scans the full events table.

Revision ID: 0005
Revises: 0004
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index("ix_events_received_at", "events", ["received_at"])


def downgrade() -> None:
    op.drop_index("ix_events_received_at", table_name="events")
