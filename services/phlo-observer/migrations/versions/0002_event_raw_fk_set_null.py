"""events.raw_event_id: ON DELETE SET NULL

Raw payloads expire (default 14d) long before the events that reference them
(default 90d). Without a cascade rule the retention sweep's raw_events DELETE
fails on the foreign key and the whole cleanup transaction rolls back, so
nothing is ever purged. SET NULL keeps the normalized event while dropping
the expired source link.

Revision ID: 0002
Revises: 0001
Create Date: 2025-01-02
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_FK_NAME = "events_raw_event_id_fkey"


def upgrade() -> None:
    with op.batch_alter_table("events") as batch:
        batch.drop_constraint(_FK_NAME, type_="foreignkey")
        batch.create_foreign_key(
            _FK_NAME,
            "raw_events",
            ["raw_event_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    with op.batch_alter_table("events") as batch:
        batch.drop_constraint(_FK_NAME, type_="foreignkey")
        batch.create_foreign_key(
            _FK_NAME,
            "raw_events",
            ["raw_event_id"],
            ["id"],
        )
