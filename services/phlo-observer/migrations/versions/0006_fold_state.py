"""fold_state bookkeeping on projections + keyed baseline samples.

Projection reducers now carry per-field ``(observed_at, event_id)`` write
keys so late/out-of-order events fold in place instead of regressing newer
state — incremental ingest and ``rebuild-projections`` converge. The new
nullable ``fold_state`` JSONB columns persist that bookkeeping on runs,
entities, assets and relationship edges.

``observe_baselines.samples`` changes shape from ``[value, ...]`` to
``[[observed_at_iso, event_id, value], ...]`` so the rolling window keeps
the newest *observed* samples regardless of arrival order. Existing
samples cannot be re-keyed to their producing events reliably (the link
was never stored), so each legacy sample anchors at the row's
``updated_at`` with its ordinal as the key suffix — preserving both the
values and their recorded order.

Revision ID: 0006
Revises: 0005
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    for table in ("runs", "observe_entities", "observe_relationships", "observe_assets"):
        op.add_column(table, sa.Column("fold_state", _JSON, nullable=True))
    op.execute(
        """
        UPDATE observe_baselines
        SET samples = COALESCE(
            (
                SELECT jsonb_agg(
                    jsonb_build_array(
                        to_char(
                            observe_baselines.updated_at AT TIME ZONE 'UTC',
                            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                        ),
                        lpad(s.idx::text, 8, '0'),
                        s.value
                    )
                    ORDER BY s.idx
                )
                FROM jsonb_array_elements(observe_baselines.samples)
                    WITH ORDINALITY AS s(value, idx)
            ),
            '[]'::jsonb
        )
        WHERE jsonb_array_length(samples) > 0
          AND jsonb_typeof(samples -> 0) IS DISTINCT FROM 'array'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE observe_baselines
        SET samples = COALESCE(
            (
                SELECT jsonb_agg(s.value -> 2 ORDER BY s.value -> 0, s.value -> 1)
                FROM jsonb_array_elements(observe_baselines.samples) AS s(value)
            ),
            '[]'::jsonb
        )
        WHERE jsonb_typeof(samples -> 0) = 'array'
        """
    )
    for table in ("observe_assets", "observe_relationships", "observe_entities", "runs"):
        op.drop_column(table, "fold_state")
