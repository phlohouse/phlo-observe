"""At most one open insight per dedupe key.

Two observer replicas ingesting the same signal concurrently could each find
no open row and insert one, leaving duplicate open insights the dedupe map
was designed to prevent. A partial unique index on
``dedupe_key WHERE state = 'open'`` makes the invariant the database's
responsibility instead of a race window's.

Pre-existing duplicates are collapsed before the index is created: the
earliest-created row per key keeps the open state and absorbs the union of
every duplicate's ``producers`` timeline and evidence ids (the next
``_apply_producers`` rewrites those fields from the merged timeline anyway);
losing rows are marked ``suppressed`` with ``attributes.deduped_into``
pointing at the keeper so the evidence trail survives. Insights are derived
state — a rebuild regenerates exactly one open row per key.

Revision ID: 0007
Revises: 0006
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        WITH open_rows AS (
            SELECT insight_id, dedupe_key, created_at,
                   row_number() OVER (
                       PARTITION BY dedupe_key
                       ORDER BY created_at, insight_id
                   ) AS rn
            FROM observe_insights
            WHERE state = 'open' AND dedupe_key IS NOT NULL
        ),
        keepers AS (
            SELECT dedupe_key, insight_id AS keeper_id
            FROM open_rows
            WHERE rn = 1
        ),
        merged AS (
            SELECT o.dedupe_key,
                   keepers.keeper_id,
                   (SELECT jsonb_agg(v ORDER BY v ->> 'at', v ->> 'eid')
                    FROM (
                        SELECT DISTINCT p.value AS v
                        FROM open_rows o2
                        JOIN observe_insights i2 ON i2.insight_id = o2.insight_id
                        CROSS JOIN LATERAL jsonb_array_elements(
                            COALESCE(i2.attributes -> 'producers', '[]'::jsonb)
                        ) AS p(value)
                        WHERE o2.dedupe_key = o.dedupe_key
                    ) dedup
                    WHERE jsonb_typeof(dedup.v) = 'object') AS producers,
                   (SELECT jsonb_agg(v ORDER BY v #>> '{}')
                    FROM (
                        SELECT DISTINCT e.value AS v
                        FROM open_rows o2
                        JOIN observe_insights i2 ON i2.insight_id = o2.insight_id
                        CROSS JOIN LATERAL jsonb_array_elements(
                            COALESCE(i2.evidence_event_ids, '[]'::jsonb)
                        ) AS e(value)
                        WHERE o2.dedupe_key = o.dedupe_key
                    ) dedup) AS evidence
            FROM open_rows o
            JOIN keepers ON keepers.dedupe_key = o.dedupe_key
            GROUP BY o.dedupe_key, keepers.keeper_id
            HAVING count(*) > 1
        ),
        keeper_upd AS (
            UPDATE observe_insights k
            SET attributes = (
                    CASE WHEN m.producers IS NOT NULL
                         THEN jsonb_set(
                             COALESCE(k.attributes, '{}'::jsonb),
                             '{producers}', m.producers)
                         ELSE COALESCE(k.attributes, '{}'::jsonb)
                    END
                ) || jsonb_build_object('dedupe_merged', true),
                evidence_event_ids = COALESCE(m.evidence, k.evidence_event_ids),
                updated_at = now()
            FROM merged m
            WHERE k.insight_id = m.keeper_id
            RETURNING k.insight_id
        )
        UPDATE observe_insights i
        SET state = 'suppressed',
            attributes = COALESCE(i.attributes, '{}'::jsonb)
                || jsonb_build_object('deduped_into', m.keeper_id::text),
            updated_at = now()
        FROM merged m
        JOIN open_rows o ON o.dedupe_key = m.dedupe_key AND o.rn > 1
        WHERE i.insight_id = o.insight_id
        """
    )
    op.create_index(
        "uq_observe_insights_open_dedupe",
        "observe_insights",
        ["dedupe_key"],
        unique=True,
        postgresql_where=sa.text("state = 'open' AND dedupe_key IS NOT NULL"),
    )


def downgrade() -> None:
    # Losers stay suppressed: the duplicate-open state they held was invalid,
    # and dropping the index does not restore it.
    op.drop_index("uq_observe_insights_open_dedupe", table_name="observe_insights")
