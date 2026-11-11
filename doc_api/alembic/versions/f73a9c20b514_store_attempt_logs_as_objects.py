"""Store sparse attempt logs as JSON objects keyed by attempt.

Revision ID: f73a9c20b514
Revises: e62f8d19a403
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "f73a9c20b514"
down_revision = "e62f8d19a403"
branch_labels = None
depends_on = None

FIELDS = ("log", "log_user", "final_log", "final_log_user")


def convert_rows(convert):
    connection = op.get_bind()
    jobs = sa.table("jobs", sa.column("id", sa.Uuid()),
                    sa.column("previous_attempts", sa.Integer()),
                    *(sa.column(field, JSONB(none_as_null=True)) for field in FIELDS))
    last_id = None
    while True:
        query = sa.select(jobs).where(sa.or_(*(jobs.c[field].is_not(None) for field in FIELDS)))
        if last_id is not None:
            query = query.where(jobs.c.id > last_id)
        rows = connection.execute(query.order_by(jobs.c.id).limit(500)).mappings().all()
        if not rows:
            return
        for row in rows:
            values = {field: convert(row[field], row["previous_attempts"]) for field in FIELDS}
            connection.execute(jobs.update().where(jobs.c.id == row["id"]).values(**values))
        last_id = rows[-1]["id"]


def upgrade():
    convert_rows(lambda entries, _: {
        str(index): entry for index, entry in enumerate(entries or []) if entry is not None
    } or None)


def downgrade():
    def as_array(entries, previous_attempts):
        if not entries:
            return None
        # Old readers use the last element as the current attempt. Keep trailing
        # placeholders so a quiet current attempt cannot expose an older log.
        current_index = 0 if previous_attempts is None else previous_attempts
        last_index = max(current_index, *(int(key) for key in entries))
        return [entries.get(str(index)) for index in range(last_index + 1)]
    convert_rows(as_array)
