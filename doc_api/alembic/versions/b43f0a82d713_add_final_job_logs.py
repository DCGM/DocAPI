"""Separate final reports from progress logs without guessing legacy contents."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "b43f0a82d713"
down_revision = "a43e9b71c602"
branch_labels = None
depends_on = None


def upgrade():
    for field in ("final_log", "final_log_user"):
        op.add_column("jobs", sa.Column(field, JSONB(none_as_null=True), nullable=True))


def downgrade():
    for field in ("final_log", "final_log_user"):
        op.drop_column("jobs", field)
