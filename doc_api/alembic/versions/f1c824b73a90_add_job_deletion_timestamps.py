"""Add job data deletion timestamps.

Revision ID: f1c824b73a90
Revises: dea31fd02e5b
"""
from alembic import op
import sqlalchemy as sa


revision = "f1c824b73a90"
down_revision = "dea31fd02e5b"
branch_labels = None
depends_on = None


def upgrade():
    for name in ("inputs_deleted_at", "results_deleted_at", "artifacts_deleted_at"):
        op.add_column("jobs", sa.Column(name, sa.DateTime(timezone=True), nullable=True))


def downgrade():
    for name in ("artifacts_deleted_at", "results_deleted_at", "inputs_deleted_at"):
        op.drop_column("jobs", name)
