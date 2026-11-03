"""Retain the original image count on each job.

Revision ID: 97c2e01b6a45
Revises: f1c824b73a90
"""
from alembic import op
import sqlalchemy as sa


revision = "97c2e01b6a45"
down_revision = "f1c824b73a90"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("jobs", sa.Column("image_count", sa.Integer(), nullable=True))
    # Prefer the original definition: image rows may already have been cleaned up.
    # Legacy definitions without an image list fall back to the remaining rows.
    op.execute(sa.text("""
        UPDATE jobs
        SET image_count = CASE
            WHEN jsonb_typeof(definition -> 'images') = 'array'
                THEN jsonb_array_length(definition -> 'images')
            ELSE (SELECT COUNT(*) FROM images WHERE images.job_id = jobs.id)
        END
    """))
    op.alter_column("jobs", "image_count", existing_type=sa.Integer(), nullable=False)


def downgrade():
    op.drop_column("jobs", "image_count")
