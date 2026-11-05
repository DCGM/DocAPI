"""Track input URL presence; the URLs remain in the original job definition.

Revision ID: c42d8f1e6a90
Revises: 97c2e01b6a45
"""
from alembic import op
import sqlalchemy as sa


revision = "c42d8f1e6a90"
down_revision = "97c2e01b6a45"
branch_labels = None
depends_on = None


def upgrade():
    for kind in ("image", "alto", "page"):
        op.add_column("images", sa.Column(f"{kind}_url_provided", sa.Boolean(),
                                          nullable=False, server_default=sa.false()))
    # Definitions survive image cleanup and downgrade. Recover presence flags
    # from them without copying the URLs into image records.
    op.execute(sa.text("""
        UPDATE images AS image
        SET image_url_provided = item.value ->> 'image_url' IS NOT NULL,
            alto_url_provided = item.value ->> 'alto_url' IS NOT NULL,
            page_url_provided = item.value ->> 'page_url' IS NOT NULL
        FROM jobs AS job
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE WHEN jsonb_typeof(job.definition -> 'images') = 'array'
                 THEN job.definition -> 'images' ELSE '[]'::jsonb END
        ) AS item(value)
        WHERE image.job_id = job.id AND image.name = item.value ->> 'name'
          AND (item.value ->> 'image_url' IS NOT NULL
               OR item.value ->> 'alto_url' IS NOT NULL
               OR item.value ->> 'page_url' IS NOT NULL)
    """))
    for kind in ("image", "alto", "page"):
        op.alter_column("images", f"{kind}_url_provided", server_default=None)


def downgrade():
    for kind in ("page", "alto", "image"):
        op.drop_column("images", f"{kind}_url_provided")
