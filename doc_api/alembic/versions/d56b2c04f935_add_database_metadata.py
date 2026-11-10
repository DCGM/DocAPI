"""Record the running code commit and last successful migration timestamp."""
from alembic import op
import sqlalchemy as sa

revision = "d56b2c04f935"
down_revision = "c43a1b93e824"
branch_labels = None
depends_on = None


def upgrade():
    table = op.create_table(
        "database_metadata",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=False),
        sa.Column("git_commit", sa.String(64), nullable=True),
        sa.Column("migrated_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("id = 1", name="ck_database_metadata_singleton"),
    )
    # No historical values can be inferred. The online migration runner records
    # this migration's actual commit/time after the whole batch succeeds.
    op.bulk_insert(table, [{"id": 1, "git_commit": None, "migrated_at": None}])


def downgrade():
    op.drop_table("database_metadata")
