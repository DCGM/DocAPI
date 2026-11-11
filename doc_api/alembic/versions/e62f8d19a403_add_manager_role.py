"""Add the manager role without granting workflow overrides."""
from alembic import op
import sqlalchemy as sa

revision = "e62f8d19a403"
down_revision = "d56b2c04f935"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TYPE keyrole ADD VALUE 'MANAGER'")


def downgrade():
    # Never silently promote managers to admins or discard their keys.
    if op.get_bind().scalar(sa.text("SELECT EXISTS (SELECT 1 FROM keys WHERE role::text = 'MANAGER')")):
        raise RuntimeError("Reassign manager keys to another role before downgrading.")
    op.execute("ALTER TYPE keyrole RENAME TO keyrole_with_manager")
    op.execute("CREATE TYPE keyrole AS ENUM ('READONLY', 'USER', 'WORKER', 'ADMIN')")
    op.execute("ALTER TABLE keys ALTER COLUMN role TYPE keyrole USING role::text::keyrole")
    op.execute("DROP TYPE keyrole_with_manager")
