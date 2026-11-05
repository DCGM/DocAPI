"""Add an optional notification email to API keys."""
from alembic import op
import sqlalchemy as sa

revision = "a43e9b71c602"
down_revision = "e51a7b3d902c"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("keys", sa.Column("email", sa.String(320), nullable=True))
    op.add_column("keys", sa.Column("email_notifications", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.alter_column("keys", "email_notifications", server_default=None)


def downgrade():
    op.drop_column("keys", "email_notifications")
    op.drop_column("keys", "email")
