"""readonly key role

Revision ID: 8caddabf41e7
Revises: 034c101797ae
Create Date: 2025-10-24 16:53:01.000517

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8caddabf41e7'
down_revision: Union[str, Sequence[str], None] = '034c101797ae'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add the new value 'READONLY' to the existing enum type 'keyrole'
    op.execute("ALTER TYPE keyrole ADD VALUE IF NOT EXISTS 'READONLY';")


def downgrade() -> None:
    """Downgrade schema."""
    # Downgrade by recreating the enum type without 'READONLY'
    op.execute("ALTER TYPE keyrole RENAME TO keyrole_old;")
    op.execute("CREATE TYPE keyrole AS ENUM ('USER', 'WORKER', 'ADMIN');")
    op.execute("ALTER TABLE keys ALTER COLUMN role TYPE keyrole USING role::text::keyrole;")
    op.execute("DROP TYPE keyrole_old;")
