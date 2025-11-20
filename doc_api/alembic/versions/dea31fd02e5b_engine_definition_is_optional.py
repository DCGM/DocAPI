"""engine definition is optional

Revision ID: dea31fd02e5b
Revises: 803f39610312
Create Date: 2025-11-20 14:43:24.415094

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'dea31fd02e5b'
down_revision: Union[str, Sequence[str], None] = '803f39610312'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.alter_column(
        'engines',
        'definition',
        existing_type=sa.dialects.postgresql.JSONB(),
        nullable=True,
    )


def downgrade():
    op.alter_column(
        'engines',
        'definition',
        existing_type=sa.dialects.postgresql.JSONB(),
        nullable=False,
    )
