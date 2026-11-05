"""Track final-report delivery and bounded retries across checker instances."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "c43a1b93e824"
down_revision = "b43f0a82d713"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'job_notifications',
        sa.Column('id', UUID(), primary_key=True),
        sa.Column('job_id', UUID(), sa.ForeignKey('jobs.id', ondelete='CASCADE'), nullable=False),
        sa.Column('channel', sa.String(8), nullable=False),
        sa.Column('recipient', sa.String(320), nullable=False),
        sa.Column('subject', sa.Text(), nullable=False),
        sa.Column('body', sa.Text(), nullable=False),
        sa.Column('state', sa.String(16), nullable=False),
        sa.Column('delivery_attempts', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('next_attempt_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('claimed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('claim_token', UUID(), nullable=True),
        sa.Column('sent_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.UniqueConstraint('job_id', 'channel', 'recipient', name='uq_job_notification_recipient'),
    )
    op.create_index('ix_job_notification_delivery', 'job_notifications', ['state', 'next_attempt_at'])


def downgrade():
    op.drop_table('job_notifications')
