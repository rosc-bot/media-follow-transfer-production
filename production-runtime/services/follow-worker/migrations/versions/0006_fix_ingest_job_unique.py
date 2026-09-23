"""Update channel_ingest_jobs unique constraint to include share_hash.

Revision ID: 0006_fix_ingest_job_unique
Revises: 0005_cloud_disk_inventory
Create Date: 2026-09-21
"""

from alembic import op

revision = '0006_fix_ingest_job_unique'
down_revision = '0005_cloud_disk_inventory'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('channel_ingest_jobs') as batch_op:
        batch_op.drop_constraint('uq_ingest_job_source', type_='unique')
        batch_op.create_unique_constraint('uq_ingest_job_source', ['channel_id', 'message_id', 'share_hash'])


def downgrade() -> None:
    with op.batch_alter_table('channel_ingest_jobs') as batch_op:
        batch_op.drop_constraint('uq_ingest_job_source', type_='unique')
        batch_op.create_unique_constraint('uq_ingest_job_source', ['channel_id', 'message_id'])
