"""Operational indexes for queue, ingest and watchlist reads."""
from alembic import op

revision = '0002_media_pipeline_indexes'
down_revision = '0001_initial_media_pipeline'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index('ix_watchlist_status_updated', 'series_watchlist', ['status', 'updated_at'])
    op.create_index('ix_resource_episode', 'resources', ['tmdb_id', 'season', 'episode'])
    op.create_index('ix_channel_enabled_role', 'channel_settings', ['enabled', 'role'])
    op.create_index('ix_channel_ingest_jobs_share_hash', 'channel_ingest_jobs', ['share_hash'])
    op.create_index('ix_ingest_job_status', 'channel_ingest_jobs', ['status', 'created_at'])
    op.create_index('ix_transfer_job_status', 'transfer_jobs', ['status', 'updated_at'])
    op.create_index('ix_transfer_queue_claim', 'transfer_queue_tasks', ['status', 'next_run_at', 'priority'])


def downgrade() -> None:
    for name, table in (
        ('ix_transfer_queue_claim', 'transfer_queue_tasks'), ('ix_transfer_job_status', 'transfer_jobs'),
        ('ix_ingest_job_status', 'channel_ingest_jobs'), ('ix_channel_ingest_jobs_share_hash', 'channel_ingest_jobs'),
        ('ix_channel_enabled_role', 'channel_settings'), ('ix_resource_episode', 'resources'),
        ('ix_watchlist_status_updated', 'series_watchlist')):
        op.drop_index(name, table_name=table)
