"""Initial isolated schema for media-follow-transfer."""
import sqlalchemy as sa
from alembic import op

revision = '0001_initial_media_pipeline'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('series_watchlist',
        sa.Column('id', sa.Integer(), primary_key=True), sa.Column('tmdb_id', sa.Integer(), nullable=False),
        sa.Column('title', sa.String(512), nullable=False), sa.Column('year', sa.Integer()),
        sa.Column('media_type', sa.String(32), nullable=False), sa.Column('season', sa.Integer(), nullable=False),
        sa.Column('status', sa.String(32), nullable=False), sa.Column('follow_mode', sa.String(32), nullable=False),
        sa.Column('total_episodes', sa.Integer()), sa.Column('last_aired_episode', sa.Integer()),
        sa.Column('collected_episodes', sa.JSON(), nullable=False), sa.Column('poster_path', sa.String(1024)),
        sa.Column('source', sa.String(128)), sa.Column('subscriber_tg_id', sa.Integer()),
        sa.Column('last_sync_at', sa.DateTime(timezone=True)), sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint('tmdb_id', 'season', 'subscriber_tg_id', name='uq_watchlist_series_subscriber'))
    op.create_table('resources',
        sa.Column('id', sa.Integer(), primary_key=True), sa.Column('identity_key', sa.String(512), nullable=False),
        sa.Column('tmdb_id', sa.Integer()), sa.Column('title', sa.String(512)), sa.Column('media_type', sa.String(32), nullable=False),
        sa.Column('year', sa.Integer()), sa.Column('season', sa.Integer()), sa.Column('episode', sa.Integer()),
        sa.Column('episode_key', sa.String(64)), sa.Column('version_key', sa.String(128)), sa.Column('cloud_name', sa.String(64)),
        sa.Column('share_url', sa.Text(), nullable=False), sa.Column('source_type', sa.String(32), nullable=False),
        sa.Column('source_channel_id', sa.String(128)), sa.Column('source_message_id', sa.Integer()),
        sa.Column('status', sa.String(32), nullable=False), sa.Column('file_names', sa.JSON(), nullable=False),
        sa.Column('transferred_folder_id', sa.String(256)), sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('accepted_at', sa.DateTime(timezone=True)), sa.UniqueConstraint('identity_key', name='uq_resource_identity_key'))
    op.create_table('channel_settings',
        sa.Column('id', sa.Integer(), primary_key=True), sa.Column('channel_id', sa.String(128), nullable=False),
        sa.Column('channel_name', sa.String(512)), sa.Column('enabled', sa.Boolean(), nullable=False),
        sa.Column('role', sa.String(32), nullable=False), sa.Column('transfer_mode', sa.String(32), nullable=False),
        sa.Column('accept_forward', sa.Boolean(), nullable=False), sa.Column('default_provider', sa.String(64)),
        sa.Column('default_category', sa.String(128)), sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False), sa.UniqueConstraint('channel_id', name='uq_channel_setting_id'))
    op.create_table('cloud_configs',
        sa.Column('id', sa.Integer(), primary_key=True), sa.Column('name', sa.String(64), nullable=False, unique=True),
        sa.Column('domain_pattern', sa.String(256)), sa.Column('auth_ref', sa.Text()), sa.Column('target_folder_id', sa.String(256)),
        sa.Column('enabled', sa.Boolean(), nullable=False), sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False))
    op.create_table('app_settings',
        sa.Column('id', sa.Integer(), primary_key=True), sa.Column('key', sa.String(128), nullable=False, unique=True),
        sa.Column('value', sa.Text()), sa.Column('is_secret_ref', sa.Boolean(), nullable=False), sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False))
    op.create_table('channel_ingest_messages',
        sa.Column('id', sa.Integer(), primary_key=True), sa.Column('channel_id', sa.String(128), nullable=False),
        sa.Column('message_id', sa.Integer(), nullable=False), sa.Column('source_type', sa.String(32), nullable=False),
        sa.Column('is_forward', sa.Boolean(), nullable=False), sa.Column('payload', sa.JSON(), nullable=False),
        sa.Column('status', sa.String(32), nullable=False), sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint('channel_id', 'message_id', name='uq_ingest_message_source'))
    op.create_table('channel_ingest_jobs',
        sa.Column('id', sa.Integer(), primary_key=True), sa.Column('channel_id', sa.String(128), nullable=False),
        sa.Column('message_id', sa.Integer(), nullable=False), sa.Column('source_type', sa.String(32), nullable=False),
        sa.Column('is_forward', sa.Boolean(), nullable=False), sa.Column('share_url', sa.Text()), sa.Column('share_hash', sa.String(128)),
        sa.Column('status', sa.String(32), nullable=False), sa.Column('parsed_data', sa.JSON(), nullable=False),
        sa.Column('media_type', sa.String(32)), sa.Column('tmdb_id', sa.Integer()), sa.Column('title', sa.String(512)),
        sa.Column('year', sa.Integer()), sa.Column('season', sa.Integer()), sa.Column('detected_episodes', sa.JSON(), nullable=False),
        sa.Column('identity_status', sa.String(32)), sa.Column('ready_for_transfer', sa.Boolean(), nullable=False),
        sa.Column('transfer_status', sa.String(32)), sa.Column('error_message', sa.Text()),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False), sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint('channel_id', 'message_id', name='uq_ingest_job_source'))
    op.create_table('transfer_jobs',
        sa.Column('id', sa.Integer(), primary_key=True), sa.Column('resource_id', sa.Integer(), nullable=False),
        sa.Column('provider', sa.String(64), nullable=False), sa.Column('status', sa.String(32), nullable=False),
        sa.Column('target_folder_id', sa.String(256)), sa.Column('expected_files', sa.JSON(), nullable=False),
        sa.Column('result', sa.JSON(), nullable=False), sa.Column('attempt_count', sa.Integer(), nullable=False),
        sa.Column('max_retries', sa.Integer(), nullable=False), sa.Column('last_error', sa.Text()),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False), sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False))
    op.create_table('transfer_queue_tasks',
        sa.Column('id', sa.Integer(), primary_key=True), sa.Column('task_type', sa.String(64), nullable=False),
        sa.Column('resource_id', sa.Integer(), nullable=False), sa.Column('idempotency_key', sa.String(512), nullable=False),
        sa.Column('status', sa.String(32), nullable=False), sa.Column('priority', sa.Integer(), nullable=False),
        sa.Column('payload', sa.JSON(), nullable=False), sa.Column('result', sa.JSON(), nullable=False), sa.Column('error_message', sa.Text()),
        sa.Column('attempt_count', sa.Integer(), nullable=False), sa.Column('max_retries', sa.Integer(), nullable=False),
        sa.Column('next_run_at', sa.DateTime(timezone=True), nullable=False), sa.Column('locked_at', sa.DateTime(timezone=True)),
        sa.Column('locked_by', sa.String(128)), sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False), sa.UniqueConstraint('idempotency_key', name='uq_transfer_queue_idempotency'))


def downgrade() -> None:
    # Downgrade is intentionally not used by production migration tooling.
    for table in ('transfer_queue_tasks','transfer_jobs','channel_ingest_jobs','channel_ingest_messages','app_settings','cloud_configs','channel_settings','resources','series_watchlist'):
        op.drop_table(table)
