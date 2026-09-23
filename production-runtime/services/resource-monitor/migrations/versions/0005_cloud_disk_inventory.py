"""Add cloud_disk_inventory table for physical cloud disk assets.

Revision ID: 0005_cloud_disk_inventory
Revises: 0004_cloud_configs_channel_id
Create Date: 2026-09-21
"""

import sqlalchemy as sa
from alembic import op

revision = '0005_cloud_disk_inventory'
down_revision = '0004_cloud_configs_channel_id'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'cloud_disk_inventory',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('title', sa.String(length=255), nullable=False),
        sa.Column('clean_title', sa.String(length=255), nullable=False),
        sa.Column('season', sa.Integer(), server_default='1', nullable=False),
        sa.Column('tmdb_id', sa.Integer(), nullable=True),
        sa.Column('episode', sa.Integer(), nullable=False),
        sa.Column('file_name', sa.String(length=512), nullable=False),
        sa.Column('rel_path', sa.String(length=1024), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.UniqueConstraint('clean_title', 'tmdb_id', 'season', 'episode', name='uq_inventory_item'),
    )
    op.create_index('ix_inventory_clean_title', 'cloud_disk_inventory', ['clean_title'])
    op.create_index('ix_inventory_tmdb_id', 'cloud_disk_inventory', ['tmdb_id'])


def downgrade() -> None:
    op.drop_index('ix_inventory_tmdb_id', table_name='cloud_disk_inventory')
    op.drop_index('ix_inventory_clean_title', table_name='cloud_disk_inventory')
    op.drop_table('cloud_disk_inventory')
