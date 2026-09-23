from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class CloudConfig(Base):
    __tablename__ = 'cloud_configs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    domain_pattern: Mapped[str | None] = mapped_column(String(256))
    auth_ref: Mapped[str | None] = mapped_column(Text)
    target_folder_id: Mapped[str | None] = mapped_column(String(256))
    ongoing_target_folder_id: Mapped[str | None] = mapped_column(String(256))
    channel_id: Mapped[str | None] = mapped_column(String(64))
    enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

from sqlalchemy import Index, UniqueConstraint


class CloudDiskInventory(Base):
    __tablename__ = 'cloud_disk_inventory'
    __table_args__ = (
        UniqueConstraint('clean_title', 'tmdb_id', 'season', 'episode', name='uq_inventory_item'),
        Index('ix_inventory_clean_title', 'clean_title'),
        Index('ix_inventory_tmdb_id', 'tmdb_id'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    clean_title: Mapped[str] = mapped_column(String(255), nullable=False)
    season: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    tmdb_id: Mapped[int | None] = mapped_column(Integer)
    episode: Mapped[int] = mapped_column(Integer, nullable=False)
    file_name: Mapped[str] = mapped_column(String(512), nullable=False)
    rel_path: Mapped[str | None] = mapped_column(String(1024))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
