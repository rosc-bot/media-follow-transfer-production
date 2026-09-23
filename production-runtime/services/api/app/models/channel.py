from datetime import datetime

from sqlalchemy import Boolean, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class ChannelSetting(Base):
    __tablename__ = 'channel_settings'
    __table_args__ = (UniqueConstraint('channel_id', name='uq_channel_setting_id'), Index('ix_channel_enabled_role', 'enabled', 'role'))

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    channel_id: Mapped[str] = mapped_column(String(128), nullable=False)
    channel_name: Mapped[str | None] = mapped_column(String(512))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    role: Mapped[str] = mapped_column(String(32), default='RESOURCE', nullable=False)
    transfer_mode: Mapped[str] = mapped_column(String(32), default='OFF', nullable=False)
    accept_forward: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    default_provider: Mapped[str | None] = mapped_column(String(64))
    default_category: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
