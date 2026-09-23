from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.core.config import get_settings


class Base(DeclarativeBase):
    pass


def build_engine(url: str | None = None) -> AsyncEngine:
    target = url or get_settings().database_url
    kwargs = {'future': True}
    if target.startswith('sqlite'):
        kwargs['connect_args'] = {'check_same_thread': False}
    return create_async_engine(target, **kwargs)


engine = build_engine()
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session


async def create_schema() -> None:
    from app.models import import_all_models

    import_all_models()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
