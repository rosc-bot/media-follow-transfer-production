from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    app_env: str = 'development'
    database_url: str = Field(default='sqlite+aiosqlite:///./data/media_follow_transfer.db')
    sync_database_url: str = Field(default='sqlite:///./data/media_follow_transfer.db')
    telegram_bot_token: str = ''
    telegram_bot_username: str = '@zhuixin001_bot'
    transfer_success_chat: str = '@guangyazhauncun'
    resource_publish_chat: str = '@guangyaziyuanfenxiang'
    failure_notification_chat: str = ''
    telegram_api_id: int | None = None
    telegram_api_hash: str = ''
    tg_session_path: str = './data/tg_monitor.session'
    summary_db_path: str = './data/tg_messages.db'
    resource_db_path: str = './data/resource_messages.db'
    resource_monitor_session: str = './data/tg_monitor.session'
    resource_messages_db: str = './data/resource_messages.db'
    tmdb_api_key: str = ''
    tmdb_access_token: str = ''
    tmdb_base_url: str = 'https://api.themoviedb.org/3'
    follow_poll_interval_seconds: int = 900
    follow_recent_episode_window: int = 30
    cloud_write_enabled: bool = False
    #: Process-scoped canary override — NEVER set in the long-lived .env.
    #: tools/run_transfer_canary.py reads it from its own environment so a
    #: single canary process can write while the ordinary Transfer Worker
    #: keeps CLOUD_WRITE_ENABLED=false / transfer_paused=1 (Phase 2C §九/§十三).
    canary_cloud_write_enabled: bool = False
    admin_tg_id: int | None = 8586984520
    channel_id: str = '-1004387965244'
    framehdr_enabled: bool = True
    framehdr_base_url: str = 'https://framehdr.com'
    framehdr_username: str = ''
    framehdr_password: str = ''
    framehdr_cookie_file: str = './data/framehdr_cookies.json'

    @field_validator('telegram_api_id', 'admin_tg_id', mode='before')
    @classmethod
    def parse_optional_int(cls, value: object) -> int | None:
        if value is None or str(value).strip() == '':
            return None
        return int(value)

    @field_validator('database_url', 'sync_database_url')
    @classmethod
    def reject_legacy_database_targets(cls, value: str) -> str:
        lowered = value.lower()
        forbidden = ('zhuixin', 'tg-media', 'submission', 'watchlist.db', 'tg_messages.db')
        if any(token in lowered for token in forbidden):
            raise ValueError('new project database URL must not target a legacy database')
        return value

    def assert_safe_for_bootstrap(self) -> None:
        if self.app_env == 'production' and self.cloud_write_enabled is False:
            return
        if self.database_url.endswith('watchlist.db') or self.database_url.endswith('tg_messages.db'):
            raise ValueError('legacy database path rejected')


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.assert_safe_for_bootstrap()
    return settings


settings = get_settings()
