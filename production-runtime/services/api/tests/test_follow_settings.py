from app.core.config import Settings


def test_settings_exposes_tmdb_and_follow_worker_controls():
    settings = Settings(tmdb_api_key='tmdb-key', tmdb_base_url='https://tmdb.example/3', follow_poll_interval_seconds=120)

    assert settings.tmdb_api_key == 'tmdb-key'
    assert settings.tmdb_base_url == 'https://tmdb.example/3'
    assert settings.follow_poll_interval_seconds == 120
