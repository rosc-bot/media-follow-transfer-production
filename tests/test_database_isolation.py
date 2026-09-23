import pytest
from pydantic import ValidationError

from app.core.config import Settings


def test_legacy_sqlite_targets_are_rejected():
    with pytest.raises(ValidationError):
        Settings(database_url='sqlite+aiosqlite:///./watchlist.db')
    with pytest.raises(ValidationError):
        Settings(sync_database_url='sqlite:///./tg_messages.db')


def test_new_database_target_is_allowed():
    settings = Settings(database_url='sqlite+aiosqlite:///./isolated.db', sync_database_url='sqlite:///./isolated.db')
    settings.assert_safe_for_bootstrap()
