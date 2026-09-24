import pytest
import telethon

from app.monitor.telegram_gateway import TelegramGateway


class _EventBuilder:
    def __init__(self, name):
        self.name = name


class _Events:
    @staticmethod
    def NewMessage():
        return _EventBuilder("new")

    @staticmethod
    def MessageEdited():
        return _EventBuilder("edited")


class _FakeClient:
    def __init__(self, *_args):
        self.handlers = []
        self.connected = False

    def on(self, builder):
        def register(handler):
            self.handlers.append((builder.name, handler))
            return handler

        return register

    async def start(self):
        self.connected = True

    async def get_me(self):
        return type("Me", (), {"first_name": "test", "username": "test"})()

    async def run_until_disconnected(self):
        return None

    def is_connected(self):
        return self.connected

    async def disconnect(self):
        self.connected = False


@pytest.mark.asyncio
async def test_resource_monitor_registers_message_edited_on_the_single_client(monkeypatch, tmp_path):
    monkeypatch.setattr(telethon, "TelegramClient", _FakeClient)
    monkeypatch.setattr(telethon, "events", _Events)
    gateway = TelegramGateway(
        session_path=str(tmp_path / "one-session"),
        api_id=1,
        api_hash="test",
        summary_db_path=str(tmp_path / "summary.db"),
        resource_db_path=str(tmp_path / "resource.db"),
    )

    async def no_channel_settings():
        return {}

    monkeypatch.setattr(gateway, "load_channel_settings", no_channel_settings)

    await gateway.start()
    registrations = [name for name, _handler in gateway.client.handlers]
    await gateway.stop()

    assert registrations == ["new", "edited"]
