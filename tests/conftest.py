import pytest
from app.transfer.notifier import TransferNotifier, NotificationResult

@pytest.fixture(autouse=True)
def mock_telegram_notifier(monkeypatch):
    """Globally mock TransferNotifier to prevent real telegram messages from leaking during tests."""
    async def fake_notify_success(self, *args, **kwargs):
        return NotificationResult(
            sent=True,
            target_chat_id="@mock_chat",
            target_source="mock",
            telegram_message_id=99999,
        )
    async def fake_notify_failure(self, *args, **kwargs):
        return NotificationResult(
            sent=True,
            target_chat_id="@mock_chat",
            target_source="mock",
            telegram_message_id=99999,
        )
    monkeypatch.setattr(TransferNotifier, "notify_success_result", fake_notify_success)
    monkeypatch.setattr(TransferNotifier, "notify_failure_result", fake_notify_failure)
    monkeypatch.setattr(TransferNotifier, "notify_success", fake_notify_success)
    monkeypatch.setattr(TransferNotifier, "notify_failure", fake_notify_failure)
    monkeypatch.setattr(TransferNotifier, "_send_telegram", fake_notify_success)
    monkeypatch.setattr(TransferNotifier, "_send_telegram_photo_result", fake_notify_success)
    monkeypatch.setattr(TransferNotifier, "_send_telegram_result", fake_notify_success)
