import httpx
import pytest


@pytest.fixture(autouse=True)
def mock_telegram_notifier(monkeypatch):
    """Fail closed at HTTP I/O; keep real notifier routing/cards/results intact.

    Notification tests must explicitly mock HTTP requests. Intercept both HTTPX
    transports so an omitted mock can never emit a real Telegram message.
    """
    original_async = httpx.AsyncHTTPTransport.handle_async_request
    original_sync = httpx.HTTPTransport.handle_request

    async def guarded_async_request(self, request):
        if request.url.host == "api.telegram.org":
            raise httpx.ConnectError("Telegram network access is disabled in tests", request=request)
        return await original_async(self, request)

    def guarded_sync_request(self, request):
        if request.url.host == "api.telegram.org":
            raise httpx.ConnectError("Telegram network access is disabled in tests", request=request)
        return original_sync(self, request)

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", guarded_async_request)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", guarded_sync_request)
