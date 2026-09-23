from collections.abc import Awaitable, Callable

from app.monitor.channel_filter import accepts_message
from app.monitor.outbox import ResourceOutbox
from app.monitor.telegram_serializer import serialize_telegram_message


class ResourceMonitor:
    def __init__(self, *, outbox_path: str, ingest_handler: Callable[[dict], Awaitable[object]] | None = None):
        self.outbox = ResourceOutbox(outbox_path)
        self.ingest_handler = ingest_handler

    def capture(self, message, chat, setting) -> dict | None:
        serialized = serialize_telegram_message(message, chat, setting)
        if not accepts_message(setting, is_forward=serialized['is_forward']):
            return None
        self.outbox.enqueue(serialized)
        return serialized

    async def deliver_pending(self, limit: int = 50) -> int:
        if self.ingest_handler is None:
            return 0
        delivered = 0
        for item in self.outbox.pending(limit):
            row_id=item.pop('id')
            try:
                await self.ingest_handler(item)
                self.outbox.mark_sent(row_id); delivered += 1
            except Exception as exc:  # noqa: BLE001 - durable outbox must retain every delivery error
                self.outbox.mark_retry(row_id, str(exc))
        return delivered

    async def run_telethon(self, *, session: str, api_id: int, api_hash: str, channels: list[str | int], settings_by_channel: dict[str, object]) -> None:
        """Listen only to configured resource/manual channels; summary groups are never subscribed."""
        from telethon import TelegramClient, events
        client = TelegramClient(session, api_id, api_hash)

        @client.on(events.NewMessage(chats=channels))
        async def on_message(event):
            chat = await event.get_chat()
            channel_id = str(getattr(event, 'chat_id', None) or getattr(chat, 'id', ''))
            setting = settings_by_channel.get(channel_id)
            self.capture(event.message, chat, setting)
            await self.deliver_pending()

        await client.start()
        await client.run_until_disconnected()
