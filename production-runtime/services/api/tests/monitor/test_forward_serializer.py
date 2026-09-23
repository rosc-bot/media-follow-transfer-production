from types import SimpleNamespace

from app.models.channel import ChannelSetting
from app.monitor.telegram_serializer import serialize_telegram_message


def test_telethon_forward_is_preserved():
    message = SimpleNamespace(id=8, chat_id=-100, text='资源 S01E01', message='资源 S01E01', media=None,
                              fwd_from=SimpleNamespace(), forward=None, forward_date=None, entities=None, buttons=None, date=None)
    chat = SimpleNamespace(id=-100, username='test', title='测试频道')
    setting = ChannelSetting(channel_id='-100', role='MANUAL_INGEST', accept_forward=True, transfer_mode='AUTO')
    payload = serialize_telegram_message(message, chat, setting)
    assert payload['source_type'] == 'manual_forward'
    assert payload['is_forward'] is True
    assert payload['channel_id'] == '-100'
