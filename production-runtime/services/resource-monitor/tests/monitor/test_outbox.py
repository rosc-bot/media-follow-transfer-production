import sqlite3
from types import SimpleNamespace

from app.models.channel import ChannelSetting
from app.monitor.resource_monitor import ResourceMonitor


def test_resource_outbox_persists_message_before_delivery(tmp_path):
    monitor=ResourceMonitor(outbox_path=str(tmp_path/'resource_messages.db'))
    msg=SimpleNamespace(id=9,chat_id=-1,text='资源 S01E01 https://pan.guangyapan.com/s/x',message='资源 S01E01 https://pan.guangyapan.com/s/x',media=None,fwd_from=None,forward=None,forward_date=None,entities=None,buttons=None,date=None)
    chat=SimpleNamespace(id=-1,username='resource',title='资源频道')
    setting=ChannelSetting(channel_id='-1',role='RESOURCE',enabled=True,transfer_mode='AUTO')
    assert monitor.capture(msg,chat,setting) is not None
    with sqlite3.connect(tmp_path/'resource_messages.db') as db:
        assert db.execute('select count(*) from messages').fetchone()[0] == 1
        assert db.execute('select count(*) from resource_outbox where status="PENDING"').fetchone()[0] == 1
