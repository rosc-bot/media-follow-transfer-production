"""Unit tests for Telegram Link Extractor."""

from app.ingest.url_extractor import extract_all_urls, identify_cloud_drive


class DummyEntity:
    def __init__(self, url=None, ent_type="text_link"):
        self.url = url
        self.type = ent_type


class DummyButton:
    def __init__(self, text="", url=None, callback_data=None):
        self.text = text
        self.url = url
        self.data = callback_data


class DummyMessage:
    def __init__(
        self,
        text=None,
        caption=None,
        message=None,
        media=None,
        entities=None,
        caption_entities=None,
        buttons=None,
        reply_markup=None,
    ):
        self.text = text
        self.caption = caption
        self.message = message or text
        self.media = media
        self.entities = entities or []
        self.caption_entities = caption_entities or []
        self.buttons = buttons
        self.reply_markup = reply_markup


def test_plain_text_url():
    msg = DummyMessage(text="资源已发布，夸克网盘链接：https://pan.quark.cn/test 欢迎转存")
    res = extract_all_urls(msg)
    assert "https://pan.quark.cn/test" in res["text_urls"]
    assert "https://pan.quark.cn/test" in res["all_urls"]


def test_hidden_entity_link():
    msg_entity = DummyMessage(
        text="点击查看资源",
        entities=[DummyEntity(url="https://pan.baidu.com/s/hidden123", ent_type="text_link")],
    )
    res_entity = extract_all_urls(msg_entity)
    assert "https://pan.baidu.com/s/hidden123" in res_entity["entity_urls"]

    msg_md = DummyMessage(text="最新影视剧集：[查看资源](https://pan.quark.cn/s/md_hidden_456)")
    res_md = extract_all_urls(msg_md)
    assert "https://pan.quark.cn/s/md_hidden_456" in res_md["entity_urls"]
    assert res_md["text_urls"] == []


def test_inline_keyboard_button_url():
    btn = DummyButton(text="🔗 光鸭云盘：查看资源", url="https://pan.guangyapan.com/s/gy123456")
    msg = DummyMessage(text="今日新片更新，点击下方按钮转存：", buttons=[[btn]])
    res = extract_all_urls(msg)
    assert "https://pan.guangyapan.com/s/gy123456" in res["button_urls"]


def test_duplicate_link_deduplication():
    identical_url = "https://pan.quark.cn/s/same_resource_url"
    btn = DummyButton(text="网盘直达", url=identical_url)
    msg = DummyMessage(text=f"点击链接或下方按钮：{identical_url}", buttons=[[btn]])
    res = extract_all_urls(msg)
    assert res["all_urls"] == [identical_url]


def test_cloud_drive_identification():
    assert identify_cloud_drive("https://pan.guangyapan.com/s/gy1") == "guangya"
    assert identify_cloud_drive("https://pan.quark.cn/s/qk1") == "quark"
    assert identify_cloud_drive("https://unknown-domain-test.xyz/file/video.mkv") is None
