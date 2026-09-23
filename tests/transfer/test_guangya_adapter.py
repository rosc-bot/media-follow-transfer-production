import pytest

from app.core.exceptions import TransferNotAllowed
from app.transfer.adapters.guangya import GuangyaAdapter


@pytest.mark.asyncio
async def test_guangya_adapter_never_writes_when_disabled():
    adapter=GuangyaAdapter(write_enabled=False)
    with pytest.raises(TransferNotAllowed):
        await adapter.transfer({'share_url':'https://pan.guangyapan.com/s/x','target_folder_id':'1','auth_token':'token'})


def test_guangya_auth_parsing_is_secret_safe():
    assert GuangyaAdapter.parse_auth_tokens('{"access_token":"a","refresh_token":"b"}') == ('a','b')
    assert GuangyaAdapter.share_parts('https://pan.guangyapan.com/s/share?code=123') == ('share','123')
