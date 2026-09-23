import pytest

from app.transfer.adapters.guangya import GuangyaAdapter


class NoNetworkGuangyaAdapter(GuangyaAdapter):
    async def post(self, client, url, payload, headers):
        raise AssertionError('skip_readback must be rejected before any cloud request')


@pytest.mark.asyncio
async def test_guangya_adapter_does_not_allow_skip_readback_to_report_success():
    adapter = NoNetworkGuangyaAdapter(write_enabled=True)

    outcome = await adapter.transfer({
        'share_url': 'https://pan.guangyapan.com/s/share',
        'target_folder_id': 'target',
        'auth_token': 'access-token',
        'skip_readback': True,
    })

    assert outcome.success is False
    assert outcome.verified is False
    assert outcome.error == 'readback verification is mandatory'
