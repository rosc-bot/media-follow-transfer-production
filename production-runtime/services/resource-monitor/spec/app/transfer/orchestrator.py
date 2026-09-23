from app.core.exceptions import TransferVerificationError
from app.transfer.adapters import CloudAdapter, DryRunAdapter
from app.transfer.adapters.alist import AlistAdapter
from app.transfer.adapters.guangya import GuangyaAdapter
from app.transfer.adapters.mobile import MobileAdapter
from app.transfer.completion_policy import verified_completion
from app.transfer.status import TransferOutcome


class TransferOrchestrator:
    def __init__(self, adapters: dict[str, CloudAdapter] | None = None) -> None:
        self.adapters = adapters or {
            'guangya': GuangyaAdapter(), 'mobile': MobileAdapter(), 'alist': AlistAdapter(), 'dry-run': DryRunAdapter(),
        }

    async def execute(self, payload: dict) -> TransferOutcome:
        provider = str(payload.get('provider') or 'guangya').lower()
        adapter = self.adapters.get(provider)
        if adapter is None:
            raise ValueError(f'unsupported cloud provider: {provider}')
        outcome = await adapter.transfer(payload)
        if not outcome.success:
            raise TransferVerificationError(outcome.error or f'{provider} transfer failed')
        expected = payload.get('expected_files') or []
        if not verified_completion(expected, list(outcome.remote_files), provider_verified=outcome.verified):
            raise TransferVerificationError('provider accepted transfer but remote readback did not verify expected files')
        return outcome
