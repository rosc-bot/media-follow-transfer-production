from typing import Protocol

from app.core.config import get_settings
from app.core.exceptions import TransferNotAllowed
from app.transfer.status import TransferOutcome


class CloudAdapter(Protocol):
    async def transfer(self, payload: dict) -> TransferOutcome: ...


#: Guard (Phase 2C §九/§十三): canary write permission must come from the
#: process environment only (CANARY_CLOUD_WRITE_ENABLED=true on the one-shot
#: canary command), never from the long-lived .env.  The ordinary Transfer
#: Worker therefore always boots with cloud_write_enabled=false in production
#: until explicitly authorized.  Two gates are required for any real cloud
#: write: the settings gate AND the canary process gate.
def effective_cloud_write_enabled(*, permit_canary_env: bool = False) -> bool:
    settings = get_settings()
    return bool(settings.cloud_write_enabled or (permit_canary_env and settings.canary_cloud_write_enabled))


class BaseAdapter:
    provider = 'unknown'

    def __init__(self, *, write_enabled: bool | None = None) -> None:
        self.write_enabled = get_settings().cloud_write_enabled if write_enabled is None else write_enabled

    async def transfer(self, payload: dict) -> TransferOutcome:
        if not self.write_enabled:
            raise TransferNotAllowed('cloud writes are disabled for development/test mode')
        raise NotImplementedError


class DryRunAdapter(BaseAdapter):
    provider = 'dry-run'

    async def transfer(self, payload: dict) -> TransferOutcome:
        return TransferOutcome(success=True, verified=True, remote_folder_id='dry-run', remote_files=tuple(payload.get('expected_files', [])))
