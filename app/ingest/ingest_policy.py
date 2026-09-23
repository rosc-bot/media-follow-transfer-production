from dataclasses import dataclass

from app.core.constants import (
    CHANNEL_ROLE_MANUAL_INGEST,
    SOURCE_MANUAL_FORWARD,
)


@dataclass(frozen=True)
class IngestDecision:
    accepted: bool
    source_type: str
    auto_transfer: bool
    reason: str = ''


def decide_ingest(*, source_type: str, is_forward: bool, setting: object | None) -> IngestDecision:
    role = getattr(setting, 'role', None) if setting else None
    enabled = getattr(setting, 'enabled', True)
    accept_forward = getattr(setting, 'accept_forward', False)
    transfer_mode = getattr(setting, 'transfer_mode', 'OFF')
    if enabled is False:
        return IngestDecision(False, source_type, False, 'channel disabled')
    if role == 'PUBLISH_ONLY':
        return IngestDecision(False, source_type, False, 'PUBLISH_ONLY channel is not an ingest source')
    if role == 'SUCCESS_NOTIFICATION':
        return IngestDecision(False, source_type, False, 'SUCCESS_NOTIFICATION channel is not an ingest source')
    if source_type in ('watchlist_scout', 'framehdr'):
        return IngestDecision(True, source_type, True)
    if source_type == SOURCE_MANUAL_FORWARD:
        if not is_forward or (role and role != CHANNEL_ROLE_MANUAL_INGEST) or not accept_forward:
            return IngestDecision(False, source_type, False, 'manual forward requires enabled MANUAL_INGEST channel')
        return IngestDecision(True, source_type, transfer_mode == 'AUTO')
    return IngestDecision(True, source_type, transfer_mode == 'AUTO')
