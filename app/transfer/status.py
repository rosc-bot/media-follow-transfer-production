from dataclasses import dataclass
from enum import StrEnum


class TransferStatus(StrEnum):
    QUEUED = 'QUEUED'
    RUNNING = 'RUNNING'
    RETRY_WAIT = 'RETRY_WAIT'
    # COMPLETED is the current writer's terminal value. SUCCESS is retained
    # for historical rows created by the legacy queue implementation.
    COMPLETED = 'COMPLETED'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'
    SKIPPED = 'SKIPPED'


EXECUTION_ACTIVE_STATUSES = frozenset({
    str(TransferStatus.QUEUED),
    str(TransferStatus.RUNNING),
    str(TransferStatus.RETRY_WAIT),
})
REVIEW_STATUS = 'PENDING'


def is_execution_active_task(task: object) -> bool:
    """A review-fenced queued row is not claimable and cannot reserve an episode."""
    status = str(getattr(task, 'status', '') or '').strip().upper()
    if status not in EXECUTION_ACTIVE_STATUSES:
        return False
    if status == 'RUNNING':
        return True
    payload = getattr(task, 'payload', None) or {}
    classification = str(payload.get('preflight_classification') or '').strip().upper()
    return classification not in {'NEEDS_REVIEW', 'REJECTED'}


SUCCESS_TERMINAL_STATUSES = frozenset({
    TransferStatus.SUCCESS,
    TransferStatus.COMPLETED,
})


def is_success_terminal(status: object) -> bool:
    """Treat legacy SUCCESS and current COMPLETED as the same success state."""
    return str(status) in {str(value) for value in SUCCESS_TERMINAL_STATUSES}


@dataclass(frozen=True)
class TransferOutcome:
    success: bool
    verified: bool
    remote_folder_id: str | None = None
    remote_files: tuple[str, ...] = ()
    error: str | None = None
    remote_series_folder_id: str | None = None
    remote_destination_kind: str | None = None
    remote_file_records: tuple[dict, ...] = ()
    rename_status: str | None = None
    promotion_status: str | None = None
    verified_episode_files: tuple[dict, ...] = ()
