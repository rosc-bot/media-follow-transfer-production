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
