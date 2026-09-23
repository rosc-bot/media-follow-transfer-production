from dataclasses import dataclass
from enum import StrEnum


class TransferStatus(StrEnum):
    QUEUED = 'QUEUED'
    RUNNING = 'RUNNING'
    RETRY_WAIT = 'RETRY_WAIT'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'
    SKIPPED = 'SKIPPED'


@dataclass(frozen=True)
class TransferOutcome:
    success: bool
    verified: bool
    remote_folder_id: str | None = None
    remote_files: tuple[str, ...] = ()
    error: str | None = None
    remote_series_folder_id: str | None = None
    remote_destination_kind: str | None = None
