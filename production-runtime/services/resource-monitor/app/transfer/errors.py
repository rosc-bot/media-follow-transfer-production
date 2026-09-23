"""Structured transfer error taxonomy.

Every cloud failure is classified into a :class:`TransferErrorCategory` with an
explicit retry policy so the queue worker never has to guess from message text.

Retry semantics
---------------
* ``retry_current`` — same resource may be retried with backoff (timeouts, 5xx, 429).
* ``switch_resource`` — the resource itself is bad; future candidates must be
  given a chance (no permanent blocking, but no blind same-resource retry).
* ``terminal`` — do not retry and do not switch: block investigation instead.
"""

from enum import StrEnum

import httpx

from app.core.exceptions import DomainError, TransferVerificationError


class TransferErrorCategory(StrEnum):
    AUTH_EXPIRED = 'AUTH_EXPIRED'
    AUTH_INVALID = 'AUTH_INVALID'
    INVALID_SHARE = 'INVALID_SHARE'
    SHARE_NOT_FOUND = 'SHARE_NOT_FOUND'
    EMPTY_SHARE = 'EMPTY_SHARE'
    NO_VIDEO_FILES = 'NO_VIDEO_FILES'
    EPISODE_MISMATCH = 'EPISODE_MISMATCH'
    RATE_LIMITED = 'RATE_LIMITED'
    NETWORK_TIMEOUT = 'NETWORK_TIMEOUT'
    NETWORK_ERROR = 'NETWORK_ERROR'
    REMOTE_5XX = 'REMOTE_5XX'
    READBACK_UNVERIFIED = 'READBACK_UNVERIFIED'
    DESTINATION_NOT_FOUND = 'DESTINATION_NOT_FOUND'
    INSUFFICIENT_SPACE = 'INSUFFICIENT_SPACE'
    UNKNOWN = 'UNKNOWN'


class TransferRetryPolicy(StrEnum):
    RETRY_CURRENT = 'retry_current'
    SWITCH_RESOURCE = 'switch_resource'
    TERMINAL = 'terminal'


RETRY_POLICY: dict[TransferErrorCategory, TransferRetryPolicy] = {
    TransferErrorCategory.AUTH_EXPIRED: TransferRetryPolicy.TERMINAL,
    TransferErrorCategory.AUTH_INVALID: TransferRetryPolicy.TERMINAL,
    TransferErrorCategory.INVALID_SHARE: TransferRetryPolicy.SWITCH_RESOURCE,
    TransferErrorCategory.SHARE_NOT_FOUND: TransferRetryPolicy.SWITCH_RESOURCE,
    TransferErrorCategory.EMPTY_SHARE: TransferRetryPolicy.SWITCH_RESOURCE,
    TransferErrorCategory.NO_VIDEO_FILES: TransferRetryPolicy.SWITCH_RESOURCE,
    TransferErrorCategory.EPISODE_MISMATCH: TransferRetryPolicy.SWITCH_RESOURCE,
    TransferErrorCategory.RATE_LIMITED: TransferRetryPolicy.RETRY_CURRENT,
    TransferErrorCategory.NETWORK_TIMEOUT: TransferRetryPolicy.RETRY_CURRENT,
    TransferErrorCategory.NETWORK_ERROR: TransferRetryPolicy.RETRY_CURRENT,
    TransferErrorCategory.REMOTE_5XX: TransferRetryPolicy.RETRY_CURRENT,
    TransferErrorCategory.READBACK_UNVERIFIED: TransferRetryPolicy.RETRY_CURRENT,
    TransferErrorCategory.DESTINATION_NOT_FOUND: TransferRetryPolicy.TERMINAL,
    TransferErrorCategory.INSUFFICIENT_SPACE: TransferRetryPolicy.TERMINAL,
    TransferErrorCategory.UNKNOWN: TransferRetryPolicy.RETRY_CURRENT,
}

TERMINAL_CATEGORIES = frozenset(
    category for category, policy in RETRY_POLICY.items() if policy == TransferRetryPolicy.TERMINAL
)

#: Categories whose queue task must end as FAILED instead of being re-scheduled:
#: terminal errors block investigation, switch_resource errors mark the resource
#: as unusable so a future candidate is not starved by endless same-resource retries.
NON_RETRYABLE_CATEGORIES = frozenset(
    category
    for category, policy in RETRY_POLICY.items()
    if policy in (TransferRetryPolicy.TERMINAL, TransferRetryPolicy.SWITCH_RESOURCE)
)


class GuangyaTransferError(DomainError):
    """A transfer failure with an explicit category."""

    def __init__(self, category: TransferErrorCategory, message: str) -> None:
        super().__init__(message)
        self.category = category


class GuangyaAuthExpiredError(GuangyaTransferError):
    """Access token expired and the single allowed refresh did not recover it."""

    def __init__(self, message: str = 'Guangya access token is expired and refresh did not restore credentials') -> None:
        super().__init__(TransferErrorCategory.AUTH_EXPIRED, message)


class NoVideoFilesError(GuangyaTransferError):
    """The share contains no file with a video extension."""

    def __init__(self, message: str = 'share contains no transferable video files') -> None:
        super().__init__(TransferErrorCategory.NO_VIDEO_FILES, message)


class ReadbackVerificationError(GuangyaTransferError):
    """Restore finished but the target directory did not contain every expected file."""

    def __init__(self, message: str = 'remote readback did not verify every expected file') -> None:
        super().__init__(TransferErrorCategory.READBACK_UNVERIFIED, message)


def classify_error(exc: BaseException) -> TransferErrorCategory:
    """Map any exception to one canonical category. Allowed to grow with new states."""
    if isinstance(exc, GuangyaTransferError):
        return exc.category
    if isinstance(exc, TransferVerificationError):
        return TransferErrorCategory.READBACK_UNVERIFIED
    if isinstance(exc, httpx.TimeoutException):
        return TransferErrorCategory.NETWORK_TIMEOUT
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status == 401:
            return TransferErrorCategory.AUTH_EXPIRED
        if status == 403:
            return TransferErrorCategory.AUTH_INVALID
        if status == 429:
            return TransferErrorCategory.RATE_LIMITED
        if status >= 500:
            return TransferErrorCategory.REMOTE_5XX
        if status in (404, 410):
            return TransferErrorCategory.SHARE_NOT_FOUND
        return TransferErrorCategory.UNKNOWN
    if isinstance(exc, httpx.HTTPError):
        return TransferErrorCategory.NETWORK_ERROR
    if isinstance(exc, TimeoutError):
        return TransferErrorCategory.NETWORK_TIMEOUT
    if isinstance(exc, OSError):
        return TransferErrorCategory.NETWORK_ERROR
    return TransferErrorCategory.UNKNOWN
