class DomainError(Exception):
    """Base exception for the new media pipeline."""


class LegacyDatabaseTargetError(DomainError):
    pass


class IngestRejected(DomainError):
    pass


class TransferNotAllowed(DomainError):
    pass


class TransferVerificationError(DomainError):
    pass
