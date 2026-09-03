"""Public errors raised by the MemWeave domain and protocol layers."""


class MemWeaveError(Exception):
    """Base class for expected MemWeave errors."""


class ProtocolVersionError(MemWeaveError):
    """Raised when a protocol version is unsupported."""


class AuthorizationError(MemWeaveError):
    """Raised when a caller attempts an operation outside its scope."""


class StaleWriteError(MemWeaveError):
    """Raised when an update is based on an old memory version."""


class ProjectionConflictError(MemWeaveError, ValueError):
    """Raised when an applied stream sequence is reused for different content."""


class SessionProjectionIntegrityError(MemWeaveError, RuntimeError):
    """Raised when a session snapshot is not backed by a complete receipt chain."""


class SessionSequenceGapError(MemWeaveError, ValueError):
    """Raised when a session event is not the next contiguous sequence."""
