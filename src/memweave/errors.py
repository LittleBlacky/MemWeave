"""Public errors raised by the MemWeave domain and protocol layers."""


class MemWeaveError(Exception):
    """Base class for expected MemWeave errors."""


class ProtocolVersionError(MemWeaveError):
    """Raised when a protocol version is unsupported."""


class AuthorizationError(MemWeaveError):
    """Raised when a caller attempts an operation outside its scope."""


class StaleWriteError(MemWeaveError):
    """Raised when an update is based on an old memory version."""
