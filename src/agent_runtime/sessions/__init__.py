"""Built-in sessions; database.py is reserved for a future backend."""

from .base import Session, SessionError, ToolRecoveryRequired
from .local import LocalSession

__all__ = ["Session", "LocalSession", "SessionError", "ToolRecoveryRequired"]
