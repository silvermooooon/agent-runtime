"""Shared durable command and execution-ownership contracts.

The inbox is control state, not another event log. Canonical run and output facts
remain in DatabaseSession's journal. Leases fence local commits and dispatch
checks; they cannot retract a request already sent to an external service.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime

from ..sessions.base import SessionError

COMMAND_STATES = frozenset({"accepted", "applied", "completed", "stopped", "failed", "rejected"})


class CoordinationError(SessionError):
    """Durable command or execution coordination failed."""


class CommandConflictError(CoordinationError):
    """An existing command ID was reused for different immutable content."""


class ExecutionLeaseError(CoordinationError):
    """The execution lease is missing, expired, or superseded."""


@dataclass(frozen=True)
class ExecutionLease:
    tenant_id: str
    session_id: str
    holder: str
    fence: int
    expires_at: datetime


def validate_identifier(value, name):
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"{name} must be a non-empty string without NUL characters")
    return value


def validate_ttl(ttl_seconds):
    """Return a finite, positive lease duration in seconds."""
    if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, (int, float)):
        raise ValueError("ttl_seconds must be a finite positive number")
    try:
        ttl = float(ttl_seconds)
    except (OverflowError, ValueError) as error:
        raise ValueError("ttl_seconds must be a finite positive number") from error
    if not math.isfinite(ttl) or ttl <= 0:
        raise ValueError("ttl_seconds must be a finite positive number")
    return ttl


def validate_limit(limit):
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("limit must be a positive integer")
    return limit


def canonical_payload(payload):
    """Return an isolated JSON object and its stable SHA-256 content digest.

    Object keys must be strings, avoiding JSON's coercion of keys such as 1 and
    '1' into ambiguous duplicate names. NaN/infinities are never accepted.
    """
    if not isinstance(payload, dict):
        raise ValueError("command payload must be a JSON object")

    def check_keys(value, ancestors):
        if isinstance(value, (dict, list, tuple)):
            if id(value) in ancestors:
                raise ValueError("command payload must not contain circular values")
            ancestors = ancestors | {id(value)}
        if isinstance(value, dict):
            if any(not isinstance(key, str) for key in value):
                raise ValueError("command payload object keys must be strings")
            for item in value.values():
                check_keys(item, ancestors)
        elif isinstance(value, (list, tuple)):
            for item in value:
                check_keys(item, ancestors)

    try:
        check_keys(payload, set())
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        normalized = json.loads(encoded)
    except (TypeError, ValueError, RecursionError) as error:
        raise ValueError(f"command payload must be finite JSON: {error}") from error
    return normalized, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def command_transition(record):
    """Describe a command projection as (command_id, state, run_id), or None.

    A missing command_id on a run terminal/resume is resolved by its canonical
    session/run association. Interrupted/unknown tool work remains applied and
    must be recovered or explicitly abandoned before the next prompt.
    """
    kind, data = record["type"], record["data"]
    command_id = data.get("command_id")
    run_id = record.get("run_id") or data.get("run_id")
    if kind == "run_started" and command_id:
        return command_id, "applied", run_id
    if kind == "run_resumed":
        return command_id, "applied", run_id
    if kind == "run_completed":
        return command_id, "completed", run_id
    if kind == "run_interrupted" and data.get("status") in ("stopped", "failed"):
        return command_id, data["status"], run_id
    if kind == "history_reset" and command_id:
        return command_id, "stopped", run_id
    if kind == "command_rejected" and command_id:
        return command_id, "rejected", run_id
    return None
