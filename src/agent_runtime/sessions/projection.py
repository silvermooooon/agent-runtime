"""In-memory context projection. The journal is the only persisted source of truth."""

from copy import deepcopy

from ..transcript import current_system_message
from ..types import user_message


def append_messages(state, messages, record, field):
    state["messages"].extend(deepcopy(messages))
    state["message_ids"].extend(f"{record['id']}/{field}/{i}" for i in range(len(messages)))


def replace_messages(state, messages, record, field):
    state["messages"], state["message_ids"] = [], []
    state["compaction"] = None
    append_messages(state, messages, record, field)


def apply_compaction(state, record):
    data = record["data"]
    if state["phase"] not in ("idle", "after_turn", "model") or (
        state["phase"] == "model" and state["request"] is not None
    ):
        raise ValueError("Compaction requires a completed turn or an unstarted model request")
    if not isinstance(data["summary"], str) or not data["summary"].strip():
        raise ValueError("Compaction summary must be non-empty text")
    boundary = data["first_kept_message_id"]
    index = state["message_ids"].index(boundary) if boundary is not None else len(state["messages"])
    kept, kept_ids = state["messages"][index:], state["message_ids"][index:]
    first_kept = next((m for m in kept if m["role"] != "system"), None)
    if first_kept and first_kept["role"] == "toolResult":
        raise ValueError("Compaction must keep tool results with their assistant tool call")
    previous_id = (state["compaction"] or {}).get("summary_message_id")
    if previous_id in kept_ids:
        raise ValueError("The previous summary must be replaced, not retained")
    if not any(m["role"] != "system" for m in state["messages"][:index]):
        raise ValueError("Nothing to compact before the selected boundary")
    system = current_system_message(state["messages"])
    summary = user_message(
        "The conversation before this point was compacted into the following summary:\n\n"
        + data["summary"]
    )
    summary["timestamp"] = record["time"]
    # Preserve the effective system message in the compacted context.
    replace_messages(state, [system] if system else [], record, "system")
    append_messages(state, [summary], record, "summary")
    summary_id = state["message_ids"][-1]
    for message, message_id in zip(kept, kept_ids):
        if message["role"] != "system":
            state["messages"].append(message)
            state["message_ids"].append(message_id)
    state["compaction"] = {
        **deepcopy(data),
        "event_id": record["id"],
        "summary_message_id": summary_id,
    }
