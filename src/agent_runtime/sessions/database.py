"""Optional PostgreSQL session, opened asynchronously with an in-memory journal view."""

from copy import deepcopy
from uuid import uuid4

from .base import Session, replay_records, saved_options


class DatabaseSession(Session):
    durable = True

    def __init__(self, store, *, tenant_id, audit_context, session_id=None):
        """Construct a NEW session. Existing sessions must use await open()."""
        super().__init__(session_id)
        if not tenant_id:
            raise ValueError("tenant_id is required")
        self.store = store
        self.tenant_id = tenant_id
        self.audit_context = audit_context
        self._records = []

    @classmethod
    async def open(cls, store, **kwargs):
        session = cls(store, **kwargs)
        records = await store.read_records(session.tenant_id, session.session_id)
        session._state = replay_records(records)
        session._seq = len(records)
        session._records = records
        return session

    def read_records(self, after_seq=-1):
        """Loaded view, with this writer's commits. No synchronous network I/O."""
        return deepcopy(self._records[after_seq + 1 :])

    async def read_latest_records(self, after_seq=-1):
        """Read current storage without changing this execution's projection."""
        return await self.store.read_records(self.tenant_id, self.session_id, after_seq)

    async def _persist(self, seq, record):
        await self.store.append(self, record)
        self._records.append(deepcopy(record))

    async def _import_records(self, records):
        await self.store.import_records(self.tenant_id, self.session_id, records)
        self._records = deepcopy(records)

    async def fork(self, event_id, *, session_id=None, audit_context=None):
        target = DatabaseSession(
            self.store,
            tenant_id=self.tenant_id,
            session_id=session_id,
            audit_context=audit_context or self.audit_context,
        )
        return await self.fork_into(event_id, target)

    def wrap_stream_fn(self, stream_fn):
        """Record each physical adapter invocation, including recovery and compaction."""
        from ..storage.tracking import tracked_stream

        async def stream(model, context, options):
            purpose = options.get("_purpose", "generation")
            attempt = uuid4().hex
            data = dict(
                attempt_id=attempt, provider=model.provider, model=model.id, purpose=purpose
            )
            if purpose == "compaction":
                data.update(messages=context.messages, options=saved_options(options))
            await self.commit("model_call_started", **data)
            return await tracked_stream(self, attempt, stream_fn, model, context, options)

        return stream
