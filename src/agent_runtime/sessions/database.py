"""Optional PostgreSQL session, retaining only the current recovery state."""

from uuid import uuid4

from .base import Session, SessionError, replay_records, saved_options


class DatabaseSession(Session):
    durable = True

    def __init__(self, store, *, tenant_id, user_id=None, session_id=None):
        """Construct a NEW session. Existing sessions must use await open()."""
        super().__init__(session_id, tenant_id=tenant_id, user_id=user_id)
        if not tenant_id:
            raise ValueError("tenant_id is required")
        self.store = store

    @classmethod
    async def open(cls, store, *, tenant_id, session_id):
        session = cls(store, tenant_id=tenant_id, session_id=session_id)
        records = await store.read_recovery_records(session.info.tenant_id, session.session_id)
        if not records:
            raise FileNotFoundError(f"Unknown session: {session_id}")
        session._restore(records)
        session._require_identity()
        return session

    def read_records(self, after_seq=-1):
        raise SessionError("Database history requires await session.aread_records()")

    def snapshot_at(self, event_id):
        raise SessionError("Database history requires await session.asnapshot_at(event_id)")

    async def aread_records(self, after_seq=-1):
        """Read storage on demand without retaining history or changing execution state."""
        return await self.store.read_records(self.info.tenant_id, self.session_id, after_seq)

    async def read_latest_records(self, after_seq=-1):
        return await self.aread_records(after_seq)

    async def asnapshot_at(self, event_id):
        records = await self.store.read_recovery_records(
            self.info.tenant_id, self.session_id, event_id=event_id
        )
        return replay_records(records)

    def _require_identity(self):
        if not self.info.tenant_id or not self.info.user_id:
            raise ValueError(
                "DatabaseSession requires tenant_id and user_id before its first write"
            )

    async def _persist(self, seq, record):
        self._require_identity()
        await self.store.append(self, record)

    async def _import_records(self, records):
        self._require_identity()
        await self.store.import_records(self.info, records)

    async def fork(self, event_id, *, session_id=None):
        target = DatabaseSession(
            self.store,
            tenant_id=self.info.tenant_id,
            session_id=session_id,
            user_id=self.info.user_id,
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
