"""PostgreSQL journal + retained audit example. Uses an offline model, no LLM charges.

Apply the packaged DDL first, configure AGENT_DB_URL, then:
uv run --extra postgres --extra s3 --env-file .env python examples/database_session.py
"""

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from agent_runtime import Agent, AssistantMessageEventStream, Model, assistant_message
from agent_runtime.sessions import DatabaseSession
from agent_runtime.storage import ArchiveConfig, DatabaseConfig
from agent_runtime.storage.audit import AuditContext
from agent_runtime.storage.postgres import PostgresStore
from agent_runtime.storage.s3 import S3Archive


def offline(model, context, options):
    stream = AssistantMessageEventStream()
    stream.push(
        {
            "type": "done",
            "message": assistant_message(
                model,
                content=[{"type": "text", "text": "Stored in PostgreSQL."}],
                stopReason="stop",
            ),
        }
    )
    return stream


async def main():
    config = ArchiveConfig.from_env()
    store = await PostgresStore.connect(
        DatabaseConfig.from_env(), archive=S3Archive(config) if config.enabled else None
    )
    try:
        session = await DatabaseSession.open(
            store,
            tenant_id="example",
            session_id="database-example",
            audit_context=AuditContext("example-user"),
        )
        await store.audit(
            tenant_id="example",
            audit_id=uuid4().hex,
            actor_id="example-user",
            actor_type="user",
            user_id="example-user",
            session_id=session.session_id,
            action="input_submitted",
            resource_type="session",
            resource_id=session.session_id,
            outcome="success",
        )
        agent = Agent(
            model=Model("offline", "local", "fake", ""), session=session, stream_fn=offline
        )
        if session.resumable:
            await agent.resume()
        else:
            await agent.prompt("Record this conversation")
        start = datetime.now(timezone.utc) - timedelta(days=1)
        end = datetime.now(timezone.utc) + timedelta(days=1)
        print("Journal revision:", session.revision)
        print(
            "Usage (offline adapter intentionally has unknown tokens):",
            await store.daily_usage("example", "example-user", start, end),
        )
    finally:
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())
