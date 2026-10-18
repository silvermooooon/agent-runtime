"""Small retained facts, projected in the same transaction as the source event."""

from datetime import datetime, timezone

from psycopg.types.json import Jsonb


def instant(milliseconds):
    return datetime.fromtimestamp(milliseconds / 1000, timezone.utc)


async def project(conn, session, record):
    tenant, sid = session.info.tenant_id, session.session_id
    kind, data, run = record["type"], record["data"], record["run_id"]
    user_id = session.info.user_id
    at = instant(record["time"])
    if kind in ("run_started", "run_resumed"):
        source = data["source"] if kind == "run_started" else session.run_source
        root_run_id = data["root_run_id"] if kind == "run_started" else session.root_run_id

        await conn.execute(
            """INSERT INTO agent_runs
            (tenant_id,session_id,run_id,user_id,root_run_id,source,started_at,status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,'running')
            ON CONFLICT (tenant_id,session_id,run_id)
            DO UPDATE SET status='running',finished_at=NULL""",
            (
                tenant,
                sid,
                run,
                user_id,
                root_run_id,
                source,
                at,
            ),
        )
    elif kind in ("run_completed", "run_interrupted"):
        await conn.execute(
            """UPDATE agent_runs SET status=%s,finished_at=%s
            WHERE tenant_id=%s AND session_id=%s AND run_id=%s""",
            ("completed" if kind == "run_completed" else data["status"], at, tenant, sid, run),
        )
    elif kind == "model_call_started":
        await conn.execute(
            """INSERT INTO agent_model_usage
            (tenant_id,attempt_id,session_id,run_id,root_run_id,user_id,provider,model,
             purpose,started_at,status,usage_status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'unknown','unknown')""",
            (
                tenant,
                data["attempt_id"],
                sid,
                run,
                session.root_run_id,
                user_id,
                data["provider"],
                data["model"],
                data["purpose"],
                at,
            ),
        )
    elif kind == "model_call_finished":
        message = data.get("message") or {}
        usage = message.get("usage", {})
        # Custom adapters must explicitly mark a complete, provider-reported usage result.
        known = message.get("usageStatus") == "known"
        keys = ("input", "output", "cacheRead", "cacheWrite", "totalTokens")
        if known and any(type(usage.get(k)) is not int or usage[k] < 0 for k in keys):
            known = False
        await conn.execute(
            """UPDATE agent_model_usage SET finished_at=%s,status=%s,
            input_tokens=%s,output_tokens=%s,cache_read_tokens=%s,cache_write_tokens=%s,
            total_tokens=%s,usage_status=%s,metadata=%s
            WHERE tenant_id=%s AND attempt_id=%s""",
            (
                at,
                data["status"],
                *(usage.get(k) if known else None for k in keys),
                "known" if known else "unknown",
                Jsonb(
                    {
                        "usage": usage,
                        "provider_response_id": message.get("responseId"),
                        "event_id": record["id"],
                    }
                ),
                tenant,
                data["attempt_id"],
            ),
        )
