-- PostgreSQL 14+. Apply explicitly once; runtime startup never performs migrations.
BEGIN;
CREATE TABLE agent_sessions (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    next_seq bigint NOT NULL DEFAULT 0 CHECK (next_seq >= 0),
    metadata jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (tenant_id, session_id)
);
CREATE INDEX agent_sessions_updated ON agent_sessions (updated_at);
CREATE TABLE agent_session_events (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    seq bigint NOT NULL CHECK (seq >= 0),
    event_id text NOT NULL,
    record json NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (tenant_id, session_id, seq),
    UNIQUE (tenant_id, session_id, event_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES agent_sessions
);
CREATE TABLE agent_session_archives (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    start_seq bigint NOT NULL CHECK (start_seq >= 0),
    end_seq bigint NOT NULL CHECK (end_seq >= start_seq),
    object_uri text NOT NULL,
    version_id text,
    sha256 text NOT NULL,
    format_version smallint NOT NULL DEFAULT 1 CHECK (format_version = 1),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    metadata jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (tenant_id, session_id, start_seq),
    FOREIGN KEY (tenant_id, session_id) REFERENCES agent_sessions
);
CREATE TABLE agent_runs (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    run_id text NOT NULL,
    user_id text NOT NULL,
    root_run_id text NOT NULL,
    source text NOT NULL CHECK (source IN ('user', 'subagent', 'system')),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    status text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (tenant_id, session_id, run_id)
);
CREATE INDEX agent_runs_user_time ON agent_runs (tenant_id, user_id, started_at);
CREATE INDEX agent_runs_user_finished ON agent_runs (tenant_id, user_id, finished_at);
CREATE TABLE agent_audit_events (
    tenant_id text NOT NULL,
    audit_id text NOT NULL,
    actor_id text NOT NULL,
    actor_type text NOT NULL CHECK (actor_type IN ('user', 'agent', 'system')),
    user_id text NOT NULL,
    session_id text,
    run_id text,
    occurred_at timestamptz NOT NULL,
    action text NOT NULL,
    resource_type text NOT NULL,
    resource_id text NOT NULL,
    outcome text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (tenant_id, audit_id)
);
CREATE INDEX agent_audit_user_time ON agent_audit_events (tenant_id, user_id, occurred_at);
CREATE TABLE agent_model_usage (
    tenant_id text NOT NULL,
    attempt_id text NOT NULL,
    session_id text NOT NULL,
    run_id text,
    root_run_id text,
    user_id text NOT NULL,
    provider text NOT NULL,
    model text NOT NULL,
    purpose text NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    status text NOT NULL,
    input_tokens bigint,
    output_tokens bigint,
    cache_read_tokens bigint,
    cache_write_tokens bigint,
    total_tokens bigint,
    usage_status text NOT NULL CHECK (usage_status IN ('known', 'unknown')),
    metadata jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (tenant_id, attempt_id)
);
CREATE INDEX agent_usage_user_time ON agent_model_usage (tenant_id, user_id, finished_at);
CREATE INDEX agent_usage_root ON agent_model_usage (tenant_id, root_run_id);
COMMIT;
