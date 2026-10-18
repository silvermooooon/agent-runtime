-- PostgreSQL 14+. Apply explicitly once after 001, 002 and 003.
-- SDK-owned acceptance/ownership control only: runs, results and semantic facts
-- remain in the seven existing canonical Runtime tables and journal.
BEGIN;
CREATE TABLE agent_commands (
    tenant_id text NOT NULL,
    command_id text NOT NULL,
    session_id text NOT NULL,
    command_seq bigint NOT NULL CHECK (command_seq >= 1),
    kind text NOT NULL,
    -- Opaque semantic input may contain JSON Unicode escapes (including NUL),
    -- matching the canonical journal's json representation rather than jsonb.
    payload json NOT NULL,
    payload_hash text NOT NULL,
    parent_session_id text,
    run_id text,
    state text NOT NULL DEFAULT 'accepted'
        CHECK (state IN ('accepted', 'applied', 'completed', 'stopped', 'failed', 'rejected')),
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    cancel_requested_at timestamptz,
    applied_journal_seq bigint CHECK (applied_journal_seq >= 0),
    applied_event_id text,
    completed_journal_seq bigint CHECK (completed_journal_seq >= 0),
    completed_event_id text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, command_id),
    UNIQUE (tenant_id, session_id, command_seq),
    UNIQUE (tenant_id, session_id, run_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES agent_sessions
);
CREATE INDEX agent_commands_pending
    ON agent_commands (tenant_id, session_id, command_seq)
    WHERE state = 'accepted' AND kind = 'prompt';
CREATE INDEX agent_commands_kind
    ON agent_commands (tenant_id, session_id, kind, command_seq);
CREATE INDEX agent_commands_parent
    ON agent_commands (tenant_id, parent_session_id, session_id, command_seq);
CREATE INDEX agent_commands_state
    ON agent_commands (tenant_id, state, accepted_at, command_id);
CREATE TABLE agent_session_leases (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    holder text NOT NULL,
    fence bigint NOT NULL CHECK (fence >= 1),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, session_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES agent_sessions
);
CREATE INDEX agent_session_leases_expiration ON agent_session_leases (expires_at);
COMMIT;
