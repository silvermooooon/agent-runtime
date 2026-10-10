-- Existing deployments apply this migration once, after 001_sessions.sql.
-- Only positions are indexed. Checkpoint states remain exclusively in the journal.
BEGIN;
CREATE TABLE agent_session_checkpoints (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    seq bigint NOT NULL CHECK (seq >= 0),
    event_id text NOT NULL,
    PRIMARY KEY (tenant_id, session_id, seq),
    UNIQUE (tenant_id, session_id, event_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES agent_sessions
);
COMMIT;
