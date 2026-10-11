-- Session ownership is projected from its creation/fork journal events.
BEGIN;
ALTER TABLE agent_sessions ADD COLUMN user_id text;
CREATE INDEX agent_sessions_user_time ON agent_sessions (tenant_id, user_id, created_at);
COMMIT;
