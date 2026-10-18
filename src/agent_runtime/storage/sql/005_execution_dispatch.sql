-- Bounded central recovery reads; no additional task state or ownership table.
BEGIN;
-- Keep routing keys separate from JSON: semantic JSON may contain escaped NUL,
-- which PostgreSQL's JSON field extraction cannot process.
ALTER TABLE agent_session_events ADD COLUMN IF NOT EXISTS run_id text;
ALTER TABLE agent_commands ADD COLUMN IF NOT EXISTS parent_run_id text;
CREATE INDEX IF NOT EXISTS agent_commands_recovery
    ON agent_commands (updated_at,tenant_id,command_id)
    WHERE kind='prompt' AND state IN ('accepted','applied');
CREATE INDEX IF NOT EXISTS agent_runs_root_time
    ON agent_runs (tenant_id,root_run_id,started_at,session_id,run_id);
CREATE INDEX IF NOT EXISTS agent_events_run_seq
    ON agent_session_events (tenant_id,session_id,run_id,seq);
CREATE INDEX IF NOT EXISTS agent_commands_child_results
    ON agent_commands (accepted_at,tenant_id,command_id)
    WHERE kind='child_result' AND state='accepted';
CREATE INDEX IF NOT EXISTS agent_commands_parent_run
    ON agent_commands (tenant_id,parent_session_id,parent_run_id,session_id)
    WHERE kind='prompt' AND parent_session_id IS NOT NULL;
COMMIT;
