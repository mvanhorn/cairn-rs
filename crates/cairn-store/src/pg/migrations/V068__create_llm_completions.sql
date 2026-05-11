-- Issue #668: LLM chain-of-thought body projection.
--
-- Sibling to the `provider_calls` / `llm_traces` projections — those
-- hold metadata (tokens, latency, cost). This table holds the actual
-- round-trip *body*: system prompt, user messages, response text,
-- and proposed tool calls. Operators use it to audit what the LLM
-- was shown and what it said.
--
-- `trace_id` is the primary key and matches
-- `provider_calls.provider_call_id` for the same call, so operators
-- can join the body to the metadata row.
--
-- Text fields are redacted at emit time (see
-- `cairn_providers::redact::redact_secrets`) and individually
-- capped by `CAIRN_LLM_TRACE_MAX_FIELD_BYTES` (default 256 KiB).

CREATE TABLE IF NOT EXISTS llm_completions (
    trace_id         TEXT    PRIMARY KEY,
    tenant_id        TEXT    NOT NULL,
    workspace_id     TEXT    NOT NULL,
    project_id       TEXT    NOT NULL,
    session_id       TEXT    NOT NULL,
    run_id           TEXT,
    model_id         TEXT    NOT NULL,
    system_prompt    TEXT    NOT NULL DEFAULT '',
    messages_json    TEXT    NOT NULL DEFAULT '[]',
    response_text    TEXT    NOT NULL DEFAULT '',
    tool_calls_json  TEXT    NOT NULL DEFAULT '[]',
    recorded_at_ms   BIGINT  NOT NULL,
    created_at       BIGINT  NOT NULL
);

-- Index on (session_id, recorded_at_ms) for the UI "Reasoning" tab's
-- "list bodies for a session in chronological order" query. Also
-- covers the common operator query "show me what the LLM saw for
-- this session". run_id index for per-run filtering in telemetry
-- views.
CREATE INDEX IF NOT EXISTS idx_llm_completions_session_time
    ON llm_completions (session_id, recorded_at_ms);

CREATE INDEX IF NOT EXISTS idx_llm_completions_run
    ON llm_completions (run_id)
    WHERE run_id IS NOT NULL;

-- Tenant-scoped cleanup index for the retention sweeper (follow-up
-- PR) — deleting rows older than N days per tenant.
CREATE INDEX IF NOT EXISTS idx_llm_completions_tenant_time
    ON llm_completions (tenant_id, recorded_at_ms);
