-- Dogfood R7 observability gap: add the tools[] array shipped TO the
-- model to `llm_completions` so operators can see what tool surface
-- the LLM had at decision time.
--
-- Motivation: R7 surfaced that the parent run's LLM emitted 5 identical
-- `spawn_subagent` calls in a row even with `complete_run` available.
-- Diagnosing whether the model actually saw `complete_run` in its
-- `tools[]` array required re-reading the orchestrator source because
-- the persisted trace body only carried `tool_calls_json` (what the
-- model emitted) and `messages_json` (what it saw in its message
-- history) — NOT the `tools[]` array the request shipped with.
--
-- The new column carries the JSON-serialised OpenAI-shape tool defs
-- the orchestrator built in `cairn-orchestrator::decide_impl::decide`
-- and handed to `RoutedGenerationService::generate`. Post-redaction.
--
-- Defaults: empty JSON array. Pre-V071 events replayed from the log
-- carry `#[serde(default = "default_empty_json_array")]` on
-- `LlmCompletionRecorded.tool_defs_json` (see `cairn-domain/src/events.rs`),
-- so they deserialise with `"[]"` rather than `""`. That keeps the
-- bound value a valid JSON array and matches this column DEFAULT so
-- operators reading legacy traces always see parseable JSON.

ALTER TABLE llm_completions
    ADD COLUMN tool_defs_json TEXT NOT NULL DEFAULT '[]';
