-- Issue #670 G2: add LLM delegation context to the subagent_spawns
-- read model.
--
-- The `goal` column mirrors the `ActionProposal.tool_args["goal"]`
-- string the LLM emitted on a `spawn_subagent` decision. The `role`
-- column mirrors `ActionProposal.tool_name` (one of `executor`,
-- `researcher`, `reviewer`). Both are populated by the execute layer
-- before `RuntimeEvent::SubagentSpawned` is appended (see G2 in
-- `#670`).
--
-- Defaults: empty string. Pre-G2 events on the event log deserialise
-- with empty strings via `#[serde(default)]` on `SubagentSpawned.goal`
-- and `SubagentSpawned.role`, so replay against a fresh pg schema
-- lands legal rows without NULL columns. The NOT NULL constraint
-- holds on inserts from post-G2 emitters, which always populate both
-- fields from the ActionProposal.

ALTER TABLE subagent_spawns
    ADD COLUMN goal TEXT NOT NULL DEFAULT '';

ALTER TABLE subagent_spawns
    ADD COLUMN role TEXT NOT NULL DEFAULT '';
