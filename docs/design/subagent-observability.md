# Subagent-spawn observability (#661)

This doc is the operator pointer for telling whether cairn's orchestrator is
actually delegating work to subagents, or whether it's running every task inline
and ignoring the delegation guidance baked into the role prompt.

## Why this exists

Across four dogfood rounds of the roguelike task, the orchestrator LLM never
once emitted a `spawn_subagent` decision — every tool call landed inline despite
the task being structurally amenable to a planner → executor → reviewer split.
#662 rewrote the orchestrator role prompt with explicit trigger conditions and
a worked delegation example. #661 (this PR) is the observability layer that
answers the follow-up question: *did the prompt rewrite change LLM behaviour?*

If the metrics below stay at their pre-#662 baseline through subsequent dogfood
rounds, the prompt still isn't working and we escalate.

## How to read the `/metrics` scrape

Three Prometheus series carry the subagent-delegation signal. Scrape
`GET /metrics` on any running cairn-app instance:

| Series | Type | What it means |
|---|---|---|
| `cairn_orchestrator_subagent_spawn_total` | counter | Number of `SpawnSubagent` proposals the LLM has emitted since process start. Bumped once per proposal at DECIDE time — includes proposals that subsequently fail validation / permission checks, so this measures LLM *intent*. A persistent `0` across many runs is the diagnostic signal #661 surfaces: the LLM is not delegating. |
| `cairn_orchestrator_iterations_per_run` | histogram | Distribution of final-iteration counts observed when a run reaches a terminal state (completed / failed / canceled). The p95 bucket tells you whether runs are shorter after delegation lands — a well-delegated run spends fewer top-level iterations because the work moved to children. |
| `cairn_orchestrator_inline_run_ratio` | gauge | Fraction of the most recent 100 terminal runs that finished without spawning any subagent. Expected to drop below 1.0 after #662's prompt cascades through a few dogfood rounds. Omitted when the window is empty (`cairn_orchestrator_inline_run_ratio_samples 0`) — a 0.0 sample with no data would mislead. |

A companion gauge `cairn_orchestrator_inline_run_ratio_samples` reports the
current window size (0..=100) so dashboards can tell "0 because new" from
"0 because zero delegation".

## How to read a single run

`GET /v1/runs/:id` now returns three subagent-lineage fields on the run
record, populated by the detail endpoint only:

```json
{
  "run": {
    "run_id": "r_abc",
    "state": "completed",
    "subagents_spawned": 2,
    "subagents_completed": 1,
    "subagents_failed": 1
  }
}
```

- `subagents_spawned` — total child runs under this parent (non-terminal + terminal)
- `subagents_completed` — child runs that reached `RunState::Completed`
- `subagents_failed` — child runs that reached `RunState::Failed` or `RunState::Canceled`

Counts are computed at GET time from `RunReadModel::list_by_parent_run` so the
projection stays flat — no new column, no migration. The list endpoint
(`GET /v1/runs`) omits these fields to keep batch scans cheap.

## Sanity check commands

```bash
# Counter at boot — should read 0 until the first LLM delegation lands.
curl -s http://localhost:3000/metrics | grep cairn_orchestrator_subagent_spawn_total

# Last-100-runs inline ratio + window size.
curl -s http://localhost:3000/metrics | grep cairn_orchestrator_inline_run_ratio

# Per-run subagent counts on a specific run.
curl -s -H "Authorization: Bearer $CAIRN_ADMIN_TOKEN" \
  http://localhost:3000/v1/runs/r_abc | jq '.run | {subagents_spawned, subagents_completed, subagents_failed}'
```

## Closing the loop

The diagnostic outcome we're looking for, one round after #662's prompt lands
on a live dogfood target:

- `cairn_orchestrator_subagent_spawn_total` climbs monotonically as runs fire,
- `cairn_orchestrator_inline_run_ratio` drops below 1.0,
- `cairn_orchestrator_iterations_per_run` p95 shifts left (shorter top-level runs).

Any of these stuck at their pre-#662 values after multiple rounds is a signal
that the prompt rewrite did not move LLM behaviour. The escalation path is to
look at the DECIDE-phase traces on the runs in question (`GET /v1/runs/:id/telemetry`
if RFC 021 OTLP export is enabled) and refine the prompt again.
