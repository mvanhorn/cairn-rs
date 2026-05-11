# Evaluations

Eval datasets, rubrics, runs, baselines, scorecards, matrices (guardrail, memory-quality, permissions, prompt-comparison, provider-routing, skill-health), and dashboard.

Source of truth: [`tests/compat/http_routes.tsv`](../../tests/compat/http_routes.tsv). Drift from this table against the live router is enforced by `cargo test -p cairn-api --test compat_catalog_sync`.

**Routes: 29**

| Method | Path | Classification | Notes |
|---|---|---|---|
| `GET` | `/v1/evals/assets/:asset_id/export` | Preserve |  |
| `GET` | `/v1/evals/assets/:asset_id/report` | Preserve |  |
| `GET` | `/v1/evals/assets/:asset_id/trend` | Preserve |  |
| `GET` | `/v1/evals/assets/:asset_id/winner` | Preserve |  |
| `GET` | `/v1/evals/baselines` | Preserve | query: tenant_id?; { items, has_more } |
| `POST` | `/v1/evals/baselines` | Preserve |  |
| `GET` | `/v1/evals/baselines/:id` | Preserve |  |
| `GET` | `/v1/evals/compare` | Preserve |  |
| `GET` | `/v1/evals/dashboard` | Preserve |  |
| `GET` | `/v1/evals/datasets` | Preserve | query: limit?; { items } |
| `POST` | `/v1/evals/datasets` | Preserve |  |
| `GET` | `/v1/evals/datasets/:id` | Preserve |  |
| `POST` | `/v1/evals/datasets/:id/entries` | Preserve |  |
| `GET` | `/v1/evals/matrices/guardrail` | Preserve | { rows } |
| `GET` | `/v1/evals/matrices/memory-quality` | Preserve | { rows } |
| `GET` | `/v1/evals/matrices/permissions` | Preserve | { rows } |
| `GET` | `/v1/evals/matrices/prompt-comparison` | Preserve | { rows } |
| `GET` | `/v1/evals/matrices/provider-routing` | Preserve |  |
| `GET` | `/v1/evals/matrices/skill-health` | Preserve | { rows } |
| `GET` | `/v1/evals/rubrics` | Preserve | query: tenant_id?; { items, has_more } |
| `POST` | `/v1/evals/rubrics` | Preserve |  |
| `GET` | `/v1/evals/rubrics/:id` | Preserve |  |
| `GET` | `/v1/evals/runs` | Preserve | query: tenant_id?, workspace_id?, project_id?, limit?, offset?, include_archived?; { items, has_more } |
| `POST` | `/v1/evals/runs` | Preserve | 201 on create; 409 when `eval_run_id` already exists (issue #244); 404 on dangling dataset/rubric/baseline |
| `GET` | `/v1/evals/runs/:id` | Preserve |  |
| `DELETE` | `/v1/evals/runs/:id` | Preserve | Soft-delete via `EvalRunArchived` (issue #244). 204 on first delete and on re-delete (idempotent). 404 when missing or cross-project |
| `POST` | `/v1/evals/runs/:id/compare-baseline` | Preserve |  |
| `POST` | `/v1/evals/runs/:id/complete` | Preserve |  |
| `POST` | `/v1/evals/runs/:id/score` | Preserve |  |
| `POST` | `/v1/evals/runs/:id/score-rubric` | Preserve |  |
| `POST` | `/v1/evals/runs/:id/start` | Preserve |  |
| `GET` | `/v1/evals/scorecard/:asset_id` | Preserve |  |
| `GET` | `/v1/evals/scorecards` | Preserve | Scorecard summary list for the EvalsPage picker (issue #244). One row per `(project, prompt_asset_id)` with at least one completed, non-archived run. Sorted by `best_task_success_rate` desc. `{ items: ScorecardSummary[], has_more }` |

<!-- TODO: contract bodies (tracked as follow-up) -->
