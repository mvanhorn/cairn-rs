# Sessions

Session aggregate: list/detail, active-runs, activity, cost, events, export, LLM traces, and child runs. A session groups a series of runs under one conversation or workflow.

Source of truth: [`tests/compat/http_routes.tsv`](../../tests/compat/http_routes.tsv). Drift from this table against the live router is enforced by `cargo test -p cairn-api --test compat_catalog_sync`.

**Routes: 12**

| Method | Path | Classification | Notes |
|---|---|---|---|
| `GET` | `/v1/sessions` | Preserve |  |
| `POST` | `/v1/sessions` | Preserve |  |
| `GET` | `/v1/sessions/:id` | Preserve |  |
| `GET` | `/v1/sessions/:id/active-runs` | Preserve |  |
| `GET` | `/v1/sessions/:id/activity` | Preserve |  |
| `GET` | `/v1/sessions/:id/cost` | Preserve |  |
| `GET` | `/v1/sessions/:id/events` | Preserve |  |
| `GET` | `/v1/sessions/:id/export` | Preserve |  |
| `GET` | `/v1/sessions/:id/llm-traces` | Preserve | path param: id; LLM call traces for session |
| `GET` | `/v1/sessions/:session_id/llm-traces/:trace_id/body` | Preserve | #668 chain-of-thought body; post-redaction `{system_prompt, messages_json, response_text, tool_calls_json, model_id, recorded_at_ms}`; 404 when trace id unknown OR `CAIRN_LLM_TRACE_BODIES_ENABLED=false` |
| `GET` | `/v1/sessions/:id/runs` | Preserve |  |
| `DELETE` | `/v1/sessions/:id/snapshots` | Preserve | F65 PR-5: admin-only immediate reap of every workspace snapshot belonging to the session. Returns `{reaped, at_ms}`. |
| `POST` | `/v1/sessions/import` | Preserve |  |

<!-- TODO: contract bodies (tracked as follow-up) -->
