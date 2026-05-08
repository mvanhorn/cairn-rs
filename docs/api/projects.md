# Projects

Project-scoped sub-resources: repos, run-templates, triggers (enable/disable/resume), plugin activation, memory/knowledge-provider configuration, per-family scoring policies, and cross-family provider + ingest-job reads (RFC 030). Project CRUD itself lives under `/v1/admin/workspaces/:ws/projects` (see `admin.md`).

Source of truth: [`tests/compat/http_routes.tsv`](../../tests/compat/http_routes.tsv). Drift from this table against the live router is enforced by `cargo test -p cairn-api --test compat_catalog_sync`.

**Routes: 30**

| Method | Path | Classification | Notes |
|---|---|---|---|
| `DELETE` | `/v1/projects/:proj/plugins/:id` | Preserve |  |
| `GET` | `/v1/projects/:tenant/:workspace/:project/costs` | Preserve | F29 CD-2: lifetime cost rollup (µUSD + tokens + provider calls). Zeros for never-billed projects. |
| `POST` | `/v1/projects/:proj/plugins/:id` | Preserve |  |
| `PUT` | `/v1/projects/:project/knowledge-provider` | Preserve | RFC 029: configure the project's knowledge provider. Body `{"provider_ref": "cairn-default"}` or `{"provider_ref": "plugin:<id>"}`. Emits `KnowledgeProviderConfigured`. |
| `PUT` | `/v1/projects/:project/memory-provider` | Preserve | RFC 030: configure the project's memory provider. Body `{"provider_ref": "cairn-default"}` or `{"provider_ref": "plugin:<id>"}`. Emits `MemoryProviderConfigured`. |
| `GET` | `/v1/projects/:project/providers` | Preserve | RFC 030: atomic read of both provider slots + resolved snapshots. |
| `GET` | `/v1/projects/:project/ingest-jobs` | Preserve | RFC 030: cross-family ingest-job listing. `?family=memory\|knowledge\|all` filter (default `all`). |
| `PUT` | `/v1/projects/:project/scoring-policy` | Preserve | RFC 029 PR-B2 legacy endpoint → **308 Permanent Redirect** to `/knowledge-scoring-policy` under RFC 030. Preserves body + method; operator CLIs from the pre-RFC-030 world keep working through the rollout. |
| `PUT` | `/v1/projects/:project/memory-scoring-policy` | Preserve | RFC 030: configure memory-family scoring policy. Non-zero weights on unsupported dimensions persist and return `warnings[]` (informational). |
| `PUT` | `/v1/projects/:project/knowledge-scoring-policy` | Preserve | RFC 030: knowledge-family scoring policy. Same warnings semantics. |
| `GET` | `/v1/projects/:project/memory-scoring-policy` | Preserve | RFC 030: read stored memory policy (or `ScoringPolicy::default()` + `using_default: true` when absent). |
| `GET` | `/v1/projects/:project/knowledge-scoring-policy` | Preserve | RFC 030: knowledge-family read. |
| `GET` | `/v1/projects/:project/memory-scoring-policy/valid-dimensions` | Preserve | RFC 030: dimensions the resolved memory provider surfaces. UIs grey out invalid weight controls. |
| `GET` | `/v1/projects/:project/knowledge-scoring-policy/valid-dimensions` | Preserve | RFC 030: knowledge-family. |
| `DELETE` | `/v1/projects/:project/local-paths` | Preserve | Detach a `host=local_fs` repo; body `{path}`. |
| `GET` | `/v1/projects/:project/repos` | Preserve |  |
| `POST` | `/v1/projects/:project/repos` | Preserve | `host` defaults to `"github"`; `local_fs` accepts an absolute path; `gitlab | gitea | confluence` return 501. |
| `DELETE` | `/v1/projects/:project/repos/:owner/:repo` | Preserve |  |
| `GET` | `/v1/projects/:project/repos/:owner/:repo` | Preserve |  |
| `GET` | `/v1/projects/:project/run-templates` | Preserve |  |
| `POST` | `/v1/projects/:project/run-templates` | Preserve |  |
| `DELETE` | `/v1/projects/:project/run-templates/:template_id` | Preserve |  |
| `GET` | `/v1/projects/:project/run-templates/:template_id` | Preserve |  |
| `GET` | `/v1/projects/:project/triggers` | Preserve |  |
| `POST` | `/v1/projects/:project/triggers` | Preserve |  |
| `DELETE` | `/v1/projects/:project/triggers/:trigger_id` | Preserve |  |
| `GET` | `/v1/projects/:project/triggers/:trigger_id` | Preserve |  |
| `POST` | `/v1/projects/:project/triggers/:trigger_id/disable` | Preserve |  |
| `POST` | `/v1/projects/:project/triggers/:trigger_id/enable` | Preserve |  |
| `POST` | `/v1/projects/:project/triggers/:trigger_id/resume` | Preserve |  |

<!-- TODO: contract bodies (tracked as follow-up) -->
