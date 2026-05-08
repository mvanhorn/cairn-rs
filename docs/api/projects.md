# Projects

Project-scoped sub-resources: repos, run-templates, triggers (enable/disable/resume), plugin activation, knowledge-provider configuration, and scoring-policy configuration. Project CRUD itself lives under `/v1/admin/workspaces/:ws/projects` (see `admin.md`).

Source of truth: [`tests/compat/http_routes.tsv`](../../tests/compat/http_routes.tsv). Drift from this table against the live router is enforced by `cargo test -p cairn-api --test compat_catalog_sync`.

**Routes: 21**

| Method | Path | Classification | Notes |
|---|---|---|---|
| `DELETE` | `/v1/projects/:proj/plugins/:id` | Preserve |  |
| `GET` | `/v1/projects/:tenant/:workspace/:project/costs` | Preserve | F29 CD-2: lifetime cost rollup (µUSD + tokens + provider calls). Zeros for never-billed projects. |
| `POST` | `/v1/projects/:proj/plugins/:id` | Preserve |  |
| `PUT` | `/v1/projects/:project/knowledge-provider` | Preserve | RFC 029: configure the project's knowledge provider. Body `{"provider_ref": "cairn-default"}` or `{"provider_ref": "plugin:<id>"}`. Emits `KnowledgeProviderConfigured`. |
| `PUT` | `/v1/projects/:project/scoring-policy` | Preserve | RFC 029 PR-B2: configure the project's scoring policy. Body is a JSON-serialized `ScoringPolicy`. Rejects (400) writes referencing dimensions the resolved provider declared `not_supported`. |
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
