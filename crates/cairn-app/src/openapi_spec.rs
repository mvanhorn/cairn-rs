//! Static OpenAPI 3.0 specification for the Cairn API.
//!
//! Served at `GET /v1/openapi.json`.  The Swagger UI at `GET /v1/docs`
//! loads this spec from the CDN-hosted swagger-ui bundle.

/// OpenAPI 3.0 specification as a static JSON string.
///
/// Groups endpoints by tag: Health, Sessions, Runs, Tasks, Approvals,
/// Providers, Memory, Events, Evals, Admin.
pub const OPENAPI_JSON: &str = r##"{
  "openapi": "3.0.3",
  "info": {
    "title": "Cairn API",
    "description": "Self-hostable control plane for production AI agent deployments.\n\nAll `/v1/` endpoints require `Authorization: Bearer <token>`. `/health` and `/v1/docs` are public. `/v1/stream` requires bearer auth via the `?token=` query parameter (browsers cannot set custom headers on SSE connections).\n\n**Database:** Set `DATABASE_URL=postgres://user:pass@host/db` for persistent storage, or `--db memory` for ephemeral in-memory mode.\n\n**Rate limiting:** All responses include `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` headers. Token-authenticated requests: 1000 req/min. IP-only: 100 req/min. Exceeded requests return `429` with `Retry-After`.",
    "version": "0.1.0",
    "contact": {
      "name": "cairn-rs",
      "url": "https://github.com/avifenesh/cairn-rs"
    },
    "license": { "name": "MIT" }
  },
  "servers": [
    { "url": "http://localhost:3000", "description": "Local dev" }
  ],
  "components": {
    "securitySchemes": {
      "bearerAuth": {
        "type": "http",
        "scheme": "bearer",
        "description": "Admin or service-account bearer token"
      }
    },
    "schemas": {
      "Error": {
        "type": "object",
        "description": "Canonical error envelope. Every HTTP error response uses this shape: `status_code` mirrors the HTTP status, `code` is a stable machine-readable sentinel, `message` is a human-readable operator message, and `request_id` is the correlation id (also echoed in the `x-request-id` response header). Some errors carry additional structured context under `details` — this field is optional and its schema is endpoint-specific (e.g. rotate-waitpoint-hmac returns partition breakdown; all_providers_exhausted returns per-attempt diagnostics).",
        "properties": {
          "status_code": { "type": "integer", "format": "int32", "description": "HTTP status code echoed in the body for parsers that inspect JSON only." },
          "code":        { "type": "string", "description": "Stable machine-readable error sentinel (e.g. `not_found`, `invalid_state_transition`, `lease_expired`, `all_providers_exhausted`)." },
          "message":     { "type": "string", "description": "Human-readable operator message. Must not carry internal details such as SQL fragments, driver error text, or credential-adjacent fragments (SEC-007)." },
          "request_id":  { "type": "string", "nullable": true, "description": "Per-request correlation id; also emitted as the `x-request-id` response header. The key is always present; the value is `null` when the handler has not been instrumented to thread the id into the body (the header still carries it)." },
          "details":     { "type": "object", "nullable": true, "description": "Optional endpoint-specific structured context. Schema varies by endpoint. Absent for most errors; some endpoints emit `null` explicitly.", "additionalProperties": true }
        },
        "required": ["status_code", "code", "message", "request_id"]
      },
      "ProjectKey": {
        "type": "object",
        "properties": {
          "tenant_id":    { "type": "string" },
          "workspace_id": { "type": "string" },
          "project_id":   { "type": "string" }
        }
      },
      "SessionRecord": {
        "type": "object",
        "description": "Current-state projection of a session. F65 PR-1 adds `goal_title`, `issue_budget`, `max_attempts`, and `attempts_used`; all are additive and carry serde defaults so legacy event logs deserialize cleanly.",
        "properties": {
          "session_id":  { "type": "string" },
          "project":     { "$ref": "#/components/schemas/ProjectKey" },
          "state":       { "type": "string", "enum": ["open","completed","failed","archived"] },
          "version":     { "type": "integer" },
          "created_at":  { "type": "integer", "description": "Unix ms" },
          "updated_at":  { "type": "integer" },
          "goal_title":     { "type": "string", "nullable": true, "description": "F65: operator-visible short title describing the session's goal. Empty on legacy-shape replay." },
          "issue_budget":   { "$ref": "#/components/schemas/IssueBudget", "nullable": true, "description": "F65: per-session budget envelope. Null means no session-level override." },
          "max_attempts":   { "type": "integer", "description": "F65: maximum session attempts. Defaults to 5 when absent on replay." },
          "attempts_used":  { "type": "integer", "description": "F65: count of attempts used so far within the session." }
        }
      },
      "IssueBudget": {
        "type": "object",
        "description": "F65: per-session budget envelope. Every field is optional — `null` at any field means 'unlimited at this layer'; circuit-breaker enforcement (PR-3) falls back to per-run defaults when a field is absent.",
        "properties": {
          "max_tokens":       { "type": "integer", "nullable": true, "description": "Cap on total LLM tokens (input + output) spent across the session." },
          "max_cost_micros":  { "type": "integer", "nullable": true, "description": "Cap on total provider cost, in USD micros (1 USD = 1_000_000). Integer-valued to match the codebase-wide cost convention." },
          "max_wall_seconds": { "type": "integer", "nullable": true, "description": "Cap on wall-clock seconds elapsed from first attempt start to terminal outcome." }
        }
      },
      "BreakerKind": {
        "type": "string",
        "description": "F65: kinds of circuit breakers enforced by the orchestrator in PR-3.",
        "enum": ["round", "tokens", "no_tool_use_consecutive", "wall_clock"]
      },
      "BreakerOverrides": {
        "type": "object",
        "description": "F65 PR-3: per-run overrides for any subset of the four circuit-breaker caps. Tighten-only — each override must be less than or equal to the corresponding operator-configured default resolved via the RuntimeConfig 3-layer fallback (store → env → default). Loosening requests return HTTP 400 `invalid_breaker_override`.",
        "properties": {
          "round_cap":          { "type": "integer", "nullable": true, "description": "Cap on orchestrator iterations (tighter than default only)." },
          "token_cap":          { "type": "integer", "nullable": true, "description": "Cap on cumulative LLM tokens (input + output). Tighter than default only." },
          "no_tool_use_streak": { "type": "integer", "nullable": true, "description": "Cap on consecutive DECIDE rounds with zero tool-use proposals. Tighter than default only." },
          "wall_clock_ms":      { "type": "integer", "nullable": true, "description": "Cap on wall-clock milliseconds from orchestrator loop start. Tighter than default only." }
        }
      },
      "OrchestrateRequest": {
        "type": "object",
        "description": "F65 PR-3: request body for POST /v1/runs/{id}/orchestrate. All fields optional. `breaker_overrides` tightens the operator-configured defaults on a per-run basis.",
        "properties": {
          "goal":                { "type": "string", "nullable": true },
          "max_iterations":      { "type": "integer", "nullable": true, "description": "Legacy iteration cap. Still enforced independently of `breaker_overrides.round_cap` — whichever cap is tighter wins. If both are provided the run terminates under `MaxIterationsReached` or `BreakerTripped(Round)` respectively depending on which one fires first." },
          "timeout_ms":          { "type": "integer", "nullable": true, "description": "Legacy wall-clock timeout. Still enforced independently of `breaker_overrides.wall_clock_ms` — whichever cap is tighter wins. If both are provided the run terminates under `TimedOut` or `BreakerTripped(WallClock)` respectively depending on which one fires first." },
          "mode":                { "type": "string", "enum": ["direct", "plan", "execute"], "nullable": true },
          "approval_timeout_ms": { "type": "integer", "nullable": true },
          "breaker_overrides":   { "$ref": "#/components/schemas/BreakerOverrides", "nullable": true }
        }
      },
      "ApprovePlanRequest": {
        "type": "object",
        "description": "Request body for POST /v1/runs/{id}/approve (RFC 018 plan review). Typed + `deny_unknown_fields` per #427 — unknown keys (e.g. `reviewerComments` camelCase) return 422.",
        "additionalProperties": false,
        "properties": {
          "reviewer_comments": { "type": "string", "nullable": true, "description": "Optional operator note attached to the approval audit event." }
        }
      },
      "RejectPlanRequest": {
        "type": "object",
        "description": "Request body for POST /v1/runs/{id}/reject (RFC 018 plan review). Typed + `deny_unknown_fields` per #427.",
        "additionalProperties": false,
        "properties": {
          "reason": { "type": "string", "nullable": true, "description": "Optional operator-provided reason. Defaults to \"rejected by operator\" when omitted or empty." }
        }
      },
      "RevisePlanRequest": {
        "type": "object",
        "description": "Request body for POST /v1/runs/{id}/revise (RFC 018 plan review). Typed + `deny_unknown_fields` per #427.",
        "additionalProperties": false,
        "properties": {
          "reviewer_comments": { "type": "string", "description": "Required. An empty string returns 400." }
        },
        "required": ["reviewer_comments"]
      },
      "RunCostAlertResponse": {
        "type": "object",
        "description": "Response body for POST /v1/runs/{id}/cost-alert — #431. Returns the created alert so the UI does not need a follow-up GET to learn the value it just set.",
        "properties": {
          "run_id":           { "type": "string" },
          "tenant_id":        { "type": "string" },
          "threshold_micros": { "type": "integer", "format": "int64", "minimum": 0 }
        },
        "required": ["run_id", "tenant_id", "threshold_micros"]
      },
      "PatchSourceRequest": {
        "type": "object",
        "description": "Partial-update request body for PATCH /v1/sources/{id} (#426). `name` and `description` are optional; absent fields preserve the current value. `deny_unknown_fields` — any unknown key returns 422. PR #555 review (Copilot): explicit `null` is NOT a way to clear a field — omit the key instead. The handler collapses `null` to the same as missing via `#[serde(default)] Option<String>`.",
        "additionalProperties": false,
        "properties": {
          "tenant_id":    { "type": "string" },
          "workspace_id": { "type": "string" },
          "project_id":   { "type": "string" },
          "name":         { "type": "string" },
          "description":  { "type": "string" }
        },
        "required": ["tenant_id", "workspace_id", "project_id"]
      },
      "OrchestrateTerminationBreakerTripped": {
        "type": "object",
        "description": "F65 PR-3: response body shape for `termination = \"breaker_tripped\"`. HTTP 200 — the run was cleanly terminated by a circuit-breaker trip; the run's `state` is flipped to the terminal `Failed` state with `FailureClass::ExecutionError` before the response returns.",
        "properties": {
          "termination":  { "type": "string", "enum": ["breaker_tripped"] },
          "which":        { "$ref": "#/components/schemas/BreakerKind" },
          "measured":     { "type": "integer" },
          "limit":        { "type": "integer" },
          "at_iteration": { "type": "integer" }
        },
        "required": ["termination", "which", "measured", "limit", "at_iteration"]
      },
      "CircuitBreakerTrip": {
        "type": "object",
        "description": "F65: one circuit-breaker trip event.",
        "properties": {
          "which":        { "$ref": "#/components/schemas/BreakerKind" },
          "measured":     { "type": "integer", "description": "Measured value that crossed the limit." },
          "limit":        { "type": "integer", "description": "Configured limit that was exceeded." },
          "at_iteration": { "type": "integer", "description": "0-based iteration number at which the trip was observed." }
        },
        "required": ["which", "measured", "limit", "at_iteration"]
      },
      "TerminationReason": {
        "type": "object",
        "description": "F65: exhaustive classification of why a session attempt ended. `kind` is the discriminator; the payload fields depend on the kind.",
        "properties": {
          "kind": {
            "type": "string",
            "enum": [
              "complete_run",
              "circuit_breaker_tripped",
              "lease_lost",
              "provider_error",
              "operator_cancel",
              "crashed"
            ]
          },
          "which":        { "$ref": "#/components/schemas/BreakerKind", "description": "Present only when `kind == circuit_breaker_tripped`." },
          "measured":     { "type": "integer", "description": "Present only when `kind == circuit_breaker_tripped`." },
          "limit":        { "type": "integer", "description": "Present only when `kind == circuit_breaker_tripped`." },
          "at_iteration": { "type": "integer", "description": "Present only when `kind == circuit_breaker_tripped`." },
          "message":      { "type": "string", "description": "Present when `kind` is `provider_error` or `crashed`." }
        },
        "required": ["kind"]
      },
      "SessionOutcome": {
        "type": "object",
        "description": "F65: rich terminal envelope emitted once per session when it closes. PR-1 defines the shape; PR-6 wires the summarizer that populates `compacted_summary` and `next_step_hint`.",
        "properties": {
          "session_id":            { "type": "string" },
          "root_run_id":           { "type": "string" },
          "project":               { "$ref": "#/components/schemas/ProjectKey" },
          "checkpoint_id":         { "type": "string" },
          "workspace_snapshot_id": { "type": "string", "nullable": true, "description": "Workspace snapshot captured for this outcome. Null on ephemeral backends." },
          "termination_reason":    { "$ref": "#/components/schemas/TerminationReason" },
          "compacted_summary":     { "type": "string", "description": "JSON-encoded summary produced by the LLM summarizer in PR-6. Empty-string placeholder on pre-PR-6 outcomes." },
          "next_step_hint":        { "type": "string", "nullable": true },
          "cost_micros":           { "type": "integer", "description": "Total provider cost in USD micros (1 USD = 1_000_000)." },
          "emitted_at":            { "type": "integer", "description": "Unix-epoch ms." }
        },
        "required": ["session_id", "root_run_id", "project", "checkpoint_id", "termination_reason", "compacted_summary", "cost_micros", "emitted_at"]
      },
      "RunRecord": {
        "type": "object",
        "properties": {
          "run_id":        { "type": "string" },
          "session_id":    { "type": "string" },
          "parent_run_id": { "type": "string", "nullable": true },
          "project":       { "$ref": "#/components/schemas/ProjectKey" },
          "state": {
            "type": "string",
            "enum": ["pending","running","paused","waiting_approval","waiting_dependency","completed","failed","canceled"]
          },
          "failure_class": { "type": "string", "nullable": true },
          "version":       { "type": "integer" },
          "created_at":    { "type": "integer" },
          "updated_at":    { "type": "integer" },
          "terminal_write_recovery": {
            "$ref": "#/components/schemas/TerminalRecoveryRecord",
            "nullable": true,
            "description": "F64: present only when the cairn-side terminal-write recovery loop fired for this run (the bridge workaround for FF#371). Omitted on the hot path."
          },
          "subagents_spawned": {
            "type": "integer",
            "nullable": true,
            "description": "#661: count of child runs (`spawn_subagent` delegations) observed for this run. Populated by `GET /v1/runs/:id` (detail) — omitted from list responses to keep the batch shape flat. Counted from `RunReadModel::list_by_parent_run`; includes non-terminal children."
          },
          "subagents_completed": {
            "type": "integer",
            "nullable": true,
            "description": "#661: child runs that reached `completed` terminal state. Populated alongside `subagents_spawned` by the detail endpoint."
          },
          "subagents_failed": {
            "type": "integer",
            "nullable": true,
            "description": "#661: child runs that reached `failed` or `canceled` terminal state. `canceled` is aggregated here because an operator cancelling a delegated child saw the delegation as unsuccessful."
          }
        }
      },
      "TerminalRecoveryRecord": {
        "type": "object",
        "description": "F64: summary of the most recent terminal-write recovery loop, if one fired for this run. Bridge workaround for the FF#371 dual-door deadlock. Retained for historical audit even after the upstream fix lands — only the active retry-loop code retires at that point; the schema + OpenAPI field stay so existing annotations remain inspectable.",
        "properties": {
          "fcall":         { "type": "string", "description": "Which terminal FCALL the loop wrapped: `complete`, `fail`, or `cancel`." },
          "attempts":      { "type": "integer", "description": "Number of re-claim + retry attempts (>= 1)." },
          "wall_time_ms":  { "type": "integer", "description": "Milliseconds spent in the recovery loop (sum of backoff sleeps + FCALL round-trips)." },
          "outcome":       {
            "type": "string",
            "description": "Machine-readable recovery result. `recovered` = retry succeeded. `deadlocked` = schedule exhausted, F62 TerminalWriteDeadlock fallback fired. `non_transient_retry_error` / `non_transient_reclaim_error` = audit-only diagnostic strings for non-transient errors inside the loop. Dashboards should surface `recovered` + `deadlocked` prominently.",
            "enum": ["recovered", "deadlocked", "non_transient_retry_error", "non_transient_reclaim_error"]
          },
          "occurred_at_ms":{ "type": "integer", "description": "Wall-clock ms when the loop finished." }
        },
        "required": ["fcall", "attempts", "wall_time_ms", "outcome", "occurred_at_ms"]
      },
      "CommandOutcome": {
        "type": "object",
        "description": "F47: one bash-class tool invocation distilled from a tool_result frame. `exit_code` is always emitted; the value is `null` when the tool_result did not structurally expose one. The extractor never fabricates exit codes — do not infer success from a missing code.",
        "properties": {
          "tool_name": { "type": "string", "description": "Tool name from the proposal (e.g. `bash`, `shell_exec`)." },
          "cmd":       { "type": "string", "description": "For bash-class tools, the `command` argument. Truncated to 500 chars." },
          "exit_code": { "type": "integer", "nullable": true, "description": "Exit code surfaced by the tool_result, or `null` when not structurally exposed." }
        },
        "required": ["tool_name", "cmd", "exit_code"]
      },
      "RunCompletion": {
        "type": "object",
        "description": "F47 PR2: operator-visible shape of a run's completion annotation on GET /v1/runs/:id. Populated after `LoopTermination::Completed` is persisted via the `RunCompletionAnnotated` event. Omitted for runs that are still running, failed, canceled, or completed before F47 PR2 shipped (no annotation ever landed on the event log).",
        "properties": {
          "summary": { "type": "string", "description": "LLM free-text summary from the CompleteRun action proposal." },
          "verification": { "$ref": "#/components/schemas/CompletionVerification" },
          "completed_at": { "type": "integer", "description": "Wall-clock ms at which the orchestrator emitted the annotation." }
        },
        "required": ["summary", "verification", "completed_at"]
      },
      "RunDetailResponse": {
        "type": "object",
        "description": "Response body for GET /v1/runs/:id. Wraps the RunRecord alongside child tasks and the F47 PR2 completion annotation.",
        "properties": {
          "run":        { "$ref": "#/components/schemas/RunRecord" },
          "tasks":      { "type": "array", "items": { "$ref": "#/components/schemas/TaskRecord" } },
          "completion": { "$ref": "#/components/schemas/RunCompletion", "nullable": true, "description": "F47 PR2 annotation. Omitted from the response body when absent (`skip_serializing_if = Option::is_none`)." }
        },
        "required": ["run", "tasks"]
      },
      "CompletionVerification": {
        "type": "object",
        "description": "F47 PR1 sidecar attached to the `orchestrate_finished` SSE event on `termination=completed` runs. Warning / error lines extracted from tool_result text give operators an independent signal alongside the LLM's free-text `summary`. Non-authoritative: the extractor reports what tool outputs say, not whether the run succeeded.",
        "properties": {
          "warnings": {
            "type": "array",
            "description": "Tool-output lines matched by the warning signal (e.g. `warning: unused import`). Full matched line, truncated to 500 chars. Capped at 50 entries.",
            "items": { "type": "string" }
          },
          "errors": {
            "type": "array",
            "description": "Tool-output lines matched by the error signal (e.g. `error[E0308]:`, `error:`). Same truncation / cap rules as warnings.",
            "items": { "type": "string" }
          },
          "commands": {
            "type": "array",
            "description": "Per-bash-class-tool invocations: command text and (optional) exit code.",
            "items": { "$ref": "#/components/schemas/CommandOutcome" }
          },
          "tool_results_scanned": {
            "type": "integer",
            "description": "How many InvokeTool results were scanned to produce this summary. `0` means Done reached with no recorded tool calls."
          },
          "extractor_version": {
            "type": "integer",
            "description": "Version of the extractor logic (1 = F47 PR1). Bumped when the matching or truncation policy changes."
          }
        },
        "required": ["warnings", "errors", "commands", "tool_results_scanned", "extractor_version"]
      },
      "TaskRecord": {
        "type": "object",
        "properties": {
          "task_id":          { "type": "string" },
          "project":          { "$ref": "#/components/schemas/ProjectKey" },
          "parent_run_id":    { "type": "string", "nullable": true },
          "state": {
            "type": "string",
            "enum": ["queued","leased","running","completed","failed","canceled","paused","waiting_dependency","retryable_failed","dead_lettered"]
          },
          "lease_owner":      { "type": "string", "nullable": true },
          "lease_expires_at": { "type": "integer", "nullable": true },
          "version":          { "type": "integer" },
          "created_at":       { "type": "integer" },
          "updated_at":       { "type": "integer" }
        }
      },
      "DependencyKind": {
        "type": "string",
        "enum": ["success_only"],
        "description": "Edge-kind taxonomy for task dependencies. Mirrors FF 0.2's `dependency_kind` FCALL argument. Today only `success_only` is supported (downstream becomes eligible when upstream terminates successfully; any non-success outcome cascades as skipped)."
      },
      "TaskDependency": {
        "type": "object",
        "properties": {
          "dependent_task_id":   { "type": "string" },
          "depends_on_task_id":  { "type": "string" },
          "project":             { "$ref": "#/components/schemas/ProjectKey" },
          "created_at_ms":       { "type": "integer" },
          "dependency_kind":     { "$ref": "#/components/schemas/DependencyKind" },
          "data_passing_ref":    { "type": "string", "nullable": true, "maxLength": 256, "pattern": "^[A-Za-z0-9._:/-]*$" }
        },
        "required": ["dependent_task_id","depends_on_task_id","project","created_at_ms"]
      },
      "TaskDependencyRecord": {
        "type": "object",
        "properties": {
          "dependency":     { "$ref": "#/components/schemas/TaskDependency" },
          "resolved_at_ms": { "type": "integer", "nullable": true }
        },
        "required": ["dependency"]
      },
      "ApprovalRecord": {
        "type": "object",
        "properties": {
          "approval_id":  { "type": "string" },
          "project":      { "$ref": "#/components/schemas/ProjectKey" },
          "run_id":       { "type": "string", "nullable": true },
          "task_id":      { "type": "string", "nullable": true },
          "requirement":  { "type": "string", "enum": ["required","advisory"] },
          "decision":     { "type": "string", "enum": ["approved","rejected"], "nullable": true },
          "created_at":   { "type": "integer" },
          "updated_at":   { "type": "integer" }
        }
      },
      "ListResponse": {
        "type": "object",
        "properties": {
          "items":    { "type": "array", "items": {} },
          "has_more": { "type": "boolean" }
        }
      },
      "EventEnvelope": {
        "type": "object",
        "properties": {
          "event_id":     { "type": "string" },
          "causation_id": { "type": "string", "nullable": true },
          "source":       { "type": "object" },
          "payload":      { "type": "object", "description": "RuntimeEvent payload" }
        }
      },
      "AppendResult": {
        "type": "object",
        "properties": {
          "event_id": { "type": "string" },
          "position": { "type": "integer" },
          "appended": { "type": "boolean" }
        }
      },
      "TemplateSummary": {
        "type": "object",
        "properties": {
          "id":          { "type": "string" },
          "name":        { "type": "string" },
          "description": { "type": "string" },
          "category":    { "type": "string", "enum": ["chatbot","code_assistant","data_pipeline","customer_support"] },
          "file_count":  { "type": "integer" }
        },
        "required": ["id", "name", "description", "category", "file_count"]
      },
      "TemplateFile": {
        "type": "object",
        "properties": {
          "path":        { "type": "string", "description": "Relative file path within the template" },
          "description": { "type": "string" },
          "content":     { "type": "string" }
        },
        "required": ["path", "description", "content"]
      },
      "Template": {
        "type": "object",
        "properties": {
          "id":          { "type": "string" },
          "name":        { "type": "string" },
          "description": { "type": "string" },
          "category":    { "type": "string", "enum": ["chatbot","code_assistant","data_pipeline","customer_support"] },
          "files":       { "type": "array", "items": { "$ref": "#/components/schemas/TemplateFile" } }
        },
        "required": ["id", "name", "description", "category", "files"]
      },
      "ApplyTemplateRequest": {
        "type": "object",
        "properties": {
          "project_id": { "type": "string" }
        },
        "required": ["project_id"]
      },
      "ApplyTemplateResult": {
        "type": "object",
        "properties": {
          "template_id":   { "type": "string" },
          "project_id":    { "type": "string" },
          "files_created": { "type": "array", "items": { "type": "string" } }
        },
        "required": ["template_id", "project_id", "files_created"]
      },
      "UsageReport": {
        "type": "object",
        "properties": {
          "tenant_id":            { "type": "string" },
          "tier":                 { "type": "string", "enum": ["free","pro","enterprise"] },
          "sessions_used":        { "type": "integer" },
          "max_sessions":         { "type": "integer" },
          "runs_today":           { "type": "integer" },
          "max_runs_per_day":     { "type": "integer" },
          "tokens_this_month":    { "type": "integer", "format": "int64" },
          "max_tokens_per_month": { "type": "integer", "format": "int64" },
          "features_enabled":     { "type": "array", "items": { "type": "string" } }
        },
        "required": ["tenant_id", "tier"]
      },
      "ResourceUsage": {
        "type": "object",
        "properties": {
          "used":         { "type": "integer" },
          "limit":        { "type": "integer" },
          "remaining":    { "type": "integer" },
          "percent_used": { "type": "number", "format": "double" }
        }
      },
      "DetailedUsageReport": {
        "type": "object",
        "properties": {
          "tenant_id": { "type": "string" },
          "tier":      { "type": "string", "enum": ["free","pro","enterprise"] },
          "sessions":  { "$ref": "#/components/schemas/ResourceUsage" },
          "runs":      { "$ref": "#/components/schemas/ResourceUsage" },
          "tokens":    { "$ref": "#/components/schemas/ResourceUsage" }
        },
        "required": ["tenant_id", "tier", "sessions", "runs", "tokens"]
      },
      "SystemInfo": {
        "type": "object",
        "properties": {
          "version":         { "type": "string" },
          "deployment_mode": { "type": "string", "enum": ["local","self_hosted_team"] },
          "store_backend":   { "type": "string", "enum": ["memory","postgres"] },
          "uptime_secs":     { "type": "integer" },
          "capabilities":    { "type": "object" },
          "environment":     { "type": "object" }
        }
      },
      "SystemRole": {
        "type": "object",
        "properties": {
          "role":        { "type": "string", "description": "Process role: all, api, worker" },
          "serves_http": { "type": "boolean" },
          "runs_workers": { "type": "boolean" }
        },
        "required": ["role", "serves_http", "runs_workers"]
      },
      "EventCountResponse": {
        "type": "object",
        "properties": {
          "total":   { "type": "integer", "format": "int64" },
          "by_type": { "type": "object", "additionalProperties": { "type": "integer" } }
        },
        "required": ["total", "by_type"]
      },
      "RebuildProjectionsResponse": {
        "type": "object",
        "properties": {
          "ok":               { "type": "boolean" },
          "events_replayed":  { "type": "integer" },
          "duration_ms":      { "type": "integer" }
        }
      },
      "ExportBundleRequest": {
        "type": "object",
        "properties": {
          "project_id": { "type": "string", "nullable": true },
          "format":     { "type": "string", "enum": ["json","yaml"], "default": "json" }
        }
      },
      "ApplyBundleRequest": {
        "type": "object",
        "properties": {
          "project_id":        { "type": "string" },
          "bundle":            { "type": "object", "description": "Full CairnBundle envelope" },
          "conflict_strategy": { "type": "string", "enum": ["skip","overwrite","rename"], "default": "skip" },
          "existing_ids":      { "type": "array", "items": { "type": "string" }, "default": [] }
        },
        "required": ["project_id", "bundle"]
      },
      "WorkspaceUsageReport": {
        "type": "object",
        "properties": {
          "workspace_id":        { "type": "string" },
          "active_runs":         { "type": "integer" },
          "max_concurrent_runs": { "type": "integer" },
          "runs_this_hour":      { "type": "integer" },
          "max_runs_per_hour":   { "type": "integer" },
          "tokens_today":        { "type": "integer", "format": "int64" },
          "max_tokens_per_day":  { "type": "integer", "format": "int64" },
          "storage_mb":          { "type": "integer", "format": "int64" },
          "max_storage_mb":      { "type": "integer", "format": "int64" }
        },
        "required": ["workspace_id"]
      }
    }
  },
  "security": [{ "bearerAuth": [] }],
  "paths": {
    "/health": {
      "get": {
        "tags": ["Health"],
        "summary": "Liveness probe",
        "description": "Returns `{\"ok\":true}` when the server is running. No auth required.",
        "security": [],
        "operationId": "getHealth",
        "responses": {
          "200": { "description": "Server is alive", "content": { "application/json": { "schema": { "type": "object", "properties": { "ok": { "type": "boolean" } } } } } }
        }
      }
    },
    "/v1/health/detailed": {
      "get": {
        "tags": ["Health"],
        "summary": "Detailed health check",
        "description": "Returns per-component health: store, Ollama, event buffer, memory RSS.",
        "operationId": "getDetailedHealth",
        "responses": {
          "200": { "description": "Health report", "content": { "application/json": { "schema": { "type": "object" } } } }
        }
      }
    },
    "/v1/status": {
      "get": {
        "tags": ["Health"],
        "summary": "Runtime and store health",
        "operationId": "getStatus",
        "responses": { "200": { "description": "System status" } }
      }
    },
    "/v1/dashboard": {
      "get": {
        "tags": ["Health"],
        "summary": "Operator dashboard overview",
        "description": "Active runs, tasks, pending approvals, failed runs (24h), cost summary.",
        "operationId": "getDashboard",
        "responses": { "200": { "description": "Dashboard data" } }
      }
    },
    "/v1/rate-limit": {
      "get": {
        "tags": ["Health"],
        "summary": "Current rate-limit quota",
        "security": [],
        "operationId": "getRateLimit",
        "responses": { "200": { "description": "Quota status" } }
      }
    },
    "/v1/sessions": {
      "get": {
        "tags": ["Sessions"],
        "summary": "List active sessions",
        "operationId": "listSessions",
        "parameters": [
          { "name": "limit",  "in": "query", "schema": { "type": "integer", "default": 50 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0  } }
        ],
        "responses": { "200": { "description": "Session list" } }
      },
      "post": {
        "tags": ["Sessions"],
        "summary": "Create a new session",
        "operationId": "createSession",
        "requestBody": {
          "required": true,
          "content": { "application/json": { "schema": {
            "type": "object",
            "properties": {
              "tenant_id":    { "type": "string" },
              "workspace_id": { "type": "string" },
              "project_id":   { "type": "string" },
              "session_id":   { "type": "string" }
            }
          }}}
        },
        "responses": {
          "201": { "description": "Created session", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/SessionRecord" } } } }
        }
      }
    },
    "/v1/sessions/{id}/runs": {
      "get": {
        "tags": ["Sessions"],
        "summary": "List runs in a session",
        "operationId": "listSessionRuns",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Run list" } }
      }
    },
    "/v1/sessions/{id}/snapshots": {
      "delete": {
        "tags": ["Sessions"],
        "summary": "F65 PR-5: admin-only immediate reap of all workspace snapshots belonging to a session",
        "description": "Walks workspace_snapshots for the session and reaps each live row (on-disk directory removal + WorkspaceSnapshotReaped event emission). Admin-only per locked Q4. Returns {reaped: u32, at_ms: u64}.",
        "operationId": "deleteSessionSnapshots",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": {
          "200": {
            "description": "Reap result",
            "content": { "application/json": { "schema": {
              "type": "object",
              "properties": {
                "reaped": { "type": "integer", "description": "Count of snapshots reaped in this call" },
                "at_ms":  { "type": "integer", "description": "Unix-ms timestamp of the reap" }
              },
              "required": ["reaped", "at_ms"]
            }}}
          },
          "401": { "description": "Unauthorized (admin token required)" },
          "403": { "description": "Forbidden (non-admin token)" }
        }
      }
    },
    "/v1/sessions/{id}/events": {
      "get": {
        "tags": ["Sessions"],
        "summary": "Entity-scoped event stream for a session",
        "operationId": "listSessionEvents",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "cursor", "in": "query", "schema": { "type": "integer" } },
          { "name": "limit",  "in": "query", "schema": { "type": "integer" } }
        ],
        "responses": { "200": { "description": "Events page" } }
      }
    },
    "/v1/sessions/{id}/llm-traces": {
      "get": {
        "tags": ["Sessions"],
        "summary": "LLM call traces for a session",
        "operationId": "getSessionLlmTraces",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Trace list" } }
      }
    },
    "/v1/runs": {
      "get": {
        "tags": ["Runs"],
        "summary": "List runs",
        "operationId": "listRuns",
        "parameters": [
          { "name": "limit",  "in": "query", "schema": { "type": "integer" } },
          { "name": "offset", "in": "query", "schema": { "type": "integer" } }
        ],
        "responses": { "200": { "description": "Run list" } }
      },
      "post": {
        "tags": ["Runs"],
        "summary": "Start a new run",
        "operationId": "createRun",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": {
          "201": { "description": "Created run", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/RunRecord" } } } }
        }
      }
    },
    "/v1/runs/{id}": {
      "get": {
        "tags": ["Runs"],
        "summary": "Get run by ID",
        "operationId": "getRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": {
          "200": {
            "description": "Run detail. Includes a `completion` object once the run has been annotated via `RunCompletionAnnotated` (F47 PR2) — absent for running / failed / canceled / force-completed runs.",
            "content": {
              "application/json": {
                "schema": { "$ref": "#/components/schemas/RunDetailResponse" }
              }
            }
          },
          "404": { "description": "Not found" }
        }
      }
    },
    "/v1/runs/{id}/claim": {
      "post": {
        "tags": ["Runs"],
        "summary": "Claim a run's execution lease (Fabric-only semantic; no-op on in-memory path)",
        "description": "Activates the run's FF execution so downstream FCALLs (pause / enter_waiting_approval / resolve_approval / signals) accept it. Unlike POST /v1/tasks/{id}/claim, this endpoint takes no body: runs are not worker-pulled, so the caller never advertises worker identity here — the Fabric runtime uses its own configured worker_instance_id + lease_ttl_ms. NOT idempotent — re-claiming an already-active run fails at FF's grant gate (`execution_not_eligible`) and surfaces as a 500. Callers must claim once per lifecycle. A second claim after a suspend/resume cycle is legitimate and dispatches through FF's `ff_claim_resumed_execution` path.",
        "operationId": "claimRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": {
          "200": { "description": "Run record after active-lease activation" },
          "404": { "description": "Run not found" },
          "500": { "description": "Underlying runtime error (including re-claim of an already-active run)" }
        }
      }
    },
    "/v1/runs/{id}/pause": {
      "post": {
        "tags": ["Runs"],
        "summary": "Pause a running run",
        "operationId": "pauseRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object", "properties": { "reason_kind": { "type": "string" }, "actor": { "type": "string" }, "resume_after_ms": { "type": "integer" } } } } } },
        "responses": { "200": { "description": "Paused run" } }
      }
    },
    "/v1/runs/{id}/resume": {
      "post": {
        "tags": ["Runs"],
        "summary": "Resume a paused run",
        "operationId": "resumeRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Resumed run" } }
      }
    },
    "/v1/runs/{id}/tasks": {
      "get": {
        "tags": ["Runs"],
        "summary": "List tasks for a run",
        "operationId": "listRunTasks",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Task list" } }
      }
    },
    "/v1/runs/{id}/approvals": {
      "get": {
        "tags": ["Runs"],
        "summary": "List approvals for a run",
        "operationId": "listRunApprovals",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Approval list" } }
      }
    },
    "/v1/runs/{id}/cost": {
      "get": {
        "tags": ["Runs"],
        "summary": "Cost breakdown for a run",
        "operationId": "getRunCost",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Cost summary" } }
      }
    },
    "/v1/runs/{id}/events": {
      "get": {
        "tags": ["Runs", "Events"],
        "summary": "Event stream for a run",
        "description": "Returns an `EventsPage { events, next_cursor, has_more }` wrapper ALWAYS — #429 removed the dual-shape behaviour where passing `from=N` returned a bare array. `from` is still accepted as a legacy alias for `cursor`, but the response wrapper is unconditional now.",
        "operationId": "listRunEvents",
        "parameters": [
          { "name": "id",     "in": "path",  "required": true, "schema": { "type": "string" } },
          { "name": "cursor", "in": "query", "schema": { "type": "integer" }, "description": "Exclusive lower-bound position; next page starts after this event." },
          { "name": "from",   "in": "query", "schema": { "type": "integer" }, "description": "Legacy alias for `cursor`. Same semantics; only the response shape was unified (#429)." },
          { "name": "limit",  "in": "query", "schema": { "type": "integer", "default": 50, "minimum": 1, "maximum": 500 } }
        ],
        "responses": {
          "200": {
            "description": "Events page (`{ events, next_cursor, has_more }` — always wrapped per #429).",
            "content": { "application/json": { "schema": { "type": "object", "properties": { "events": { "type": "array", "items": { "type": "object" } }, "next_cursor": { "type": "integer", "nullable": true }, "has_more": { "type": "boolean" } }, "required": ["events", "has_more"] } } }
          },
          "404": { "description": "Run not found", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/v1/runs/{id}/telemetry": {
      "get": {
        "tags": ["Runs", "Observability"],
        "summary": "Live-aggregated per-run telemetry (provider calls + tool invocations + totals)",
        "description": "Returns the run state + stuck flag, every provider call with model/tokens/cost/latency, every tool invocation with duration, and running totals suitable for the operator observability panel. Aggregated at read time from the InMemory projection.",
        "operationId": "getRunTelemetry",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": {
          "200": { "description": "Run telemetry payload" },
          "404": { "description": "Run not found or not visible to tenant" }
        }
      }
    },
    "/v1/projects/{tenant}/{workspace}/{project}/costs": {
      "get": {
        "tags": ["Costs", "Observability"],
        "summary": "Lifetime cost rollup for a project (F29 CD-2)",
        "description": "Returns the lifetime cost, token, and provider-call totals for every session under the given (tenant, workspace, project) triple. Zeros are returned when the project has not emitted any provider calls yet. Time-range slicing is a follow-up; v1 is lifetime-total.",
        "operationId": "getProjectCosts",
        "parameters": [
          { "name": "tenant",    "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "workspace", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "project",   "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Project cost summary" },
          "403": { "description": "Tenant scope mismatch" }
        }
      }
    },
    "/v1/workspaces/{tenant}/{workspace}/costs": {
      "get": {
        "tags": ["Costs", "Observability"],
        "summary": "Lifetime cost rollup for a workspace (F29 CD-2)",
        "description": "Returns the lifetime cost, token, and provider-call totals aggregated across every project in the given (tenant, workspace). Zeros are returned when the workspace has not emitted any provider calls yet.",
        "operationId": "getWorkspaceCosts",
        "parameters": [
          { "name": "tenant",    "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "workspace", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Workspace cost summary" },
          "403": { "description": "Tenant scope mismatch" }
        }
      }
    },
    "/v1/runs/{id}/tool-invocations": {
      "get": {
        "tags": ["Runs"],
        "summary": "Tool invocations for a run",
        "operationId": "listRunToolInvocations",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Tool invocation list" } }
      }
    },
    "/v1/tasks": {
      "get": {
        "tags": ["Tasks"],
        "summary": "List all tasks (operator view)",
        "operationId": "listTasks",
        "parameters": [
          { "name": "limit",  "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0   } }
        ],
        "responses": { "200": { "description": "Task array" } }
      }
    },
    "/v1/tasks/{id}/claim": {
      "post": {
        "tags": ["Tasks"],
        "summary": "Claim a queued task",
        "operationId": "claimTask",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object", "properties": { "worker_id": { "type": "string" }, "lease_duration_ms": { "type": "integer", "default": 30000 } }, "required": ["worker_id"] } } } },
        "responses": { "200": { "description": "Claimed task" }, "400": { "description": "Invalid transition" } }
      }
    },
    "/v1/tasks/{id}/release-lease": {
      "post": {
        "tags": ["Tasks"],
        "summary": "Release a task lease back to queued",
        "operationId": "releaseTaskLease",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Task back in queued state" } }
      }
    },
    "/v1/tasks/{id}/dependencies": {
      "get": {
        "tags": ["Tasks"],
        "summary": "List unresolved prerequisite tasks blocking this task",
        "operationId": "listTaskDependencies",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "TaskDependencyRecord array (empty means no active blockers)" } }
      },
      "post": {
        "tags": ["Tasks"],
        "summary": "Declare a task-level dependency (FF flow edge)",
        "description": "Both tasks must share the same session (FF flows are session-scoped). Cross-session, cross-project, or self-dependency declares are rejected with 422. Re-declaring an existing edge with a different `dependency_kind` or `data_passing_ref` returns 409 dependency_conflict; identical replay is idempotent 201. `data_passing_ref` is an opaque caller-supplied string forwarded to FF edge storage — cairn never dereferences it. Downstream consumers are responsible for interpreting the value.",
        "operationId": "addTaskDependency",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "depends_on_task_id": { "type": "string", "description": "Prerequisite task id (must share session with the dependent task)." },
                  "dependency_kind":    { "type": "string", "enum": ["success_only"], "default": "success_only", "description": "Edge kind. Today only `success_only` is supported." },
                  "data_passing_ref":   { "type": "string", "maxLength": 256, "pattern": "^[A-Za-z0-9._:/-]*$", "nullable": true, "description": "Opaque reference stored on the FF edge. Charset limited for round-trip safety; empty string treated as absent." }
                },
                "required": ["depends_on_task_id"]
              }
            }
          }
        },
        "responses": {
          "201": { "description": "Dependency record (TaskDependencyRecord)" },
          "404": { "description": "One of the tasks not found" },
          "409": { "description": "Edge already exists with different kind/ref (dependency_conflict)" },
          "422": { "description": "Cross-session, self-dependency, or invalid data_passing_ref" }
        }
      }
    },
    "/v1/approvals": {
      "get": {
        "tags": ["Approvals"],
        "summary": "List approvals (unified — plan + tool-call, F45)",
        "description": "Merged operator inbox across both approval kinds. Every item carries a `kind` discriminator (`plan` | `tool_call`). Plan-approval rows flatten `ApprovalRecord`; tool-call rows flatten `ToolCallApprovalRecord`. Supersedes the pre-F45 `/v1/tool-call-approvals` list, which now 308-redirects here.",
        "operationId": "listApprovals",
        "parameters": [
          { "name": "kind",         "in": "query", "schema": { "type": "string", "enum": ["plan","tool_call"] }, "description": "Narrow to one kind; absent = both." },
          { "name": "state",        "in": "query", "schema": { "type": "string", "enum": ["pending","approved","rejected","timeout"] } },
          { "name": "run_id",       "in": "query", "schema": { "type": "string" } },
          { "name": "session_id",   "in": "query", "schema": { "type": "string" }, "description": "Tool-call native; excludes plan approvals when set." },
          { "name": "tenant_id",    "in": "query", "schema": { "type": "string" } },
          { "name": "workspace_id", "in": "query", "schema": { "type": "string" } },
          { "name": "project_id",   "in": "query", "schema": { "type": "string" } },
          { "name": "limit",        "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset",       "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Merged approval records, newest first" } }
      }
    },
    "/v1/approvals/pending": {
      "get": {
        "tags": ["Approvals"],
        "summary": "List pending plan approvals",
        "operationId": "listPendingApprovals",
        "responses": { "200": { "description": "Pending plan approvals" } }
      }
    },
    "/v1/approvals/{id}": {
      "get": {
        "tags": ["Approvals"],
        "summary": "Fetch any approval by id (unified, F45)",
        "description": "Resolves tool-call first, then plan. Response carries a `kind` discriminator.",
        "operationId": "getApproval",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": {
          "200": { "description": "Unified approval record" },
          "404": { "description": "Not found (or cross-tenant)" }
        }
      }
    },
    "/v1/approvals/{id}/approve": {
      "post": {
        "tags": ["Approvals"],
        "summary": "Approve an approval (kind-aware)",
        "description": "For plan approvals the body is ignored. For tool-call approvals `scope` is required: `{type:\"once\"}` resolves this call only; `{type:\"session\", match_policy?}` widens to matching calls in the same session (omitted `match_policy` inherits the proposal's). `approved_tool_args` overrides any prior amendment. `operator_id` in the body must match the authenticated principal when present (else 400 `identity_mismatch`).",
        "operationId": "approveApproval",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": false, "content": { "application/json": { "schema": { "type": "object", "properties": {
          "operator_id": { "type": "string" },
          "scope": { "type": "object", "oneOf": [
            { "type": "object", "properties": { "type": { "type": "string", "enum": ["once"] } }, "required": ["type"] },
            { "type": "object", "properties": { "type": { "type": "string", "enum": ["session"] }, "match_policy": { "type": "object" } }, "required": ["type"] }
          ] },
          "approved_tool_args": {}
        } } } } },
        "responses": {
          "200": { "description": "Approved" },
          "400": { "description": "operator_id in body does not match authenticated principal" },
          "404": { "description": "Unknown id" },
          "409": { "description": "Approval already resolved" },
          "422": { "description": "tool-call approval missing required `scope`" }
        }
      }
    },
    "/v1/approvals/{id}/reject": {
      "post": {
        "tags": ["Approvals"],
        "summary": "Reject an approval (kind-aware)",
        "operationId": "rejectApproval",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": false, "content": { "application/json": { "schema": { "type": "object", "properties": {
          "operator_id": { "type": "string" },
          "reason":      { "type": "string" }
        } } } } },
        "responses": {
          "200": { "description": "Rejected" },
          "400": { "description": "operator_id mismatch" },
          "404": { "description": "Unknown id" },
          "409": { "description": "Approval already resolved" }
        }
      }
    },
    "/v1/approvals/{id}/deny": {
      "post": {
        "tags": ["Approvals"],
        "summary": "Alias of /reject (legacy route)",
        "operationId": "denyApproval",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Rejected" } }
      }
    },
    "/v1/approvals/{id}/amend": {
      "patch": {
        "tags": ["Approvals"],
        "summary": "Amend tool-call arguments (tool-call kind only)",
        "description": "Non-resolving — operator must still approve/reject. Returns 422 `unsupported_on_plan_approval` when the id points at a plan approval, and 403 `self_amend_forbidden` if the proposal's `tool_name` is `amend_approval`.",
        "operationId": "amendApproval",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object", "properties": {
          "operator_id":   { "type": "string" },
          "new_tool_args": {}
        }, "required": ["new_tool_args"] } } } },
        "responses": {
          "200": { "description": "Amended; state remains pending" },
          "400": { "description": "operator_id mismatch" },
          "403": { "description": "Cannot amend amend_approval tool calls" },
          "404": { "description": "Unknown id" },
          "409": { "description": "Proposal already resolved" },
          "422": { "description": "Amend not supported on plan approvals" }
        }
      }
    },
    "/v1/tool-call-approvals": {
      "get": {
        "tags": ["Approvals"],
        "summary": "Deprecated — 308-redirects to /v1/approvals?kind=tool_call",
        "description": "Deprecated in F45. Clients should call `/v1/approvals?kind=tool_call`. This path 308-redirects (preserves method + body); response carries `Deprecation: true`.",
        "operationId": "listToolCallApprovals",
        "deprecated": true,
        "responses": { "308": { "description": "Permanent Redirect to /v1/approvals" } }
      }
    },
    "/v1/tool-call-approvals/{call_id}": {
      "get": {
        "tags": ["Approvals"],
        "summary": "Deprecated — 308-redirects to /v1/approvals/{id}",
        "operationId": "getToolCallApproval",
        "deprecated": true,
        "parameters": [{ "name": "call_id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "308": { "description": "Permanent Redirect" } }
      }
    },
    "/v1/tool-call-approvals/{call_id}/approve": {
      "post": {
        "tags": ["Approvals"],
        "summary": "Deprecated — 308-redirects to /v1/approvals/{id}/approve",
        "operationId": "approveToolCallApproval",
        "deprecated": true,
        "parameters": [{ "name": "call_id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "308": { "description": "Permanent Redirect" } }
      }
    },
    "/v1/tool-call-approvals/{call_id}/reject": {
      "post": {
        "tags": ["Approvals"],
        "summary": "Deprecated — 308-redirects to /v1/approvals/{id}/reject",
        "operationId": "rejectToolCallApproval",
        "deprecated": true,
        "parameters": [{ "name": "call_id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "308": { "description": "Permanent Redirect" } }
      }
    },
    "/v1/tool-call-approvals/{call_id}/amend": {
      "patch": {
        "tags": ["Approvals"],
        "summary": "Deprecated — 308-redirects to /v1/approvals/{id}/amend",
        "operationId": "amendToolCallApproval",
        "deprecated": true,
        "parameters": [{ "name": "call_id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "308": { "description": "Permanent Redirect" } }
      }
    },
    "/v1/providers": {
      "get": {
        "tags": ["Providers"],
        "summary": "List provider bindings",
        "operationId": "listProviders",
        "responses": { "200": { "description": "Provider list" } }
      }
    },
    "/v1/providers/health": {
      "get": {
        "tags": ["Providers"],
        "summary": "Provider health status",
        "operationId": "getProviderHealth",
        "responses": { "200": { "description": "Health records" } }
      }
    },
    "/v1/providers/connections": {
      "get": {
        "tags": ["Providers"],
        "summary": "List provider connections",
        "operationId": "listProviderConnections",
        "parameters": [
          { "name": "tenant_id", "in": "query", "required": true, "schema": { "type": "string" } },
          { "name": "limit",     "in": "query", "schema": { "type": "integer", "default": 50 } },
          { "name": "offset",    "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Provider connection list" } }
      },
      "post": {
        "tags": ["Providers"],
        "summary": "Register a provider connection (entitlement-gated)",
        "description": "Creates a new provider connection. Requires a tier that supports external providers (returns 403 in local_eval tier).",
        "operationId": "createProviderConnection",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object", "properties": { "tenant_id": { "type": "string" }, "provider_connection_id": { "type": "string" }, "provider_family": { "type": "string" }, "adapter_type": { "type": "string" } }, "required": ["tenant_id", "provider_connection_id", "provider_family", "adapter_type"] } } } },
        "responses": {
          "201": { "description": "Provider connection created" },
          "403": { "description": "Entitlement tier does not allow external provider connections" }
        }
      }
    },
    "/v1/models/catalog": {
      "get": {
        "tags": ["Models"],
        "summary": "List the bundled model catalog (LiteLLM + cairn overlay)",
        "description": "Read-only projection of the bundled LiteLLM pricing catalog plus any cairn TOML overlay and operator overrides. Supports filter, search, capability filters, cost ceiling, free-only shortcut, and pagination. Callable by any authenticated operator — the UI provider wizard and cost calculator read from here.",
        "operationId": "listModelCatalog",
        "parameters": [
          { "name": "provider",          "in": "query", "schema": { "type": "string" }, "description": "Exact-match provider family (e.g. openai, anthropic, openrouter)." },
          { "name": "tier",              "in": "query", "schema": { "type": "string", "enum": ["brain", "mid", "light"] }, "description": "Routing tier." },
          { "name": "search",            "in": "query", "schema": { "type": "string" }, "description": "Case-insensitive substring across id, display_name, and provider." },
          { "name": "supports_tools",    "in": "query", "schema": { "type": "boolean" } },
          { "name": "supports_json_mode","in": "query", "schema": { "type": "boolean" } },
          { "name": "reasoning",         "in": "query", "schema": { "type": "boolean" } },
          { "name": "max_cost_per_1m",   "in": "query", "schema": { "type": "number" }, "description": "Upper bound on cost_per_1m_input (USD)." },
          { "name": "free_only",         "in": "query", "schema": { "type": "boolean" }, "description": "When true, only models with zero input+output cost." },
          { "name": "limit",             "in": "query", "schema": { "type": "integer", "default": 100, "maximum": 1000 } },
          { "name": "offset",            "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": {
          "200": {
            "description": "Filtered, paginated model list",
            "content": { "application/json": { "schema": { "type": "object", "properties": {
              "items":   { "type": "array", "items": { "type": "object" } },
              "total":   { "type": "integer" },
              "hasMore": { "type": "boolean" }
            }, "required": ["items", "total", "hasMore"] } } }
          },
          "422": { "description": "Validation error (invalid limit/offset/tier)" },
          "503": { "description": "model_catalog_unavailable — bundled catalog is empty" }
        }
      }
    },
    "/v1/models/catalog/providers": {
      "get": {
        "tags": ["Models"],
        "summary": "Unique provider families in the model catalog, with counts",
        "description": "Lets the UI build a provider-filter dropdown without a full catalog scan. Cached after the first call for the process lifetime; admin CRUD overrides do NOT invalidate this cache.",
        "operationId": "listCatalogProviders",
        "responses": {
          "200": {
            "description": "Providers with entry counts",
            "content": { "application/json": { "schema": { "type": "object", "properties": {
              "providers": { "type": "array", "items": { "type": "object", "properties": {
                "name":  { "type": "string" },
                "count": { "type": "integer" }
              }, "required": ["name", "count"] } }
            }, "required": ["providers"] } } }
          },
          "503": { "description": "model_catalog_unavailable" }
        }
      }
    },
    "/v1/providers/ollama/models": {
      "get": {
        "tags": ["Providers"],
        "summary": "List locally available Ollama models",
        "operationId": "listOllamaModels",
        "responses": { "200": { "description": "Model list" } }
      }
    },
    "/v1/providers/ollama/generate": {
      "post": {
        "tags": ["Providers"],
        "summary": "Generate text via local Ollama (blocking)",
        "operationId": "ollamaGenerate",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object", "properties": { "model": { "type": "string" }, "prompt": { "type": "string" }, "temperature": { "type": "number" }, "max_tokens": { "type": "integer" } } } } } },
        "responses": { "200": { "description": "Generated text + metadata" } }
      }
    },
    "/v1/chat/stream": {
      "post": {
        "tags": ["Chat"],
        "summary": "Stream tokens from any configured LLM provider via SSE",
        "operationId": "chatStream",
        "description": "Routes to the first available provider: Bedrock, Ollama, OpenAI-compat brain, worker, or OpenRouter.",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "SSE stream: token / done / error events" } }
      }
    },
    "/v1/providers/ollama/pull": {
      "post": {
        "tags": ["Providers"],
        "summary": "Pull an Ollama model",
        "operationId": "ollamaPull",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object", "properties": { "model": { "type": "string" } } } } } },
        "responses": { "200": { "description": "Pull started" } }
      }
    },
    "/v1/memory/ingest": {
      "post": {
        "tags": ["Memory"],
        "summary": "Ingest a document into the knowledge store",
        "operationId": "memoryIngest",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object", "properties": { "tenant_id": { "type": "string" }, "workspace_id": { "type": "string" }, "project_id": { "type": "string" }, "source_id": { "type": "string" }, "document_id": { "type": "string" }, "content": { "type": "string" }, "source_type": { "type": "string" } } } } } },
        "responses": { "200": { "description": "Ingestion result" } }
      }
    },
    "/v1/memory/search": {
      "get": {
        "tags": ["Memory"],
        "summary": "Lexical retrieval over the knowledge store",
        "operationId": "memorySearch",
        "parameters": [
          { "name": "query_text",   "in": "query", "required": true,  "schema": { "type": "string"  } },
          { "name": "tenant_id",    "in": "query", "required": false, "schema": { "type": "string"  } },
          { "name": "workspace_id", "in": "query", "required": false, "schema": { "type": "string"  } },
          { "name": "project_id",   "in": "query", "required": false, "schema": { "type": "string"  } },
          { "name": "limit",        "in": "query", "required": false, "schema": { "type": "integer" } }
        ],
        "responses": { "200": { "description": "Ranked search results" } }
      }
    },
    "/v1/memory/embed": {
      "post": {
        "tags": ["Memory"],
        "summary": "Embed texts via Ollama",
        "operationId": "memoryEmbed",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object", "properties": { "texts": { "type": "array", "items": { "type": "string" } }, "model": { "type": "string" } } } } } },
        "responses": { "200": { "description": "Embedding vectors" } }
      }
    },
    "/v1/events": {
      "get": {
        "tags": ["Events"],
        "summary": "Cursor-based replay of the global event log",
        "description": "Returns up to `limit` events strictly after `after` position. Use `Last-Event-ID` on reconnect.",
        "operationId": "listEvents",
        "parameters": [
          { "name": "after", "in": "query", "schema": { "type": "integer", "description": "Return events after this position" } },
          { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 100 } }
        ],
        "responses": { "200": { "description": "Event page" } }
      }
    },
    "/v1/events/append": {
      "post": {
        "tags": ["Events"],
        "summary": "Append events (idempotent write)",
        "description": "Accepts an array of `EventEnvelope<RuntimeEvent>`. Causation-ID deduplication ensures at-least-once safety.",
        "operationId": "appendEvents",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "array", "items": { "$ref": "#/components/schemas/EventEnvelope" } } } } },
        "responses": {
          "201": { "description": "Append results", "content": { "application/json": { "schema": { "type": "array", "items": { "$ref": "#/components/schemas/AppendResult" } } } } }
        }
      }
    },
    "/v1/stream": {
      "get": {
        "tags": ["Events"],
        "summary": "Real-time SSE event stream",
        "description": "Emits live events. On connect a `connected` event carries the current head position. Reconnect with `Last-Event-ID` to replay up to 1 000 missed events. Requires bearer token via `Authorization: Bearer <token>` header OR `?token=<token>` query parameter (browsers cannot set custom headers on EventSource).",
        "security": [{ "bearerAuth": [] }],
        "operationId": "streamEvents",
        "parameters": [
          { "name": "token", "in": "query", "required": false, "description": "Bearer token fallback for EventSource clients that cannot set the `Authorization` header (browsers). Accepts the same values as the `Authorization` header.", "schema": { "type": "string" } },
          { "name": "Last-Event-ID", "in": "header", "required": false, "description": "Replay events since this position. Up to 1 000 missed events are replayed.", "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "SSE stream", "content": { "text/event-stream": {} } },
          "401": { "description": "Missing or invalid bearer token (header absent AND `?token=` absent/invalid)" }
        }
      }
    },
    "/v1/evals/runs": {
      "get": {
        "tags": ["Evals"],
        "summary": "List eval runs",
        "description": "Lists eval runs for the active project scope. Archived runs are excluded by default; pass `include_archived=true` to surface soft-deleted runs (issue #244).",
        "operationId": "listEvalRuns",
        "parameters": [
          { "name": "tenant_id",        "in": "query", "schema": { "type": "string" } },
          { "name": "workspace_id",     "in": "query", "schema": { "type": "string" } },
          { "name": "project_id",       "in": "query", "schema": { "type": "string" } },
          { "name": "limit",            "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset",           "in": "query", "schema": { "type": "integer", "default": 0 } },
          { "name": "include_archived", "in": "query", "schema": { "type": "boolean", "default": false } }
        ],
        "responses": { "200": { "description": "Eval run list" } }
      },
      "post": {
        "tags": ["Evals"],
        "summary": "Create an eval run",
        "description": "Creates a new eval run. Duplicate `eval_run_id` returns 409 Conflict (issue #244).",
        "operationId": "createEvalRun",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": { "schema": { "type": "object" } }
          }
        },
        "responses": {
          "201": { "description": "Eval run created" },
          "404": { "description": "Referenced dataset/rubric/baseline not found or not in tenant" },
          "409": { "description": "Duplicate eval_run_id — the id already exists (same or cross-project)" }
        }
      }
    },
    "/v1/evals/runs/{id}": {
      "get": {
        "tags": ["Evals"],
        "summary": "Get an eval run",
        "operationId": "getEvalRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": {
          "200": { "description": "Eval run record" },
          "404": { "description": "Eval run not found" }
        }
      },
      "delete": {
        "tags": ["Evals"],
        "summary": "Soft-delete an eval run (issue #244)",
        "description": "Archives the run via an `EvalRunArchived` event so audit trails remain intact. Project scope must match — cross-project DELETE returns 404. Already-archived runs return 204 (idempotent). The default list view hides archived rows; pass `include_archived=true` on `GET /v1/evals/runs` to surface them.",
        "operationId": "deleteEvalRun",
        "parameters": [
          { "name": "id",           "in": "path",  "required": true,  "schema": { "type": "string" } },
          { "name": "tenant_id",    "in": "query", "schema": { "type": "string" } },
          { "name": "workspace_id", "in": "query", "schema": { "type": "string" } },
          { "name": "project_id",   "in": "query", "schema": { "type": "string" } }
        ],
        "responses": {
          "204": { "description": "Archived (or already archived — idempotent)" },
          "404": { "description": "Eval run not found in this project scope" }
        }
      }
    },
    "/v1/evals/scorecards": {
      "get": {
        "tags": ["Evals"],
        "summary": "List scorecard summaries (issue #244)",
        "description": "One row per `(project, prompt_asset_id)` with at least one completed, non-archived eval run whose `prompt_release_id`/`prompt_version_id` are set. Sorted by `best_task_success_rate` descending. Populates the EvalsPage scorecard picker.",
        "operationId": "listEvalScorecards",
        "parameters": [
          { "name": "tenant_id",    "in": "query", "schema": { "type": "string" } },
          { "name": "workspace_id", "in": "query", "schema": { "type": "string" } },
          { "name": "project_id",   "in": "query", "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "Scorecard summary list" } }
      }
    },
    "/v1/evals/rubrics": {
      "get": {
        "tags": ["Evals"],
        "summary": "List rubrics for a tenant (issue #138)",
        "operationId": "listEvalRubrics",
        "parameters": [{ "name": "tenant_id", "in": "query", "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Eval rubric list" } }
      },
      "post": {
        "tags": ["Evals"],
        "summary": "Create a rubric for a tenant",
        "operationId": "createEvalRubric",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Rubric created" } }
      }
    },
    "/v1/evals/baselines": {
      "get": {
        "tags": ["Evals"],
        "summary": "List baselines for a tenant (issue #138)",
        "operationId": "listEvalBaselines",
        "parameters": [{ "name": "tenant_id", "in": "query", "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Eval baseline list" } }
      },
      "post": {
        "tags": ["Evals"],
        "summary": "Create a baseline for a tenant",
        "operationId": "createEvalBaseline",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Baseline created" } }
      }
    },
    "/v1/traces": {
      "get": {
        "tags": ["Evals"],
        "summary": "All recent LLM call traces",
        "operationId": "listTraces",
        "parameters": [{ "name": "limit", "in": "query", "schema": { "type": "integer", "default": 500 } }],
        "responses": { "200": { "description": "LLM call traces" } }
      }
    },
    "/v1/admin/audit-log": {
      "get": {
        "tags": ["Admin"],
        "summary": "Audit log entries for the operator tenant",
        "operationId": "listAuditLog",
        "parameters": [
          { "name": "limit",    "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "since_ms", "in": "query", "schema": { "type": "integer" } }
        ],
        "responses": { "200": { "description": "Audit entries" } }
      }
    },
    "/v1/admin/tenants": {
      "get": {
        "tags": ["Admin"],
        "summary": "List tenants (admin only)",
        "operationId": "listTenants",
        "parameters": [
          { "name": "limit",  "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Tenant list" } }
      },
      "post": {
        "tags": ["Admin"],
        "summary": "Create a new tenant",
        "operationId": "createTenant",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object", "properties": { "tenant_id": { "type": "string" }, "name": { "type": "string" } }, "required": ["tenant_id", "name"] } } } },
        "responses": { "201": { "description": "Created tenant" } }
      }
    },
    "/v1/admin/tenants/{id}": {
      "get": {
        "tags": ["Admin"],
        "summary": "Fetch a single tenant record",
        "operationId": "getTenant",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Tenant record" },
          "404": { "description": "Tenant not found" }
        }
      },
      "patch": {
        "tags": ["Admin"],
        "summary": "Edit a tenant (RFC 026 PR-A2)",
        "description": "PATCH semantics — every field is optional; omitted fields preserve the stored value. An all-`null` body returns 422 `empty_patch`. Guarded by `TenantAdminGuard`: god-token (`CAIRN_ADMIN_TOKEN`) bypasses for cross-tenant bootstrap; real operators require `TenantRole::Admin` on the target tenant (otherwise 403 with the structured `tenant_role_missing` envelope). Emits `TenantUpdated` and an `AuditLogEntryRecorded` entry.",
        "operationId": "updateTenant",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "requestBody": {
          "required": true,
          "content": { "application/json": { "schema": { "type": "object", "properties": { "name": { "type": "string", "nullable": true } } } } }
        },
        "responses": {
          "200": { "description": "Updated tenant record" },
          "403": { "description": "Structured `tenant_role_missing` body when the caller is a non-admin operator without `TenantRole::Admin` on the target." },
          "404": { "description": "Tenant not found" },
          "422": { "description": "Empty patch body" }
        }
      }
    },
    "/v1/admin/tenants/{tenant_id}/operator-profiles/{id}": {
      "patch": {
        "tags": ["Admin"],
        "summary": "Edit an operator profile (RFC 026 PR-A2)",
        "description": "PATCH semantics — any subset of `display_name`, `email`, or `role` may be supplied; omitted fields stay as-is. An all-`null` body returns 422 `empty_patch`. Guarded by `TenantAdminGuard`; cross-tenant ids return 404 rather than 403 so operator presence is not revealed to a non-tenant-admin. Emits `OperatorProfileUpdated` plus an `AuditLogEntryRecorded` entry. `role` here is the `WorkspaceRole` carried on the operator profile (default role for workspace assignments); the tenant-scope `TenantRole` has its own endpoint (PR-A0).",
        "operationId": "updateOperatorProfile",
        "parameters": [
          { "name": "tenant_id", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "id",        "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "requestBody": {
          "required": true,
          "content": { "application/json": { "schema": { "type": "object", "properties": {
            "display_name": { "type": "string", "nullable": true },
            "email":        { "type": "string", "nullable": true },
            "role":         { "type": "string", "nullable": true, "enum": ["owner", "admin", "member", "viewer"] }
          } } } }
        },
        "responses": {
          "200": { "description": "Updated operator profile" },
          "403": { "description": "Structured `tenant_role_missing` body when the caller lacks `TenantRole::Admin` on `:tenant_id`." },
          "404": { "description": "Operator profile not found for this tenant" },
          "422": { "description": "Empty patch body or invalid email" }
        }
      }
    },
    "/v1/settings": {
      "get": {
        "tags": ["Admin"],
        "summary": "Deployment configuration",
        "operationId": "getSettings",
        "responses": { "200": { "description": "Settings including mode, backend, feature flags" } }
      }
    },
    "/v1/settings/defaults/{scope}/{scope_id}/{key}": {
      "get": {
        "tags": ["Admin"],
        "summary": "Fetch a single stored default setting by exact scope",
        "description": "Returns the stored default at the exact `(scope, scope_id, key)` triple. This endpoint is exact-lookup — for fallback resolution across the scope cascade use `GET /v1/settings/defaults/resolve/{key}?project=...`. 404 when no value has been persisted at this triple.",
        "operationId": "getDefaultSetting",
        "parameters": [
          { "name": "scope", "in": "path", "required": true, "schema": { "type": "string", "enum": ["system", "tenant", "workspace", "project"] } },
          { "name": "scope_id", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "key", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "`{ scope, scope_id, key, value, source }`" },
          "404": { "description": "Setting not set at this scope" }
        }
      },
      "put": {
        "tags": ["Admin"],
        "summary": "Set a scoped default setting",
        "description": "Persists `value` as the default for `key` at the given scope. Scope layers cascade: System < Tenant < Workspace < Project (project overrides tenant overrides system). For `scope=system`, the conventional `scope_id` is `system`. For Tenant/Workspace/Project, `scope_id` is the respective entity id. Model-id keys (`brain_model`, `generate_model`, `stream_model`, `embed_model`) validate only that the value is a non-empty string shorter than 256 chars — forward references (models not yet advertised by any provider connection) are accepted so setup scripts can order `PUT brain_model` before `POST /v1/providers/connections`. The authoritative 'is this model routable' check runs at orchestrate time and returns 503 `preferred_model_unavailable` with the full connection inventory when a configured default has no backing connection.",
        "operationId": "setDefaultSetting",
        "parameters": [
          { "name": "scope", "in": "path", "required": true, "schema": { "type": "string", "enum": ["system", "tenant", "workspace", "project"] } },
          { "name": "scope_id", "in": "path", "required": true, "schema": { "type": "string" }, "description": "Scope entity id (`system` for system scope)." },
          { "name": "key", "in": "path", "required": true, "schema": { "type": "string" }, "description": "Setting key (e.g. `brain_model`, `generate_model`, `max_tokens`, `temperature`)." }
        ],
        "requestBody": {
          "required": true,
          "content": { "application/json": { "schema": { "type": "object", "required": ["value"], "properties": { "value": { "description": "Setting value — type depends on the key (string for models, number for tokens/temperature)." } } } } }
        },
        "responses": {
          "200": { "description": "Persisted setting" },
          "400": { "description": "Invalid scope or malformed body" },
          "422": { "description": "Value failed per-key validation (unknown model, out-of-range numeric, oversized string)" }
        }
      },
      "delete": {
        "tags": ["Admin"],
        "summary": "Clear a scoped default setting",
        "operationId": "clearDefaultSetting",
        "parameters": [
          { "name": "scope", "in": "path", "required": true, "schema": { "type": "string", "enum": ["system", "tenant", "workspace", "project"] } },
          { "name": "scope_id", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "key", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "Cleared; resolution now falls through to the next scope layer or the hardcoded default" } }
      }
    },
    "/v1/settings/defaults/all": {
      "get": {
        "tags": ["Admin"],
        "summary": "List every persisted default setting across all scopes",
        "description": "Flat list of all settings explicitly set via `PUT /v1/settings/defaults/...`. Unset keys are omitted. For the effective value of a specific key with fallback resolution, use `GET /v1/settings/defaults/resolve/{key}?project=...`.",
        "operationId": "listAllDefaultSettings",
        "responses": { "200": { "description": "`{ settings: [...], total: n }`" } }
      }
    },
    "/v1/settings/defaults/resolve/{key}": {
      "get": {
        "tags": ["Admin"],
        "summary": "Resolve the effective default for a key",
        "description": "Walks the scope cascade (Project → Workspace → Tenant → System → env → hardcoded) and returns the first layer's value. Requires `?project=<project_id>` to anchor the resolution.",
        "operationId": "resolveDefaultSetting",
        "parameters": [
          { "name": "key", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "project", "in": "query", "required": true, "schema": { "type": "string" }, "description": "Project id anchoring the scope cascade." }
        ],
        "responses": { "200": { "description": "`{ key, value }`" } }
      }
    },
    "/v1/db/status": {
      "get": {
        "tags": ["Admin"],
        "summary": "Database health and migration state",
        "operationId": "getDbStatus",
        "responses": { "200": { "description": "Backend type, connected flag, migration count" } }
      }
    },
    "/v1/metrics": {
      "get": {
        "tags": ["Admin"],
        "summary": "JSON request metrics",
        "description": "**Scope (#428):** process-level (not tenant-scoped) — aggregate latency percentiles, request counts, and error rate across the whole deployment. Not gated with `AdminRoleGuard` because existing Prometheus scrapers depend on unauthenticated-but-token-gated access; adding a workspace-role requirement would break monitoring rigs. Treat as admin-equivalent at the network / token layer.",
        "operationId": "getMetrics",
        "responses": { "200": { "description": "Rolling latency percentiles, request counts, error rate" } }
      }
    },
    "/v1/stats": {
      "get": {
        "tags": ["Admin"],
        "summary": "Lightweight aggregate counts for the deployment (admin-only)",
        "description": "**Scope (#428):** cross-tenant — event counts, active-run counts, active-task counts, and session counts are aggregated across every tenant. Gated with `AdminRoleGuard`; non-admin callers get 403. Per-tenant counts are available via `/v1/tenants/:id/stats` or `/v1/fleet`.",
        "operationId": "getStats",
        "responses": {
          "200": { "description": "Deployment-wide aggregate counts" },
          "403": { "description": "Caller lacks the admin role", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/v1/events/recent": {
      "get": {
        "tags": ["Admin"],
        "summary": "Recent events across every tenant (admin-only)",
        "description": "**Scope (#428):** cross-tenant — streams the last N entries from the global event log with no tenant filter. Gated with `AdminRoleGuard`; per-tenant callers should use `GET /v1/stream` (SSE, tenant-scoped) or `GET /v1/runs/:id/events` (run-scoped).",
        "operationId": "getRecentEvents",
        "parameters": [
          { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "default": 50, "minimum": 1, "maximum": 500 } }
        ],
        "responses": {
          "200": { "description": "Recent events across all tenants" },
          "403": { "description": "Caller lacks the admin role", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/v1/providers/registry": {
      "get": {
        "tags": ["Admin"],
        "summary": "Cross-tenant snapshot of every provider connection (admin-only)",
        "description": "**Scope (#428):** cross-tenant — returns `connection_id`, `backend`, and `model` for every provider binding cached in this process, plus the fallback chain and static catalog. Gated with `AdminRoleGuard`; non-admin callers would otherwise learn which providers other tenants have configured.",
        "operationId": "getProviderRegistry",
        "responses": {
          "200": { "description": "All provider connections + fallbacks + catalog" },
          "403": { "description": "Caller lacks the admin role", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/v1/metrics/prometheus": {
      "get": {
        "tags": ["Admin"],
        "summary": "Prometheus-format metrics scrape endpoint",
        "operationId": "getMetricsPrometheus",
        "responses": { "200": { "description": "text/plain Prometheus exposition format" } }
      }
    },
    "/v1/openapi.json": {
      "get": {
        "tags": ["Admin"],
        "summary": "OpenAPI 3.0 specification",
        "security": [],
        "operationId": "getOpenApiSpec",
        "responses": { "200": { "description": "This document", "content": { "application/json": {} } } }
      }
    },
    "/v1/docs": {
      "get": {
        "tags": ["Admin"],
        "summary": "Swagger UI",
        "security": [],
        "operationId": "getSwaggerUi",
        "responses": { "200": { "description": "Interactive API explorer", "content": { "text/html": {} } } }
      }
    },
    "/v1/system/info": {
      "get": {
        "tags": ["System"],
        "summary": "Comprehensive system information",
        "description": "Returns build metadata, runtime capabilities, deployment mode, store backend, uptime, and sanitised environment config (secrets masked).",
        "operationId": "getSystemInfo",
        "responses": {
          "200": { "description": "System info", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/SystemInfo" } } } }
        }
      }
    },
    "/v1/system/role": {
      "get": {
        "tags": ["System"],
        "summary": "Current process deployment role",
        "description": "Returns the RFC 011 process role (all, api, worker) and which subsystems are active.",
        "operationId": "getSystemRole",
        "responses": {
          "200": { "description": "Process role", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/SystemRole" } } } }
        }
      }
    },
    "/v1/templates": {
      "get": {
        "tags": ["Templates"],
        "summary": "List starter templates",
        "description": "Returns summaries of all registered starter templates (RFC 012). Built-in templates include simple-chatbot, code-reviewer, and data-analyst.",
        "operationId": "listTemplates",
        "responses": {
          "200": { "description": "Template summaries", "content": { "application/json": { "schema": { "type": "array", "items": { "$ref": "#/components/schemas/TemplateSummary" } } } } }
        }
      }
    },
    "/v1/templates/{id}": {
      "get": {
        "tags": ["Templates"],
        "summary": "Get template detail",
        "description": "Returns the full template including all file contents (prompts, configs, eval suites).",
        "operationId": "getTemplate",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" }, "description": "Template ID (e.g. simple-chatbot, code-reviewer, data-analyst)" }],
        "responses": {
          "200": { "description": "Full template with files", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Template" } } } },
          "404": { "description": "Template not found" }
        }
      }
    },
    "/v1/templates/{id}/apply": {
      "post": {
        "tags": ["Templates"],
        "summary": "Apply template to a project",
        "description": "Scaffolds a project by creating the template's file tree under `projects/{project_id}/`. Returns the list of created file paths.",
        "operationId": "applyTemplate",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": {
          "required": true,
          "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ApplyTemplateRequest" } } }
        },
        "responses": {
          "200": { "description": "Files created", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ApplyTemplateResult" } } } },
          "404": { "description": "Template not found" }
        }
      }
    },
    "/v1/entitlements": {
      "get": {
        "tags": ["Entitlements"],
        "summary": "Current plan and usage limits",
        "description": "Returns the tenant's plan tier, current usage counters, limits, and enabled features (RFC 014). Pass `?tenant_id=` to query a specific tenant; defaults to 'default'.",
        "operationId": "getEntitlements",
        "parameters": [
          { "name": "tenant_id", "in": "query", "required": false, "schema": { "type": "string", "default": "default" } }
        ],
        "responses": {
          "200": { "description": "Usage report", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/UsageReport" } } } },
          "404": { "description": "No plan assigned to tenant" }
        }
      }
    },
    "/v1/entitlements/usage": {
      "get": {
        "tags": ["Entitlements"],
        "summary": "Detailed usage breakdown",
        "description": "Per-resource usage with remaining capacity and percentage used. Useful for dashboard gauges and quota warnings.",
        "operationId": "getEntitlementUsage",
        "parameters": [
          { "name": "tenant_id", "in": "query", "required": false, "schema": { "type": "string", "default": "default" } }
        ],
        "responses": {
          "200": { "description": "Detailed usage", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/DetailedUsageReport" } } } },
          "404": { "description": "No plan assigned to tenant" }
        }
      }
    },
    "/v1/admin/rebuild-projections": {
      "post": {
        "tags": ["Admin"],
        "summary": "Rebuild all read-model projections",
        "description": "Performs a snapshot → replay cycle: exports the current event log and replays every event through `apply_projection`. Use after schema changes or bug fixes that affect projection logic.",
        "operationId": "rebuildProjections",
        "responses": {
          "200": { "description": "Rebuild result", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/RebuildProjectionsResponse" } } } }
        }
      }
    },
    "/v1/admin/event-count": {
      "get": {
        "tags": ["Admin"],
        "summary": "Event log cardinality",
        "description": "Total event count and per-type breakdown. Useful for health checks and spotting unexpected event distributions.",
        "operationId": "getEventCount",
        "responses": {
          "200": { "description": "Event counts", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/EventCountResponse" } } } }
        }
      }
    },
    "/v1/admin/event-log": {
      "get": {
        "tags": ["Admin"],
        "summary": "Raw event log viewer",
        "description": "Paginated raw event log with optional position-based cursor. Max 500 per page.",
        "operationId": "getEventLog",
        "parameters": [
          { "name": "from",  "in": "query", "schema": { "type": "integer", "default": 0 }, "description": "Start position (1-based)" },
          { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 100, "maximum": 500 } }
        ],
        "responses": {
          "200": { "description": "Event page with has_more flag", "content": { "application/json": { "schema": { "type": "object", "properties": { "events": { "type": "array", "items": { "$ref": "#/components/schemas/EventEnvelope" } }, "has_more": { "type": "boolean" } } } } } }
        }
      }
    },
    "/v1/admin/snapshot": {
      "post": {
        "tags": ["Admin"],
        "summary": "Export full event log snapshot",
        "description": "Downloads the complete in-memory event log as a JSON file attachment. Use for backups before destructive operations.",
        "operationId": "createSnapshot",
        "responses": {
          "200": { "description": "JSON snapshot file", "content": { "application/json": { "schema": { "type": "object", "description": "StoreSnapshot with all events in position order" } } } }
        }
      }
    },
    "/v1/admin/restore": {
      "post": {
        "tags": ["Admin"],
        "summary": "Restore from snapshot",
        "description": "Clears all in-memory state and replays the uploaded event log. Irreversible — take a snapshot first.",
        "operationId": "restoreSnapshot",
        "requestBody": {
          "required": true,
          "content": { "application/json": { "schema": { "type": "object", "description": "StoreSnapshot previously exported via /v1/admin/snapshot" } } }
        },
        "responses": {
          "200": { "description": "Restore result", "content": { "application/json": { "schema": { "type": "object", "properties": { "ok": { "type": "boolean" }, "event_count": { "type": "integer" }, "replayed": { "type": "integer" } } } } } }
        }
      }
    },
    "/v1/admin/rotate-token": {
      "post": {
        "tags": ["Admin"],
        "summary": "Rotate admin bearer token at runtime",
        "description": "Replaces the active admin token with a new one. The old token is immediately revoked. Requires the current token in the Authorization header. The new token must be at least 16 characters.",
        "operationId": "rotateAdminToken",
        "requestBody": {
          "required": true,
          "content": { "application/json": { "schema": { "type": "object", "required": ["new_token"], "properties": { "new_token": { "type": "string", "minLength": 16, "description": "New admin bearer token (min 16 chars)" } } } } }
        },
        "responses": {
          "200": { "description": "Token rotated", "content": { "application/json": { "schema": { "type": "object", "properties": { "status": { "type": "string", "example": "rotated" } } } } } },
          "400": { "description": "new_token too short (min 16 chars)" }
        }
      }
    },
    "/v1/admin/backup": {
      "post": {
        "tags": ["Admin"],
        "summary": "Create SQLite database backup",
        "description": "Copies the active SQLite database file to a timestamped backup. Only available when the SQLite backend is active (returns 404 otherwise).",
        "operationId": "createBackup",
        "responses": {
          "200": { "description": "Backup created", "content": { "application/json": { "schema": { "type": "object", "properties": { "status": { "type": "string", "example": "backed_up" }, "path": { "type": "string" }, "size_bytes": { "type": "integer" } } } } } },
          "404": { "description": "SQLite backend not active" }
        }
      }
    },
    "/v1/webhooks/github": {
      "post": {
        "tags": ["Webhooks"],
        "summary": "Receive GitHub webhook events",
        "description": "Verifies HMAC-SHA256 signature, parses the event, and dispatches based on configured event-to-action mappings. Auth is via webhook signature, not bearer token.",
        "operationId": "githubWebhook",
        "responses": {
          "200": { "description": "Event processed or ignored" },
          "401": { "description": "Invalid or missing signature" },
          "503": { "description": "GitHub App not configured" }
        }
      }
    },
    "/v1/webhooks/github/actions": {
      "get": {
        "tags": ["Webhooks"],
        "summary": "List GitHub webhook event-to-action mappings",
        "operationId": "listWebhookActions",
        "responses": {
          "200": { "description": "Current action mappings", "content": { "application/json": { "schema": { "type": "object" } } } }
        }
      },
      "put": {
        "tags": ["Webhooks"],
        "summary": "Replace GitHub webhook event-to-action mappings",
        "operationId": "setWebhookActions",
        "requestBody": {
          "required": true,
          "content": { "application/json": { "schema": { "type": "object", "properties": { "actions": { "type": "array", "items": { "type": "object", "properties": { "event_pattern": { "type": "string" }, "label_filter": { "type": "string" }, "repo_filter": { "type": "string" }, "action": { "type": "string", "enum": ["create_and_orchestrate", "acknowledge", "ignore"] } } } } } } } }
        },
        "responses": {
          "200": { "description": "Actions updated" },
          "503": { "description": "GitHub App not configured" }
        }
      }
    },
    "/v1/webhooks/github/scan": {
      "post": {
        "tags": ["Webhooks"],
        "summary": "Scan a repo for open issues and queue them for sequential processing",
        "description": "Lists open issues from a GitHub repo via the App API, creates a session+run for each, and processes them one at a time through the orchestrator. Each issue gets its own PR with an approval gate before merge.",
        "operationId": "githubScan",
        "requestBody": {
          "required": true,
          "content": { "application/json": { "schema": { "type": "object", "required": ["repo", "installation_id"], "properties": { "repo": { "type": "string", "description": "owner/repo" }, "installation_id": { "type": "integer", "description": "GitHub App installation ID" }, "labels": { "type": "string", "description": "Comma-separated label filter" }, "limit": { "type": "integer", "description": "Max issues to scan (default 30, max 100)" } } } } }
        },
        "responses": {
          "200": { "description": "Issues queued for processing" },
          "503": { "description": "GitHub App not configured" }
        }
      }
    },
    "/v1/webhooks/github/queue": {
      "get": {
        "tags": ["Webhooks"],
        "summary": "View the current issue processing queue",
        "operationId": "githubQueue",
        "responses": {
          "200": { "description": "Queue status", "content": { "application/json": { "schema": { "type": "object" } } } }
        }
      }
    },
    "/v1/bundles/export": {
      "post": {
        "tags": ["Bundles"],
        "summary": "Export project artifacts as a portable bundle",
        "description": "Exports prompt assets, releases, and knowledge documents from a project into the RFC 013 CairnBundle format.",
        "operationId": "exportBundle",
        "requestBody": {
          "required": true,
          "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ExportBundleRequest" } } }
        },
        "responses": {
          "200": { "description": "CairnBundle envelope", "content": { "application/json": { "schema": { "type": "object" } } } }
        }
      }
    },
    "/v1/bundles/apply": {
      "post": {
        "tags": ["Bundles"],
        "summary": "Apply a bundle to a project",
        "description": "Validates, plans, and applies a CairnBundle into the target project with conflict resolution. Supports skip/overwrite/rename strategies.",
        "operationId": "applyBundle",
        "requestBody": {
          "required": true,
          "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ApplyBundleRequest" } } }
        },
        "responses": {
          "200": { "description": "Import result with per-artifact outcomes", "content": { "application/json": { "schema": { "type": "object", "properties": { "artifacts_imported": { "type": "integer" }, "artifacts_skipped": { "type": "integer" }, "outcomes": { "type": "array", "items": { "type": "object" } } } } } } },
          "400": { "description": "Bundle validation failed" }
        }
      }
    },
    "/v1/overview": {
      "get": {
        "tags": ["Health"],
        "summary": "High-level operator overview",
        "description": "Combines status and dashboard: store backend, deployment mode, uptime, active counts, cost summary, feature flags.\n\n**Scope (#428):** cross-deployment (not tenant-scoped) — the response body carries only process-level health and component statuses, no per-tenant data, so it intentionally stays ungated. If a future field is added that carries tenant-specific counts, the handler must adopt `AdminRoleGuard`.",
        "operationId": "getOverview",
        "responses": { "200": { "description": "Overview data" } }
      }
    },
    "/v1/prompts/assets": {
      "get": {
        "tags": ["Prompts"],
        "summary": "List prompt assets",
        "operationId": "listPromptAssets",
        "parameters": [
          { "name": "limit",  "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Prompt asset list" } }
      },
      "post": {
        "tags": ["Prompts"],
        "summary": "Create a prompt asset",
        "operationId": "createPromptAsset",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Prompt asset created" } }
      }
    },
    "/v1/prompts/releases": {
      "get": {
        "tags": ["Prompts"],
        "summary": "List prompt releases",
        "operationId": "listPromptReleases",
        "parameters": [
          { "name": "limit",  "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Prompt release list" } }
      },
      "post": {
        "tags": ["Prompts"],
        "summary": "Create a prompt release",
        "operationId": "createPromptRelease",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Prompt release created" } }
      }
    },
    "/v1/notifications": {
      "get": {
        "tags": ["Notifications"],
        "summary": "List notifications",
        "operationId": "listNotifications",
        "responses": { "200": { "description": "Notification list" } }
      }
    },
    "/v1/notifications/read-all": {
      "post": {
        "tags": ["Notifications"],
        "summary": "Mark all notifications as read",
        "operationId": "markAllNotificationsRead",
        "responses": { "200": { "description": "Marked read" } }
      }
    },
    "/v1/notifications/{id}/read": {
      "post": {
        "tags": ["Notifications"],
        "summary": "Mark a single notification as read",
        "operationId": "markNotificationRead",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Marked read" } }
      }
    },
    "/v1/decisions": {
      "get": {
        "tags": ["Decisions"],
        "summary": "List recent decisions (RFC 019)",
        "operationId": "listDecisions",
        "responses": { "200": { "description": "Decision list" } }
      }
    },
    "/v1/decisions/cache": {
      "get": {
        "tags": ["Decisions"],
        "summary": "List active cached decisions (learned rules)",
        "operationId": "listDecisionCache",
        "responses": { "200": { "description": "Cached decisions" } }
      }
    },
    "/v1/decisions/{id}": {
      "get": {
        "tags": ["Decisions"],
        "summary": "Get decision with full reasoning chain",
        "operationId": "getDecision",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Decision detail" } }
      }
    },
    "/v1/decisions/{id}/invalidate": {
      "post": {
        "tags": ["Decisions"],
        "summary": "Invalidate a specific cached decision",
        "operationId": "invalidateDecision",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Invalidated" } }
      }
    },
    "/v1/decisions/invalidate": {
      "post": {
        "tags": ["Decisions"],
        "summary": "Bulk invalidate by scope and kind",
        "operationId": "bulkInvalidateDecisions",
        "responses": { "200": { "description": "Invalidation count" } }
      }
    },
    "/v1/decisions/evaluate": {
      "post": {
        "tags": ["Decisions"],
        "summary": "Evaluate a decision request (RFC 019 8-step pipeline)",
        "description": "Drives a DecisionRequest through scope, visibility, guardrail, budget, cache, approval, cache-write, and return steps. Cached decisions are persisted to the event log so they survive restart (RFC 020 §'Decision Cache Survival').",
        "operationId": "evaluateDecision",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["kind"],
                "properties": {
                  "kind": { "type": "object" },
                  "principal": { "type": "object" },
                  "subject": { "type": "object" },
                  "tenant_id": { "type": "string" },
                  "workspace_id": { "type": "string" },
                  "project_id": { "type": "string" },
                  "correlation_id": { "type": "string" }
                }
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Decision evaluated; body carries decision_id, outcome, source, cached, cache_hit." },
          "400": { "description": "Malformed request." }
        }
      }
    },
    "/v1/decisions/invalidate-by-rule": {
      "post": {
        "tags": ["Decisions"],
        "summary": "Invalidate decisions referencing a guardrail rule",
        "operationId": "invalidateByRule",
        "responses": { "200": { "description": "Invalidation count" } }
      }
    },
    "/v1/runs/{id}/approve": {
      "post": {
        "tags": ["Plan Review"],
        "summary": "Approve a plan artifact (RFC 018)",
        "description": "Records an operator approval for a Plan-mode run. Audit event attributed to the authenticated principal (T6a-H7). Request body is validated against `ApprovePlanRequest` with `deny_unknown_fields` — typos such as `reviewerComments` (camelCase) return 422 (#427).",
        "operationId": "approvePlan",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": false, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ApprovePlanRequest" } } } },
        "responses": {
          "200": { "description": "Approved, next_step: create_execute_run" },
          "404": { "description": "Run not found", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "422": { "description": "Invalid request body (e.g. unknown field, wrong type)", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/v1/runs/{id}/reject": {
      "post": {
        "tags": ["Plan Review"],
        "summary": "Reject a plan artifact",
        "description": "Records an operator rejection. Request body validated against `RejectPlanRequest` with `deny_unknown_fields` (#427).",
        "operationId": "rejectPlan",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": false, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/RejectPlanRequest" } } } },
        "responses": {
          "200": { "description": "Rejected" },
          "404": { "description": "Run not found", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "422": { "description": "Invalid request body", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/v1/runs/{id}/revise": {
      "post": {
        "tags": ["Plan Review"],
        "summary": "Request plan revision, creates new Plan-mode run",
        "description": "Creates a new Plan-mode run seeded from the original. `reviewer_comments` is required — a revise without comments is a client error (400). Body validated against `RevisePlanRequest` with `deny_unknown_fields` (#427).",
        "operationId": "revisePlan",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/RevisePlanRequest" } } } },
        "responses": {
          "201": { "description": "New plan run created" },
          "400": { "description": "reviewer_comments missing or empty", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "404": { "description": "Run not found", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "422": { "description": "Invalid request body", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/v1/sqeq/initialize": {
      "post": {
        "tags": ["SQ/EQ Protocol"],
        "summary": "Initialize SQ/EQ transport session (RFC 021)",
        "operationId": "sqeqInitialize",
        "responses": { "200": { "description": "Session established" } }
      }
    },
    "/v1/sqeq/submit": {
      "post": {
        "tags": ["SQ/EQ Protocol"],
        "summary": "Submit a command via SQ/EQ",
        "operationId": "sqeqSubmit",
        "responses": { "202": { "description": "Submission accepted" } }
      }
    },
    "/v1/sqeq/events": {
      "get": {
        "tags": ["SQ/EQ Protocol"],
        "summary": "SSE event stream with scope filtering",
        "operationId": "sqeqEvents",
        "responses": { "200": { "description": "Event stream" } }
      }
    },
    "/.well-known/agent.json": {
      "get": {
        "tags": ["A2A"],
        "summary": "A2A Agent Card (RFC 021)",
        "operationId": "a2aAgentCard",
        "responses": { "200": { "description": "Agent Card JSON" } }
      }
    },
    "/v1/a2a/tasks": {
      "post": {
        "tags": ["A2A"],
        "summary": "Submit an A2A task",
        "operationId": "a2aSubmitTask",
        "responses": { "201": { "description": "Task submitted" } }
      }
    },
    "/v1/a2a/tasks/{id}": {
      "get": {
        "tags": ["A2A"],
        "summary": "Get A2A task status",
        "operationId": "a2aGetTask",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Task status" } }
      }
    },
    "/v1/projects/{project}/local-paths": {
      "delete": {
        "tags": ["Projects"],
        "summary": "Detach a local_fs path from a project",
        "description": "Removes a local-filesystem pseudo-repo previously attached via `POST /v1/projects/{project}/repos` with `host=local_fs`. Separate from the `/repos/{owner}/{repo}` endpoint because arbitrary filesystem paths can't be split into two path segments.",
        "operationId": "detachProjectLocalPath",
        "parameters": [{ "name": "project", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["path"],
                "properties": { "path": { "type": "string" } }
              }
            }
          }
        },
        "responses": {
          "204": { "description": "Detached" },
          "404": { "description": "No such path attached to this project" }
        }
      }
    },
    "/v1/integrations/github/verify-installation": {
      "post": {
        "tags": ["Integrations"],
        "summary": "Verify a GitHub App installation",
        "description": "Mints a JWT from the provided app_id + private_key, exchanges it for an installation access token, and reports the installation's owner and repo count. Does not mutate server state.",
        "operationId": "verifyGithubInstallation",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["app_id", "private_key", "installation_id"],
                "properties": {
                  "app_id":          { "type": "integer" },
                  "private_key":     { "type": "string", "description": "PEM-encoded RSA private key" },
                  "installation_id": { "type": "integer" }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Verification succeeded",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "verified":   { "type": "boolean" },
                    "owner":      { "type": "string" },
                    "repo_count": { "type": "integer" },
                    "expires_at": { "type": "string" }
                  }
                }
              }
            }
          },
          "400": { "description": "Invalid request (bad PEM, empty key, etc.)" },
          "502": { "description": "GitHub API error — credentials or installation ID rejected" }
        }
      }
    },
    "/v1/runs/{id}/orchestrate": {
      "post": {
        "tags": ["Runs"],
        "summary": "Kick off orchestration for a run (F65)",
        "description": "Starts the orchestration loop. The request body is an `OrchestrateRequest` (see schema). Returns 202 when the loop has been enqueued.\n\n**Idempotency (#433).** This endpoint honors the optional `Idempotency-Key` request header. When present, the first response is cached per (tenant, endpoint, key) for 5 minutes; retries with the same key + same body replay the first response verbatim (with an `idempotent-replayed: true` response header). Retries with the same key but a DIFFERENT body return 409 `idempotency_key_reuse`. Concurrent retries with the same key while the first is still in flight return 409 `idempotency_in_progress`. Clients should generate a fresh Idempotency-Key per logical submission (UUID v4 works well) and re-use it only on transport retries.",
        "operationId": "orchestrateRun",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } },
          {
            "name": "Idempotency-Key",
            "in": "header",
            "required": false,
            "description": "Client-supplied key that makes a retry of this request safe. Same key + same body replays the prior response; same key + different body 409s. 1..=255 ASCII chars.",
            "schema": { "type": "string", "minLength": 1, "maxLength": 255 }
          }
        ],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/OrchestrateRequest" } } } },
        "responses": {
          "202": { "description": "Orchestration enqueued" },
          "400": { "description": "Invalid request (includes malformed `Idempotency-Key` header)", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "404": { "description": "Run not found", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "409": { "description": "Idempotency-Key conflict — either reused with a different body (`idempotency_key_reuse`) or a request with the same key is still in flight (`idempotency_in_progress`).", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/v1/runs/{id}/cancel": {
      "post": {
        "tags": ["Runs"],
        "summary": "Cancel a run",
        "operationId": "cancelRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Run cancelled" }, "404": { "description": "Run not found" } }
      }
    },
    "/v1/runs/{id}/recover": {
      "post": {
        "tags": ["Runs"],
        "summary": "Force run recovery — no-op legacy",
        "description": "**Deprecated.** Manual recovery used to drive cairn-side `RecoveryServiceImpl::recover_interrupted_runs`; recovery now runs unconditionally inside FlowFabric's background scanners (14 total). This endpoint is a 202 stub preserved so dashboards that hit it don't break. Scheduled for removal at v2.\n\nDeprecation is signalled via RFC 8594 response headers: `Deprecation` (the day the endpoint was retired), `Sunset` (the planned removal date), and `Link; rel=\"deprecation\"` (docs URL). Pre-#430 this endpoint returned `\"deprecated\": true` in the body; body markers are invisible to SDK generators and API gateways, so the signal moved into headers per spec.",
        "deprecated": true,
        "operationId": "recoverRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": {
          "202": {
            "description": "Recovery request accepted (no-op). Inspect `Deprecation` + `Sunset` response headers per RFC 8594.",
            "headers": {
              "Deprecation": { "description": "HTTP-date at which this endpoint was deprecated (RFC 8594).", "schema": { "type": "string" } },
              "Sunset":      { "description": "HTTP-date at which this endpoint will be removed (RFC 8594).",      "schema": { "type": "string" } },
              "Link":        { "description": "Link header with `rel=\"deprecation\"` pointing at human-readable docs.", "schema": { "type": "string" } }
            }
          },
          "404": { "description": "Run not found", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/v1/runs/{id}/spawn": {
      "post": {
        "tags": ["Runs"],
        "summary": "Spawn a subagent child run",
        "operationId": "spawnSubagentRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": {
          "201": {
            "description": "Child run created",
            "content": { "application/json": { "schema": { "type": "object", "properties": { "parent_run_id": { "type": "string" }, "child_run_id": { "type": "string" } } } } }
          },
          "404": { "description": "Parent run not found" }
        }
      }
    },
    "/v1/runs/{id}/intervene": {
      "post": {
        "tags": ["Runs"],
        "summary": "Operator intervention on a run",
        "operationId": "interveneRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Intervention recorded" }, "404": { "description": "Run not found" } }
      }
    },
    "/v1/runs/{id}/diagnose": {
      "get": {
        "tags": ["Runs"],
        "summary": "Build a diagnosis report for a (potentially stuck) run",
        "operationId": "diagnoseRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Diagnosis report" }, "404": { "description": "Run not found" } }
      }
    },
    "/v1/runs/{id}/sla": {
      "get": {
        "tags": ["Runs"],
        "summary": "Fetch SLA status for a run",
        "operationId": "getRunSla",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "SLA status" }, "404": { "description": "Run or SLA not found" } }
      },
      "post": {
        "tags": ["Runs"],
        "summary": "Configure SLA for a run",
        "operationId": "setRunSla",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "SLA configured" }, "404": { "description": "Run not found" } }
      }
    },
    "/v1/runs/{id}/children": {
      "get": {
        "tags": ["Runs"],
        "summary": "List child (subagent) runs for a parent run",
        "operationId": "listChildRuns",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Child run list" }, "404": { "description": "Parent run not found" } }
      }
    },
    "/v1/runs/{id}/interventions": {
      "get": {
        "tags": ["Runs"],
        "summary": "List operator interventions recorded against a run",
        "operationId": "listRunInterventions",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Intervention list" }, "404": { "description": "Run not found" } }
      }
    },
    "/v1/runs/{id}/cost-alert": {
      "post": {
        "tags": ["Runs"],
        "summary": "Set a cost alert threshold for a run (RFC 010)",
        "description": "Configures a cost alert that fires when total run cost crosses `threshold_micros`. Returns the created alert record per #431 so the UI can render the configured threshold without a follow-up GET.",
        "operationId": "setRunCostAlert",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": {
          "201": { "description": "Alert configured — returns the created alert record.", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/RunCostAlertResponse" } } } },
          "404": { "description": "Run not found", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/v1/runs/{id}/audit": {
      "get": {
        "tags": ["Runs"],
        "summary": "Audit trail for a run",
        "operationId": "getRunAuditTrail",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Audit trail" }, "404": { "description": "Run not found" } }
      }
    },
    "/v1/runs/{id}/export": {
      "get": {
        "tags": ["Runs"],
        "summary": "Export a run as a portable bundle",
        "operationId": "exportRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Run bundle" }, "404": { "description": "Run not found" } }
      }
    },
    "/v1/runs/{id}/checkpoint": {
      "post": {
        "tags": ["Runs"],
        "summary": "Force-capture a checkpoint for a run",
        "operationId": "createRunCheckpoint",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "201": { "description": "Checkpoint recorded" }, "404": { "description": "Run not found" } }
      }
    },
    "/v1/runs/{id}/checkpoint-strategy": {
      "post": {
        "tags": ["Runs"],
        "summary": "Set checkpoint strategy for a run",
        "operationId": "setCheckpointStrategy",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Strategy set" }, "404": { "description": "Run not found" } }
      }
    },
    "/v1/runs/{id}/replay": {
      "post": {
        "tags": ["Runs"],
        "summary": "Replay a run from the event log",
        "operationId": "replayRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Replay enqueued" }, "404": { "description": "Run not found" } }
      }
    },
    "/v1/runs/{id}/replay-to-checkpoint": {
      "post": {
        "tags": ["Runs"],
        "summary": "Replay a run up to a specific checkpoint",
        "operationId": "replayRunToCheckpoint",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Replay enqueued" }, "404": { "description": "Run or checkpoint not found" } }
      }
    },
    "/v1/runs/stalled": {
      "get": {
        "tags": ["Runs"],
        "summary": "List stalled runs with diagnosis reports",
        "operationId": "listStalledRuns",
        "parameters": [
          { "name": "minutes", "in": "query", "schema": { "type": "integer", "default": 30 } },
          { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Stalled-run list" } }
      }
    },
    "/v1/runs/escalated": {
      "get": {
        "tags": ["Runs"],
        "summary": "List recovery-escalated runs for the tenant",
        "operationId": "listEscalatedRuns",
        "parameters": [
          { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Escalated-run list" } }
      }
    },
    "/v1/runs/sla-breached": {
      "get": {
        "tags": ["Runs"],
        "summary": "List SLA-breached runs for the tenant",
        "operationId": "listSlaBreachedRuns",
        "parameters": [
          { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "SLA-breach list" } }
      }
    },
    "/v1/runs/cost-alerts": {
      "get": {
        "tags": ["Runs"],
        "summary": "List triggered run cost alerts",
        "operationId": "listRunCostAlerts",
        "responses": { "200": { "description": "Triggered cost alert list" } }
      }
    },
    "/v1/runs/resume-due": {
      "get": {
        "tags": ["Runs"],
        "summary": "List paused runs whose resume time has arrived",
        "operationId": "listDueRunResumes",
        "responses": { "200": { "description": "Due-resume run list" } }
      }
    },
    "/v1/runs/process-scheduled-resumes": {
      "post": {
        "tags": ["Runs"],
        "summary": "Process all paused runs whose resume time has arrived",
        "operationId": "processScheduledRunResumes",
        "responses": { "200": { "description": "Resume batch processed" } }
      }
    },
    "/v1/runs/batch": {
      "post": {
        "tags": ["Runs"],
        "summary": "Batch-create multiple runs in one request",
        "operationId": "batchCreateRuns",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "array", "items": { "type": "object" } } } } },
        "responses": { "201": { "description": "Runs created" } }
      }
    },
    "/v1/workers": {
      "get": {
        "tags": ["Workers"],
        "summary": "List registered external workers",
        "operationId": "listWorkers",
        "parameters": [
          { "name": "limit",  "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Worker list" } }
      }
    },
    "/v1/workers/register": {
      "post": {
        "tags": ["Workers"],
        "summary": "Register a new external worker",
        "operationId": "registerWorker",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object", "required": ["worker_id"], "properties": { "worker_id": { "type": "string" }, "display_name": { "type": "string" } } } } } },
        "responses": { "201": { "description": "Worker registered" } }
      }
    },
    "/v1/workers/{id}": {
      "get": {
        "tags": ["Workers"],
        "summary": "Get a worker by id",
        "operationId": "getWorker",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Worker record" }, "404": { "description": "Worker not found" } }
      }
    },
    "/v1/workers/{id}/claim": {
      "post": {
        "tags": ["Workers"],
        "summary": "Worker claims a task for execution",
        "operationId": "workerClaimTask",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Task claimed" }, "404": { "description": "Worker not found" } }
      }
    },
    "/v1/workers/{id}/heartbeat": {
      "post": {
        "tags": ["Workers"],
        "summary": "Worker sends a heartbeat",
        "operationId": "workerHeartbeat",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Heartbeat accepted" }, "404": { "description": "Worker not found" } }
      }
    },
    "/v1/workers/{id}/report": {
      "post": {
        "tags": ["Workers"],
        "summary": "Worker reports task outcome",
        "operationId": "workerReport",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Report accepted" }, "404": { "description": "Worker not found" } }
      }
    },
    "/v1/workers/{id}/suspend": {
      "post": {
        "tags": ["Workers"],
        "summary": "Suspend an external worker",
        "operationId": "suspendWorker",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Worker suspended" }, "404": { "description": "Worker not found" } }
      }
    },
    "/v1/workers/{id}/reactivate": {
      "post": {
        "tags": ["Workers"],
        "summary": "Reactivate a suspended worker",
        "operationId": "reactivateWorker",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Worker reactivated" }, "404": { "description": "Worker not found" } }
      }
    },
    "/v1/feed": {
      "get": {
        "tags": ["Feed"],
        "summary": "List feed items for the active project scope",
        "operationId": "listFeedItems",
        "parameters": [
          { "name": "tenant_id", "in": "query", "schema": { "type": "string" } },
          { "name": "workspace_id", "in": "query", "schema": { "type": "string" } },
          { "name": "project_id", "in": "query", "schema": { "type": "string" } },
          { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Feed items" } }
      }
    },
    "/v1/feed/{id}/read": {
      "post": {
        "tags": ["Feed"],
        "summary": "Mark a feed item as read",
        "operationId": "markFeedItemRead",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Marked read" }, "404": { "description": "Feed item not found" } }
      }
    },
    "/v1/feed/read-all": {
      "post": {
        "tags": ["Feed"],
        "summary": "Mark every feed item in the project scope as read",
        "operationId": "markAllFeedItemsRead",
        "responses": { "200": { "description": "Changed count", "content": { "application/json": { "schema": { "type": "object", "properties": { "changed": { "type": "integer" } } } } } } }
      }
    },
    "/v1/skills": {
      "get": {
        "tags": ["Skills"],
        "summary": "List skills available in the active project scope",
        "operationId": "listSkills",
        "parameters": [
          { "name": "tenant_id", "in": "query", "schema": { "type": "string" } },
          { "name": "workspace_id", "in": "query", "schema": { "type": "string" } },
          { "name": "project_id", "in": "query", "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "Skill list" } }
      },
      "post": {
        "tags": ["Skills"],
        "summary": "Register a skill",
        "operationId": "createSkill",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Skill created" } }
      }
    },
    "/v1/skills/{id}": {
      "get": {
        "tags": ["Skills"],
        "summary": "Get a skill by id",
        "operationId": "getSkill",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Skill record" }, "404": { "description": "Skill not found" } }
      },
      "delete": {
        "tags": ["Skills"],
        "summary": "Delete a skill",
        "operationId": "deleteSkill",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "204": { "description": "Deleted" }, "404": { "description": "Skill not found" } }
      }
    },
    "/v1/auth/tokens": {
      "get": {
        "tags": ["Auth"],
        "summary": "List auth tokens for the caller's tenant",
        "operationId": "listAuthTokens",
        "parameters": [
          { "name": "limit",  "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Auth token list" } }
      },
      "post": {
        "tags": ["Auth"],
        "summary": "Mint a new auth token",
        "operationId": "createAuthToken",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Auth token created" } }
      }
    },
    "/v1/auth/tokens/{id}": {
      "delete": {
        "tags": ["Auth"],
        "summary": "Revoke an auth token",
        "operationId": "deleteAuthToken",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "204": { "description": "Revoked" }, "404": { "description": "Token not found" } }
      }
    },
    "/v1/projects/{project}/triggers": {
      "get": {
        "tags": ["Triggers"],
        "summary": "List triggers for a project",
        "operationId": "listProjectTriggers",
        "parameters": [{ "name": "project", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Trigger list" } }
      },
      "post": {
        "tags": ["Triggers"],
        "summary": "Create a trigger in a project",
        "operationId": "createProjectTrigger",
        "parameters": [{ "name": "project", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Trigger created" } }
      }
    },
    "/v1/projects/{project}/triggers/{trigger_id}": {
      "get": {
        "tags": ["Triggers"],
        "summary": "Get a trigger by id",
        "operationId": "getProjectTrigger",
        "parameters": [
          { "name": "project", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "trigger_id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "Trigger record" }, "404": { "description": "Trigger not found" } }
      },
      "delete": {
        "tags": ["Triggers"],
        "summary": "Delete a trigger",
        "operationId": "deleteProjectTrigger",
        "parameters": [
          { "name": "project", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "trigger_id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "204": { "description": "Deleted" }, "404": { "description": "Trigger not found" } }
      }
    },
    "/v1/projects/{project}/triggers/{trigger_id}/enable": {
      "post": {
        "tags": ["Triggers"],
        "summary": "Enable a trigger",
        "operationId": "enableProjectTrigger",
        "parameters": [
          { "name": "project", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "trigger_id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "Trigger enabled" }, "404": { "description": "Trigger not found" } }
      }
    },
    "/v1/projects/{project}/triggers/{trigger_id}/disable": {
      "post": {
        "tags": ["Triggers"],
        "summary": "Disable a trigger",
        "operationId": "disableProjectTrigger",
        "parameters": [
          { "name": "project", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "trigger_id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "Trigger disabled" }, "404": { "description": "Trigger not found" } }
      }
    },
    "/v1/projects/{project}/triggers/{trigger_id}/resume": {
      "post": {
        "tags": ["Triggers"],
        "summary": "Resume a paused trigger",
        "operationId": "resumeProjectTrigger",
        "parameters": [
          { "name": "project", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "trigger_id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "Trigger resumed" }, "404": { "description": "Trigger not found" } }
      }
    },
    "/v1/projects/{project}/run-templates": {
      "get": {
        "tags": ["Run templates"],
        "summary": "List run templates for a project",
        "operationId": "listProjectRunTemplates",
        "parameters": [{ "name": "project", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Run template list" } }
      },
      "post": {
        "tags": ["Run templates"],
        "summary": "Create a run template in a project",
        "operationId": "createProjectRunTemplate",
        "parameters": [{ "name": "project", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Run template created" } }
      }
    },
    "/v1/projects/{project}/run-templates/{template_id}": {
      "get": {
        "tags": ["Run templates"],
        "summary": "Get a run template by id",
        "operationId": "getProjectRunTemplate",
        "parameters": [
          { "name": "project", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "template_id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "Run template" }, "404": { "description": "Template not found" } }
      },
      "delete": {
        "tags": ["Run templates"],
        "summary": "Delete a run template",
        "operationId": "deleteProjectRunTemplate",
        "parameters": [
          { "name": "project", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "template_id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "204": { "description": "Deleted" }, "404": { "description": "Template not found" } }
      }
    },
    "/v1/costs": {
      "get": {
        "tags": ["Costs"],
        "summary": "List per-session cost records for the caller's tenant",
        "description": "Newest-first. The `limit` defaults to 200 and is clamped at 1 000 per page (issue #423); paginate via `offset` + `has_more`. `since_ms` bounds the `updated_at_ms` lower window.",
        "operationId": "listTenantCosts",
        "parameters": [
          { "name": "since_ms", "in": "query", "schema": { "type": "integer", "format": "int64" } },
          { "name": "limit",    "in": "query", "schema": { "type": "integer", "default": 200, "maximum": 1000 } },
          { "name": "offset",   "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Per-session cost page" } }
      }
    },
    "/v1/evals/runs/{id}/start": {
      "post": {
        "tags": ["Evals"],
        "summary": "Start an eval run",
        "operationId": "startEvalRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Run started" }, "404": { "description": "Run not found" } }
      }
    },
    "/v1/evals/runs/{id}/complete": {
      "post": {
        "tags": ["Evals"],
        "summary": "Complete an eval run",
        "operationId": "completeEvalRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Run completed" }, "404": { "description": "Run not found" } }
      }
    },
    "/v1/evals/runs/{id}/score": {
      "post": {
        "tags": ["Evals"],
        "summary": "Record a per-entry score for an eval run",
        "operationId": "scoreEvalRun",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Score recorded" }, "404": { "description": "Run not found" } }
      }
    },
    "/v1/tool-invocations": {
      "get": {
        "tags": ["Tools"],
        "summary": "List tool invocations for a run",
        "operationId": "listToolInvocations",
        "parameters": [
          { "name": "run_id", "in": "query", "schema": { "type": "string" } },
          { "name": "state",  "in": "query", "schema": { "type": "string" } },
          { "name": "limit",  "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Tool invocation page" } }
      },
      "post": {
        "tags": ["Tools"],
        "summary": "Record a tool invocation start",
        "operationId": "createToolInvocation",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Invocation recorded" } }
      }
    },
    "/v1/tool-invocations/{id}": {
      "get": {
        "tags": ["Tools"],
        "summary": "Get a tool invocation by id",
        "operationId": "getToolInvocation",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Invocation view" }, "404": { "description": "Not found" } }
      }
    },
    "/v1/tool-invocations/{id}/complete": {
      "post": {
        "tags": ["Tools"],
        "summary": "Mark a tool invocation as completed",
        "operationId": "completeToolInvocation",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Invocation completed" }, "404": { "description": "Not found" } }
      }
    },
    "/v1/tool-invocations/{id}/cancel": {
      "post": {
        "tags": ["Tools"],
        "summary": "Cancel (and record failure for) a tool invocation",
        "operationId": "cancelToolInvocation",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Invocation cancelled" }, "404": { "description": "Not found" } }
      }
    },
    "/v1/tool-invocations/{id}/progress": {
      "get": {
        "tags": ["Tools"],
        "summary": "Get latest progress snapshot for a tool invocation",
        "operationId": "getToolInvocationProgress",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Progress snapshot" }, "404": { "description": "No progress recorded" } }
      }
    },
    "/v1/checkpoints": {
      "get": {
        "tags": ["Tools"],
        "summary": "List checkpoints for a run",
        "operationId": "listCheckpoints",
        "parameters": [
          { "name": "run_id", "in": "query", "required": true, "schema": { "type": "string" } },
          { "name": "limit",  "in": "query", "schema": { "type": "integer", "default": 100 } }
        ],
        "responses": { "200": { "description": "Checkpoint list" }, "400": { "description": "run_id is required" } }
      }
    },
    "/v1/checkpoints/{id}": {
      "get": {
        "tags": ["Tools"],
        "summary": "Get a checkpoint by id",
        "operationId": "getCheckpoint",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Checkpoint" }, "404": { "description": "Not found" } }
      }
    },
    "/v1/checkpoints/{id}/restore": {
      "post": {
        "tags": ["Tools"],
        "summary": "Restore run state from a checkpoint",
        "operationId": "restoreCheckpoint",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Restored" }, "404": { "description": "Not found" } }
      }
    },
    "/v1/memory/deep-search": {
      "post": {
        "tags": ["Memory"],
        "summary": "Deep search across memory documents (rerank + graph expansion)",
        "operationId": "memoryDeepSearch",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Search results" } }
      }
    },
    "/v1/memory/diagnostics": {
      "get": {
        "tags": ["Memory"],
        "summary": "Memory pipeline diagnostics (index health, embedder queue depth, etc.)",
        "operationId": "getMemoryDiagnostics",
        "responses": { "200": { "description": "Diagnostics payload" } }
      }
    },
    "/v1/integrations": {
      "get": {
        "tags": ["Integrations"],
        "summary": "List integrations configured for the active project",
        "operationId": "listIntegrations",
        "responses": { "200": { "description": "Integration list" } }
      },
      "post": {
        "tags": ["Integrations"],
        "summary": "Register an integration",
        "operationId": "createIntegration",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Integration created" } }
      }
    },
    "/v1/integrations/{integration_id}": {
      "get": {
        "tags": ["Integrations"],
        "summary": "Get an integration by id",
        "operationId": "getIntegration",
        "parameters": [{ "name": "integration_id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Integration record" }, "404": { "description": "Not found" } }
      },
      "delete": {
        "tags": ["Integrations"],
        "summary": "Delete an integration",
        "operationId": "deleteIntegration",
        "parameters": [{ "name": "integration_id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "204": { "description": "Deleted" }, "404": { "description": "Not found" } }
      }
    },
    "/v1/providers/connections/{id}/resolve-key": {
      "post": {
        "tags": ["Providers"],
        "summary": "Resolve the API key for a provider connection (admin only)",
        "operationId": "resolveProviderConnectionKey",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Resolved key" }, "404": { "description": "Connection not found" } }
      }
    },
    "/v1/providers/connections/{id}/retry-policy": {
      "post": {
        "tags": ["Providers"],
        "summary": "Set retry policy for a provider connection",
        "operationId": "setProviderRetryPolicy",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Policy set" }, "404": { "description": "Connection not found" } }
      }
    },
    "/v1/providers/connections/{id}/test": {
      "post": {
        "tags": ["Providers"],
        "summary": "Test a provider connection by running a synthetic call",
        "operationId": "testProviderConnection",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Test result" }, "404": { "description": "Connection not found" } }
      }
    },
    "/v1/plugins/catalog": {
      "get": {
        "tags": ["Plugins"],
        "summary": "Browse the plugin marketplace catalog (RFC 015)",
        "operationId": "listPluginCatalog",
        "responses": { "200": { "description": "Catalog list" } }
      }
    },
    "/v1/plugins/{id}/install": {
      "post": {
        "tags": ["Plugins"],
        "summary": "Install a plugin by id",
        "operationId": "installPlugin",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Plugin installed" }, "404": { "description": "Plugin not found" } }
      }
    },
    "/v1/plugins/{id}/uninstall": {
      "post": {
        "tags": ["Plugins"],
        "summary": "Uninstall a plugin by id",
        "operationId": "uninstallPlugin",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Plugin uninstalled" }, "404": { "description": "Plugin not found" } }
      }
    },
    "/v1/plugins/{id}/verify": {
      "post": {
        "tags": ["Plugins"],
        "summary": "Verify a plugin's manifest signature",
        "operationId": "verifyPlugin",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Verification result" }, "404": { "description": "Plugin not found" } }
      }
    },
    "/v1/plugins/{id}/credentials": {
      "post": {
        "tags": ["Plugins"],
        "summary": "Set credentials for a plugin",
        "operationId": "setPluginCredentials",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Credentials set" }, "404": { "description": "Plugin not found" } }
      }
    },
    "/v1/webhooks/github/webhook": {
      "post": {
        "tags": ["Integrations"],
        "summary": "GitHub webhook delivery endpoint",
        "operationId": "githubWebhook",
        "responses": { "200": { "description": "Accepted" }, "401": { "description": "Signature mismatch" } }
      }
    },
    "/v1/webhooks/github/queue/concurrency": {
      "get": {
        "tags": ["Integrations"],
        "summary": "Get GitHub webhook queue concurrency configuration",
        "operationId": "getGithubQueueConcurrency",
        "responses": { "200": { "description": "Concurrency config" } }
      },
      "post": {
        "tags": ["Integrations"],
        "summary": "Set GitHub webhook queue concurrency",
        "operationId": "setGithubQueueConcurrency",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Concurrency updated" } }
      }
    },
    "/v1/webhooks/github/queue/pause": {
      "post": {
        "tags": ["Integrations"],
        "summary": "Pause GitHub webhook queue processing",
        "operationId": "pauseGithubQueue",
        "responses": { "200": { "description": "Queue paused" } }
      }
    },
    "/v1/webhooks/github/queue/resume": {
      "post": {
        "tags": ["Integrations"],
        "summary": "Resume GitHub webhook queue processing",
        "operationId": "resumeGithubQueue",
        "responses": { "200": { "description": "Queue resumed" } }
      }
    },
    "/v1/webhooks/github/queue/{issue}/retry": {
      "post": {
        "tags": ["Integrations"],
        "summary": "Retry a failed webhook delivery for an issue",
        "operationId": "retryGithubQueueIssue",
        "parameters": [{ "name": "issue", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Retried" }, "404": { "description": "Issue not found" } }
      }
    },
    "/v1/webhooks/github/queue/{issue}/skip": {
      "post": {
        "tags": ["Integrations"],
        "summary": "Skip a failed webhook delivery for an issue",
        "operationId": "skipGithubQueueIssue",
        "parameters": [{ "name": "issue", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Skipped" }, "404": { "description": "Issue not found" } }
      }
    },
    "/v1/admin/capabilities": {
      "get": {
        "tags": ["Admin"],
        "summary": "List admin-reachable capabilities for the current deployment",
        "operationId": "getAdminCapabilities",
        "responses": { "200": { "description": "Capability map" } }
      }
    },
    "/v1/admin/entitlements": {
      "get": {
        "tags": ["Admin"],
        "summary": "Licensed entitlements and feature flags",
        "operationId": "getAdminEntitlements",
        "responses": { "200": { "description": "Entitlements" } }
      }
    },
    "/v1/admin/license": {
      "get": {
        "tags": ["Admin"],
        "summary": "Current license state",
        "operationId": "getLicense",
        "responses": { "200": { "description": "License record" } }
      }
    },
    "/v1/admin/license/activate": {
      "post": {
        "tags": ["Admin"],
        "summary": "Activate a license key",
        "operationId": "activateLicense",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Activated" }, "400": { "description": "Invalid license" } }
      }
    },
    "/v1/admin/license/override": {
      "post": {
        "tags": ["Admin"],
        "summary": "Override the license (admin emergency)",
        "operationId": "overrideLicense",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Override applied" } }
      }
    },
    "/v1/admin/logs": {
      "get": {
        "tags": ["Admin"],
        "summary": "Structured request log tail from the in-memory ring buffer",
        "operationId": "listRequestLogs",
        "parameters": [
          { "name": "limit",    "in": "query", "schema": { "type": "integer", "default": 200 } },
          { "name": "level",    "in": "query", "schema": { "type": "string", "description": "Comma-separated: info,warn,error" } },
          { "name": "since_ms", "in": "query", "schema": { "type": "integer", "format": "int64" } }
        ],
        "responses": { "200": { "description": "Request log entries" } }
      }
    },
    "/v1/admin/notifications/failed": {
      "get": {
        "tags": ["Admin"],
        "summary": "List failed notification deliveries for the caller's tenant",
        "operationId": "listFailedNotifications",
        "parameters": [
          { "name": "limit",  "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Failed notification list" } }
      }
    },
    "/v1/admin/tenants/{tenant_id}/operators/{operator_id}/tenant-roles": {
      "get": {
        "tags": ["Admin"],
        "summary": "List an operator's tenant-role grants (RFC 026 PR-A4)",
        "description": "Returns every `(tenant_id, operator_id)` grant ever recorded for the operator — active and revoked — so the OperatorsPage can surface the full grant set per-row. Tenant-scoped under `:tenant_id`; `TenantAdminGuard` authorizes on that tenant. Cross-tenant operator ids return 404 so presence is never revealed. Body items echo the `OperatorTenantRoleRecord` projection shape.",
        "operationId": "listOperatorTenantRoles",
        "parameters": [
          { "name": "tenant_id",   "in": "path", "required": true, "schema": { "type": "string" }, "description": "Tenant the caller is admin on." },
          { "name": "operator_id", "in": "path", "required": true, "schema": { "type": "string" }, "description": "Operator whose grants to list." }
        ],
        "responses": {
          "200": { "description": "Grant list (active + revoked)." },
          "403": { "description": "Structured `tenant_role_missing` for non-admin callers." },
          "404": { "description": "Operator profile not found for this tenant." }
        }
      }
    },
    "/v1/admin/operators/{id}/tenant-roles/{tenant}/promote": {
      "post": {
        "tags": ["Admin"],
        "summary": "Grant a tenant-scope role to an operator (RFC 026 PR-A0)",
        "description": "Upserts the (tenant, operator) pair in `operator_tenant_roles`; re-granting an already-granted pair clears any prior revocation. Guarded by `TenantAdminGuard` — accepts god-token (`CAIRN_ADMIN_TOKEN`) for bootstrapping OR an existing `TenantRole::Admin` on the target tenant so tenant-admins can delegate.",
        "operationId": "promoteTenantRole",
        "parameters": [
          { "name": "id",     "in": "path", "required": true, "schema": { "type": "string" }, "description": "Operator id." },
          { "name": "tenant", "in": "path", "required": true, "schema": { "type": "string" }, "description": "Target tenant id." }
        ],
        "requestBody": {
          "required": true,
          "content": { "application/json": { "schema": { "type": "object", "required": ["role"], "properties": { "role": { "type": "string", "enum": ["admin", "member", "read_only"] } } } } }
        },
        "responses": {
          "201": { "description": "Role granted — body echoes the projected row." },
          "403": { "description": "Structured `tenant_role_missing` body when the caller is a non-admin operator without TenantRole::Admin on the target." }
        }
      }
    },
    "/v1/admin/operators/{id}/tenant-roles/{tenant}": {
      "delete": {
        "tags": ["Admin"],
        "summary": "Revoke an operator's tenant-scope role (RFC 026 PR-A0)",
        "description": "Soft delete — the projection row is retained with `revoked_at_ms` + `revoked_by` set so the audit trail survives. Returns 404 when no grant has ever existed for the (operator, tenant) pair.",
        "operationId": "revokeTenantRole",
        "parameters": [
          { "name": "id",     "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "tenant", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Revoked — body echoes the updated row with revocation fields populated." },
          "403": { "description": "Structured `tenant_role_missing` for non-admin callers." },
          "404": { "description": "No grant exists for this (operator, tenant)." }
        }
      }
    },
    "/v1/admin/workspaces": {
      "get": {
        "tags": ["Admin"],
        "summary": "List workspaces across all tenants (admin only)",
        "operationId": "adminListWorkspaces",
        "responses": { "200": { "description": "Workspace list" } }
      }
    },
    "/v1/approval-policies": {
      "get": {
        "tags": ["Approvals"],
        "summary": "List approval policies for the caller's tenant",
        "operationId": "listApprovalPolicies",
        "responses": { "200": { "description": "Approval policy list" } }
      },
      "post": {
        "tags": ["Approvals"],
        "summary": "Create an approval policy",
        "operationId": "createApprovalPolicy",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Policy created" } }
      }
    },
    "/v1/approvals/{id}/resolve": {
      "post": {
        "tags": ["Approvals"],
        "summary": "Resolve an approval (approve or deny with decision payload)",
        "operationId": "resolveApproval",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Resolved" }, "404": { "description": "Approval not found" } }
      }
    },
    "/v1/assistant/message": {
      "post": {
        "tags": ["Assistant"],
        "summary": "Send a message to the in-app assistant",
        "operationId": "sendAssistantMessage",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Assistant reply" } }
      }
    },
    "/v1/assistant/voice": {
      "post": {
        "tags": ["Assistant"],
        "summary": "Submit a voice-formatted message to the assistant",
        "operationId": "sendAssistantVoice",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Assistant reply" } }
      }
    },
    "/v1/assistant/sessions": {
      "get": {
        "tags": ["Assistant"],
        "summary": "List assistant sessions for the caller",
        "operationId": "listAssistantSessions",
        "responses": { "200": { "description": "Assistant session list" } }
      }
    },
    "/v1/assistant/sessions/{sessionId}": {
      "get": {
        "tags": ["Assistant"],
        "summary": "Get an assistant session by id",
        "operationId": "getAssistantSession",
        "parameters": [{ "name": "sessionId", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Assistant session" }, "404": { "description": "Not found" } }
      }
    },
    "/v1/channels": {
      "get": {
        "tags": ["Channels"],
        "summary": "List notification channels configured for the caller's tenant",
        "operationId": "listChannels",
        "responses": { "200": { "description": "Channel list" } }
      },
      "post": {
        "tags": ["Channels"],
        "summary": "Register a notification channel",
        "operationId": "createChannel",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Channel created" } }
      }
    },
    "/v1/config": {
      "get": {
        "tags": ["Admin"],
        "summary": "Dump server configuration (secrets redacted)",
        "operationId": "getServerConfig",
        "responses": { "200": { "description": "Config dump" } }
      }
    },
    "/v1/config/{key}": {
      "get": {
        "tags": ["Admin"],
        "summary": "Get a single config value by key",
        "operationId": "getConfigValue",
        "parameters": [{ "name": "key", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Value" }, "404": { "description": "Not found" } }
      },
      "put": {
        "tags": ["Admin"],
        "summary": "Set a config value",
        "operationId": "setConfigValue",
        "parameters": [{ "name": "key", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Set" } }
      },
      "delete": {
        "tags": ["Admin"],
        "summary": "Delete a config value",
        "operationId": "deleteConfigValue",
        "parameters": [{ "name": "key", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "204": { "description": "Deleted" }, "404": { "description": "Not found" } }
      }
    },
    "/v1/evals/datasets": {
      "get": {
        "tags": ["Evals"],
        "summary": "List eval datasets for the caller's tenant",
        "operationId": "listEvalDatasets",
        "parameters": [
          { "name": "tenant_id", "in": "query", "schema": { "type": "string" } },
          { "name": "limit",     "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset",    "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Dataset list" } }
      },
      "post": {
        "tags": ["Evals"],
        "summary": "Create an eval dataset",
        "operationId": "createEvalDataset",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Dataset created" } }
      }
    },
    "/v1/evals/matrices/guardrail": {
      "get": {
        "tags": ["Evals"],
        "summary": "Guardrail matrix (per-release violation counts)",
        "operationId": "getGuardrailMatrix",
        "responses": { "200": { "description": "Matrix" } }
      }
    },
    "/v1/evals/matrices/memory-quality": {
      "get": {
        "tags": ["Evals"],
        "summary": "Memory-quality matrix",
        "operationId": "getMemoryQualityMatrix",
        "responses": { "200": { "description": "Matrix" } }
      }
    },
    "/v1/evals/matrices/permissions": {
      "get": {
        "tags": ["Evals"],
        "summary": "Permissions matrix",
        "operationId": "getPermissionsMatrix",
        "responses": { "200": { "description": "Matrix" } }
      }
    },
    "/v1/evals/matrices/prompt-comparison": {
      "get": {
        "tags": ["Evals"],
        "summary": "Prompt-comparison matrix",
        "operationId": "getPromptComparisonMatrix",
        "responses": { "200": { "description": "Matrix" } }
      }
    },
    "/v1/evals/matrices/skill-health": {
      "get": {
        "tags": ["Evals"],
        "summary": "Skill-health matrix",
        "operationId": "getSkillHealthMatrix",
        "responses": { "200": { "description": "Matrix" } }
      }
    },
    "/v1/export/{format}": {
      "get": {
        "tags": ["Admin"],
        "summary": "Export a portable bundle in the given format",
        "operationId": "exportBundleByFormat",
        "parameters": [{ "name": "format", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Export archive" }, "400": { "description": "Unsupported format" } }
      }
    },
    "/v1/fleet": {
      "get": {
        "tags": ["Admin"],
        "summary": "Fleet overview (hosts, roles, deployment mode)",
        "description": "**Scope (#428):** tenant-scoped — the `TenantScope` extractor injects the caller's tenant; non-admin callers only see fleet members bound to their own tenant. Admin principals see every tenant. The path stays outside `/v1/admin/` because it's already correctly scoped.",
        "operationId": "getFleet",
        "responses": {
          "200": { "description": "Fleet overview" },
          "401": { "description": "Missing or invalid bearer token", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/v1/graph/trace": {
      "get": {
        "tags": ["Graph"],
        "summary": "Graph trace query",
        "operationId": "getGraphTrace",
        "responses": { "200": { "description": "Trace data" } }
      }
    },
    "/v1/import/reports": {
      "get": {
        "tags": ["Admin"],
        "summary": "List recent import reports",
        "operationId": "listImportReports",
        "responses": { "200": { "description": "Import reports" } }
      }
    },
    "/v1/import/preview": {
      "post": {
        "tags": ["Admin"],
        "summary": "Preview what an import bundle would change",
        "operationId": "previewImport",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Preview" } }
      }
    },
    "/v1/import/validate": {
      "post": {
        "tags": ["Admin"],
        "summary": "Validate an import bundle",
        "operationId": "validateImport",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Validation result" } }
      }
    },
    "/v1/import/apply": {
      "post": {
        "tags": ["Admin"],
        "summary": "Apply an import bundle",
        "operationId": "applyImport",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Applied" } }
      }
    },
    "/v1/ingest/jobs": {
      "get": {
        "tags": ["Ingest"],
        "summary": "List ingest jobs for the caller's project",
        "operationId": "listIngestJobs",
        "responses": { "200": { "description": "Ingest jobs" } }
      },
      "post": {
        "tags": ["Ingest"],
        "summary": "Create an ingest job",
        "operationId": "createIngestJob",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Job created" } }
      }
    },
    "/v1/memories": {
      "get": {
        "tags": ["Memory"],
        "summary": "List memory documents",
        "operationId": "listMemories",
        "responses": { "200": { "description": "Memory list" } }
      },
      "post": {
        "tags": ["Memory"],
        "summary": "Create a memory document",
        "operationId": "createMemory",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Memory created" } }
      }
    },
    "/v1/memories/search": {
      "get": {
        "tags": ["Memory"],
        "summary": "Search memory documents",
        "operationId": "searchMemories",
        "parameters": [
          { "name": "q",     "in": "query", "required": true, "schema": { "type": "string" } },
          { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 100 } }
        ],
        "responses": { "200": { "description": "Search results" } }
      }
    },
    "/v1/memories/{id}/accept": {
      "post": {
        "tags": ["Memory"],
        "summary": "Accept a memory document",
        "operationId": "acceptMemory",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Accepted" }, "404": { "description": "Not found" } }
      }
    },
    "/v1/memories/{id}/reject": {
      "post": {
        "tags": ["Memory"],
        "summary": "Reject a memory document",
        "operationId": "rejectMemory",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Rejected" }, "404": { "description": "Not found" } }
      }
    },
    "/v1/onboarding/status": {
      "get": {
        "tags": ["Admin"],
        "summary": "Onboarding status for the caller",
        "operationId": "getOnboardingStatus",
        "responses": { "200": { "description": "Onboarding status" } }
      }
    },
    "/v1/onboarding/template": {
      "post": {
        "tags": ["Admin"],
        "summary": "Apply an onboarding template",
        "operationId": "applyOnboardingTemplate",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Template applied" } }
      }
    },
    "/v1/onboarding/templates": {
      "get": {
        "tags": ["Admin"],
        "summary": "List available onboarding templates",
        "operationId": "listOnboardingTemplates",
        "responses": { "200": { "description": "Onboarding template list" } }
      }
    },
    "/v1/plugins": {
      "get": {
        "tags": ["Plugins"],
        "summary": "List plugins registered with the host",
        "operationId": "listPlugins",
        "parameters": [
          { "name": "limit",  "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Plugin list" } }
      },
      "post": {
        "tags": ["Plugins"],
        "summary": "Register a plugin manifest",
        "operationId": "createPlugin",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Plugin registered" } }
      }
    },
    "/v1/policies/decisions": {
      "get": {
        "tags": ["Admin"],
        "summary": "List recent guardrail-policy decisions",
        "operationId": "listPolicyDecisions",
        "responses": { "200": { "description": "Decision log" } }
      }
    },
    "/v1/providers/bindings": {
      "get": {
        "tags": ["Providers"],
        "summary": "List provider bindings for the caller's tenant",
        "operationId": "listProviderBindings",
        "parameters": [
          { "name": "tenant_id", "in": "query", "schema": { "type": "string" } },
          { "name": "limit",     "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset",    "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Binding list" } }
      },
      "post": {
        "tags": ["Providers"],
        "summary": "Create a provider binding",
        "operationId": "createProviderBinding",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Binding created" } }
      }
    },
    "/v1/providers/bindings/cost-ranking": {
      "get": {
        "tags": ["Providers"],
        "summary": "Per-binding cost ranking for the tenant",
        "operationId": "listBindingCostRanking",
        "parameters": [
          { "name": "tenant_id", "in": "query", "schema": { "type": "string" } },
          { "name": "limit",     "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset",    "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Cost ranking" } }
      }
    },
    "/v1/providers/budget": {
      "get": {
        "tags": ["Providers"],
        "summary": "List provider budgets",
        "operationId": "listProviderBudgets",
        "responses": { "200": { "description": "Budget list" } }
      },
      "post": {
        "tags": ["Providers"],
        "summary": "Set a provider budget",
        "operationId": "setProviderBudget",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Budget set" } }
      }
    },
    "/v1/providers/policies": {
      "get": {
        "tags": ["Providers"],
        "summary": "List route policies",
        "operationId": "listRoutePolicies",
        "responses": { "200": { "description": "Route policy list" } }
      },
      "post": {
        "tags": ["Providers"],
        "summary": "Create a route policy",
        "operationId": "createRoutePolicy",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Policy created" } }
      }
    },
    "/v1/providers/pools": {
      "get": {
        "tags": ["Providers"],
        "summary": "List provider connection pools",
        "operationId": "listProviderPools",
        "responses": { "200": { "description": "Pool list" } }
      },
      "post": {
        "tags": ["Providers"],
        "summary": "Create a provider connection pool",
        "operationId": "createProviderPool",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Pool created" } }
      }
    },
    "/v1/providers/run-health-checks": {
      "post": {
        "tags": ["Providers"],
        "summary": "Run all due provider health checks now",
        "operationId": "runProviderHealthChecks",
        "responses": { "200": { "description": "Records produced by the batch" } }
      }
    },
    "/v1/settings/tls": {
      "get": {
        "tags": ["Admin"],
        "summary": "TLS certificate settings",
        "operationId": "getTlsSettings",
        "responses": { "200": { "description": "TLS settings" } }
      }
    },
    "/v1/soul": {
      "get": {
        "tags": ["Admin"],
        "summary": "Get the current soul document",
        "operationId": "getSoul",
        "responses": { "200": { "description": "Soul document" } }
      },
      "put": {
        "tags": ["Admin"],
        "summary": "Replace the soul document",
        "operationId": "putSoul",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "200": { "description": "Soul updated" } }
      }
    },
    "/v1/soul/history": {
      "get": {
        "tags": ["Admin"],
        "summary": "Soul document revision history",
        "operationId": "getSoulHistory",
        "responses": { "200": { "description": "History" } }
      }
    },
    "/v1/soul/patches": {
      "get": {
        "tags": ["Admin"],
        "summary": "List soul patches pending review",
        "operationId": "listSoulPatches",
        "responses": { "200": { "description": "Patch list" } }
      }
    },
    "/v1/sources": {
      "get": {
        "tags": ["Memory"],
        "summary": "List knowledge sources for the caller's project",
        "operationId": "listSources",
        "responses": { "200": { "description": "Source list" } }
      },
      "post": {
        "tags": ["Memory"],
        "summary": "Register a knowledge source",
        "operationId": "createSource",
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object" } } } },
        "responses": { "201": { "description": "Source created" } }
      }
    },
    "/v1/sources/{id}": {
      "get": {
        "tags": ["Memory"],
        "summary": "Fetch a source detail record",
        "operationId": "getSource",
        "parameters": [
          { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "tenant_id", "in": "query", "required": true, "schema": { "type": "string" } },
          { "name": "workspace_id", "in": "query", "required": true, "schema": { "type": "string" } },
          { "name": "project_id", "in": "query", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Source detail" },
          "404": { "description": "Source not found", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      },
      "patch": {
        "tags": ["Memory"],
        "summary": "Partially update a knowledge source (#426)",
        "description": "Updates an existing source's name and/or description. Absent fields preserve the current value — PATCH semantics per RFC 7231 §4.3.4. The verb was changed from PUT to PATCH in #426 because the handler has never had full-replacement semantics.",
        "operationId": "patchSource",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "requestBody": { "required": true, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/PatchSourceRequest" } } } },
        "responses": {
          "200": { "description": "Source updated" },
          "404": { "description": "Source not found", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
          "422": { "description": "Unknown or malformed field", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      },
      "delete": {
        "tags": ["Memory"],
        "summary": "Deactivate a knowledge source",
        "operationId": "deleteSource",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": {
          "200": { "description": "Source deactivated" },
          "404": { "description": "Source not found", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
        }
      }
    },
    "/v1/sources/process-refresh": {
      "post": {
        "tags": ["Memory"],
        "summary": "Process all due source refreshes now",
        "operationId": "processSourceRefresh",
        "responses": { "200": { "description": "Refresh batch processed" } }
      }
    },
    "/v1/streams/runtime": {
      "get": {
        "tags": ["Events"],
        "summary": "Alternate SSE event stream (legacy)",
        "description": "Supplementary live event stream. Bearer auth required — same contract as `/v1/stream` (`Authorization` header OR `?token=` query parameter).",
        "operationId": "streamRuntimeEvents",
        "security": [{ "bearerAuth": [] }],
        "parameters": [
          { "name": "token", "in": "query", "required": false, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "SSE stream" }, "401": { "description": "Unauthorized" } }
      }
    },
    "/v1/tasks/expired": {
      "get": {
        "tags": ["Tasks"],
        "summary": "List tasks whose lease has expired",
        "operationId": "listExpiredTasks",
        "parameters": [
          { "name": "limit",  "in": "query", "schema": { "type": "integer", "default": 100 } },
          { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
        ],
        "responses": { "200": { "description": "Expired task list" } }
      }
    },
    "/v1/tasks/expire-leases": {
      "post": {
        "tags": ["Tasks"],
        "summary": "Force-expire task leases past their deadline",
        "operationId": "expireTaskLeases",
        "responses": { "200": { "description": "Expired task ids" } }
      }
    },
    "/v1/tasks/{id}/cancel": {
      "post": {
        "tags": ["Tasks"],
        "summary": "Cancel a task",
        "operationId": "cancelTask",
        "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "Task cancelled" }, "404": { "description": "Task not found" } }
      }
    },
    "/v1/poll/run": {
      "post": {
        "tags": ["Runs"],
        "summary": "Internal: tick scheduled polling runs",
        "operationId": "pollRun",
        "responses": { "200": { "description": "Poll tick accepted" } }
      }
    }
  }
}"##;

/// Swagger UI HTML — loads the CDN bundle and points it at `/v1/openapi.json`.
pub const SWAGGER_UI_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Cairn API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  <style>
    body { margin: 0; background: #09090b; }
    .swagger-ui .topbar { background: #18181b; border-bottom: 1px solid #27272a; }
    .swagger-ui .topbar .download-url-wrapper { display: none; }
    .swagger-ui .info .title { color: #e4e4e7; }
    .swagger-ui .scheme-container { background: #18181b; }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({
      url: "/v1/openapi.json",
      dom_id: "#swagger-ui",
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
      layout: "BaseLayout",
      deepLinking: true,
      persistAuthorization: true,
      tryItOutEnabled: true,
    });
  </script>
</body>
</html>"##;
