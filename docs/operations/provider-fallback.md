# Provider fallback — deployment guide

Operator-facing recipe for configuring cairn's two-axis LLM fallback system so
a single model's outage doesn't stall your runs.

## The problem

Every production-grade control plane eventually hits one of:

- Rate-limit bursts (free-tier daily caps, per-minute throughput ceilings)
- Transient 5xx / timeout from the upstream provider
- A model is temporarily degraded (empty response, hallucinated tool call)
- An entire provider connection goes down (credential rotation, API outage)

cairn handles all four via **a composed two-axis fallback chain**. If you
configure it correctly the orchestrator routes around these failures
automatically. If you don't, a single 5xx can take down the run.

## The two axes

### Axis 1 — Models within a connection (`ModelChain`)

Every `provider_connection` you register carries a `supported_models`
array. That array *is* the fallback chain for that connection. On a
fallback-eligible error (`RateLimited`, 5xx, `EmptyResponse`, timeout,
`StructuredOutputInvalid`) cairn advances to the next model in the
array.

Additional behavior within a single model:

- **Same-model retry with backoff** (issue #693 R3-A). cairn retries
  the *same* model up to 2 times (3 attempts total) on transient
  errors before advancing to the next model. Backoff is `1s → 3s`.
  This absorbs provider request-queue jitter without wasting the
  chain's options on a recoverable blip.
- **Rate-limit cooldown**. When a model returns `RateLimited`, cairn
  records a 5-minute cooldown for that `(tenant, binding, model)`
  tuple. Subsequent dispatches skip it until the cooldown expires.
  Rate-limited models do *not* consume the same-model retry budget —
  cairn advances to the next model immediately.

### Axis 2 — Connections across providers (`RoutedGenerationService`)

Every active `provider_connection` on your tenant becomes a binding in
the cross-axis chain. When a connection's `ModelChain` exhausts
(every model fallback-failed), cairn advances to the next connection.

Order is **registration order within the tenant**: the first
connection you created gets tried first. There's no explicit
operator-visible ordering today — future work if it matters.

### Composition

```
tenant has 3 active connections: [openrouter, zai, anthropic]
  ├─ openrouter.supported_models = [minimax, gemma, nemotron]
  ├─ zai.supported_models        = [glm-4.7, glm-4.6]
  └─ anthropic.supported_models  = [claude-sonnet-4, claude-haiku-4-5]

Full fallback matrix (up to 3 attempts per cell via same-model retry):
  openrouter.minimax    → openrouter.gemma      → openrouter.nemotron
  → zai.glm-4.6         → zai.glm-4-flash
  → anthropic.sonnet-4  → anthropic.haiku-4-5
  → AllProvidersExhausted → escalate_to_operator approval card
```

All seven cells must fail for a run to trip exhaustion. In practice
rate-limit bursts hit maybe one cell; a provider outage hits one
connection (three cells); even both together leaves four cells
healthy.

## Recommended deployment

### Minimum for production

**≥3 models per connection, ≥2 connections active.**

This gives you a 6-cell matrix. Even a single-provider outage still
has 3 cells on the other provider; even a rate-limit burst on the
preferred model has 5 fallbacks.

### Recommended

**≥3 models per connection, ≥3 connections active, one of which is a
paid tier.**

Three connections cover provider outages. A paid-tier connection as
the last-resort binding ensures runs complete even when your free-tier
budget is exhausted — at predictable cost.

### Anti-patterns we've seen

- **Single-model connection**. A single `supported_models` entry
  turns Axis 1 into a no-op. One timeout → exhaustion. Observed in
  dogfood R1–R3 — the R3-A fix (retry+backoff) mitigates this but
  does not replace configuring alternates.
- **Two connections against the same provider family**. If both are
  OpenRouter and OpenRouter's auth service goes down, both fail
  identically. Mix providers.
- **All-free-tier connections**. Free tiers share global quotas
  across users; an "internet-scale 429" will hit all of them
  simultaneously. Mix at least one paid tier.

## Configuring via HTTP API

### Register a credential, then a connection

```bash
# 1. Credential
curl -X POST "$CAIRN_URL/v1/admin/tenants/$TENANT/credentials" \
  -H "Authorization: Bearer $CAIRN_ADMIN_TOKEN" \
  -H "content-type: application/json" \
  -d '{"provider_id":"openrouter","plaintext_value":"sk-or-..."}'

# 2. Connection — note the MULTIPLE entries in supported_models
curl -X POST "$CAIRN_URL/v1/providers/connections" \
  -H "Authorization: Bearer $CAIRN_ADMIN_TOKEN" \
  -H "content-type: application/json" \
  -d '{
    "tenant_id": "'"$TENANT"'",
    "provider_connection_id": "openrouter-primary",
    "provider_family": "openrouter",
    "adapter_type": "openrouter",
    "credential_id": "<id from step 1>",
    "endpoint_url": "https://openrouter.ai/api/v1",
    "supported_models": [
      "anthropic/claude-sonnet-4",
      "openai/gpt-4.1",
      "google/gemini-2.5-flash"
    ]
  }'
```

Models are tried in the order they appear in `supported_models`. Put
your preferred model first.

### Repeat for a second provider family

```bash
# Z.ai fallback connection
curl -X POST "$CAIRN_URL/v1/providers/connections" \
  ...
  -d '{
    "provider_connection_id": "zai-fallback",
    "provider_family": "zai",
    "adapter_type": "zai",
    "credential_id": "<zai credential id>",
    "endpoint_url": "https://api.z.ai/api/paas/v4",
    "supported_models": ["glm-4.7", "glm-4.6"]
  }'
```

cairn treats both connections as one big fallback chain; you don't
need to tell it explicitly "use z.ai after openrouter."

### System defaults

Set which model cairn prefers system-wide:

```bash
curl -X PUT "$CAIRN_URL/v1/settings/defaults/system/system/generate_model" \
  -H "Authorization: Bearer $CAIRN_ADMIN_TOKEN" \
  -d '{"value": "anthropic/claude-sonnet-4"}'

curl -X PUT "$CAIRN_URL/v1/settings/defaults/system/system/brain_model" \
  -H "Authorization: Bearer $CAIRN_ADMIN_TOKEN" \
  -d '{"value": "anthropic/claude-sonnet-4"}'
```

The default model must appear in the `supported_models` of at least
one connection, or you'll get `422 no_provider_for_model` on the
first orchestrate.

## Observing the fallback in action

### Logs

Every dispatch attempt emits a structured `routed_generation: dispatch
attempt` tracing span at INFO with `binding_id`, `model_id`,
`attempt_index`, and `tool_count`. On failure you get a
`routed_generation: dispatch failed (retryable)` WARN with the
`reason` (`timed_out` | `rate_limited` | `upstream_5xx` |
`empty_response` | `response_format` | `transport_failure`).

Per-model same-model retries emit an additional `model_chain:
retrying after transient provider error (#693 R3-A)` WARN with
`model_attempt_index` and `backoff_ms` *before* the sleep — so a
killed process still shows the retry was initiated.

### Prometheus metrics (`/metrics`)

- `cairn_orchestrator_provider_call_total{model, status}` —
  successful / failed calls per model.
- `cairn_orchestrator_prose_playing_detected_total` — fires when
  an LLM emits echo-via-bash instead of structured actions (see
  [echo-via-bash detector](../design/rfcs/echo-via-bash-detector.md)
  if present, or the `crates/cairn-orchestrator/src/echo_detector.rs`
  module for the full heuristic).

### Per-run telemetry

`GET /v1/runs/:id/telemetry` returns every provider call on the run
with `model`, `status`, `latency_ms`, and `error_class` — the
authoritative view of what the fallback chain actually did on one
specific run.

## What happens when the chain fully exhausts

`LoopTermination::AllProvidersExhausted` fires. The orchestrate
handler:

1. Submits an `escalate_to_operator` approval card carrying the full
   attempt summary (one row per failed `(binding, model)` attempt
   with its `reason_code`). Visible at
   `GET /v1/approvals?state=pending`.
2. Transitions the run to `state=waiting_approval` (issue #693 R3-B).
   Operators looking at `GET /v1/runs/:id` see "waiting_approval" —
   not the stale "running" that pre-R3-B produced.
3. Returns HTTP 502 with the same summary inline.

The operator resolves the approval card with one of:

- **Approve** → run returns to `Running` for another decide turn. Use
  this after rotating credentials, adding a provider connection, or
  rebalancing `supported_models`. The next attempt will see the new
  config.
- **Reject** → run transitions to `Failed` with
  `FailureClass::ApprovalRejected`. Use this when the goal itself has
  become obsolete or the provider situation won't recover on the
  timescale of the run.

## Tuning knobs (code constants — change via PR, not env var today)

All defined in `crates/cairn-runtime/src/services/model_chain.rs`:

| Constant | Default | When to change |
|---|---|---|
| `DEFAULT_MAX_RETRIES_PER_MODEL` | 2 | Raise to 3-4 if your chosen models have high first-attempt jitter; lower to 0-1 if you'd rather exhaust fast onto the next model. |
| `DEFAULT_RETRY_BASE_BACKOFF` | 1 s | Raise to 2-3 s if your primary provider needs >3 s queue-jitter recovery; lower to 500 ms on premium tiers where jitter is sub-second. |
| `DEFAULT_RATE_LIMIT_COOLDOWN` | 5 min | Raise to 1 h if your free-tier daily caps reset at midnight UTC; lower on paid tiers where 429 is a rare burst condition. |
| `DEFAULT_PER_CALL_TIMEOUT` | 360 s | Raise only if you're running local Ollama with heavy CPU inference. Never lower — adapter-level timeouts should be the governor. |

Configurable env-var plumbing is out of scope today; it's tracked as
follow-up work. The constants are conservative for the common free-tier
+ paid-tier deployment; tune only after you have a concrete reason from
production telemetry.

## See also

- `crates/cairn-runtime/src/services/model_chain.rs` — `ModelChain`
  implementation and per-model retry logic.
- `crates/cairn-runtime/src/services/routed_generation.rs` —
  `RoutedGenerationService` composition and cross-binding iteration.
- `crates/cairn-app/src/handlers/providers.rs` — HTTP API for
  registering connections.
- `docs/operations/metrics.md` — full metrics catalogue.
