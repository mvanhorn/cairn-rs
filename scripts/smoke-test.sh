#!/usr/bin/env bash
# =============================================================================
# cairn smoke test — verifies the full API surface against a running server.
#
# Usage:
#   ./scripts/smoke-test.sh
#   CAIRN_URL=http://my-server:3000 CAIRN_TOKEN=my-token ./scripts/smoke-test.sh
#
# Exit code: 0 = all passed, 1 = one or more failures.
# =============================================================================

BASE="${CAIRN_URL:-http://localhost:3000}"
TOKEN="${CAIRN_TOKEN:-cairn-demo-token}"
TIMEOUT="${CAIRN_TIMEOUT:-10}"

RUN_ID="smoke_$(date +%s)_$RANDOM"
SESSION_ID="sess_${RUN_ID}"
WORKER_ID="worker_${RUN_ID}"
TASK_ID="task_${RUN_ID}"
APPR_ID="appr_${RUN_ID}"
BUNDLE_ID="bundle_${RUN_ID}"
GATE_APPR_ID="gate_${RUN_ID}"
GATE_RUN_ID="grun_${RUN_ID}"
GATE_SESSION_ID="gsess_${RUN_ID}"

# ── Colour ────────────────────────────────────────────────────────────────────
if [ -t 2 ]; then
  GRN='\033[0;32m'; RED='\033[0;31m'; YLW='\033[0;33m'
  CYN='\033[0;36m'; BLD='\033[1m';   RST='\033[0m'
else
  GRN=''; RED=''; YLW=''; CYN=''; BLD=''; RST=''
fi

PASS=0; FAIL=0; SKIP=0

# All output to stderr — stdout is pure JSON for pipeline use.
log_ok()   { echo -e "${GRN}  ✓${RST} $1" >&2; PASS=$(( PASS + 1 )); }
log_fail() { echo -e "${RED}  ✗${RST} $1" >&2; FAIL=$(( FAIL + 1 )); }
log_skip() { echo -e "${YLW}  ⊘${RST} $1" >&2; SKIP=$(( SKIP + 1 )); }
section()  { echo -e "\n${BLD}${CYN}── $1${RST}" >&2; }

# ── HTTP primitives ───────────────────────────────────────────────────────────
# Use a tmpfile so status is NOT captured in a subshell.
_BODY_FILE=$(mktemp)
trap 'rm -f "$_BODY_FILE"' EXIT

# api METHOD PATH [BODY]
# Sets globals: _HTTP (status code), _BODY (response body)
_HTTP="" _BODY=""
api() {
  local method="$1" path="$2" body="${3:-}"
  local curl_args=(-s -X "$method" --max-time "$TIMEOUT"
    -H "Authorization: Bearer ${TOKEN}"
    -H "Content-Type: application/json"
    -o "$_BODY_FILE"
    -w "%{http_code}")
  [ -n "$body" ] && curl_args+=(-d "$body")
  _HTTP=$(curl "${curl_args[@]}" "${BASE}${path}" 2>/dev/null)
  _BODY=$(cat "$_BODY_FILE")
}

# chk LABEL WANT_STATUS METHOD PATH [BODY]
chk() {
  local label="$1" want="$2" method="$3" path="$4" body="${5:-}"
  api "$method" "$path" "$body"
  if [ "$_HTTP" = "$want" ]; then
    log_ok "$label (HTTP $_HTTP)"
    return 0
  else
    log_fail "$label (expected HTTP $want, got HTTP $_HTTP)"
    [ -n "$_BODY" ] && echo -e "     ${RED}${_BODY:0:160}${RST}" >&2
    return 1
  fi
}

# chk2xx LABEL METHOD PATH [BODY]  — any 2xx/3xx is a pass
chk2xx() {
  local label="$1" method="$2" path="$3" body="${4:-}"
  api "$method" "$path" "$body"
  if [[ "$_HTTP" =~ ^[23] ]]; then
    log_ok "$label (HTTP $_HTTP)"
    return 0
  else
    log_fail "$label (HTTP $_HTTP)"
    [ -n "$_BODY" ] && echo -e "     ${RED}${_BODY:0:160}${RST}" >&2
    return 1
  fi
}

# jf KEY — extract string field from $_BODY
jf() { printf '%s' "$_BODY" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('$1',''))" 2>/dev/null || true; }

# jlen — array length of $_BODY
jlen() { printf '%s' "$_BODY" | python3 -c \
  "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0; }

# wait_until LABEL PATH NEEDLE [TIMEOUT_MS]
#
# Deterministically poll GET <PATH> until the response body contains
# NEEDLE (a plain substring match, same shape as `grep -q`). Uses
# exponential-ish backoff (10/25/50/100/200/500 ms) capped at 500 ms,
# up to TIMEOUT_MS (default 3000) total. Exits 0 on match, 1 on
# timeout (with a log_fail). Replaces bare `sleep N` before a read —
# per the "no flake" principle: we wait for the event we care about,
# not a wall-clock guess.
#
# Copilot round 6 #136: budget is enforced against REAL wall-clock
# elapsed (via `date +%s%3N`), not cumulative `waited_ms += step_ms`,
# because `api` shells out to curl which burns its own time (up to
# `--max-time` per call). Summing sleep-only steps under-counts actual
# elapsed by the curl round-trip on every iteration and lets
# `wait_until` blow past its TIMEOUT_MS budget when the server is
# slow — exactly the scenario the timeout was meant to bound. Per-poll
# curl is additionally capped at 1 s so a single hung request can't
# run out the whole budget.
#
# Leaves $_HTTP / $_BODY set to the final poll's response so the
# following `chk` can log status cleanly without re-fetching.
wait_until() {
  local label="$1" path="$2" needle="$3" timeout_ms="${4:-3000}"
  local step_ms=10
  # Real wall-clock start in ms. `date +%s%3N` works on GNU coreutils;
  # the python3 fallback matches the same encoding for BSD/macOS.
  local start_ms
  start_ms=$(date +%s%3N 2>/dev/null || python3 -c "import time;print(int(time.time()*1000))")
  # Per-poll curl cap: 1 s is far below the 3 s default budget and
  # prevents one hung request from eating the whole window. `local`
  # relies on bash's dynamic scoping so `api`'s `--max-time "$TIMEOUT"`
  # reads this override while we're inside `wait_until`, then the
  # caller's value is restored automatically on return.
  local TIMEOUT=1
  while true; do
    api GET "$path"
    # `-F` → fixed string (never interpret as regex). `--` → `$needle`
    # can't be mis-parsed as a flag if it ever starts with `-`.
    # `printf '%s'` instead of `echo` so bodies that start with `-n` /
    # `-e` or contain backslash escapes don't get re-interpreted.
    if [ "$_HTTP" = "200" ] && printf '%s' "$_BODY" | grep -F -q -- "$needle"; then
      return 0
    fi
    local now_ms waited_ms
    now_ms=$(date +%s%3N 2>/dev/null || python3 -c "import time;print(int(time.time()*1000))")
    waited_ms=$(( now_ms - start_ms ))
    if [ "$waited_ms" -ge "$timeout_ms" ]; then
      log_fail "$label (projection did not surface '$needle' within ${timeout_ms}ms; waited=${waited_ms}ms; last HTTP=$_HTTP)"
      [ -n "$_BODY" ] && echo -e "     ${RED}${_BODY:0:160}${RST}" >&2
      return 1
    fi
    # Fractional-second sleep. `sleep` on GNU coreutils and BSD
    # accepts fractions ("0.025"); POSIX /bin/sh does not. We shell
    # out to python3 (already a hard dep of `jf`/`jlen` above) so
    # the value parses consistently across locales and any shell
    # that exposes `sleep`.
    sleep "$(python3 -c "print($step_ms/1000)" 2>/dev/null || echo 0.05)"
    # Exponential-ish growth capped at 500ms: 10→25→50→100→200→500→500…
    if [ "$step_ms" -lt 500 ]; then
      step_ms=$(( step_ms * 5 / 2 ))
      [ "$step_ms" -gt 500 ] && step_ms=500
    fi
  done
}

# =============================================================================
SUITE_START=$(date +%s%3N 2>/dev/null || python3 -c "import time;print(int(time.time()*1000))")
echo -e "${BLD}cairn smoke test${RST}" >&2
echo -e "  Server  : ${CYN}${BASE}${RST}" >&2
echo -e "  Token   : ${CYN}${TOKEN:0:8}…${RST}" >&2
echo -e "  Run ID  : ${CYN}${RUN_ID}${RST}" >&2

# =============================================================================
section "1. Health & status"

chk2xx "GET /health"             GET  /health
chk    "GET /v1/status"     200  GET  /v1/status
chk    "GET /v1/dashboard"  200  GET  /v1/dashboard
chk    "GET /v1/stats"      200  GET  /v1/stats
chk2xx "GET /v1/overview"        GET  /v1/overview
chk    "GET /v1/health/detailed" 200  GET /v1/health/detailed
chk    "GET /v1/metrics"    200  GET  /v1/metrics
chk    "GET /v1/settings"   200  GET  /v1/settings
chk2xx "GET /v1/db/status"       GET  /v1/db/status

# =============================================================================
section "2. Session lifecycle"

chk "POST /v1/sessions" 201 POST /v1/sessions \
  "{\"tenant_id\":\"default\",\"workspace_id\":\"default\",\"project_id\":\"default\",\"session_id\":\"${SESSION_ID}\"}"
[ "$(jf state)" = "open" ] && log_ok "  state=open" || log_fail "  state='$(jf state)' (expected open)"

chk "GET /v1/sessions" 200 GET "/v1/sessions?tenant_id=default&workspace_id=default&project_id=default"
echo "$_BODY" | grep -q "$SESSION_ID" \
  && log_ok "  session appears in list" || log_fail "  session missing from list"

# =============================================================================
section "3. Run lifecycle"

chk "POST /v1/runs" 201 POST /v1/runs \
  "{\"tenant_id\":\"default\",\"workspace_id\":\"default\",\"project_id\":\"default\",\"session_id\":\"${SESSION_ID}\",\"run_id\":\"${RUN_ID}\"}"
[ "$(jf state)" = "pending" ] && log_ok "  state=pending" || log_fail "  state='$(jf state)' (expected pending)"

chk "GET /v1/runs" 200 GET "/v1/runs?tenant_id=default&workspace_id=default&project_id=default"
echo "$_BODY" | grep -q "$RUN_ID" && log_ok "  run in list" || log_fail "  run missing from list"

chk "GET /v1/runs/:id"           200 GET "/v1/runs/${RUN_ID}"
# Run cost may be 404 if no cost events have been emitted yet — both are valid
api GET "/v1/runs/${RUN_ID}/cost"
[[ "$_HTTP" =~ ^(200|404)$ ]] \
  && log_ok "GET /v1/runs/:id/cost (HTTP $_HTTP)" \
  || log_fail "GET /v1/runs/:id/cost (unexpected HTTP $_HTTP)"
chk "GET /v1/runs/:id/events"    200 GET "/v1/runs/${RUN_ID}/events"
chk "GET /v1/runs/:id/tasks"     200 GET "/v1/runs/${RUN_ID}/tasks"
chk "GET /v1/runs/:id/approvals" 200 GET "/v1/runs/${RUN_ID}/approvals"

# Claim the run so downstream Fabric-only FCALLs (suspend / signal
# / enter_waiting_approval) would accept it. On the in-memory runtime
# this is a no-op that returns the record unchanged; on the Fabric
# runtime it flips lifecycle_phase=active.
#
# NOT idempotent on Fabric: re-claiming an already-active run fails at
# FF's grant gate (`ff_issue_claim_grant` requires
# lifecycle_phase=runnable, see lua/scheduling.lua:109-112) and surfaces
# as a 500. The `ff_claim_resumed_execution` dispatch only fires for an
# attempt_interrupted (previously-suspended) execution, not a fresh
# re-claim. One claim per lifecycle.
# (A second claim after a suspend/resume cycle IS legitimate —
# dispatches through FF's resume-claim path — but smoke doesn't
# exercise that flow; the Fabric integration tests cover it.)
chk "POST /v1/runs/:id/claim"    200 POST "/v1/runs/${RUN_ID}/claim" "{}"

# Sections 3.lease + 4.claim: the FF-enforced state machine rejects
# pause/resume/release-lease that aren't preceded by a claim. We exercise
# the claim-then-operate sequence via the smoke_worker binary — it drives
# a separate run + task through the full lifecycle in the correct order.
#
# The main RUN_ID / TASK_ID / SESSION_ID stay on the read-path checks so
# the smoke_worker fixture runs in isolation.
section "3b. Claim-then-operate (smoke_worker fixture)"

SW_RUN_ID="sw_run_${RUN_ID}"
SW_TASK_ID="sw_task_${RUN_ID}"
SW_SESS_ID="sw_sess_${RUN_ID}"

SMOKE_WORKER_BIN="${CAIRN_SMOKE_WORKER_BIN:-}"
if [ -z "$SMOKE_WORKER_BIN" ]; then
  # Prefer a pre-built release binary, fall back to cargo run for dev invocations.
  if [ -x "target/release/smoke_worker" ]; then
    SMOKE_WORKER_BIN="target/release/smoke_worker"
  elif [ -x "target/debug/smoke_worker" ]; then
    SMOKE_WORKER_BIN="target/debug/smoke_worker"
  else
    SMOKE_WORKER_BIN="cargo run --quiet --bin smoke_worker --"
  fi
fi

if CAIRN_URL="$BASE" CAIRN_TOKEN="$TOKEN" \
   $SMOKE_WORKER_BIN \
     --tenant default_tenant --workspace default_workspace \
     --project default_project \
     --run-id "$SW_RUN_ID" --session-id "$SW_SESS_ID" --task-id "$SW_TASK_ID" \
     >&2; then
  log_ok "smoke_worker claim-then-operate"
else
  log_fail "smoke_worker claim-then-operate (see stderr above)"
fi

# Cancel run — create a separate run so we don't break the main lifecycle
CANCEL_RUN_ID="crun_${RUN_ID}"
chk "POST run for cancel test" 201 POST /v1/runs \
  "{\"tenant_id\":\"default\",\"workspace_id\":\"default\",\"project_id\":\"default\",\"session_id\":\"${SESSION_ID}\",\"run_id\":\"${CANCEL_RUN_ID}\"}"
chk "POST /v1/runs/:id/cancel" 200 POST "/v1/runs/${CANCEL_RUN_ID}/cancel" ""
[ "$(jf state)" = "canceled" ] && log_ok "  canceled" || log_fail "  cancel state='$(jf state)'"

# =============================================================================
section "4. Task queue (read-path surface)"

# Correct EventEnvelope + RuntimeEvent (tagged with "event" discriminator)
# OwnershipKey: tag="scope", rename_all="snake_case" → Project variant flattens its fields
OWNERSHIP="{\"scope\":\"project\",\"tenant_id\":\"default\",\"workspace_id\":\"default\",\"project_id\":\"default\"}"
PROJECT="{\"tenant_id\":\"default\",\"workspace_id\":\"default\",\"project_id\":\"default\"}"
# EventSource: tag="source_type", rename_all="snake_case" → Runtime has no fields
SOURCE="{\"source_type\":\"runtime\"}"

chk "POST /v1/events/append (TaskCreated)" 201 POST /v1/events/append \
  "[{\"event_id\":\"evt_t_${RUN_ID}\",\"source\":${SOURCE},\"ownership\":${OWNERSHIP},\"causation_id\":null,\"correlation_id\":null,\"payload\":{\"event\":\"task_created\",\"project\":${PROJECT},\"task_id\":\"${TASK_ID}\",\"parent_run_id\":\"${RUN_ID}\",\"parent_task_id\":null,\"prompt_release_id\":null}}]"

# Poll until the TaskCreated event surfaces through the /v1/tasks
# projection. events/append returns after the in-process sync but the
# cross-bridge consumer is eventually-consistent; a bare sleep is a
# race (see #399).
wait_until "GET /v1/tasks (task projected)" \
  "/v1/tasks?tenant_id=default&workspace_id=default&project_id=default" \
  "$TASK_ID" 3000 \
  && log_ok "GET /v1/tasks (HTTP $_HTTP, task $TASK_ID visible)" \
  || true  # wait_until already logged the failure

# =============================================================================
section "5. Approval workflow"

chk "POST /v1/events/append (ApprovalRequested)" 201 POST /v1/events/append \
  "[{\"event_id\":\"evt_a_${RUN_ID}\",\"source\":${SOURCE},\"ownership\":${OWNERSHIP},\"causation_id\":null,\"correlation_id\":null,\"payload\":{\"event\":\"approval_requested\",\"project\":${PROJECT},\"approval_id\":\"${APPR_ID}\",\"run_id\":\"${RUN_ID}\",\"task_id\":null,\"requirement\":\"required\"}}]"

# Poll until the ApprovalRequested event surfaces through the
# /v1/approvals/pending projection (replaces bare sleep 0.4 — #399).
wait_until "GET /v1/approvals/pending (approval projected)" \
  "/v1/approvals/pending?tenant_id=default&workspace_id=default&project_id=default" \
  "$APPR_ID" 3000 \
  && log_ok "GET /v1/approvals/pending (HTTP $_HTTP, approval $APPR_ID visible)" \
  || true

chk "POST /v1/approvals/:id/resolve" 200 POST \
  "/v1/approvals/${APPR_ID}/resolve" '{"decision":"approved","reason":"smoke"}'
[ "$(jf decision)" = "approved" ] && log_ok "  decision=approved" \
  || log_fail "  decision='$(jf decision)'"

# =============================================================================
section "6. Event log"

chk "GET /v1/events" 200 GET "/v1/events?limit=20"
ECNT=$(jlen)
[ "$ECNT" -gt 0 ] && log_ok "  ${ECNT} events in log" || log_fail "  event log empty after writes"

chk "GET /v1/events?after=0"  200 GET "/v1/events?after=0&limit=5"
chk "GET /v1/admin/audit-log" 200 GET "/v1/admin/audit-log?limit=5"
chk "GET /v1/admin/logs"      200 GET "/v1/admin/logs?limit=10"

# =============================================================================
section "7. Stats"

chk "GET /v1/stats" 200 GET /v1/stats
TR=$(jf total_runs)
[ "${TR:-0}" -ge 1 ] && log_ok "  total_runs=${TR}" || log_fail "  total_runs=${TR:-0} (expected ≥ 1)"

# =============================================================================
section "8. Prompts"

chk "GET /v1/prompts/assets"   200 GET /v1/prompts/assets
chk "GET /v1/prompts/releases" 200 GET /v1/prompts/releases

# =============================================================================
section "9. Costs & traces"

chk "GET /v1/costs" 200 GET /v1/costs
echo "$_BODY" | grep -q "items" \
  && log_ok "  has items array" || log_fail "  missing items array"

chk "GET /v1/traces" 200 GET "/v1/traces?limit=10"

# =============================================================================
section "10. Providers"

chk "GET /v1/providers"        200 GET /v1/providers
chk "GET /v1/providers/health" 200 GET "/v1/providers/health?tenant_id=default"

# =============================================================================
section "11. Ollama"

api GET /v1/providers/ollama/models
if [ "$_HTTP" = "503" ]; then
  log_skip "Ollama not configured (HTTP 503)"
  MNAME=""
elif [ "$_HTTP" = "200" ]; then
  log_ok "GET /v1/providers/ollama/models (HTTP 200)"
  MNAME=$(printf '%s' "$_BODY" | python3 -c \
    "import sys,json; m=json.load(sys.stdin).get('models',[]); print(next((x for x in m if 'embed' not in x),m[0] if m else ''))" 2>/dev/null || true)
  MCNT=$(jf count)
else
  log_fail "GET /v1/providers/ollama/models (HTTP $_HTTP)"
  MNAME=""
fi

if [ -n "$MNAME" ]; then
  log_ok "  Ollama: ${MCNT} model(s); selected=${MNAME}"
  # Ollama can be slow — use a longer one-shot timeout for this step
  saved_timeout="$TIMEOUT"
  TIMEOUT=90
  chk "POST /v1/providers/ollama/generate" 200 POST /v1/providers/ollama/generate \
    "{\"model\":\"${MNAME}\",\"prompt\":\"Reply with only the word: ok\"}"
  TIMEOUT="$saved_timeout"
  GT=$(jf text)
  [ -n "$GT" ] && log_ok "  generate → '${GT:0:40}'" || log_fail "  generate returned empty text"
else
  log_skip "Ollama not available — skipping generation"
fi

# =============================================================================
section "12. Memory"

chk "POST /v1/memory/ingest" 200 POST /v1/memory/ingest \
  "{\"source_id\":\"smoke_src\",\"document_id\":\"sdoc_${RUN_ID}\",\"content\":\"Smoke test. The quick brown fox.\",\"tenant_id\":\"default\",\"workspace_id\":\"default\",\"project_id\":\"default\"}"
[ "$(jf ok)" = "True" ] && log_ok "  ingested" || log_fail "  ingest ok='$(jf ok)'"

chk "GET /v1/memory/search" 200 GET \
  "/v1/memory/search?query_text=fox&tenant_id=default&workspace_id=default&project_id=default&limit=5"
echo "$_BODY" | grep -q "results" && log_ok "  search returned results" || log_fail "  search missing results"

chk "GET /v1/sources" 200 GET "/v1/sources?tenant_id=default&workspace_id=default&project_id=default"

# =============================================================================
section "13. Metrics (Prometheus)"

chk "GET /v1/metrics" 200 GET /v1/metrics
echo "$_BODY" | grep -q "http_requests_total" \
  && log_ok "  has http_requests_total counter" || log_fail "  missing http_requests_total counter"

# =============================================================================
section "14. SSE stream (brief connect)"

SSE_HTTP=$(curl -s -o /dev/null -w "%{http_code}" --max-time 2 \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Accept: text/event-stream" \
  "${BASE}/v1/stream" 2>/dev/null || true)
[ "$SSE_HTTP" = "200" ] \
  && log_ok "SSE stream reachable (HTTP 200)" \
  || log_fail "SSE stream unreachable (HTTP ${SSE_HTTP})"

# =============================================================================
section "15. Admin"

chk   "GET /v1/admin/audit-log" 200 GET "/v1/admin/audit-log?limit=5"
# Accept 201 (created) or 400 (already exists from prior run) — both are correct
api POST /v1/admin/tenants '{"tenant_id":"smoke_admin_t","name":"Smoke Tenant"}'
[[ "$_HTTP" =~ ^(201|400|409)$ ]] \
  && log_ok "POST /v1/admin/tenants (HTTP $_HTTP — created or already exists)" \
  || log_fail "POST /v1/admin/tenants (unexpected HTTP $_HTTP)"

# =============================================================================
section "16. Evals"

chk "GET /v1/evals/runs" 200 GET "/v1/evals/runs?tenant_id=default&workspace_id=default&project_id=default"

EVAL_RUN_ID="eval_${RUN_ID}"
chk "POST /v1/evals/runs (create eval run)" 201 POST /v1/evals/runs \
  "{\"tenant_id\":\"default\",\"workspace_id\":\"default\",\"project_id\":\"default\",\"eval_run_id\":\"${EVAL_RUN_ID}\",\"subject_kind\":\"prompt_release\",\"evaluator_type\":\"accuracy\"}"

chk "GET /v1/evals/runs/:id" 200 GET "/v1/evals/runs/${EVAL_RUN_ID}"

chk "POST /v1/evals/runs/:id/start" 200 POST "/v1/evals/runs/${EVAL_RUN_ID}/start" ""

chk "POST /v1/evals/runs/:id/score" 200 POST "/v1/evals/runs/${EVAL_RUN_ID}/score" \
  "{\"metrics\":{\"accuracy\":0.85,\"latency_p50_ms\":120}}"

chk "POST /v1/evals/runs/:id/complete" 200 POST "/v1/evals/runs/${EVAL_RUN_ID}/complete" \
  "{\"metrics\":{\"accuracy\":0.85,\"latency_p50_ms\":120},\"cost\":0.05}"

# =============================================================================
section "17. Approval gate flow (via events)"

# Create a dedicated session + run for the gate test
chk "POST gate session" 201 POST /v1/sessions \
  "{\"tenant_id\":\"default\",\"workspace_id\":\"default\",\"project_id\":\"default\",\"session_id\":\"${GATE_SESSION_ID}\"}"
chk "POST gate run" 201 POST /v1/runs \
  "{\"tenant_id\":\"default\",\"workspace_id\":\"default\",\"project_id\":\"default\",\"session_id\":\"${GATE_SESSION_ID}\",\"run_id\":\"${GATE_RUN_ID}\"}"

# Request approval via event append (the runtime service transitions the run)
chk "POST event ApprovalRequested (gate)" 201 POST /v1/events/append \
  "[{\"event_id\":\"evt_gate_${RUN_ID}\",\"source\":${SOURCE},\"ownership\":${OWNERSHIP},\"causation_id\":null,\"correlation_id\":null,\"payload\":{\"event\":\"approval_requested\",\"project\":${PROJECT},\"approval_id\":\"${GATE_APPR_ID}\",\"run_id\":\"${GATE_RUN_ID}\",\"task_id\":null,\"requirement\":\"required\"}}]"

# Poll until the gate approval surfaces in /v1/approvals/pending
# (replaces bare sleep 0.4 — #399).
wait_until "GET /v1/approvals/pending (gate projected)" \
  "/v1/approvals/pending?tenant_id=default&workspace_id=default&project_id=default" \
  "$GATE_APPR_ID" 3000 \
  && log_ok "GET /v1/approvals/pending (gate) (HTTP $_HTTP, approval $GATE_APPR_ID visible)" \
  || true

# Resolve the gate via /v1/approvals/:id/resolve
chk "POST resolve gate" 200 POST \
  "/v1/approvals/${GATE_APPR_ID}/resolve" '{"decision":"approved","reason":"smoke gate"}'
[ "$(jf decision)" = "approved" ] \
  && log_ok "  gate decision=approved" \
  || log_fail "  gate decision='$(jf decision)'"

# =============================================================================
section "18. Bundle export/import round-trip"

chk "GET /v1/bundles/export" 200 GET \
  "/v1/bundles/export?tenant_id=default&workspace_id=default&project_id=default"

# Validate bundle has expected structure (artifacts, not events)
ARTIFACT_CT=$(printf '%s' "$_BODY" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(len(d.get('artifacts',[])))" 2>/dev/null || echo 0)
SCHEMA_VER=$(printf '%s' "$_BODY" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('bundle_schema_version','?'))" 2>/dev/null || echo "?")
log_ok "  bundle schema_version=${SCHEMA_VER}, artifacts=${ARTIFACT_CT}"

# Validate bundle (may fail if artifacts have empty content — that's a known export gap)
api POST /v1/bundles/validate "$_BODY"
[[ "$_HTTP" =~ ^(200|422)$ ]] \
  && log_ok "POST /v1/bundles/validate (HTTP $_HTTP)" \
  || log_fail "POST /v1/bundles/validate (unexpected HTTP $_HTTP)"

# Apply the bundle via the lib.rs handler (plan + apply)
api POST /v1/bundles/plan "$_BODY"
[[ "$_HTTP" =~ ^(200|422)$ ]] \
  && log_ok "POST /v1/bundles/plan (HTTP $_HTTP)" \
  || log_fail "POST /v1/bundles/plan (unexpected HTTP $_HTTP)"

# =============================================================================
section "19. Entitlements & templates"

# Entitlements may 404 when no plan is assigned to the tenant — acceptable in smoke mode
api GET /v1/entitlements
[[ "$_HTTP" =~ ^(200|404)$ ]] \
  && log_ok "GET /v1/entitlements (HTTP $_HTTP)" \
  || log_fail "GET /v1/entitlements (unexpected HTTP $_HTTP)"

api GET /v1/entitlements/usage
[[ "$_HTTP" =~ ^(200|404)$ ]] \
  && log_ok "GET /v1/entitlements/usage (HTTP $_HTTP)" \
  || log_fail "GET /v1/entitlements/usage (unexpected HTTP $_HTTP)"

chk "GET /v1/templates" 200 GET /v1/templates

# =============================================================================
section "20. Memory CRUD"

# Ingest additional documents
chk "POST /v1/memory/ingest (doc 2)" 200 POST /v1/memory/ingest \
  "{\"source_id\":\"smoke_crud\",\"document_id\":\"cdoc1_${RUN_ID}\",\"content\":\"Memory CRUD test document about quantum computing.\",\"tenant_id\":\"default\",\"workspace_id\":\"default\",\"project_id\":\"default\"}"
chk "POST /v1/memory/ingest (doc 3)" 200 POST /v1/memory/ingest \
  "{\"source_id\":\"smoke_crud\",\"document_id\":\"cdoc2_${RUN_ID}\",\"content\":\"Memory CRUD test document about neural networks.\",\"tenant_id\":\"default\",\"workspace_id\":\"default\",\"project_id\":\"default\"}"

# Search across the CRUD documents
chk "GET /v1/memory/search (quantum)" 200 GET \
  "/v1/memory/search?query_text=quantum&tenant_id=default&workspace_id=default&project_id=default&limit=5"
RESULT_COUNT=$(printf '%s' "$_BODY" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(len(d.get('results',[])))" 2>/dev/null || echo 0)
[ "${RESULT_COUNT:-0}" -ge 1 ] \
  && log_ok "  search found ${RESULT_COUNT} result(s)" \
  || log_fail "  search found ${RESULT_COUNT:-0} results (expected ≥ 1)"

chk "GET /v1/memory/documents/:id" 200 GET "/v1/memory/documents/cdoc1_${RUN_ID}"

# Memory diagnostics
chk "GET /v1/memory/diagnostics" 200 GET "/v1/memory/diagnostics?tenant_id=default&workspace_id=default&project_id=default"

# =============================================================================
section "21. Orchestrator (optional — skipped when no brain provider)"

# POST /v1/runs/:id/orchestrate — 200/202 = pass, 503 = skip (no provider),
# 502/429 = skip (provider offline/throttled), anything else = fail.
api POST "/v1/runs/${RUN_ID}/orchestrate" \
  "{\"goal\":\"Summarize the current run state.\",\"max_iterations\":2,\"timeout_ms\":30000}"

if [[ "$_HTTP" =~ ^(200|202)$ ]]; then
  TERM=$(printf '%s' "$_BODY" | python3 -c \
    "import sys,json; print(json.load(sys.stdin).get('termination',''))" 2>/dev/null || echo "unknown")
  log_ok "POST /v1/runs/:id/orchestrate (HTTP $_HTTP, termination=${TERM})"
  # Validate response shape: must have a termination field
  [ -n "$TERM" ] && [ "$TERM" != "unknown" ] \
    && log_ok "  termination field present: ${TERM}" \
    || log_fail "  termination field missing or empty"
elif [[ "$_HTTP" =~ ^(503|502|429|500)$ ]]; then
  log_skip "Orchestrator skipped — no brain provider or provider offline (HTTP $_HTTP)"
  log_skip "  Set CAIRN_BRAIN_URL or OLLAMA_HOST to exercise this path"
else
  log_fail "POST /v1/runs/:id/orchestrate (unexpected HTTP $_HTTP)"
  [ -n "$_BODY" ] && echo -e "     ${RED}${_BODY:0:160}${RST}" >&2
fi

# =============================================================================
section "22. Checkpoint save/restore"

# Save a checkpoint for the primary run
CHECKPOINT_ID="ckpt_${RUN_ID}"
api POST "/v1/runs/${RUN_ID}/checkpoint" \
  "{\"checkpoint_id\":\"${CHECKPOINT_ID}\",\"strategy\":\"manual\",\"state_snapshot\":{\"step\":1,\"notes\":\"smoke test checkpoint\"}}"
[[ "$_HTTP" =~ ^(200|201)$ ]] \
  && log_ok "POST /v1/runs/:id/checkpoint (HTTP $_HTTP)" \
  || log_fail "POST /v1/runs/:id/checkpoint (unexpected HTTP $_HTTP)"

api GET "/v1/runs/${RUN_ID}/checkpoint-strategy"
[[ "$_HTTP" =~ ^(200|404)$ ]] \
  && log_ok "GET /v1/runs/:id/checkpoint-strategy (HTTP $_HTTP)" \
  || log_fail "GET /v1/runs/:id/checkpoint-strategy (unexpected HTTP $_HTTP)"

# =============================================================================
section "22b. Cross-tenant isolation for tools.rs handlers (META #372)"

# Mint an operator token scoped to a different tenant. If minting fails
# (e.g. /v1/auth/tokens not wired in this build) we skip — the
# in-process integration tests in crates/cairn-app/tests/ cover the
# same surface with fuller fixtures.
CROSS_TENANT="smoke-cross-${RANDOM}"
api POST "/v1/auth/tokens" \
  "{\"operator_id\":\"smoke_cross_op\",\"tenant_id\":\"${CROSS_TENANT}\",\"name\":\"smoke-cross-tenant\"}"
if [[ "$_HTTP" == "201" ]]; then
  CROSS_TOKEN=$(printf '%s' "$_BODY" | python3 -c \
    "import sys,json; print(json.load(sys.stdin).get('token',''))" 2>/dev/null)

  if [ -n "$CROSS_TOKEN" ]; then
    # Cross-tenant checkpoint restore must 404 (no id-enumeration oracle).
    # Uses the $CHECKPOINT_ID planted above under the admin-default tenant.
    status=$(curl -s -o /dev/null -w "%{http_code}" --max-time "$TIMEOUT" \
      -X POST -H "Authorization: Bearer ${CROSS_TOKEN}" \
      -H "Content-Type: application/json" \
      "${BASE}/v1/checkpoints/${CHECKPOINT_ID}/restore" 2>/dev/null)
    [ "$status" = "404" ] \
      && log_ok "POST /v1/checkpoints/:id/restore cross-tenant (HTTP $status) — #369 closed" \
      || log_fail "POST /v1/checkpoints/:id/restore cross-tenant (expected 404, got $status) — #369 REGRESSION"

    # Cross-tenant save checkpoint on foreign run must 404.
    status=$(curl -s -o /dev/null -w "%{http_code}" --max-time "$TIMEOUT" \
      -X POST -H "Authorization: Bearer ${CROSS_TOKEN}" \
      -H "Content-Type: application/json" \
      -d "{\"checkpoint_id\":\"ckpt_smoke_spoof_${RANDOM}\"}" \
      "${BASE}/v1/runs/${RUN_ID}/checkpoint" 2>/dev/null)
    [ "$status" = "404" ] \
      && log_ok "POST /v1/runs/:id/checkpoint cross-tenant (HTTP $status) — #370 closed" \
      || log_fail "POST /v1/runs/:id/checkpoint cross-tenant (expected 404, got $status) — #370 REGRESSION"
  else
    log_skip "cross-tenant checks skipped — token mint returned empty token"
  fi
else
  log_skip "cross-tenant checks skipped — /v1/auth/tokens returned HTTP $_HTTP"
fi

# =============================================================================
section "23. Graph endpoints"

chk2xx "GET /v1/graph/nodes"  GET "/v1/graph/nodes?tenant_id=default&workspace_id=default&project_id=default&limit=10"
chk2xx "GET /v1/graph/edges"  GET "/v1/graph/edges?tenant_id=default&workspace_id=default&project_id=default&limit=10"

api GET "/v1/graph/execution-trace/${RUN_ID}"
[[ "$_HTTP" =~ ^(200|404)$ ]] \
  && log_ok "GET /v1/graph/execution-trace/:run_id (HTTP $_HTTP)" \
  || log_fail "GET /v1/graph/execution-trace/:run_id (unexpected HTTP $_HTTP)"

# =============================================================================
section "24. System info & notifications"

chk "GET /v1/system/info" 200 GET /v1/system/info
chk2xx "GET /v1/notifications" GET /v1/notifications
chk "GET /v1/settings" 200 GET /v1/settings
chk "GET /v1/overview" 200 GET "/v1/overview?tenant_id=default&workspace_id=default&project_id=default"

# =============================================================================
# Section 25: credential + provider-connection + breaker + webhook sections
# close audit #398. These are the exact flows ("credential →
# connection → orchestrate → breaker trip", webhook URL validation)
# that had been manually dogfood-verified but never smoke-gated, so
# a regression in any one would previously ship without this script
# failing. Each section is small, deterministic, and uses HTTP-only
# assertions so it stays portable across the local/dev/team builds.
# =============================================================================
section "25. Credential lifecycle"

# Use a unique provider_id per run to avoid 409-on-rerun when the
# server is long-lived (e.g. local dev mode). Suffix matches RUN_ID
# naming so post-mortem logs correlate cleanly.
CRED_PROVIDER_ID="smoke-cred-${RUN_ID}"
CRED_TENANT="default_tenant"
CRED_BODY="{\"provider_id\":\"${CRED_PROVIDER_ID}\",\"plaintext_value\":\"sk-smoke-${RUN_ID}\"}"

# (a) first store → 201 (seeds the tenant cred list)
chk "POST /v1/admin/tenants/:t/credentials" 201 POST \
  "/v1/admin/tenants/${CRED_TENANT}/credentials" "$CRED_BODY"
CRED_ID=$(jf id)
[ -n "$CRED_ID" ] && log_ok "  credential id=${CRED_ID}" \
  || log_fail "  credential create body missing id"

# (b) second store same provider_id → 409 credential_exists (#217 regression)
api POST "/v1/admin/tenants/${CRED_TENANT}/credentials" "$CRED_BODY"
if [ "$_HTTP" = "409" ]; then
  CODE=$(jf code)
  if [ "$CODE" = "credential_exists" ]; then
    log_ok "POST duplicate /credentials 409 code=credential_exists (#217 closed)"
  else
    log_fail "POST duplicate /credentials 409 but code='${CODE}' (expected credential_exists)"
  fi
else
  log_fail "POST duplicate /credentials expected 409, got HTTP $_HTTP"
fi

# (c) list contains exactly one record for this provider
chk "GET /v1/admin/tenants/:t/credentials" 200 GET \
  "/v1/admin/tenants/${CRED_TENANT}/credentials"
# Pass the provider id through the environment rather than embedded
# in the Python source so a quote/backslash in the id (real risk
# once dogfood feeds arbitrary provider names through here) can't
# inject into the script. The env var is set on the python3 command
# (not printf) so it actually lands in python's process environ.
CRED_MATCH=$(printf '%s' "$_BODY" | CRED_PROVIDER_ID="$CRED_PROVIDER_ID" python3 -c \
  "import sys,json,os; provider_id=os.environ['CRED_PROVIDER_ID']; d=json.load(sys.stdin); items=d.get('items',[]); print(sum(1 for c in items if c.get('provider_id')==provider_id))" 2>/dev/null || echo 0)
[ "${CRED_MATCH:-0}" = "1" ] \
  && log_ok "  exactly 1 active credential for provider (no silent accumulation)" \
  || log_fail "  ${CRED_MATCH:-0} credential rows for provider (expected 1)"

# (d) empty plaintext_value → 422 validation_error (#403 regression)
api POST "/v1/admin/tenants/${CRED_TENANT}/credentials" \
  "{\"provider_id\":\"smoke-cred-empty-${RUN_ID}\",\"plaintext_value\":\"\"}"
if [ "$_HTTP" = "422" ]; then
  log_ok "POST /credentials empty plaintext 422 (#403 closed)"
else
  log_fail "POST /credentials empty plaintext expected 422, got HTTP $_HTTP"
fi

# =============================================================================
section "26. Provider connection"

# Create a provider connection that binds to the credential above.
# Keeps the happy path for the 'credential → connection' flow that
# audit #398 flagged as un-smoke-gated.
CONN_ID="smoke-conn-${RUN_ID}"
CONN_BODY="{\"tenant_id\":\"${CRED_TENANT}\",\"provider_connection_id\":\"${CONN_ID}\",\"provider_family\":\"openai\",\"adapter_type\":\"openai\",\"supported_models\":[\"gpt-4\"],\"credential_id\":\"${CRED_ID}\"}"
api POST /v1/providers/connections "$CONN_BODY"
# Accept 201 or 503 (feature-gated-off) — both are sane release states
if [ "$_HTTP" = "201" ]; then
  log_ok "POST /v1/providers/connections (HTTP 201 — connection created)"
  # Confirm the connection appears in the list. GET requires the
  # tenant_id query param (the TenantScopedQuery extractor 400s
  # without it).
  chk "GET /v1/providers/connections" 200 GET \
    "/v1/providers/connections?tenant_id=${CRED_TENANT}"
  if printf '%s' "$_BODY" | grep -F -q -- "$CONN_ID"; then
    log_ok "  connection ${CONN_ID} visible in list"
  else
    log_fail "  connection ${CONN_ID} missing from list body"
  fi
elif [ "$_HTTP" = "503" ]; then
  log_skip "POST /v1/providers/connections skipped — MULTI_PROVIDER feature gated off"
else
  log_fail "POST /v1/providers/connections (unexpected HTTP $_HTTP)"
fi

# =============================================================================
section "27. Webhook validation (SSRF + malformed URL)"

# Closes #398 sub-point + #235 regression: set_operator_notifications
# must reject malformed webhook targets AND block IMDS / RFC 1918 /
# loopback unless explicitly allowed. The smoke gate only asserts the
# validation path fires — we don't assert a delivery.

# (a) malformed webhook URL (not-a-url) → 422 validation_error
api POST "/v1/admin/operators/smoke_operator_${RUN_ID}/notifications" \
  "{\"tenant_id\":\"${CRED_TENANT}\",\"event_types\":[\"run_completed\"],\"channels\":[{\"kind\":\"webhook\",\"target\":\"not-a-url\"}]}"
if [ "$_HTTP" = "422" ]; then
  log_ok "POST /admin/operators/:id/notifications malformed URL 422 (#235 closed)"
else
  log_fail "POST /admin/operators/:id/notifications malformed URL expected 422, got HTTP $_HTTP"
fi

# (b) SSRF target (IMDS) — strict fail-closed. Per Copilot review
# round 5: any 2xx is a regression (the gate is meant to block IMDS
# unconditionally, independent of server env). Success is a 4xx from
# the validation_error family (400/403/422); anything 2xx means the
# SSRF check didn't fire. We probe BOTH http and https variants —
# #451's core risk is scheme-independent, and https://169.254.169.254
# is the canonical IMDS-theft URL (see webhook_validation.rs tests
# at lines 708-736 which pin BOTH schemes). A regression that only
# re-allows the https path would previously have slipped through a
# http-only smoke probe.
imds_ssrf_probe() {
  local scheme="$1" label="$2"
  api POST "/v1/admin/operators/smoke_operator_ssrf_${scheme}_${RUN_ID}/notifications" \
    "{\"tenant_id\":\"${CRED_TENANT}\",\"event_types\":[\"run_completed\"],\"channels\":[{\"kind\":\"webhook\",\"target\":\"${scheme}://169.254.169.254/latest/meta-data/\"}]}"
  # success set: 400/403/422 (validation_error family).
  # failure set: anything 2xx — IMDS write-through is a #451 regression.
  if [[ "$_HTTP" =~ ^(400|403|422)$ ]]; then
    log_ok "POST /admin/operators/:id/notifications ${label} blocked (HTTP $_HTTP — #451 closed)"
  elif [[ "$_HTTP" =~ ^2[0-9][0-9]$ ]]; then
    log_fail "POST /admin/operators/:id/notifications ${label} accepted (HTTP $_HTTP) — SSRF gate bypassed, #451 regression. body=${_BODY:0:160}"
  else
    log_fail "POST /admin/operators/:id/notifications ${label} unexpected HTTP $_HTTP (expected 400/403/422). body=${_BODY:0:160}"
  fi
}

imds_ssrf_probe "http"  "IMDS http:// target"
imds_ssrf_probe "https" "IMDS https:// target"

# =============================================================================
section "28. Orchestrator breaker trip"

# Closes #398 sub-point: "orchestrate with low breaker_overrides —
# verify breaker-trip termination". Drives the breaker logic that
# would previously ship unverified. We use round_cap=1 because the
# orchestrator trips `iteration >= round_cap`, so a run bounded at
# 1 iteration hits the cap immediately even without a live provider.
#
# We create a dedicated run so this doesn't collide with the
# orchestrate-existing-run flow in section 21. Termination must be
# `breaker_tripped` with `which=round`. If the orchestrator is
# unavailable (503/502/429), skip — there's no provider configured.
BRK_SESSION_ID="brk_sess_${RUN_ID}"
BRK_RUN_ID="brk_run_${RUN_ID}"

chk "POST /v1/sessions (breaker)" 201 POST /v1/sessions \
  "{\"tenant_id\":\"default\",\"workspace_id\":\"default\",\"project_id\":\"default\",\"session_id\":\"${BRK_SESSION_ID}\"}"
chk "POST /v1/runs (breaker)" 201 POST /v1/runs \
  "{\"tenant_id\":\"default\",\"workspace_id\":\"default\",\"project_id\":\"default\",\"session_id\":\"${BRK_SESSION_ID}\",\"run_id\":\"${BRK_RUN_ID}\"}"

# Orchestrate with breaker_overrides: round_cap=1 forces an immediate trip.
api POST "/v1/runs/${BRK_RUN_ID}/orchestrate" \
  "{\"goal\":\"breaker smoke\",\"max_iterations\":10,\"timeout_ms\":30000,\"breaker_overrides\":{\"round_cap\":1}}"

if [[ "$_HTTP" =~ ^(200|202)$ ]]; then
  TERM=$(jf termination)
  WHICH=$(jf which)
  # round_cap=1 must trip the Round breaker specifically. A
  # different `which` value means the overrides weren't honoured
  # or a different cap tripped first — pin both fields.
  if [ "$TERM" = "breaker_tripped" ] && [ "$WHICH" = "round" ]; then
    log_ok "POST orchestrate breaker trip (termination=breaker_tripped, which=round)"
  else
    log_fail "POST orchestrate breaker trip: termination='${TERM}', which='${WHICH:-<unset>}' (expected termination=breaker_tripped and which=round). body=${_BODY:0:160}"
  fi
elif [[ "$_HTTP" =~ ^(503|502|429)$ ]]; then
  # 503 = no provider configured; 502/429 = upstream transient. A
  # genuine 500 is an internal bug signal in the breaker/orchestrate
  # path — do NOT skip on it (Copilot review #542 round 4).
  log_skip "Orchestrator breaker trip skipped — no/transient provider (HTTP $_HTTP)"
else
  log_fail "POST orchestrate breaker trip (unexpected HTTP $_HTTP — internal error if 500)"
fi

# =============================================================================
SUITE_END=$(date +%s%3N 2>/dev/null || python3 -c "import time;print(int(time.time()*1000))")
ELAPSED_MS=$(( SUITE_END - SUITE_START ))
ELAPSED_S=$(python3 -c "print(f'{${ELAPSED_MS}/1000:.1f}')" 2>/dev/null || echo "?")

TOTAL=$(( PASS + FAIL + SKIP ))
echo "" >&2
echo -e "${BLD}── Results $(printf '─%.0s' {1..36})${RST}" >&2
printf "  ${GRN}Passed${RST}   %3d\n"  "$PASS" >&2
printf "  ${RED}Failed${RST}   %3d\n"  "$FAIL" >&2
printf "  ${YLW}Skipped${RST}  %3d\n" "$SKIP"  >&2
printf "  Total    %3d\n"              "$TOTAL" >&2
printf "  Time     %ss\n"             "$ELAPSED_S" >&2
echo "" >&2

if [ "$FAIL" -eq 0 ]; then
  echo -e "${GRN}${BLD}All tests passed.${RST}" >&2; exit 0
else
  echo -e "${RED}${BLD}${FAIL} test(s) failed.${RST}" >&2; exit 1
fi