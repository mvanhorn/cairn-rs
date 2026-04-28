//! Application metrics collection and Prometheus rendering.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Mutex,
    },
};

pub(crate) const HTTP_DURATION_BUCKETS_MS: [u64; 10] =
    [5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000];

/// Cardinality budget for tenant-labelled gauges (#502).
///
/// Prometheus starts choking (scrape latency, TSDB ingestion cost) in the
/// 10k-100k series range per instance. Cairn at the commercial layer will
/// run many small tenants (one per customer), so uncapped
/// `cairn_active_{runs,tasks}_by_tenant{tenant=…}` can hit that ceiling.
///
/// Policy: emit at most `TENANT_METRIC_TOP_N` tenants (ranked by total
/// activity, i.e. `active_runs + active_tasks + pending_approvals`);
/// overflow aggregates into a single `tenant="__other__"` row whose value
/// is the SUM across the evicted tenants. Operators get accurate top-N
/// dashboards AND a visible signal that there's additional activity
/// beyond the cap — "__other__" > 0 is the prompt to bump N or to
/// redirect high-cardinality breakdowns to OpenTelemetry traces.
///
/// 100 is a deliberate compromise: the top decile of tenants dominate
/// activity in every real cairn deployment we've seen, and 100 × 3
/// gauges = 300 series — comfortably under the "several thousand"
/// soft ceiling for a single Prometheus scrape.
pub(crate) const TENANT_METRIC_TOP_N: usize = 100;

/// Overflow-bucket label used when the tenant count exceeds
/// `TENANT_METRIC_TOP_N` or the distinct `(provider_connection, model)`
/// combination exceeds `PROVIDER_METRIC_TOP_N`. Pinned as a constant so
/// alerting rules / dashboards can match on a stable string.
pub(crate) const CARDINALITY_OVERFLOW_LABEL: &str = "__other__";

/// Cardinality budget for provider-call series (#503).
///
/// `model` (and `provider_connection`) are operator-supplied — rotating
/// model IDs (`gpt-4o-mini`, `gpt-4o-mini-2024-07-18`,
/// `gpt-4o-mini-20241218`) accumulate one series each. `ProviderCallKey`
/// crosses four labels, so the raw hashmap can reach
/// `|connections| × |models| × |ops| × 3` — unbounded in practice.
///
/// Policy: at render time, keep the top-N `(provider_connection, model)`
/// combinations ranked by total call count; overflow rows collapse into
/// `{provider_connection="__other__", model="__other__"}`. `operation_kind`
/// + `status` are bounded enums so they're never aggregated.
///
/// The counter map itself is still unbounded (record paths insert on
/// every call) — an operator-visible overflow is better than a silent
/// cap that drops updates. Render-time aggregation gives the backpressure
/// where it matters: Prometheus scrapes.
#[cfg(feature = "metrics-providers")]
pub(crate) const PROVIDER_METRIC_TOP_N: usize = 100;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RequestCountKey {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) status: u16,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RequestDurationKey {
    pub(crate) method: String,
    pub(crate) path: String,
}

#[derive(Clone, Debug)]
pub(crate) struct HistogramSample {
    pub(crate) bucket_counts: [u64; HTTP_DURATION_BUCKETS_MS.len()],
    pub(crate) sum_ms: u64,
    pub(crate) count: u64,
}

impl Default for HistogramSample {
    fn default() -> Self {
        Self {
            bucket_counts: [0; HTTP_DURATION_BUCKETS_MS.len()],
            sum_ms: 0,
            count: 0,
        }
    }
}

/// Label set for cairn's entity-lifecycle counters. `tenant` + `workspace`
/// are both required — every cairn domain event carries a `ProjectKey` so
/// the label is always populable, and tenant-level breakdowns are the
/// primary operator concern.
#[cfg(feature = "metrics-core")]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct EntityCountKey {
    pub(crate) tenant: String,
    pub(crate) workspace: String,
    /// Terminal outcome label for `_terminal_total` counters
    /// (`completed` / `failed` / `cancelled`). Empty for
    /// `_created_total` counters.
    pub(crate) outcome: String,
    /// Failure class label for failed-outcome rows. Empty string for
    /// non-failure rows.
    pub(crate) failure_class: String,
}

/// Label set for tool-invocation counters. Tool names are bounded
/// (cairn has a finite tool catalogue), so label cardinality stays
/// in O(|tools|).
#[cfg(feature = "metrics-core")]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ToolInvocationKey {
    pub(crate) tool: String,
    /// `ok`, `error`, or `timeout`.
    pub(crate) outcome: String,
}

/// Lease-expiry counter label — tasks and runs are tracked
/// separately so operators can see which surface is losing workers.
#[cfg(feature = "metrics-core")]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct LeaseExpiryKey {
    /// `task` or `run`.
    pub(crate) entity: String,
}

#[derive(Default)]
pub struct AppMetrics {
    request_totals: Mutex<HashMap<RequestCountKey, u64>>,
    request_durations: Mutex<HashMap<RequestDurationKey, HistogramSample>>,
    active_runs_total: AtomicU64,
    active_tasks_total: AtomicU64,
    startup_complete: AtomicBool,

    // ── metrics-core ─────────────────────────────────────────────
    #[cfg(feature = "metrics-core")]
    runs_created: Mutex<HashMap<EntityCountKey, u64>>,
    #[cfg(feature = "metrics-core")]
    runs_terminal: Mutex<HashMap<EntityCountKey, u64>>,
    #[cfg(feature = "metrics-core")]
    tasks_created: Mutex<HashMap<EntityCountKey, u64>>,
    #[cfg(feature = "metrics-core")]
    tasks_terminal: Mutex<HashMap<EntityCountKey, u64>>,
    #[cfg(feature = "metrics-core")]
    tool_invocations: Mutex<HashMap<ToolInvocationKey, u64>>,
    #[cfg(feature = "metrics-core")]
    lease_expiries: Mutex<HashMap<LeaseExpiryKey, u64>>,
    #[cfg(feature = "metrics-core")]
    projection_lag_events: AtomicU64,
    /// Per-tenant queue-depth gauges bundled into one mutex so a
    /// single tenant update takes one lock and one string hash, not
    /// three. Reader side (render_prometheus) clones the whole map
    /// once per scrape.
    #[cfg(feature = "metrics-core")]
    tenant_queue_depth: Mutex<HashMap<String, TenantQueueDepth>>,

    // ── metrics-providers ───────────────────────────────────────
    /// Counter of LLM provider calls labelled by provider family +
    /// model + operation + status. Cardinality caveat: model IDs are
    /// operator-supplied, so a misconfigured cairn can emit many
    /// distinct values. Acceptable trade-off because the feature is
    /// opt-in and per-model dashboards are the primary operator ask.
    #[cfg(feature = "metrics-providers")]
    provider_calls: Mutex<HashMap<ProviderCallKey, u64>>,
    #[cfg(feature = "metrics-providers")]
    provider_call_durations: Mutex<HashMap<ProviderCallDurationKey, HistogramSample>>,
    #[cfg(feature = "metrics-providers")]
    provider_tokens: Mutex<HashMap<ProviderTokenKey, u64>>,

    // ── F65 PR-3 orchestrator breakers ──────────────────────────
    /// Total trips per breaker kind (all four). Always-on: breakers
    /// are core orchestrator policy, not an optional feature.
    breaker_trips: Mutex<HashMap<String, u64>>,
    /// Total warn-threshold crossings per breaker kind. Emitted for
    /// Round / Tokens / WallClock only (NoToolUseConsecutive skips
    /// the warning per arch §4.1 exception).
    breaker_threshold_warns: Mutex<HashMap<String, u64>>,
    /// Per-kind distribution of the measured value at trip time.
    /// NoToolUseConsecutive is deliberately omitted (always trips at
    /// exactly the cap — a single-bucket spike, not distribution-
    /// worthy). Round / Tokens / WallClock each have their own
    /// dedicated bucket array tuned to the quantity's natural range.
    breaker_round_histogram: Mutex<BreakerHistogram<6>>,
    breaker_tokens_histogram: Mutex<BreakerHistogram<6>>,
    breaker_wall_clock_histogram: Mutex<BreakerHistogram<7>>,
}

/// F65 PR-3: per-kind breaker-trip distribution sample. Distinct from
/// `HistogramSample` because breakers use widely different natural
/// ranges (iterations vs tokens vs ms) — reusing the 10-bucket
/// `HistogramSample` would force all three to share one bucket layout,
/// which operator decision 3 explicitly rejected.
#[derive(Clone, Debug)]
pub(crate) struct BreakerHistogram<const N: usize> {
    pub(crate) bucket_counts: [u64; N],
    pub(crate) sum: u64,
    pub(crate) count: u64,
}

impl<const N: usize> Default for BreakerHistogram<N> {
    fn default() -> Self {
        Self {
            bucket_counts: [0; N],
            sum: 0,
            count: 0,
        }
    }
}

/// F65 PR-3: bucket edges for the Round-breaker measured-at-trip
/// histogram. Values past the last edge roll into the `+Inf` bucket.
/// Tuned for the 1..=50 iteration range; round cap default is 30.
pub(crate) const BREAKER_ROUND_BUCKETS: [u64; 6] = [1, 5, 10, 20, 30, 50];
/// F65 PR-3: bucket edges for the Tokens-breaker measured-at-trip
/// histogram. Tuned to the 10k..=500k range; token cap default 200k.
pub(crate) const BREAKER_TOKENS_BUCKETS: [u64; 6] =
    [10_000, 50_000, 100_000, 200_000, 300_000, 500_000];
/// F65 PR-3: bucket edges for the WallClock-breaker measured-at-trip
/// histogram (milliseconds). Tuned for 30s..=30min; wall-clock default
/// 15 minutes.
pub(crate) const BREAKER_WALL_CLOCK_BUCKETS: [u64; 7] = [
    30_000, 60_000, 120_000, 300_000, 600_000, 900_000, 1_800_000,
];

/// Per-tenant gauge bundle. Held behind a single mutex so updates
/// are atomic per tenant and reader-side iteration doesn't need to
/// cross-reference three maps.
#[cfg(feature = "metrics-core")]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TenantQueueDepth {
    pub(crate) active_runs: u64,
    pub(crate) active_tasks: u64,
    pub(crate) pending_approvals: u64,
}

// ── metrics-providers types ─────────────────────────────────────

/// Histogram buckets tuned for LLM call latency. Spans from a
/// fast-path embedding (~100 ms) to a long generate on a big model
/// with heavy context (~2 minutes). Adding more buckets hurts
/// scrape size without helping dashboards.
#[cfg(feature = "metrics-providers")]
pub(crate) const PROVIDER_DURATION_BUCKETS_MS: [u64; 10] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000, 60_000, 120_000,
];

// Belt-and-suspenders: `HistogramSample::bucket_counts` is sized to
// `HTTP_DURATION_BUCKETS_MS.len()`. The provider path reuses the same
// type but indexes with `PROVIDER_DURATION_BUCKETS_MS`; if someone
// grows one array without the other, the loop in
// `record_provider_call` panics at runtime. This static assert turns
// that into a compile error.
#[cfg(feature = "metrics-providers")]
const _: () = assert!(
    PROVIDER_DURATION_BUCKETS_MS.len() == HTTP_DURATION_BUCKETS_MS.len(),
    "provider histogram reuses HistogramSample's fixed-size bucket_counts array — \
     keep PROVIDER_DURATION_BUCKETS_MS and HTTP_DURATION_BUCKETS_MS the same length",
);

#[cfg(feature = "metrics-providers")]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ProviderCallKey {
    pub(crate) provider_connection: String,
    pub(crate) model: String,
    pub(crate) operation_kind: String,
    /// `succeeded` / `failed` / `cancelled`.
    pub(crate) status: String,
}

#[cfg(feature = "metrics-providers")]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ProviderCallDurationKey {
    pub(crate) provider_connection: String,
    pub(crate) model: String,
    pub(crate) operation_kind: String,
}

#[cfg(feature = "metrics-providers")]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ProviderTokenKey {
    pub(crate) provider_connection: String,
    pub(crate) model: String,
    /// `input` or `output`.
    pub(crate) kind: String,
}

/// F65 PR-3: map a `BreakerKind` to its snake_case label used both as
/// the Prometheus `kind` label and the map-key. Stable across versions —
/// dashboards pin on these strings.
fn breaker_kind_label(which: cairn_domain::session_orchestration::BreakerKind) -> &'static str {
    use cairn_domain::session_orchestration::BreakerKind;
    match which {
        BreakerKind::Round => "round",
        BreakerKind::Tokens => "tokens",
        BreakerKind::NoToolUseConsecutive => "no_tool_use_consecutive",
        BreakerKind::WallClock => "wall_clock",
    }
}

/// F65 PR-3: record a single circuit-breaker trip. Increments the
/// per-kind counter and, for Round / Tokens / WallClock, adds a
/// measured-at-trip histogram observation. NoToolUseConsecutive is
/// excluded from the histogram (it always trips at exactly the cap).
pub(crate) fn record_breaker_trip(
    metrics: &AppMetrics,
    which: cairn_domain::session_orchestration::BreakerKind,
    measured: u64,
) {
    use cairn_domain::session_orchestration::BreakerKind;
    let label = breaker_kind_label(which);
    {
        let mut m = metrics
            .breaker_trips
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *m.entry(label.to_owned()).or_insert(0) += 1;
    }
    match which {
        BreakerKind::Round => {
            let mut h = metrics
                .breaker_round_histogram
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            observe_histogram(&mut h, &BREAKER_ROUND_BUCKETS, measured);
        }
        BreakerKind::Tokens => {
            let mut h = metrics
                .breaker_tokens_histogram
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            observe_histogram(&mut h, &BREAKER_TOKENS_BUCKETS, measured);
        }
        BreakerKind::WallClock => {
            let mut h = metrics
                .breaker_wall_clock_histogram
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            observe_histogram(&mut h, &BREAKER_WALL_CLOCK_BUCKETS, measured);
        }
        BreakerKind::NoToolUseConsecutive => {
            // Counter above already recorded; no histogram by design.
        }
    }
}

/// F65 PR-3: record a 80% warning threshold crossing.
pub(crate) fn record_breaker_threshold_warn(
    metrics: &AppMetrics,
    which: cairn_domain::session_orchestration::BreakerKind,
) {
    let label = breaker_kind_label(which);
    let mut m = metrics
        .breaker_threshold_warns
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *m.entry(label.to_owned()).or_insert(0) += 1;
}

fn observe_histogram<const N: usize>(
    h: &mut BreakerHistogram<N>,
    buckets: &[u64; N],
    measured: u64,
) {
    h.count = h.count.saturating_add(1);
    h.sum = h.sum.saturating_add(measured);
    for (idx, edge) in buckets.iter().enumerate() {
        if measured <= *edge {
            h.bucket_counts[idx] = h.bucket_counts[idx].saturating_add(1);
            return;
        }
    }
    // `+Inf` bucket is implicit via `count - sum_of_buckets`; nothing
    // to do here when the observation overflows the last edge.
}

/// F65 PR-3: render a per-kind breaker histogram in the Prometheus
/// cumulative-bucket format (`le=` labels, monotonic counts, trailing
/// `+Inf` / `_sum` / `_count`).
fn render_breaker_histogram<const N: usize>(
    lines: &mut Vec<String>,
    metric_name: &str,
    help: &str,
    h: &BreakerHistogram<N>,
    buckets: &[u64; N],
) {
    lines.push(format!("# HELP {metric_name} {help}"));
    lines.push(format!("# TYPE {metric_name} histogram"));
    let mut cumulative: u64 = 0;
    for (idx, edge) in buckets.iter().enumerate() {
        cumulative = cumulative.saturating_add(h.bucket_counts[idx]);
        lines.push(format!(
            "{metric_name}_bucket{{le=\"{edge}\"}} {cumulative}"
        ));
    }
    lines.push(format!("{metric_name}_bucket{{le=\"+Inf\"}} {}", h.count));
    lines.push(format!("{metric_name}_sum {}", h.sum));
    lines.push(format!("{metric_name}_count {}", h.count));
}

impl AppMetrics {
    pub(crate) fn mark_started(&self) {
        self.startup_complete.store(true, Ordering::Relaxed);
    }

    pub(crate) fn is_started(&self) -> bool {
        self.startup_complete.load(Ordering::Relaxed)
    }

    pub(crate) fn record_request(&self, method: &str, path: &str, status: u16, latency_ms: u64) {
        {
            let mut totals = self
                .request_totals
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let key = RequestCountKey {
                method: method.to_owned(),
                path: path.to_owned(),
                status,
            };
            *totals.entry(key).or_insert(0) += 1;
        }

        let mut durations = self
            .request_durations
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let sample = durations
            .entry(RequestDurationKey {
                method: method.to_owned(),
                path: path.to_owned(),
            })
            .or_default();
        sample.count += 1;
        sample.sum_ms = sample.sum_ms.saturating_add(latency_ms);
        for (idx, bucket) in HTTP_DURATION_BUCKETS_MS.iter().enumerate() {
            if latency_ms <= *bucket {
                sample.bucket_counts[idx] += 1;
            }
        }
    }

    pub(crate) fn set_active_counts(&self, runs: usize, tasks: usize) {
        self.active_runs_total.store(runs as u64, Ordering::Relaxed);
        self.active_tasks_total
            .store(tasks as u64, Ordering::Relaxed);
    }

    // ── metrics-core recorders ───────────────────────────────────

    #[cfg(feature = "metrics-core")]
    pub fn record_run_created(&self, tenant: &str, workspace: &str) {
        let mut map = self.runs_created.lock().unwrap_or_else(|e| e.into_inner());
        *map.entry(EntityCountKey {
            tenant: tenant.to_owned(),
            workspace: workspace.to_owned(),
            outcome: String::new(),
            failure_class: String::new(),
        })
        .or_insert(0) += 1;
    }

    #[cfg(feature = "metrics-core")]
    pub fn record_run_terminal(
        &self,
        tenant: &str,
        workspace: &str,
        outcome: &str,
        failure_class: Option<&str>,
    ) {
        let mut map = self.runs_terminal.lock().unwrap_or_else(|e| e.into_inner());
        *map.entry(EntityCountKey {
            tenant: tenant.to_owned(),
            workspace: workspace.to_owned(),
            outcome: outcome.to_owned(),
            failure_class: failure_class.unwrap_or("").to_owned(),
        })
        .or_insert(0) += 1;
    }

    #[cfg(feature = "metrics-core")]
    pub fn record_task_created(&self, tenant: &str, workspace: &str) {
        let mut map = self.tasks_created.lock().unwrap_or_else(|e| e.into_inner());
        *map.entry(EntityCountKey {
            tenant: tenant.to_owned(),
            workspace: workspace.to_owned(),
            outcome: String::new(),
            failure_class: String::new(),
        })
        .or_insert(0) += 1;
    }

    #[cfg(feature = "metrics-core")]
    pub fn record_task_terminal(
        &self,
        tenant: &str,
        workspace: &str,
        outcome: &str,
        failure_class: Option<&str>,
    ) {
        let mut map = self
            .tasks_terminal
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *map.entry(EntityCountKey {
            tenant: tenant.to_owned(),
            workspace: workspace.to_owned(),
            outcome: outcome.to_owned(),
            failure_class: failure_class.unwrap_or("").to_owned(),
        })
        .or_insert(0) += 1;
    }

    #[cfg(feature = "metrics-core")]
    pub fn record_tool_invocation(&self, tool: &str, outcome: &str) {
        let mut map = self
            .tool_invocations
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *map.entry(ToolInvocationKey {
            tool: tool.to_owned(),
            outcome: outcome.to_owned(),
        })
        .or_insert(0) += 1;
    }

    /// Called by [`crate::lease_history_subscriber`] on each `expired`
    /// frame it processes. `entity` is `"task"` or `"run"`.
    #[cfg(feature = "metrics-core")]
    pub fn record_lease_expiry(&self, entity: &str) {
        let mut map = self
            .lease_expiries
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *map.entry(LeaseExpiryKey {
            entity: entity.to_owned(),
        })
        .or_insert(0) += 1;
    }

    /// Set the event_log head position minus the last-projected
    /// position. A persistently-high value means the projection is
    /// behind the event log — read-model queries will return stale
    /// data.
    #[cfg(feature = "metrics-core")]
    pub fn set_projection_lag(&self, lag_events: u64) {
        self.projection_lag_events
            .store(lag_events, Ordering::Relaxed);
    }

    #[cfg(feature = "metrics-core")]
    pub fn set_tenant_queue_depth(
        &self,
        tenant: &str,
        active_runs: u64,
        active_tasks: u64,
        pending_approvals: u64,
    ) {
        self.tenant_queue_depth
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                tenant.to_owned(),
                TenantQueueDepth {
                    active_runs,
                    active_tasks,
                    pending_approvals,
                },
            );
    }

    /// Prune tenant-queue-depth entries not present in `keep`. Called
    /// by the scrape-time refresh so tenants that have been deleted
    /// (or paged out past the enumeration window) stop appearing in
    /// Prometheus output as phantom series.
    #[cfg(feature = "metrics-core")]
    pub fn retain_tenant_queue_depth(&self, keep: &[String]) {
        let keep_set: std::collections::HashSet<&str> = keep.iter().map(String::as_str).collect();
        self.tenant_queue_depth
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|k, _| keep_set.contains(k.as_str()));
    }

    // ── metrics-providers recorders ─────────────────────────────

    /// Record a completed provider call. `latency_ms=None` skips the
    /// histogram (some provider errors surface before the call lands,
    /// e.g. admission-denial at the router — no latency to report).
    /// `input_tokens` / `output_tokens` are also optional because
    /// failed calls typically don't report usage.
    #[cfg(feature = "metrics-providers")]
    #[allow(clippy::too_many_arguments)]
    pub fn record_provider_call(
        &self,
        provider_connection: &str,
        model: &str,
        operation_kind: &str,
        status: &str,
        latency_ms: Option<u64>,
        input_tokens: Option<u32>,
        output_tokens: Option<u32>,
    ) {
        // Counter — build the key before taking the lock so allocations
        // don't happen under contention.
        let call_key = ProviderCallKey {
            provider_connection: provider_connection.to_owned(),
            model: model.to_owned(),
            operation_kind: operation_kind.to_owned(),
            status: status.to_owned(),
        };
        {
            let mut calls = self
                .provider_calls
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *calls.entry(call_key).or_insert(0) += 1;
        }

        // Duration histogram (only if the call actually returned).
        if let Some(latency_ms) = latency_ms {
            let duration_key = ProviderCallDurationKey {
                provider_connection: provider_connection.to_owned(),
                model: model.to_owned(),
                operation_kind: operation_kind.to_owned(),
            };
            let mut durations = self
                .provider_call_durations
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            // HistogramSample::default() gives us the zeroed
            // bucket_counts array; the static_assert above guarantees
            // its fixed length matches PROVIDER_DURATION_BUCKETS_MS.
            let sample = durations.entry(duration_key).or_default();
            sample.count += 1;
            sample.sum_ms = sample.sum_ms.saturating_add(latency_ms);
            for (idx, bucket) in PROVIDER_DURATION_BUCKETS_MS.iter().enumerate() {
                if latency_ms <= *bucket {
                    sample.bucket_counts[idx] += 1;
                }
            }
        }

        // Token counters. Skip the lock entirely when neither side of
        // the token pair is present; build each key outside the lock
        // so cloning happens before contention. When both are present
        // we pay two key allocations — unavoidable given they differ
        // in the `kind` label, which is part of the hash.
        if input_tokens.is_none() && output_tokens.is_none() {
            return;
        }
        let input_key = input_tokens.map(|_| ProviderTokenKey {
            provider_connection: provider_connection.to_owned(),
            model: model.to_owned(),
            kind: "input".to_owned(),
        });
        let output_key = output_tokens.map(|_| ProviderTokenKey {
            provider_connection: provider_connection.to_owned(),
            model: model.to_owned(),
            kind: "output".to_owned(),
        });
        let mut tokens = self
            .provider_tokens
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let (Some(key), Some(n)) = (input_key, input_tokens) {
            *tokens.entry(key).or_insert(0) += u64::from(n);
        }
        if let (Some(key), Some(n)) = (output_key, output_tokens) {
            *tokens.entry(key).or_insert(0) += u64::from(n);
        }
    }

    /// Total number of HTTP requests observed (across all method/path/status
    /// combinations). Exposed for binary-level Prometheus rendering.
    pub fn http_total_requests(&self) -> u64 {
        self.request_totals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .sum()
    }

    /// Errors-by-status map (status >= 400). Exposed for binary-level
    /// Prometheus rendering.
    pub fn http_errors_by_status(&self) -> HashMap<u16, u64> {
        let mut out: HashMap<u16, u64> = HashMap::new();
        for (key, count) in self
            .request_totals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
        {
            if key.status >= 400 {
                *out.entry(key.status).or_insert(0) += *count;
            }
        }
        out
    }

    /// Requests-by-path map (summed across methods and statuses). Exposed
    /// for binary-level Prometheus rendering.
    pub fn http_requests_by_path(&self) -> HashMap<String, u64> {
        let mut out: HashMap<String, u64> = HashMap::new();
        for (key, count) in self
            .request_totals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
        {
            *out.entry(key.path.clone()).or_insert(0) += *count;
        }
        out
    }

    /// Average request latency in milliseconds, across all samples.
    /// Returns 0 when no samples have been recorded.
    pub fn http_avg_latency_ms(&self) -> u64 {
        let durations = self
            .request_durations
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let (mut total_sum, mut total_count) = (0u64, 0u64);
        for sample in durations.values() {
            total_sum = total_sum.saturating_add(sample.sum_ms);
            total_count = total_count.saturating_add(sample.count);
        }
        total_sum.checked_div(total_count).unwrap_or(0)
    }

    /// Public wrapper around the histogram-bucket percentile. Returns 0
    /// when no samples have been recorded (matches the Prometheus-friendly
    /// semantic for empty histograms).
    pub fn http_latency_percentile(&self, p: f64) -> u64 {
        self.latency_percentile(p).unwrap_or(0)
    }

    /// Public wrapper around `error_rate` as an f64 in 0.0–1.0.
    pub fn http_error_rate(&self) -> f64 {
        f64::from(self.error_rate())
    }

    /// Approximate latency percentile (p50 or p95) from histogram buckets.
    /// Returns `None` when no requests have been recorded.
    pub(crate) fn latency_percentile(&self, p: f64) -> Option<u64> {
        let durations = self
            .request_durations
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut total_count: u64 = 0;
        let mut merged = [0u64; HTTP_DURATION_BUCKETS_MS.len()];
        for sample in durations.values() {
            total_count += sample.count;
            for (i, &c) in sample.bucket_counts.iter().enumerate() {
                merged[i] += c;
            }
        }
        if total_count == 0 {
            return None;
        }
        let target = ((p / 100.0) * total_count as f64).ceil() as u64;
        let mut cumulative = 0u64;
        for (i, &c) in merged.iter().enumerate() {
            cumulative += c;
            if cumulative >= target {
                return Some(HTTP_DURATION_BUCKETS_MS[i]);
            }
        }
        Some(*HTTP_DURATION_BUCKETS_MS.last().unwrap())
    }

    /// Fraction of requests with status >= 400 (0.0–1.0).
    pub(crate) fn error_rate(&self) -> f32 {
        let totals = self
            .request_totals
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut total: u64 = 0;
        let mut errors: u64 = 0;
        for (key, &count) in totals.iter() {
            total += count;
            if key.status >= 400 {
                errors += count;
            }
        }
        if total == 0 {
            0.0
        } else {
            errors as f32 / total as f32
        }
    }

    pub fn render_prometheus(&self) -> String {
        let totals = self
            .request_totals
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let durations = self
            .request_durations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        let mut lines = vec![
            "# HELP http_requests_total Total HTTP responses by method, path, and status."
                .to_owned(),
            "# TYPE http_requests_total counter".to_owned(),
        ];
        for (key, value) in totals {
            lines.push(format!(
                "http_requests_total{{method=\"{}\",path=\"{}\",status=\"{}\"}} {}",
                prometheus_label(&key.method),
                prometheus_label(&key.path),
                key.status,
                value
            ));
        }

        lines.push(
            "# HELP http_request_duration_ms Request duration histogram in milliseconds."
                .to_owned(),
        );
        lines.push("# TYPE http_request_duration_ms histogram".to_owned());
        for (key, value) in durations {
            for (idx, bucket) in HTTP_DURATION_BUCKETS_MS.iter().enumerate() {
                lines.push(format!(
                    "http_request_duration_ms_bucket{{method=\"{}\",path=\"{}\",le=\"{}\"}} {}",
                    prometheus_label(&key.method),
                    prometheus_label(&key.path),
                    bucket,
                    value.bucket_counts[idx]
                ));
            }
            lines.push(format!(
                "http_request_duration_ms_bucket{{method=\"{}\",path=\"{}\",le=\"+Inf\"}} {}",
                prometheus_label(&key.method),
                prometheus_label(&key.path),
                value.count
            ));
            lines.push(format!(
                "http_request_duration_ms_sum{{method=\"{}\",path=\"{}\"}} {}",
                prometheus_label(&key.method),
                prometheus_label(&key.path),
                value.sum_ms
            ));
            lines.push(format!(
                "http_request_duration_ms_count{{method=\"{}\",path=\"{}\"}} {}",
                prometheus_label(&key.method),
                prometheus_label(&key.path),
                value.count
            ));
        }

        lines.push("# HELP active_runs_total Active non-terminal runs.".to_owned());
        lines.push("# TYPE active_runs_total gauge".to_owned());
        lines.push(format!(
            "active_runs_total {}",
            self.active_runs_total.load(Ordering::Relaxed)
        ));
        lines.push("# HELP active_tasks_total Active non-terminal tasks.".to_owned());
        lines.push("# TYPE active_tasks_total gauge".to_owned());
        lines.push(format!(
            "active_tasks_total {}",
            self.active_tasks_total.load(Ordering::Relaxed)
        ));

        #[cfg(feature = "metrics-core")]
        self.render_core_into(&mut lines);

        #[cfg(feature = "metrics-providers")]
        self.render_providers_into(&mut lines);

        // F65 PR-3: orchestrator circuit-breaker metrics. Always-on —
        // breakers are core orchestrator policy, not an optional
        // feature.
        self.render_breakers_into(&mut lines);

        lines.join("\n")
    }

    /// F65 PR-3: render the breaker counters + per-kind histograms.
    fn render_breakers_into(&self, lines: &mut Vec<String>) {
        // ── cairn_orchestrator_breaker_trips_total{kind}  (counter) ──
        lines.push(
            "# HELP cairn_orchestrator_breaker_trips_total Total circuit-breaker trips per kind."
                .to_owned(),
        );
        lines.push("# TYPE cairn_orchestrator_breaker_trips_total counter".to_owned());
        {
            let snapshot = self
                .breaker_trips
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            let mut rows: Vec<_> = snapshot.into_iter().collect();
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            for (kind, count) in rows {
                lines.push(format!(
                    "cairn_orchestrator_breaker_trips_total{{kind=\"{kind}\"}} {count}"
                ));
            }
        }

        // ── cairn_orchestrator_breaker_threshold_warns_total{kind}  (counter) ──
        lines.push(
            "# HELP cairn_orchestrator_breaker_threshold_warns_total Total 80% breaker-threshold warnings per kind (Round/Tokens/WallClock only)."
                .to_owned(),
        );
        lines.push("# TYPE cairn_orchestrator_breaker_threshold_warns_total counter".to_owned());
        {
            let snapshot = self
                .breaker_threshold_warns
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            let mut rows: Vec<_> = snapshot.into_iter().collect();
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            for (kind, count) in rows {
                lines.push(format!(
                    "cairn_orchestrator_breaker_threshold_warns_total{{kind=\"{kind}\"}} {count}"
                ));
            }
        }

        // ── round histogram ──
        render_breaker_histogram(
            lines,
            "cairn_orchestrator_breaker_round_measured_at_trip",
            "Distribution of iterations observed at Round-breaker trip time.",
            &self
                .breaker_round_histogram
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            &BREAKER_ROUND_BUCKETS,
        );

        // ── tokens histogram ──
        render_breaker_histogram(
            lines,
            "cairn_orchestrator_breaker_tokens_measured_at_trip",
            "Distribution of cumulative tokens observed at Tokens-breaker trip time.",
            &self
                .breaker_tokens_histogram
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            &BREAKER_TOKENS_BUCKETS,
        );

        // ── wall-clock histogram ──
        render_breaker_histogram(
            lines,
            "cairn_orchestrator_breaker_wall_clock_measured_at_trip_ms",
            "Distribution of elapsed milliseconds observed at WallClock-breaker trip time.",
            &self
                .breaker_wall_clock_histogram
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            &BREAKER_WALL_CLOCK_BUCKETS,
        );
    }

    #[cfg(feature = "metrics-core")]
    fn render_core_into(&self, lines: &mut Vec<String>) {
        // Sort rows by label-tuple before emitting so the Prometheus
        // output is byte-stable across scrapes. HashMap iteration is
        // nondeterministic; stable output keeps log analysis + golden
        // test assertions sane.
        fn render_entity_counter(
            lines: &mut Vec<String>,
            name: &str,
            help: &str,
            data: &HashMap<EntityCountKey, u64>,
            with_outcome: bool,
        ) {
            lines.push(format!("# HELP {name} {help}"));
            lines.push(format!("# TYPE {name} counter"));
            let mut entries: Vec<(&EntityCountKey, &u64)> = data.iter().collect();
            entries.sort_by(|a, b| {
                (
                    &a.0.tenant,
                    &a.0.workspace,
                    &a.0.outcome,
                    &a.0.failure_class,
                )
                    .cmp(&(
                        &b.0.tenant,
                        &b.0.workspace,
                        &b.0.outcome,
                        &b.0.failure_class,
                    ))
            });
            for (key, value) in entries {
                let mut labels = format!(
                    "tenant=\"{}\",workspace=\"{}\"",
                    prometheus_label(&key.tenant),
                    prometheus_label(&key.workspace),
                );
                if with_outcome {
                    labels.push_str(&format!(
                        ",outcome=\"{}\",failure_class=\"{}\"",
                        prometheus_label(&key.outcome),
                        prometheus_label(&key.failure_class),
                    ));
                }
                lines.push(format!("{name}{{{labels}}} {value}"));
            }
        }

        let runs_created = self
            .runs_created
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        render_entity_counter(
            lines,
            "cairn_runs_created_total",
            "Runs created, labelled by tenant + workspace.",
            &runs_created,
            false,
        );

        let runs_terminal = self
            .runs_terminal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        render_entity_counter(
            lines,
            "cairn_runs_terminal_total",
            "Runs reaching a terminal state (completed/failed/cancelled).",
            &runs_terminal,
            true,
        );

        let tasks_created = self
            .tasks_created
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        render_entity_counter(
            lines,
            "cairn_tasks_created_total",
            "Tasks created, labelled by tenant + workspace.",
            &tasks_created,
            false,
        );

        let tasks_terminal = self
            .tasks_terminal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        render_entity_counter(
            lines,
            "cairn_tasks_terminal_total",
            "Tasks reaching a terminal state (completed/failed/cancelled).",
            &tasks_terminal,
            true,
        );

        let tool_invocations = self
            .tool_invocations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        lines.push(
            "# HELP cairn_tool_invocations_total Tool invocations by name and outcome.".to_owned(),
        );
        lines.push("# TYPE cairn_tool_invocations_total counter".to_owned());
        let mut tool_entries: Vec<_> = tool_invocations.iter().collect();
        tool_entries.sort_by(|a, b| (&a.0.tool, &a.0.outcome).cmp(&(&b.0.tool, &b.0.outcome)));
        for (key, value) in tool_entries {
            lines.push(format!(
                "cairn_tool_invocations_total{{tool=\"{}\",outcome=\"{}\"}} {}",
                prometheus_label(&key.tool),
                prometheus_label(&key.outcome),
                value,
            ));
        }

        let lease_expiries = self
            .lease_expiries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        lines.push(
            "# HELP cairn_lease_expiries_total \
                FF-initiated lease expiries detected by the subscriber, by entity (task/run)."
                .to_owned(),
        );
        lines.push("# TYPE cairn_lease_expiries_total counter".to_owned());
        let mut expiry_entries: Vec<_> = lease_expiries.iter().collect();
        expiry_entries.sort_by(|a, b| a.0.entity.cmp(&b.0.entity));
        for (key, value) in expiry_entries {
            lines.push(format!(
                "cairn_lease_expiries_total{{entity=\"{}\"}} {}",
                prometheus_label(&key.entity),
                value,
            ));
        }

        lines.push(
            "# HELP cairn_projection_lag_events \
                event_log head position minus last-projected position. \
                Persistently > 0 means read-model is behind the log."
                .to_owned(),
        );
        lines.push("# TYPE cairn_projection_lag_events gauge".to_owned());
        lines.push(format!(
            "cairn_projection_lag_events {}",
            self.projection_lag_events.load(Ordering::Relaxed),
        ));

        let tenant_queue_depth = self
            .tenant_queue_depth
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let tenant_entries = cap_tenant_entries(tenant_queue_depth.into_iter().collect());

        lines.push(
            "# HELP cairn_active_runs_by_tenant Active non-terminal runs per tenant. \
             Capped at the top TENANT_METRIC_TOP_N tenants by activity; overflow \
             aggregates into tenant=\"__other__\" (#502)."
                .to_owned(),
        );
        lines.push("# TYPE cairn_active_runs_by_tenant gauge".to_owned());
        for (tenant, depth) in &tenant_entries {
            lines.push(format!(
                "cairn_active_runs_by_tenant{{tenant=\"{}\"}} {}",
                prometheus_label(tenant),
                depth.active_runs,
            ));
        }

        lines.push(
            "# HELP cairn_active_tasks_by_tenant Active non-terminal tasks per tenant. \
             Capped at the top TENANT_METRIC_TOP_N tenants; overflow aggregates \
             into tenant=\"__other__\"."
                .to_owned(),
        );
        lines.push("# TYPE cairn_active_tasks_by_tenant gauge".to_owned());
        for (tenant, depth) in &tenant_entries {
            lines.push(format!(
                "cairn_active_tasks_by_tenant{{tenant=\"{}\"}} {}",
                prometheus_label(tenant),
                depth.active_tasks,
            ));
        }

        lines.push(
            "# HELP cairn_pending_approvals_by_tenant Pending approvals awaiting decision per \
             tenant. Capped at the top TENANT_METRIC_TOP_N tenants; overflow aggregates into \
             tenant=\"__other__\"."
                .to_owned(),
        );
        lines.push("# TYPE cairn_pending_approvals_by_tenant gauge".to_owned());
        for (tenant, depth) in &tenant_entries {
            lines.push(format!(
                "cairn_pending_approvals_by_tenant{{tenant=\"{}\"}} {}",
                prometheus_label(tenant),
                depth.pending_approvals,
            ));
        }
    }

    #[cfg(feature = "metrics-providers")]
    fn render_providers_into(&self, lines: &mut Vec<String>) {
        // ── #503 cardinality cap ─────────────────────────────────────
        // `provider_connection` + `model` are operator-supplied and
        // unbounded; rotating model IDs or frequent connection swaps
        // accumulate one series each. Compute the top-N
        // (connection, model) pairs by total call count; any row
        // outside the top-N collapses into
        // `{provider_connection="__other__", model="__other__"}` at
        // render time. The in-memory counter maps stay unbounded so
        // `record_provider_call` never has to decide what to drop —
        // backpressure lives where it matters (Prometheus scrapes).
        let calls_raw = self
            .provider_calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let keep_pairs: std::collections::HashSet<(String, String)> =
            provider_top_n_pairs(&calls_raw);

        // Provider calls counter — aggregated with the cap applied.
        let calls = capped_calls(&calls_raw, &keep_pairs);
        lines.push(
            "# HELP cairn_provider_calls_total \
                LLM provider calls, labelled by provider family + model + operation + status. \
                (provider_connection, model) capped at PROVIDER_METRIC_TOP_N; overflow \
                aggregates into provider_connection=\"__other__\",model=\"__other__\" (#503)."
                .to_owned(),
        );
        lines.push("# TYPE cairn_provider_calls_total counter".to_owned());
        let mut call_entries: Vec<_> = calls.iter().collect();
        call_entries.sort_by(|a, b| {
            (
                &a.0.provider_connection,
                &a.0.model,
                &a.0.operation_kind,
                &a.0.status,
            )
                .cmp(&(
                    &b.0.provider_connection,
                    &b.0.model,
                    &b.0.operation_kind,
                    &b.0.status,
                ))
        });
        for (key, value) in call_entries {
            lines.push(format!(
                "cairn_provider_calls_total{{provider_connection=\"{}\",model=\"{}\",operation_kind=\"{}\",status=\"{}\"}} {}",
                prometheus_label(&key.provider_connection),
                prometheus_label(&key.model),
                prometheus_label(&key.operation_kind),
                prometheus_label(&key.status),
                value,
            ));
        }

        // Provider call duration histogram — same cap, but summing
        // histogram samples rather than counts.
        let durations_raw = self
            .provider_call_durations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let durations = capped_durations(&durations_raw, &keep_pairs);
        lines.push(
            "# HELP cairn_provider_call_duration_ms \
                LLM provider call wall-clock latency, labelled by provider family + model + operation. \
                (provider_connection, model) capped at PROVIDER_METRIC_TOP_N."
                .to_owned(),
        );
        lines.push("# TYPE cairn_provider_call_duration_ms histogram".to_owned());
        let mut dur_entries: Vec<_> = durations.iter().collect();
        dur_entries.sort_by(|a, b| {
            (&a.0.provider_connection, &a.0.model, &a.0.operation_kind).cmp(&(
                &b.0.provider_connection,
                &b.0.model,
                &b.0.operation_kind,
            ))
        });
        for (key, sample) in dur_entries {
            let labels = format!(
                "provider_connection=\"{}\",model=\"{}\",operation_kind=\"{}\"",
                prometheus_label(&key.provider_connection),
                prometheus_label(&key.model),
                prometheus_label(&key.operation_kind),
            );
            for (idx, bucket) in PROVIDER_DURATION_BUCKETS_MS.iter().enumerate() {
                lines.push(format!(
                    "cairn_provider_call_duration_ms_bucket{{{labels},le=\"{bucket}\"}} {}",
                    sample.bucket_counts[idx]
                ));
            }
            lines.push(format!(
                "cairn_provider_call_duration_ms_bucket{{{labels},le=\"+Inf\"}} {}",
                sample.count
            ));
            lines.push(format!(
                "cairn_provider_call_duration_ms_sum{{{labels}}} {}",
                sample.sum_ms
            ));
            lines.push(format!(
                "cairn_provider_call_duration_ms_count{{{labels}}} {}",
                sample.count
            ));
        }

        // Token counters — same cap.
        let tokens_raw = self
            .provider_tokens
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let tokens = capped_tokens(&tokens_raw, &keep_pairs);
        lines.push(
            "# HELP cairn_provider_tokens_total \
                Tokens billed by LLM providers, by family + model + kind (input/output). \
                (provider_connection, model) capped at PROVIDER_METRIC_TOP_N."
                .to_owned(),
        );
        lines.push("# TYPE cairn_provider_tokens_total counter".to_owned());
        let mut token_entries: Vec<_> = tokens.iter().collect();
        token_entries.sort_by(|a, b| {
            (&a.0.provider_connection, &a.0.model, &a.0.kind).cmp(&(
                &b.0.provider_connection,
                &b.0.model,
                &b.0.kind,
            ))
        });
        for (key, value) in token_entries {
            lines.push(format!(
                "cairn_provider_tokens_total{{provider_connection=\"{}\",model=\"{}\",kind=\"{}\"}} {}",
                prometheus_label(&key.provider_connection),
                prometheus_label(&key.model),
                prometheus_label(&key.kind),
                value,
            ));
        }
    }
}

fn prometheus_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Cap the number of emitted tenants at `TENANT_METRIC_TOP_N` (#502).
///
/// Ranks the input entries by total activity (runs + tasks + approvals)
/// descending, takes the top N, aggregates the remainder into a single
/// `__other__` row whose value is the sum across the evicted tenants.
/// The result is sorted by tenant name so the Prometheus exposition is
/// stable between scrapes.
///
/// When the input is already within the cap, returns the input sorted
/// by tenant (same shape as the old behaviour, no `__other__` row).
///
/// Declared outside `AppMetrics::render_*_into` so it's testable in
/// isolation — the render functions are too large to unit-test cleanly.
#[cfg(feature = "metrics-core")]
fn cap_tenant_entries(input: Vec<(String, TenantQueueDepth)>) -> Vec<(String, TenantQueueDepth)> {
    if input.len() <= TENANT_METRIC_TOP_N {
        let mut sorted = input;
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        return sorted;
    }

    // Rank by total activity (descending). Ties broken by name ascending
    // so the cap decision is deterministic across scrapes.
    let mut ranked = input;
    ranked.sort_by(|a, b| {
        let a_total = a.1.active_runs + a.1.active_tasks + a.1.pending_approvals;
        let b_total = b.1.active_runs + b.1.active_tasks + b.1.pending_approvals;
        b_total.cmp(&a_total).then_with(|| a.0.cmp(&b.0))
    });

    let (top, overflow) = ranked.split_at(TENANT_METRIC_TOP_N);
    let mut other = TenantQueueDepth::default();
    for (_, depth) in overflow {
        other.active_runs = other.active_runs.saturating_add(depth.active_runs);
        other.active_tasks = other.active_tasks.saturating_add(depth.active_tasks);
        other.pending_approvals = other
            .pending_approvals
            .saturating_add(depth.pending_approvals);
    }

    let mut out: Vec<(String, TenantQueueDepth)> = top.to_vec();
    out.push((CARDINALITY_OVERFLOW_LABEL.to_owned(), other));
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Compute the set of `(provider_connection, model)` pairs to retain at
/// render time for the provider metrics (#503).
///
/// Ranks by total call count across all `(operation_kind, status)`
/// combinations for each pair; ties broken by lexicographic order so
/// the cap decision is deterministic.
///
/// Returns the FULL set of distinct pairs when the input is already
/// within `PROVIDER_METRIC_TOP_N` (the "everything is kept" semantic
/// — callers treat membership in the returned set as "keep this pair
/// as-is"). When the input exceeds the cap, only the top-N pairs
/// appear in the set; everything outside folds into the `__other__`
/// overflow bucket at render time.
#[cfg(feature = "metrics-providers")]
fn provider_top_n_pairs(
    calls: &std::collections::HashMap<ProviderCallKey, u64>,
) -> std::collections::HashSet<(String, String)> {
    // Fold per-(connection, model) totals.
    let mut totals: std::collections::HashMap<(String, String), u64> =
        std::collections::HashMap::new();
    for (key, count) in calls {
        let pair = (key.provider_connection.clone(), key.model.clone());
        *totals.entry(pair).or_insert(0) += *count;
    }

    if totals.len() <= PROVIDER_METRIC_TOP_N {
        // No cap needed — return the full set.
        return totals.into_keys().collect();
    }

    // Rank and keep top-N.
    let mut ranked: Vec<((String, String), u64)> = totals.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    ranked
        .into_iter()
        .take(PROVIDER_METRIC_TOP_N)
        .map(|(pair, _)| pair)
        .collect()
}

/// Apply the top-N `(provider_connection, model)` cap to the provider-
/// calls counter map. Rows whose `(provider_connection, model)` pair is
/// in `keep_pairs` pass through unchanged; rows outside fold into an
/// `__other__` overflow bucket (preserving `operation_kind` + `status`
/// — those are bounded enums and stay as distinct rows under the fold).
///
/// Callers typically obtain `keep_pairs` from
/// [`provider_top_n_pairs`], which returns the FULL set when below the
/// cap — so "nothing to fold" is expressed by a keep-set that covers
/// every pair, not by an empty keep-set. An empty `keep_pairs` is
/// therefore an instruction to fold EVERYTHING into `__other__` (used
/// only by tests that want to exercise the fold in isolation).
#[cfg(feature = "metrics-providers")]
fn capped_calls(
    calls: &std::collections::HashMap<ProviderCallKey, u64>,
    keep_pairs: &std::collections::HashSet<(String, String)>,
) -> std::collections::HashMap<ProviderCallKey, u64> {
    // Single linear pass: each input row is either kept as-is (its
    // pair is in `keep_pairs`) or folded into the overflow bucket.
    // When `keep_pairs` covers every pair (the below-cap case), all
    // rows are kept as-is and the `HashMap` has the same shape as
    // the input.
    let mut out: std::collections::HashMap<ProviderCallKey, u64> =
        std::collections::HashMap::with_capacity(calls.len());
    for (key, count) in calls {
        let pair = (key.provider_connection.clone(), key.model.clone());
        if keep_pairs.contains(&pair) {
            out.insert(key.clone(), *count);
        } else {
            let overflow_key = ProviderCallKey {
                provider_connection: CARDINALITY_OVERFLOW_LABEL.to_owned(),
                model: CARDINALITY_OVERFLOW_LABEL.to_owned(),
                operation_kind: key.operation_kind.clone(),
                status: key.status.clone(),
            };
            *out.entry(overflow_key).or_insert(0) += *count;
        }
    }
    out
}

/// Apply the same cap to the duration-histogram map. Histogram samples
/// fold by element-wise bucket addition; `sum_ms` and `count` sum
/// normally.
#[cfg(feature = "metrics-providers")]
fn capped_durations(
    durations: &std::collections::HashMap<ProviderCallDurationKey, HistogramSample>,
    keep_pairs: &std::collections::HashSet<(String, String)>,
) -> std::collections::HashMap<ProviderCallDurationKey, HistogramSample> {
    let mut out: std::collections::HashMap<ProviderCallDurationKey, HistogramSample> =
        std::collections::HashMap::with_capacity(durations.len());
    for (key, sample) in durations {
        let pair = (key.provider_connection.clone(), key.model.clone());
        let target_key = if keep_pairs.contains(&pair) {
            key.clone()
        } else {
            ProviderCallDurationKey {
                provider_connection: CARDINALITY_OVERFLOW_LABEL.to_owned(),
                model: CARDINALITY_OVERFLOW_LABEL.to_owned(),
                operation_kind: key.operation_kind.clone(),
            }
        };
        let agg = out.entry(target_key).or_default();
        for (idx, b) in sample.bucket_counts.iter().enumerate() {
            agg.bucket_counts[idx] = agg.bucket_counts[idx].saturating_add(*b);
        }
        agg.sum_ms = agg.sum_ms.saturating_add(sample.sum_ms);
        agg.count = agg.count.saturating_add(sample.count);
    }
    out
}

/// Apply the cap to the token-counter map.
#[cfg(feature = "metrics-providers")]
fn capped_tokens(
    tokens: &std::collections::HashMap<ProviderTokenKey, u64>,
    keep_pairs: &std::collections::HashSet<(String, String)>,
) -> std::collections::HashMap<ProviderTokenKey, u64> {
    let mut out: std::collections::HashMap<ProviderTokenKey, u64> =
        std::collections::HashMap::with_capacity(tokens.len());
    for (key, value) in tokens {
        let pair = (key.provider_connection.clone(), key.model.clone());
        let target_key = if keep_pairs.contains(&pair) {
            key.clone()
        } else {
            ProviderTokenKey {
                provider_connection: CARDINALITY_OVERFLOW_LABEL.to_owned(),
                model: CARDINALITY_OVERFLOW_LABEL.to_owned(),
                kind: key.kind.clone(),
            }
        };
        *out.entry(target_key).or_insert(0) += *value;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── #502 regression: tenant cardinality cap ───────────────────────

    #[cfg(feature = "metrics-core")]
    fn tenant_depth(runs: u64, tasks: u64, approvals: u64) -> TenantQueueDepth {
        TenantQueueDepth {
            active_runs: runs,
            active_tasks: tasks,
            pending_approvals: approvals,
        }
    }

    #[cfg(feature = "metrics-core")]
    #[test]
    fn cap_tenant_entries_passthrough_below_cap() {
        let input = vec![
            ("alpha".to_owned(), tenant_depth(1, 2, 3)),
            ("bravo".to_owned(), tenant_depth(4, 5, 6)),
        ];
        let out = cap_tenant_entries(input.clone());
        assert_eq!(out.len(), input.len());
        // Must be sorted by tenant name so Prometheus output is stable.
        assert_eq!(out[0].0, "alpha");
        assert_eq!(out[1].0, "bravo");
        // No __other__ row when below the cap.
        assert!(!out.iter().any(|(t, _)| t == CARDINALITY_OVERFLOW_LABEL));
    }

    #[cfg(feature = "metrics-core")]
    #[test]
    fn cap_tenant_entries_aggregates_overflow_into_other_bucket() {
        // Build TOP_N + 3 tenants. Top TOP_N should survive; 3 fold.
        let mut input: Vec<(String, TenantQueueDepth)> = Vec::new();
        // High-activity top tenants (score: 1000 each).
        for i in 0..TENANT_METRIC_TOP_N {
            input.push((format!("hi_{i:04}"), tenant_depth(500, 400, 100)));
        }
        // Low-activity overflow tenants.
        input.push(("lo_a".to_owned(), tenant_depth(1, 2, 3)));
        input.push(("lo_b".to_owned(), tenant_depth(4, 5, 6)));
        input.push(("lo_c".to_owned(), tenant_depth(7, 8, 9)));

        let out = cap_tenant_entries(input);
        // Top-N + one __other__ bucket.
        assert_eq!(out.len(), TENANT_METRIC_TOP_N + 1);
        let other = out
            .iter()
            .find(|(t, _)| t == CARDINALITY_OVERFLOW_LABEL)
            .expect("__other__ bucket must exist when overflow occurs");
        // __other__ = sum of lo_a/b/c.
        assert_eq!(other.1.active_runs, 1 + 4 + 7);
        assert_eq!(other.1.active_tasks, 2 + 5 + 8);
        assert_eq!(other.1.pending_approvals, 3 + 6 + 9);

        // Low-activity tenants must not appear (they were evicted into
        // __other__).
        assert!(!out.iter().any(|(t, _)| t.starts_with("lo_")));
    }

    #[cfg(feature = "metrics-core")]
    #[test]
    fn cap_tenant_entries_ranks_by_total_activity() {
        // Build TOP_N-1 zero-activity tenants + 2 high-activity tenants
        // + 2 medium-activity tenants. When we push past the cap, the
        // low-activity ones should fold.
        let mut input: Vec<(String, TenantQueueDepth)> = Vec::new();
        for i in 0..TENANT_METRIC_TOP_N - 1 {
            input.push((format!("zero_{i:04}"), tenant_depth(0, 0, 0)));
        }
        input.push(("alice".to_owned(), tenant_depth(1000, 0, 0)));
        input.push(("bob".to_owned(), tenant_depth(500, 500, 0)));
        input.push(("charlie".to_owned(), tenant_depth(10, 10, 10)));
        input.push(("dave".to_owned(), tenant_depth(5, 5, 5)));
        let out = cap_tenant_entries(input);
        assert_eq!(out.len(), TENANT_METRIC_TOP_N + 1);
        // Alice and Bob (high) must survive.
        assert!(out.iter().any(|(t, _)| t == "alice"));
        assert!(out.iter().any(|(t, _)| t == "bob"));
        // At least one zero_ should have been evicted — there's
        // TOP_N - 1 of them and only TOP_N survivor-slots after the
        // 4 named ones, so some must fold.
        let zero_survivors = out.iter().filter(|(t, _)| t.starts_with("zero_")).count();
        assert!(zero_survivors < TENANT_METRIC_TOP_N - 1);
    }

    // ── #503 regression: provider cardinality cap ────────────────────

    #[cfg(feature = "metrics-providers")]
    fn pc_key(conn: &str, model: &str, op: &str, status: &str) -> ProviderCallKey {
        ProviderCallKey {
            provider_connection: conn.to_owned(),
            model: model.to_owned(),
            operation_kind: op.to_owned(),
            status: status.to_owned(),
        }
    }

    #[cfg(feature = "metrics-providers")]
    #[test]
    fn provider_top_n_passthrough_below_cap() {
        let mut calls: std::collections::HashMap<ProviderCallKey, u64> =
            std::collections::HashMap::new();
        calls.insert(pc_key("openai", "gpt-4o", "chat", "succeeded"), 100);
        calls.insert(pc_key("anthropic", "sonnet", "chat", "succeeded"), 50);
        let keep = provider_top_n_pairs(&calls);
        // Every pair in a below-cap set is kept.
        assert_eq!(keep.len(), 2);
        assert!(keep.contains(&("openai".to_owned(), "gpt-4o".to_owned())));
        assert!(keep.contains(&("anthropic".to_owned(), "sonnet".to_owned())));
    }

    #[cfg(feature = "metrics-providers")]
    #[test]
    fn provider_top_n_ranks_and_caps_at_budget() {
        let mut calls: std::collections::HashMap<ProviderCallKey, u64> =
            std::collections::HashMap::new();
        // TOP_N + 3 distinct (connection, model) pairs.
        for i in 0..PROVIDER_METRIC_TOP_N {
            calls.insert(
                pc_key("openai", &format!("gpt-model-{i:04}"), "chat", "succeeded"),
                1000,
            );
        }
        // Low-activity overflow pairs (rotating model IDs exactly the
        // case the audit flagged).
        calls.insert(
            pc_key("openai", "gpt-4o-mini-2024-07-18", "chat", "succeeded"),
            1,
        );
        calls.insert(
            pc_key("openai", "gpt-4o-mini-2024-11-20", "chat", "succeeded"),
            2,
        );
        calls.insert(
            pc_key("openai", "gpt-4o-mini-2024-12-18", "chat", "succeeded"),
            3,
        );

        let keep = provider_top_n_pairs(&calls);
        assert_eq!(keep.len(), PROVIDER_METRIC_TOP_N);
        // Low-activity rotating-model-ID pairs must be evicted.
        assert!(
            !keep.contains(&("openai".to_owned(), "gpt-4o-mini-2024-07-18".to_owned())),
            "low-activity rotating model ID must not be in the top-N"
        );
    }

    #[cfg(feature = "metrics-providers")]
    #[test]
    fn capped_calls_folds_overflow_into_other_label_pair() {
        let mut calls: std::collections::HashMap<ProviderCallKey, u64> =
            std::collections::HashMap::new();
        calls.insert(pc_key("openai", "gpt-4o", "chat", "succeeded"), 100);
        calls.insert(pc_key("openai", "gpt-old-1", "chat", "succeeded"), 3);
        calls.insert(pc_key("openai", "gpt-old-2", "chat", "succeeded"), 2);
        calls.insert(pc_key("openai", "gpt-old-3", "chat", "succeeded"), 1);

        // Keep only the top pair; force overflow.
        let keep: std::collections::HashSet<(String, String)> =
            std::iter::once(("openai".to_owned(), "gpt-4o".to_owned())).collect();
        let out = capped_calls(&calls, &keep);

        // gpt-4o kept as-is.
        assert_eq!(
            out.get(&pc_key("openai", "gpt-4o", "chat", "succeeded")),
            Some(&100)
        );
        // Three gpt-old-* collapsed into __other__/__other__ with summed count.
        let other_key = pc_key(
            CARDINALITY_OVERFLOW_LABEL,
            CARDINALITY_OVERFLOW_LABEL,
            "chat",
            "succeeded",
        );
        assert_eq!(out.get(&other_key), Some(&(3 + 2 + 1)));
        // Exactly 2 distinct rows after folding.
        assert_eq!(out.len(), 2);
    }

    #[cfg(feature = "metrics-providers")]
    #[test]
    fn capped_calls_preserves_status_dimension() {
        // operation_kind + status are bounded enums — the cap must not
        // collapse them. Distinct `status` values for the same evicted
        // pair should remain distinct rows under __other__.
        let mut calls: std::collections::HashMap<ProviderCallKey, u64> =
            std::collections::HashMap::new();
        calls.insert(pc_key("openai", "gpt-a", "chat", "succeeded"), 100);
        calls.insert(pc_key("openai", "gpt-a", "chat", "failed"), 10);
        let keep: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
        let out = capped_calls(&calls, &keep);
        // Both collapsed rows should appear — same __other__/__other__
        // on the (conn, model) axis but distinct on status.
        let succeeded = pc_key(
            CARDINALITY_OVERFLOW_LABEL,
            CARDINALITY_OVERFLOW_LABEL,
            "chat",
            "succeeded",
        );
        let failed = pc_key(
            CARDINALITY_OVERFLOW_LABEL,
            CARDINALITY_OVERFLOW_LABEL,
            "chat",
            "failed",
        );
        assert_eq!(out.get(&succeeded), Some(&100));
        assert_eq!(out.get(&failed), Some(&10));
        assert_eq!(out.len(), 2);
    }
}
