//! Orchestrator event-emitter adapters.
//!
//! Two structs live here, both instantiated once per orchestrate invocation
//! in [`super::orchestrate::orchestrate_run_handler_inner`]:
//!
//! - [`SseSink`] — adapts the runtime SSE broadcast channel to the
//!   `NotificationSink` trait consumed by `NotifyOperatorTool`.
//! - [`TracingEmitter`] — composite emitter that forwards every
//!   orchestrator event to the SSE emitter AND records decide-phase
//!   traces + persists circuit-breaker / budget-threshold events to the
//!   durable event log + increments metrics sinks.
//!
//! Extracted so the 1.8K-line `orchestrate.rs` stays under the
//! 1200-LOC-per-file target without perturbing the inner handler's
//! hot-path setup code.

use cairn_tools::NotificationSink;

/// Adapts the shared runtime SSE broadcast channel to the
/// `NotificationSink` trait `NotifyOperatorTool` expects.
///
/// Forwards each `emit(channel, severity, message)` call as an
/// `OperatorNotification` SSE frame tagged with a monotonic `seq` so
/// the replay buffer can dedupe on reconnect.
pub(super) struct SseSink {
    pub(super) tx: tokio::sync::broadcast::Sender<cairn_api::sse::SseFrame>,
    pub(super) seq: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub(super) buf: std::sync::Arc<
        std::sync::RwLock<std::collections::VecDeque<(u64, cairn_api::sse::SseFrame)>>,
    >,
}

#[async_trait::async_trait]
impl NotificationSink for SseSink {
    async fn emit(&self, channel: &str, severity: &str, message: &str) {
        let frame = cairn_api::sse::SseFrame {
            event: cairn_api::sse::SseEventName::OperatorNotification,
            data: serde_json::json!({
                "channel":  channel,
                "severity": severity,
                "message":  message,
            }),
            id: None,
            // NotificationSink has no scope context; keep tenant-agnostic.
            // Filtering happens in the SSE handler.
            tenant_id: None,
        };
        let seq = self.seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut frame_with_id = frame;
        frame_with_id.id = Some(seq.to_string());
        {
            let mut buf = self.buf.write().unwrap_or_else(|e| e.into_inner());
            if buf.len() >= 10_000 {
                buf.pop_front();
            }
            buf.push_back((seq, frame_with_id.clone()));
        }
        // Copilot review (PR #567): broadcast the id-tagged frame so
        // live SSE subscribers can advance `Last-Event-ID` in sync with
        // the replay buffer. Pre-fix the live path sent an id-less
        // frame while the replay buffer stored the id, so reconnecting
        // clients replayed frames they had already received on the
        // live stream.
        let _ = self.tx.send(frame_with_id);
    }
}

/// Composite emitter: SSE events + ProviderCallCompleted trace recording.
///
/// Wraps the base `SseOrchestratorEmitter` and additionally
///   - records decide-phase traces to the OTLP exporter,
///   - appends `CircuitBreakerTripped` / `BudgetThresholdCrossed`
///     events to the durable event log,
///   - increments per-breaker-kind trip + threshold-warning metrics.
///
/// A single orchestrate invocation builds one of these, wraps it in
/// `Arc<dyn OrchestratorEventEmitter>`, and hands it to
/// `OrchestratorLoop::with_emitter`.
pub(super) struct TracingEmitter {
    pub(super) inner: std::sync::Arc<crate::sse_hooks::SseOrchestratorEmitter>,
    pub(super) store: std::sync::Arc<cairn_store::InMemoryStore>,
    pub(super) exporter: std::sync::Arc<cairn_runtime::telemetry::OtlpExporter>,
    pub(super) fatal_error: std::sync::Mutex<Option<String>>,
    /// F65 PR-3: metrics sink for breaker trip / warning counters +
    /// per-kind measured-at-trip histograms.
    pub(super) metrics: std::sync::Arc<crate::metrics::AppMetrics>,
}

#[async_trait::async_trait]
impl cairn_orchestrator::OrchestratorEventEmitter for TracingEmitter {
    async fn on_started(&self, ctx: &cairn_orchestrator::OrchestrationContext) {
        self.inner.on_started(ctx).await;
    }
    async fn on_gather_completed(
        &self,
        ctx: &cairn_orchestrator::OrchestrationContext,
        g: &cairn_orchestrator::GatherOutput,
    ) {
        self.inner.on_gather_completed(ctx, g).await;
    }
    async fn on_decide_completed(
        &self,
        ctx: &cairn_orchestrator::OrchestrationContext,
        d: &cairn_orchestrator::DecideOutput,
    ) {
        self.inner.on_decide_completed(ctx, d).await;
        // #661: count `SpawnSubagent` proposals at the decision
        // boundary — this measures LLM *intent* to delegate, before
        // the downstream permission/role validation that can reject
        // a well-formed proposal. Hooking the domain event
        // (`SubagentSpawned`) would undercount cases where the LLM
        // tried to delegate but was refused.
        let spawn_count = d
            .proposals
            .iter()
            .filter(|p| p.action_type == cairn_domain::ActionType::SpawnSubagent)
            .count();
        for _ in 0..spawn_count {
            crate::metrics::record_subagent_spawn(self.metrics.as_ref());
        }
        crate::tracing_emitter::record_decide_trace(
            ctx,
            d,
            &self.store,
            &self.exporter,
            &self.fatal_error,
        )
        .await;
    }
    async fn on_tool_called(
        &self,
        ctx: &cairn_orchestrator::OrchestrationContext,
        name: &str,
        args: Option<&serde_json::Value>,
    ) {
        self.inner.on_tool_called(ctx, name, args).await;
    }
    async fn on_tool_result(
        &self,
        ctx: &cairn_orchestrator::OrchestrationContext,
        name: &str,
        ok: bool,
        out: Option<&serde_json::Value>,
        err: Option<&str>,
        duration_ms: u64,
    ) {
        self.inner
            .on_tool_result(ctx, name, ok, out, err, duration_ms)
            .await;
    }
    async fn on_step_completed(
        &self,
        ctx: &cairn_orchestrator::OrchestrationContext,
        d: &cairn_orchestrator::DecideOutput,
        e: &cairn_orchestrator::ExecuteOutcome,
    ) {
        self.inner.on_step_completed(ctx, d, e).await;
    }
    async fn on_breaker_tripped(
        &self,
        ctx: &cairn_orchestrator::OrchestrationContext,
        trip: &cairn_domain::session_orchestration::CircuitBreakerTrip,
    ) {
        // Forward to SSE for live dashboards.
        self.inner.on_breaker_tripped(ctx, trip).await;
        // F65 PR-3: append RuntimeEvent::CircuitBreakerTripped to the
        // durable event log so projections (session_outcome, UI) see
        // the trip alongside the SessionOutcomeEmitted that PR-4+ will
        // wire. Failures here are logged; the loop still returns
        // `LoopTermination::BreakerTripped` so the operator-visible
        // HTTP response is unaffected.
        use cairn_domain::{CircuitBreakerTripped, EventEnvelope, EventId, EventSource};
        use cairn_store::EventLog;
        let at_ms = crate::errors::now_ms();
        let envelope = EventEnvelope::for_runtime_event(
            EventId::new(format!("evt_breaker_tripped_{}", uuid::Uuid::new_v4())),
            EventSource::System,
            cairn_domain::RuntimeEvent::CircuitBreakerTripped(CircuitBreakerTripped {
                project: ctx.project.clone(),
                session_id: ctx.session_id.clone(),
                run_id: ctx.run_id.clone(),
                trip: trip.clone(),
                at_ms,
            }),
        );
        if let Err(e) = self.store.append(&[envelope]).await {
            tracing::warn!(
                run_id = %ctx.run_id,
                error = %e,
                "F65 PR-3: failed to append CircuitBreakerTripped event — \
                 SSE/dashboard still reflect the trip, durable event log missed it"
            );
        }
        // Observability: increment trip counter + measured-at-trip
        // histogram so dashboards can track how often each breaker
        // fires and how far past the threshold runs are actually
        // landing. `NoToolUseConsecutive` skips the histogram (no
        // distribution to learn from — it always trips at exactly
        // the cap).
        crate::metrics::record_breaker_trip(self.metrics.as_ref(), trip.which, trip.measured);
    }
    async fn on_budget_threshold_crossed(
        &self,
        ctx: &cairn_orchestrator::OrchestrationContext,
        which: cairn_domain::session_orchestration::BreakerKind,
        measured: u64,
        limit: u64,
        ratio_bps: u32,
    ) {
        self.inner
            .on_budget_threshold_crossed(ctx, which, measured, limit, ratio_bps)
            .await;
        use cairn_domain::{BudgetThresholdCrossed, EventEnvelope, EventId, EventSource};
        use cairn_store::EventLog;
        let at_ms = crate::errors::now_ms();
        let envelope = EventEnvelope::for_runtime_event(
            EventId::new(format!("evt_budget_crossed_{}", uuid::Uuid::new_v4())),
            EventSource::System,
            cairn_domain::RuntimeEvent::BudgetThresholdCrossed(BudgetThresholdCrossed {
                project: ctx.project.clone(),
                session_id: ctx.session_id.clone(),
                run_id: ctx.run_id.clone(),
                which_breaker: which,
                measured,
                limit,
                ratio_bps,
                at_ms,
            }),
        );
        if let Err(e) = self.store.append(&[envelope]).await {
            tracing::warn!(
                run_id = %ctx.run_id,
                error = %e,
                "F65 PR-3: failed to append BudgetThresholdCrossed event — \
                 warning surfaced on SSE but not persisted"
            );
        }
        crate::metrics::record_breaker_threshold_warn(self.metrics.as_ref(), which);
    }
    async fn on_finished(
        &self,
        ctx: &cairn_orchestrator::OrchestrationContext,
        t: &cairn_orchestrator::LoopTermination,
    ) {
        self.inner.on_finished(ctx, t).await;
    }
    fn take_fatal_error(&self) -> Option<String> {
        let mut slot = self.fatal_error.lock().unwrap_or_else(|p| p.into_inner());
        slot.take()
    }
}
