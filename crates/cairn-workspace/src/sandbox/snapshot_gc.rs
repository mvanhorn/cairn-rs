//! F65 PR-5: workspace-snapshot garbage collector.
//!
//! Hourly tokio task that enumerates `workspace_snapshots` rows, reaps
//! those whose age exceeds TTL AND whose parent Session is in a terminal
//! status (Completed | BudgetExhausted | Aborted). Emits
//! `WorkspaceSnapshotReaped` per reap.
//!
//! Decoupling: this module depends only on cairn-workspace + two traits
//! (`SnapshotGcSource` for enumeration, `WorkspaceSnapshotWriter` for the
//! reap-time update). cairn-app implements both against the real
//! cairn-store read model + writer.
//!
//! Test-clock injection: [`SnapshotGcPolicy`] accepts an
//! [`super::service::Clock`]. Integration tests inject a `TestClock`
//! plus `CAIRN_SNAPSHOT_GC_CADENCE_MS=50` to make the sweep deterministic
//! instead of racing `tokio::time::sleep`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cairn_domain::{SessionId, WorkspaceSnapshotId};

use crate::sandbox::f65::{F65SandboxEvent, F65SandboxEventSink};
use crate::sandbox::service::{Clock, SandboxService};

/// Reason recorded on a reaped snapshot. Rides on the cairn-workspace
/// event shape; the domain `WorkspaceSnapshotReaped` carries only
/// identity (the reason surfaces on metrics labels + telemetry spans).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReapReason {
    /// TTL expired and parent session is closed.
    TtlExpired,
    /// Operator invoked `DELETE /v1/sessions/:id/snapshots`.
    OperatorCleared,
}

impl ReapReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TtlExpired => "ttl_expired",
            Self::OperatorCleared => "operator_cleared",
        }
    }
}

/// One reap-candidate row surfaced by [`SnapshotGcSource::list_reap_candidates`].
///
/// Includes everything the sweeper needs to decide + emit + stamp
/// without a second roundtrip. `project` rides along so the emitted
/// `WorkspaceSnapshotReaped` event carries the right ownership key for
/// the projection writer.
#[derive(Clone, Debug)]
pub struct SnapshotGcCandidate {
    pub snapshot_id: WorkspaceSnapshotId,
    pub session_id: SessionId,
    pub project: cairn_domain::ProjectKey,
    pub created_at_ms: u64,
}

/// Enumerate + mark-reaped operations the GC sweeper needs from the
/// store. cairn-app implements this over the real cairn-store read
/// model. `list_reap_candidates` returns rows that are (a) already
/// past-TTL AND (b) belong to a Session in a terminal status —
/// the two-predicate filter happens at the SQL layer so we don't
/// ship the full snapshots table over the wire every sweep.
#[async_trait]
pub trait SnapshotGcSource: Send + Sync + 'static {
    /// Return every `workspace_snapshots` row whose age at `now_ms`
    /// exceeds `ttl_ms` AND whose parent Session is closed AND whose
    /// `reaped_at` is still NULL.
    async fn list_reap_candidates(
        &self,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<Vec<SnapshotGcCandidate>, String>;
}

/// Drop-on-the-floor source for tests that don't need the full sweeper
/// surface. Returns an empty vec on every call.
#[derive(Debug, Default)]
pub struct NoopSnapshotGcSource;

#[async_trait]
impl SnapshotGcSource for NoopSnapshotGcSource {
    async fn list_reap_candidates(
        &self,
        _now_ms: u64,
        _ttl_ms: u64,
    ) -> Result<Vec<SnapshotGcCandidate>, String> {
        Ok(Vec::new())
    }
}

/// GC sweeper configuration.
///
/// Defaults are pulled from env vars: `CAIRN_SNAPSHOT_TTL_DAYS` (default
/// `7`) and `CAIRN_SNAPSHOT_GC_CADENCE_MS` (default `3_600_000` = 1 h).
#[derive(Clone)]
pub struct SnapshotGcPolicy {
    pub ttl_ms: u64,
    pub sweep_cadence_ms: u64,
    pub clock: Arc<dyn Clock>,
}

impl SnapshotGcPolicy {
    /// Build from env, falling back to the arch-doc defaults:
    ///  - TTL: 7 days
    ///  - cadence: 1 hour
    pub fn from_env_or_default(clock: Arc<dyn Clock>) -> Self {
        let ttl_days = std::env::var("CAIRN_SNAPSHOT_TTL_DAYS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(7);
        let cadence_ms = std::env::var("CAIRN_SNAPSHOT_GC_CADENCE_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3_600_000);
        Self {
            ttl_ms: ttl_days.saturating_mul(86_400_000),
            sweep_cadence_ms: cadence_ms,
            clock,
        }
    }
}

impl std::fmt::Debug for SnapshotGcPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotGcPolicy")
            .field("ttl_ms", &self.ttl_ms)
            .field("sweep_cadence_ms", &self.sweep_cadence_ms)
            .finish()
    }
}

/// The sweeper task. Holds the service handle + enumeration source +
/// policy. [`Self::run_forever`] never returns on the happy path and is
/// meant to be spawned via `tokio::spawn` in `cairn-app/main.rs` next to
/// the existing background workers. Graceful shutdown: abort the
/// returned `JoinHandle`.
pub struct SnapshotGcSweeper {
    service: Arc<SandboxService>,
    source: Arc<dyn SnapshotGcSource>,
    f65_event_sink: Arc<dyn F65SandboxEventSink>,
    policy: SnapshotGcPolicy,
}

impl SnapshotGcSweeper {
    pub fn new(
        service: Arc<SandboxService>,
        source: Arc<dyn SnapshotGcSource>,
        f65_event_sink: Arc<dyn F65SandboxEventSink>,
        policy: SnapshotGcPolicy,
    ) -> Self {
        Self {
            service,
            source,
            f65_event_sink,
            policy,
        }
    }

    /// Single pass — list candidates, reap each, emit one event per
    /// successful reap. Exposed for integration tests that drive the
    /// sweeper deterministically without spinning up the full task.
    pub async fn sweep_once(&self) -> usize {
        let now = self.policy.clock.now_millis();
        let candidates = match self
            .source
            .list_reap_candidates(now, self.policy.ttl_ms)
            .await
        {
            Ok(c) => c,
            Err(err) => {
                eprintln!(
                    "snapshot_gc: list_reap_candidates failed: {err}; skipping this sweep tick"
                );
                return 0;
            }
        };
        let mut reaped = 0usize;
        for candidate in candidates {
            // reap_snapshot_dir honours inflight-restore leases.
            match self.service.reap_snapshot_dir(&candidate.snapshot_id) {
                Ok(true) => {
                    self.f65_event_sink
                        .publish(F65SandboxEvent::WorkspaceSnapshotReaped {
                            project: candidate.project.clone(),
                            snapshot_id: candidate.snapshot_id.clone(),
                            reason: ReapReason::TtlExpired.as_str().to_string(),
                        });
                    reaped += 1;
                }
                Ok(false) => {
                    // Deferred — either in-flight restore lease or the
                    // directory was already gone. Either way no event.
                }
                Err(err) => {
                    eprintln!(
                        "snapshot_gc: reap_snapshot_dir({}) failed: {err}; leaving row unclaimed",
                        candidate.snapshot_id.as_str()
                    );
                }
            }
        }
        reaped
    }

    /// Long-running sweep loop. Ticks every `policy.sweep_cadence_ms`
    /// milliseconds. Aborting the returned JoinHandle is the supported
    /// shutdown path; do NOT send a cancellation channel because the
    /// loop's inner awaits are all cancel-safe (`sleep` + the source's
    /// async DB read).
    pub async fn run_forever(self) {
        let cadence = Duration::from_millis(self.policy.sweep_cadence_ms.max(1));
        loop {
            let _ = self.sweep_once().await;
            tokio::time::sleep(cadence).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reap_reason_labels() {
        assert_eq!(ReapReason::TtlExpired.as_str(), "ttl_expired");
        assert_eq!(ReapReason::OperatorCleared.as_str(), "operator_cleared");
    }

    #[test]
    fn policy_defaults_are_plan_locked_values() {
        // These are the USER DECISIONS LOCKED values per plan Q1/Q2.
        // Changing them requires plan update.
        let clock: Arc<dyn Clock> = Arc::new(crate::sandbox::service::SystemClock);
        // Ensure env vars don't leak into the defaults test.
        let saved_ttl = std::env::var("CAIRN_SNAPSHOT_TTL_DAYS").ok();
        let saved_cadence = std::env::var("CAIRN_SNAPSHOT_GC_CADENCE_MS").ok();
        std::env::remove_var("CAIRN_SNAPSHOT_TTL_DAYS");
        std::env::remove_var("CAIRN_SNAPSHOT_GC_CADENCE_MS");
        let policy = SnapshotGcPolicy::from_env_or_default(clock);
        assert_eq!(policy.ttl_ms, 7 * 86_400_000);
        assert_eq!(policy.sweep_cadence_ms, 3_600_000);
        if let Some(v) = saved_ttl {
            std::env::set_var("CAIRN_SNAPSHOT_TTL_DAYS", v);
        }
        if let Some(v) = saved_cadence {
            std::env::set_var("CAIRN_SNAPSHOT_GC_CADENCE_MS", v);
        }
    }
}
