//! #670 G5: parent auto-resume callback.
//!
//! When a child subagent run reaches a terminal state, the cairn-side
//! terminal hook (`FabricRunService::{complete, fail, cancel}`) must
//! (1) deliver the `child_completed:<child_task_id>` signal to the
//! parent's waitpoint and (2) re-invoke the parent's orchestrator
//! loop so its next iteration observes the child's terminal state.
//!
//! Step (2) requires `drive_run_iteration`, which lives in cairn-app
//! (because it depends on the full `AppState` — provider routing,
//! tool registry, SSE emitter, checkpoint hook, etc.). cairn-fabric
//! can't import cairn-app — reverse dependency — so the terminal hook
//! invokes a callback trait that cairn-app wires at boot.
//!
//! This is the same shape as `ParentAutoResume` in RFC-020 Track-3's
//! tool-call cache bridge: a small inversion-of-control trait at the
//! layering boundary, implemented by the outer crate.
//!
//! # Lifetime
//!
//! The impl is installed on `FabricRunService` post-construction via
//! `set_parent_auto_resume`. If unset (tests, boot paths that skip
//! wiring, etc.), the terminal hook's resume step is a no-op and the
//! parent falls back to manual operator re-POST of `/orchestrate`.

use std::sync::Arc;

use async_trait::async_trait;
use cairn_domain::{ProjectKey, RunId, SessionId};

/// Re-drive a parent run's orchestrator loop after its child has
/// terminated. Called by the cairn-fabric terminal hook via a
/// `tokio::spawn`; the implementation is expected to be best-effort
/// (log-on-error, no propagation).
///
/// Implementations must be idempotent: the same parent may be
/// resumed multiple times across retries, and FF's atomic
/// `issue_grant_and_claim` will arbitrate concurrent-claim races
/// at the lease layer.
#[async_trait]
pub trait ParentAutoResume: Send + Sync {
    async fn resume_parent_run(&self, project: ProjectKey, session_id: SessionId, run_id: RunId);
}

/// Default no-op impl. Used when no resume callback is wired — the
/// parent stays suspended on the waitpoint until an operator
/// re-POSTs `/v1/runs/:id/orchestrate`. Pre-G5 behaviour.
pub struct NoOpParentAutoResume;

#[async_trait]
impl ParentAutoResume for NoOpParentAutoResume {
    async fn resume_parent_run(
        &self,
        _project: ProjectKey,
        _session_id: SessionId,
        _run_id: RunId,
    ) {
        // Intentional no-op.
    }
}

/// Convenience constructor for the default.
#[allow(dead_code)]
pub fn no_op() -> Arc<dyn ParentAutoResume> {
    Arc::new(NoOpParentAutoResume)
}
