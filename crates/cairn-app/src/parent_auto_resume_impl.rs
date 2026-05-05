//! #670 G5: cairn-app-side implementation of
//! `cairn_fabric::parent_auto_resume::ParentAutoResume`.
//!
//! The cairn-fabric terminal hook spawns a tokio task that calls
//! `resume_parent_run` after a child run terminates. The impl here
//! re-invokes `drive_run_iteration` on the parent, which takes the
//! parent's orchestrator loop from `WaitingDependency` → `Running` →
//! next decide turn (now with the child's terminal state observable
//! via `/children` + step_history plumbing in G7).
//!
//! # Lifecycle
//!
//! The impl holds a `Weak<AppState>` to avoid the Arc cycle that
//! would otherwise form: AppState → FabricServices → FabricRunService
//! → ParentAutoResume → AppState. The Weak upgrade can fail during
//! shutdown (after AppState's final Arc is dropped) — in that case
//! the auto-resume is a silent no-op, matching the same behaviour as
//! a process-exit-during-suspension.

use std::sync::Weak;

use async_trait::async_trait;
use cairn_domain::{ProjectKey, RunId, SessionId};
use cairn_fabric::parent_auto_resume::ParentAutoResume;

use crate::handlers::runs::{drive_run_iteration, OrchestrateRequest};
use crate::state::AppState;

/// Production impl. Re-drives the parent run via the shared
/// `drive_run_iteration` helper using the parent's persisted defaults
/// (same path the HTTP `/orchestrate` handler's inner body uses).
pub struct AppStateParentAutoResume {
    state: Weak<AppState>,
}

impl AppStateParentAutoResume {
    pub fn new(state: Weak<AppState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl ParentAutoResume for AppStateParentAutoResume {
    async fn resume_parent_run(&self, _project: ProjectKey, _session_id: SessionId, run_id: RunId) {
        let Some(state) = self.state.upgrade() else {
            tracing::debug!(
                run_id = %run_id,
                "G5 parent auto-resume: AppState is dropped (shutdown in progress); skipping",
            );
            return;
        };

        // Re-read the parent's projection row. The terminal-hook
        // has no operator identity and tenancy integrity is
        // preserved by the row's own project.
        let run = match state.runtime.runs.get(&run_id).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                tracing::warn!(
                    run_id = %run_id,
                    "G5 parent auto-resume: parent run not found; skipping",
                );
                return;
            }
            Err(err) => {
                tracing::warn!(
                    run_id = %run_id,
                    error = %err,
                    "G5 parent auto-resume: parent run lookup failed; skipping",
                );
                return;
            }
        };

        // Drive with the parent's persisted defaults (goal,
        // max_iterations, etc.). An empty OrchestrateRequest falls
        // through to those defaults in the helper.
        let body = OrchestrateRequest::default();
        match drive_run_iteration(state.clone(), run, body).await {
            Ok(_response) => {
                tracing::debug!(
                    run_id = %run_id,
                    "G5 parent auto-resume: drive_run_iteration completed",
                );
            }
            Err(_response) => {
                // Pre-loop early-return from the helper (lease,
                // credentials, etc.). Logged by the helper itself;
                // nothing to do here.
                tracing::debug!(
                    run_id = %run_id,
                    "G5 parent auto-resume: drive_run_iteration returned pre-loop error",
                );
            }
        }
    }
}
