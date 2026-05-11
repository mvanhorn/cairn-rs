//! RFC 032 PR-5: concrete [`ContractVerifier`] implementation wiring
//! the orchestrator's verifier trait to the
//! `cairn_orchestrator::contract_verifier` dispatch function.
//!
//! The orchestrator crate declares the shape of what it needs
//! ([`ContractVerifier`] trait + [`VerifierContext`] value type with a
//! [`ProjectRepoAccessQuery`] + optional [`cairn_github::GitHubClient`]);
//! this adapter ties those abstractions to AppState's concrete services.
//!
//! [`ContractVerifier`]: cairn_orchestrator::loop_runner::ContractVerifier
//! [`VerifierContext`]: cairn_orchestrator::contract_verifier::VerifierContext
//! [`ProjectRepoAccessQuery`]: cairn_orchestrator::contract_verifier::ProjectRepoAccessQuery

use std::sync::Arc;

use cairn_domain::{
    completion_contracts::{CompletionContract, ContractVerifiedOutput},
    ProjectKey, RepoAccessContext,
};
use cairn_orchestrator::{
    context::OrchestrationContext,
    contract_verifier::{
        verify_contract, ProjectRepoAccessQuery, VerifierContext, VerifierRejection,
    },
    loop_runner::ContractVerifier,
};
use cairn_workspace::sandbox::RepoId;
use cairn_workspace::ProjectRepoAccessService;

/// Adapter between `cairn_orchestrator::loop_runner::ContractVerifier`
/// and the `verify_contract` dispatch function. Holds the pieces of
/// AppState the verifier needs: the project repo allowlist (for
/// tenant-scope checks on `PullRequest` / `ExternalState` verifiers)
/// and an optional GitHub client (`None` on deployments without the
/// GitHub App wired — the PR verifier rejects with
/// [`cairn_domain::completion_contracts::ContractRejectionCode::VerifierUnavailable`]
/// in that case so contracts are never silently waved through).
pub struct ContractVerifierAdapter {
    pub project_repo_access: Arc<ProjectRepoAccessService>,
    pub github_client: Option<Arc<cairn_github::GitHubClient>>,
}

#[async_trait::async_trait]
impl ContractVerifier for ContractVerifierAdapter {
    async fn verify(
        &self,
        contract: &CompletionContract,
        ctx: &OrchestrationContext,
        final_answer: &str,
    ) -> Result<ContractVerifiedOutput, VerifierRejection> {
        let query = ProjectRepoAccessAdapter {
            inner: self.project_repo_access.clone(),
        };
        let vctx = VerifierContext {
            final_answer,
            run_id: &ctx.run_id,
            project: &ctx.project,
            working_dir: &ctx.working_dir,
            project_repo_access: &query,
            github_client: self.github_client.as_deref(),
        };
        verify_contract(contract, &vctx).await
    }
}

/// Sync wrapper that drives `ProjectRepoAccessService::list_for_project`
/// on a blocking in-process thread. The underlying service is `async
/// fn` but holds an in-memory `RwLock` map — it never awaits past the
/// lock, so calling it on a short-lived `tokio::runtime::Handle::block_on`
/// is safe + cheap. Keeping the verifier trait sync (per
/// [`ProjectRepoAccessQuery`]) lets the file / PR verifier bodies stay
/// straight-line without `.await` litter on the common paths.
struct ProjectRepoAccessAdapter {
    inner: Arc<ProjectRepoAccessService>,
}

impl ProjectRepoAccessQuery for ProjectRepoAccessAdapter {
    fn contains_repo(&self, project: &ProjectKey, repo: &str) -> bool {
        let repo_id = RepoId::new(repo);
        if repo_id.validate().is_err() {
            return false;
        }
        let ctx = RepoAccessContext {
            project: project.clone(),
        };
        // `list_for_project` is an `async fn` over an `RwLock`
        // read — no genuine await. The service has no `contains_repo`
        // today so we pull the list and membership-check. Lists are
        // typically 0–few entries (per-project allowlist), so the
        // linear scan is cheap.
        let inner = self.inner.clone();
        let repos = match tokio::runtime::Handle::try_current() {
            Ok(rt) => tokio::task::block_in_place(|| rt.block_on(inner.list_for_project(&ctx))),
            Err(_) => {
                // Called from a non-tokio context — should not happen
                // in the orchestrate handler, but default-deny keeps
                // the cross-tenant invariant.
                return false;
            }
        };
        repos.iter().any(|r| r.as_str() == repo_id.as_str())
    }
}
