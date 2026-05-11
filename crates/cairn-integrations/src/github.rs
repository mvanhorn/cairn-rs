//! GitHub integration plugin for Cairn.
//!
//! Implements the `Integration` trait using `cairn_github` for auth, webhooks,
//! and API operations. The agent prompt, tools, and event→action mappings are
//! all defaults that the operator can override.
//!
//! Also owns the plugin-specific DTOs (`GitHubEventAction`, `WebhookAction`,
//! `IssueQueueEntry`, `IssueQueueStatus`) that the cairn-app HTTP handlers
//! consume — these live with the plugin, not on `AppState`, so cairn-app
//! stays integration-agnostic.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32};

use async_trait::async_trait;
use cairn_workspace::{AllowlistPersistence, JsonFileAllowlistStore, ProjectRepoAccessService};
use tokio::sync::{RwLock, Semaphore};

use crate::{
    EventAction, EventActionMapping, Integration, IntegrationError, IntegrationEvent, QueueStats,
    WorkItem,
};

/// Env-var-backed triple lookup that delegates to
/// [`cairn_domain::ProjectKey::parse_triple`]. Kept as a thin wrapper
/// so pure-string parsing (and its rejection rules) live with the
/// domain type and stay testable without env manipulation.
fn parse_triple_env(env_var: &str) -> Option<cairn_domain::ProjectKey> {
    let raw = std::env::var(env_var).ok()?;
    cairn_domain::ProjectKey::parse_triple(&raw)
}

/// Fallback project for unmapped GitHub installations, read from
/// `CAIRN_GITHUB_DEFAULT_PROJECT` in `tenant/workspace/project` form.
/// Returns `None` when unset — callers MUST reject the webhook in that
/// case rather than fall through to a legacy `default_tenant` triple.
pub fn default_github_project_from_env() -> Option<cairn_domain::ProjectKey> {
    parse_triple_env("CAIRN_GITHUB_DEFAULT_PROJECT")
}

/// GitHub App integration plugin.
///
/// Holds credentials, installation token cache, webhook event/action
/// mappings, the issue-processing queue, and concurrency controls.
/// Created at startup when GITHUB_APP_ID + GITHUB_PRIVATE_KEY_FILE +
/// GITHUB_WEBHOOK_SECRET env vars are set, or via
/// `IntegrationRegistry::register_from_config` at runtime.
///
/// The cairn-app HTTP handlers recover this concrete type from the
/// registry via `registry.get_typed::<GitHubPlugin>("github").await`
/// and reach into `webhook_secret`, `installations`, `event_actions`,
/// and `issue_queue` directly — the `Integration` trait intentionally
/// does not surface these plugin-specific concerns.
pub struct GitHubPlugin {
    pub credentials: cairn_github::AppCredentials,
    pub webhook_secret: String,
    /// Map of installation_id → InstallationToken (auto-refreshing).
    pub installations: RwLock<HashMap<u64, cairn_github::InstallationToken>>,
    /// Operator-configured event→action mappings.
    ///
    /// `default_event_actions()` seeds the trait-level defaults;
    /// operators mutate this list via the `/v1/integrations/...` HTTP
    /// surface. Distinct from `EventActionMapping` on the generic
    /// trait because GitHub's mapping carries a label filter with
    /// semantics the generic trait does not need to know about.
    pub event_actions: RwLock<Vec<GitHubEventAction>>,
    /// Issue processing queue — ingested by `/v1/webhooks/github/scan`
    /// and drained by `process_issue_queue`.
    pub issue_queue: RwLock<VecDeque<IssueQueueEntry>>,
    /// Whether the queue dispatcher is paused by the operator.
    pub queue_paused: AtomicBool,
    /// Whether the queue dispatcher loop is currently running.
    pub queue_running: AtomicBool,
    /// Max concurrent orchestration runs (operator-configurable).
    pub max_concurrent: AtomicU32,
    /// Semaphore controlling concurrent run slots.
    pub run_semaphore: Arc<Semaphore>,
    pub http: reqwest::Client,
}

impl std::fmt::Debug for GitHubPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubPlugin")
            .field("app_id", &self.credentials.app_id)
            .finish()
    }
}

impl GitHubPlugin {
    /// Create a new GitHubPlugin with the given credentials and defaults.
    pub fn new(
        credentials: cairn_github::AppCredentials,
        webhook_secret: String,
        max_concurrent: u32,
    ) -> Self {
        Self {
            credentials,
            webhook_secret,
            installations: RwLock::new(HashMap::new()),
            event_actions: RwLock::new(Vec::new()),
            issue_queue: RwLock::new(VecDeque::new()),
            queue_paused: AtomicBool::new(false),
            queue_running: AtomicBool::new(false),
            max_concurrent: AtomicU32::new(max_concurrent),
            run_semaphore: Arc::new(Semaphore::new(max_concurrent as usize)),
            http: reqwest::Client::new(),
        }
    }

    /// Resolve the `ProjectKey` for a GitHub App installation.
    ///
    /// Today this reads the per-installation env var
    /// `CAIRN_GITHUB_INSTALLATION_<id>_PROJECT` in the canonical
    /// `tenant/workspace/project` form. When no env exists, callers
    /// fall back to `default_github_project_from_env()` (or reject
    /// entirely).
    ///
    /// A future iteration will move this mapping into the event log
    /// via a dedicated `GitHubInstallationMapping` projection; this
    /// env shim is a placeholder so webhooks stop commingling tenants.
    pub async fn project_for_installation(
        &self,
        installation_id: u64,
    ) -> Option<cairn_domain::ProjectKey> {
        let key = format!("CAIRN_GITHUB_INSTALLATION_{installation_id}_PROJECT");
        parse_triple_env(&key)
    }

    /// Create a GitHubPlugin from a config payload (runtime API).
    pub fn from_config(
        _id: &str,
        config: crate::config::GitHubConfig,
    ) -> Result<Self, crate::IntegrationError> {
        let pem_bytes = std::fs::read(&config.private_key_file).map_err(|e| {
            crate::IntegrationError::KeyFormatInvalid(format!(
                "cannot read private key file {}: {e}",
                config.private_key_file
            ))
        })?;
        let credentials =
            cairn_github::AppCredentials::new(config.app_id, &pem_bytes).map_err(|e| {
                crate::IntegrationError::KeyFormatInvalid(format!("invalid GitHub App key: {e}"))
            })?;
        Ok(Self::new(
            credentials,
            config.webhook_secret,
            config.max_concurrent,
        ))
    }

    /// Get or create an InstallationToken for the given installation ID.
    pub async fn token_for_installation(
        &self,
        installation_id: u64,
    ) -> cairn_github::InstallationToken {
        {
            let cache = self.installations.read().await;
            if let Some(token) = cache.get(&installation_id) {
                return token.clone();
            }
        }
        let token = cairn_github::InstallationToken::new(
            self.credentials.clone(),
            installation_id,
            self.http.clone(),
        );
        let mut cache = self.installations.write().await;
        cache.insert(installation_id, token.clone());
        token
    }

    /// Get a GitHubClient for the given installation.
    pub async fn client_for_installation(
        &self,
        installation_id: u64,
    ) -> cairn_github::GitHubClient {
        let token = self.token_for_installation(installation_id).await;
        cairn_github::GitHubClient::with_http(token, self.http.clone())
    }

    /// Name of the subdirectory under `CAIRN_PLUGIN_STATE_DIR` that this
    /// plugin owns. Public so callers can build the same path the
    /// plugin uses (e.g. test harnesses).
    pub const STATE_SUBDIR: &'static str = "github";

    /// Name of the allowlist JSON file inside the plugin state subdir.
    pub const ALLOWLIST_FILE: &'static str = "allowlist.json";

    /// Compute the canonical plugin-state path for this plugin's repo
    /// allowlist file, given a `CAIRN_PLUGIN_STATE_DIR` root.
    pub fn allowlist_path(plugin_state_dir: &Path) -> PathBuf {
        plugin_state_dir
            .join(Self::STATE_SUBDIR)
            .join(Self::ALLOWLIST_FILE)
    }

    /// Install plugin-owned durable persistence on the process-wide
    /// repo allowlist (closes #556).
    ///
    /// This is the integration's entry point into the persistence seam
    /// on `ProjectRepoAccessService`: the access service itself is a
    /// pure in-memory projection owned by `cairn-workspace`; the plugin
    /// supplies the durability by installing a `JsonFileAllowlistStore`
    /// at `<plugin_state_dir>/github/allowlist.json`.
    ///
    /// Called exactly once at plugin-wire time (cairn-app's `main.rs`)
    /// before the HTTP server starts accepting traffic. The access
    /// service is rehydrated from disk in the install call, so the
    /// first inbound `POST /v1/projects/.../repos` already sees every
    /// prior grant.
    ///
    /// Failure to open the state directory / parse the file is fatal —
    /// the GitHub plugin is a top-level integration and silent loss of
    /// its persistent allowlist would violate the RFC 016 recovery
    /// contract.
    pub fn install_allowlist_persistence(
        access: &ProjectRepoAccessService,
        plugin_state_dir: &Path,
    ) -> Result<PathBuf, IntegrationError> {
        let path = Self::allowlist_path(plugin_state_dir);
        let store = JsonFileAllowlistStore::open(&path).map_err(|e| {
            IntegrationError::Other(format!(
                "github plugin: open allowlist persistence at {}: {e}",
                path.display()
            ))
        })?;
        access
            .install_persistence(Arc::new(store) as Arc<dyn AllowlistPersistence>)
            .map_err(|e| {
                IntegrationError::Other(format!(
                    "github plugin: install allowlist persistence: {e}"
                ))
            })?;
        Ok(path)
    }

    /// Check if a webhook event key matches a pattern (supports `*` wildcard).
    pub fn event_matches(event_key: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if let Some(prefix) = pattern.strip_suffix(".*") {
            event_key.starts_with(prefix) && event_key.len() > prefix.len()
        } else {
            event_key == pattern
        }
    }
}

/// Default agent prompt for GitHub issue→PR agents.
///
/// Based on research from SWE-agent, OpenHands, Aider, Claude Code,
/// Cursor, and Devin. See `docs/skills/system-prompt-curator/SKILL.md`.
const DEFAULT_GITHUB_AGENT_PROMPT: &str = "\
You are a senior software engineer working autonomously. You have been \
assigned a GitHub issue and must resolve it by writing code and opening \
a pull request.\n\
\n\
Follow the workflow in the goal description. Use tool_search to discover \
available tools. Explore the codebase before writing code. Write real, \
working code — not descriptions or TODO comments. Verify your changes \
compile and tests pass before opening the PR.\n\
\n\
Do not call complete_run until you have opened a pull request.";

#[async_trait]
impl Integration for GitHubPlugin {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn id(&self) -> &str {
        "github"
    }

    fn display_name(&self) -> &str {
        "GitHub"
    }

    fn is_configured(&self) -> bool {
        true // If this struct exists, credentials were valid at startup.
    }

    fn default_agent_prompt(&self) -> &str {
        DEFAULT_GITHUB_AGENT_PROMPT
    }

    fn default_event_actions(&self) -> Vec<EventActionMapping> {
        vec![
            EventActionMapping {
                event_pattern: "issues.opened".into(),
                label_filter: Some("cairn".into()),
                repo_filter: None,
                action: EventAction::CreateAndOrchestrate,
            },
            EventActionMapping {
                event_pattern: "issues.labeled".into(),
                label_filter: Some("cairn".into()),
                repo_filter: None,
                action: EventAction::CreateAndOrchestrate,
            },
        ]
    }

    async fn verify_webhook(
        &self,
        headers: &http::HeaderMap,
        body: &[u8],
    ) -> Result<(), IntegrationError> {
        let sig = headers
            .get("X-Hub-Signature-256")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                IntegrationError::VerificationFailed("missing X-Hub-Signature-256".into())
            })?;
        cairn_github::verify_signature(sig, self.webhook_secret.as_bytes(), body).map_err(|e| {
            IntegrationError::VerificationFailed(format!("HMAC verification failed: {e}"))
        })
    }

    async fn parse_event(
        &self,
        headers: &http::HeaderMap,
        body: &[u8],
    ) -> Result<IntegrationEvent, IntegrationError> {
        let event_type = headers
            .get("X-GitHub-Event")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown");
        let delivery_id = headers
            .get("X-GitHub-Delivery")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let event = cairn_github::WebhookEvent::parse(event_type, delivery_id, body)
            .map_err(|e| IntegrationError::ParseError(e.to_string()))?;

        let installation_id = event
            .installation_id()
            .map(|id: u64| id.to_string())
            .unwrap_or_default();

        // Extract title, body, and labels from raw JSON (WebhookEvent doesn't
        // expose these directly — they live in the issue/PR payload).
        let raw: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
        let issue_or_pr = raw.get("issue").or_else(|| raw.get("pull_request"));
        let title = issue_or_pr
            .and_then(|v| v["title"].as_str())
            .map(|s| s.to_owned());
        let body_text = issue_or_pr
            .and_then(|v| v["body"].as_str())
            .map(|s| s.to_owned());
        let labels = issue_or_pr
            .and_then(|v| v["labels"].as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|l| l["name"].as_str().map(|s| s.to_owned()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(IntegrationEvent {
            integration_id: "github".into(),
            event_key: event.event_key(),
            source_id: installation_id,
            repository: event.repository().map(|r: &str| r.to_owned()),
            title,
            body: body_text,
            labels,
            raw,
        })
    }

    async fn build_goal(&self, item: &WorkItem) -> Result<String, IntegrationError> {
        // Try to fetch the full issue from the GitHub API for richer context.
        let source_id: u64 = item
            .source_id
            .parse()
            .map_err(|_| IntegrationError::Other("invalid installation_id".into()))?;
        let (owner, repo_name) = item.repo.split_once('/').unwrap_or(("", &item.repo));
        let issue_number: u64 = item
            .external_id
            .parse()
            .map_err(|_| IntegrationError::Other("invalid issue number".into()))?;

        let client = self.client_for_installation(source_id).await;

        match client.get_issue(owner, repo_name, issue_number).await {
            Ok(issue) => {
                let body = issue.body.as_deref().unwrap_or("");
                Ok(format!(
                    "## Task\n\
                     Resolve GitHub Issue #{number} in repository `{repo}` by writing code \
                     and opening a pull request.\n\n\
                     ## Issue\n\
                     **{title}**\n\n\
                     {body}\n\n\
                     ## Workflow\n\
                     Follow these steps in order.\n\n\
                     1. **Explore** — Use tool_search to find available tools. Use file-reading \
                     and search tools to understand the repo structure and find relevant code. \
                     Read at least 3-5 files before planning changes.\n\n\
                     2. **Plan** — Identify which files need to change and what the fix or \
                     feature looks like. Think through edge cases.\n\n\
                     3. **Branch** — Create a feature branch (e.g. `cairn/issue-{number}`).\n\n\
                     4. **Implement** — Write the code. Make minimal, focused changes. Follow \
                     existing code style and conventions in the repo.\n\n\
                     5. **Verify** — If the project has tests, run them. Fix any failures.\n\n\
                     6. **Deliver** — Commit your changes, push the branch, and open a PR \
                     that references issue #{number} in the title or body.\n\n\
                     7. **Complete** — After the PR is open, call escalate_to_operator for \
                     review, then complete_run with a summary.\n\n\
                     ## Tips\n\
                     - Start by exploring. Do not write code until you understand the codebase.\n\
                     - If a tool call fails, read the error and try a different approach. \
                     A command that failed once will fail again unless you change something.\n\
                     - Write real, working code — not pseudocode or TODO comments.\n\
                     - Keep changes focused on this issue only.\n\
                     - All tool calls targeting this repo need: repo=\"{repo}\".\n\
                     - Do not call complete_run until you have opened a PR.",
                    number = issue.number,
                    repo = item.repo,
                    title = issue.title,
                    body = body,
                ))
            }
            Err(_) => Ok(format!(
                "Resolve GitHub Issue #{} in repository `{}`. \
                 Explore the codebase, write a fix, and open a pull request. \
                 Use tool_search to discover available tools.",
                issue_number, item.repo
            )),
        }
    }

    async fn prepare_tool_registry(
        &self,
        base: &cairn_tools::BuiltinToolRegistry,
        item: &WorkItem,
    ) -> Arc<cairn_tools::BuiltinToolRegistry> {
        use cairn_tools::builtins::github_api::*;

        let source_id: u64 = item.source_id.parse().unwrap_or(0);
        let gh_provider = Arc::new(GitHubClientProvider::new());

        if source_id > 0 {
            let client = self.client_for_installation(source_id).await;
            gh_provider.set(client).await;
        }

        Arc::new(
            cairn_tools::BuiltinToolRegistry::from_existing(base)
                .register(Arc::new(GhApiCreateBranchTool::new(gh_provider.clone())))
                .register(Arc::new(GhApiReadFileTool::new(gh_provider.clone())))
                .register(Arc::new(GhApiWriteFileTool::new(gh_provider.clone())))
                .register(Arc::new(GhApiCreatePrTool::new(gh_provider.clone())))
                .register(Arc::new(GhApiMergePrTool::new(gh_provider.clone())))
                .register(Arc::new(GhApiReviewPrTool::new(gh_provider.clone())))
                .register(Arc::new(GhApiListContentsTool::new(gh_provider))),
        )
    }

    fn auth_exempt_paths(&self) -> Vec<String> {
        vec!["/v1/webhooks/github".into()]
    }

    async fn queue_stats(&self) -> QueueStats {
        let queue = self.issue_queue.read().await;
        let mut stats = QueueStats::default();
        for entry in queue.iter() {
            match &entry.status {
                IssueQueueStatus::Pending => stats.pending += 1,
                IssueQueueStatus::Processing => stats.processing += 1,
                IssueQueueStatus::WaitingApproval => stats.waiting_approval += 1,
                IssueQueueStatus::Completed => stats.completed += 1,
                IssueQueueStatus::Failed(_) => stats.failed += 1,
            }
        }
        stats
    }
}

// ── GitHubEventAction / WebhookAction ───────────────────────────────────────
//
// The operator-configurable event→action mapping the cairn-app webhook
// handler consumes. Distinct from the crate-level `EventActionMapping`
// because GitHub's mapping carries a label filter and the `WebhookAction`
// enum is GitHub-flavoured (comment-based Acknowledge etc.).

/// Configurable event->action mapping for GitHub webhooks.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GitHubEventAction {
    /// Event key pattern to match (e.g. "issues.opened", "issues.labeled",
    /// "push"). Supports "*" as wildcard (e.g. "issues.*" matches all
    /// issue events).
    pub event_pattern: String,
    /// Optional label filter — only trigger if the issue/PR has this
    /// label.
    #[serde(default)]
    pub label_filter: Option<String>,
    /// Optional repo filter — only trigger for this repo (owner/repo).
    #[serde(default)]
    pub repo_filter: Option<String>,
    /// What to do when the event matches.
    pub action: WebhookAction,
}

/// What to do when a webhook event matches a configured pattern.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookAction {
    /// Create a session + run and trigger orchestration. The goal is
    /// derived from the issue/PR title + body.
    CreateAndOrchestrate,
    /// Post a comment acknowledging the event.
    Acknowledge,
    /// Ignore the event (useful for explicit deny rules).
    Ignore,
}

// ── IssueQueueEntry / IssueQueueStatus ──────────────────────────────────────

/// A single issue queued for orchestration. Produced by the scan
/// handler, drained by `process_issue_queue`.
#[derive(Clone, Debug)]
pub struct IssueQueueEntry {
    pub repo: String,
    pub installation_id: u64,
    pub issue_number: u64,
    pub title: String,
    pub session_id: String,
    pub run_id: String,
    pub status: IssueQueueStatus,
}

/// Processing state of a queued GitHub issue.
#[derive(Clone, Debug, PartialEq)]
pub enum IssueQueueStatus {
    Pending,
    Processing,
    WaitingApproval,
    Completed,
    Failed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_matches_exact() {
        assert!(GitHubPlugin::event_matches(
            "issues.opened",
            "issues.opened"
        ));
        assert!(!GitHubPlugin::event_matches(
            "issues.closed",
            "issues.opened"
        ));
    }

    #[test]
    fn event_matches_wildcard() {
        assert!(GitHubPlugin::event_matches("issues.opened", "issues.*"));
        assert!(GitHubPlugin::event_matches("issues.closed", "issues.*"));
        assert!(!GitHubPlugin::event_matches("push", "issues.*"));
    }

    #[test]
    fn event_matches_star_all() {
        assert!(GitHubPlugin::event_matches("anything", "*"));
    }

    #[test]
    fn default_event_actions_are_cairn_label_only() {
        let plugin = make_test_plugin();
        let actions = plugin.default_event_actions();
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].label_filter.as_deref(), Some("cairn"));
    }

    #[test]
    fn plugin_is_configured() {
        let plugin = make_test_plugin();
        assert!(plugin.is_configured());
        assert_eq!(plugin.id(), "github");
        assert_eq!(plugin.display_name(), "GitHub");
    }

    #[test]
    fn default_prompt_mentions_pull_request() {
        let plugin = make_test_plugin();
        assert!(plugin.default_agent_prompt().contains("pull request"));
    }

    #[test]
    fn auth_exempt_paths_include_webhook() {
        let plugin = make_test_plugin();
        let paths = plugin.auth_exempt_paths();
        assert!(paths.contains(&"/v1/webhooks/github".to_owned()));
    }

    #[tokio::test]
    async fn install_allowlist_persistence_roundtrips_across_instances() {
        use cairn_domain::{ActorRef, OperatorId, ProjectKey, RepoAccessContext};
        use cairn_workspace::RepoId;

        let tmp = tempfile::TempDir::new().unwrap();

        // First boot — install persistence, grant a repo, drop the
        // access service.
        {
            let access = ProjectRepoAccessService::new();
            let path = GitHubPlugin::install_allowlist_persistence(&access, tmp.path()).unwrap();
            assert_eq!(
                path.file_name().unwrap().to_str(),
                Some("allowlist.json"),
                "canonical allowlist filename must not drift"
            );
            assert!(
                path.parent().unwrap().ends_with("github"),
                "plugin state subdir must be 'github'"
            );

            access
                .allow(
                    &RepoAccessContext {
                        project: ProjectKey::new("t", "w", "p"),
                    },
                    &RepoId::new("org/repo-1"),
                    ActorRef::Operator {
                        operator_id: OperatorId::new("op"),
                    },
                )
                .await
                .expect("allow must succeed with persistence installed");
        }

        // Second boot — fresh access service over the same state dir
        // sees the prior grant.
        let access = ProjectRepoAccessService::new();
        GitHubPlugin::install_allowlist_persistence(&access, tmp.path()).unwrap();
        assert!(
            access
                .is_allowed(
                    &RepoAccessContext {
                        project: ProjectKey::new("t", "w", "p"),
                    },
                    &RepoId::new("org/repo-1"),
                )
                .await,
            "allowlist must survive across process boundaries"
        );
    }

    #[test]
    fn allowlist_path_is_canonical() {
        let root = std::path::Path::new("/tmp/cairn-plugins-test");
        let path = GitHubPlugin::allowlist_path(root);
        assert_eq!(path, root.join("github").join("allowlist.json"));
    }

    #[tokio::test]
    async fn queue_stats_counts_correctly() {
        let plugin = make_test_plugin();
        {
            let mut queue = plugin.issue_queue.write().await;
            queue.push_back(make_queue_entry(1, IssueQueueStatus::Pending));
            queue.push_back(make_queue_entry(2, IssueQueueStatus::Processing));
            queue.push_back(make_queue_entry(3, IssueQueueStatus::Completed));
            queue.push_back(make_queue_entry(4, IssueQueueStatus::Failed("err".into())));
        }
        let stats = plugin.queue_stats().await;
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.processing, 1);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.failed, 1);
    }

    // `parse_triple` itself now lives on `cairn_domain::ProjectKey` and is
    // exercised by the tenancy unit tests. The wrapper here only adds the
    // `std::env::var` lookup; covering that in a unit test would require
    // mutating process env (flaky under cargo-test's shared process).

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn make_test_plugin() -> GitHubPlugin {
        // Use dummy credentials — we won't call the API in unit tests.
        // AppCredentials::new requires a real RSA key, so we skip it
        // and test only the non-auth methods.
        GitHubPlugin {
            credentials: unsafe_test_credentials(),
            webhook_secret: "test-secret".into(),
            installations: RwLock::new(HashMap::new()),
            event_actions: RwLock::new(Vec::new()),
            issue_queue: RwLock::new(VecDeque::new()),
            queue_paused: AtomicBool::new(false),
            queue_running: AtomicBool::new(false),
            max_concurrent: AtomicU32::new(3),
            run_semaphore: Arc::new(Semaphore::new(3)),
            http: reqwest::Client::new(),
        }
    }

    fn make_queue_entry(issue_number: u64, status: IssueQueueStatus) -> IssueQueueEntry {
        IssueQueueEntry {
            repo: "owner/repo".into(),
            installation_id: 123,
            issue_number,
            title: format!("Issue {issue_number}"),
            session_id: format!("sess_{issue_number}"),
            run_id: format!("run_{issue_number}"),
            status,
        }
    }

    /// Test-only: create AppCredentials using a generated RSA key.
    fn unsafe_test_credentials() -> cairn_github::AppCredentials {
        // Generate a minimal 2048-bit RSA key in PEM format for testing.
        // This is only used for struct construction — we don't call the GitHub API.
        let rsa_pem = include_bytes!("../tests/fixtures/test_rsa_key.pem");
        cairn_github::AppCredentials::new(12345, rsa_pem).expect("test RSA key should be valid")
    }
}
