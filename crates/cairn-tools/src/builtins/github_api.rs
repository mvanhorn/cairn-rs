//! GitHub API tools — use GitHub App installation tokens instead of `gh` CLI.
//!
//! These tools wrap `cairn_github::GitHubClient` operations for use in
//! the orchestrator. They are Deferred-tier and discovered via `tool_search`.
//!
//! Unlike the `gh` CLI tools in `github.rs`, these require no local CLI
//! installation — they use the App's installation token directly.

use std::sync::Arc;

use async_trait::async_trait;
use cairn_domain::recovery::RetrySafety;
use cairn_domain::{policy::ExecutionClass, ProjectKey};
use serde_json::Value;
use tokio::sync::RwLock;

use super::{
    PermissionLevel, ToolCategory, ToolEffect, ToolError, ToolHandler, ToolResult, ToolTier,
};

/// Shared GitHub client provider — injected at registration time.
///
/// The tools call `get()` to obtain a client for the configured installation.
/// This avoids each tool needing to know about App credentials or installation IDs.
#[derive(Default)]
pub struct GitHubClientProvider {
    client: RwLock<Option<cairn_github::GitHubClient>>,
}

impl GitHubClientProvider {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_client(client: cairn_github::GitHubClient) -> Self {
        Self {
            client: RwLock::new(Some(client)),
        }
    }

    pub async fn set(&self, client: cairn_github::GitHubClient) {
        *self.client.write().await = Some(client);
    }

    pub async fn get(&self) -> Result<cairn_github::GitHubClient, ToolError> {
        self.client
            .read()
            .await
            .clone()
            .ok_or_else(|| ToolError::Permanent("GitHub App not configured".into()))
    }
}

fn require_str<'a>(args: &'a Value, field: &str) -> Result<&'a str, ToolError> {
    args[field].as_str().ok_or_else(|| ToolError::InvalidArgs {
        field: field.into(),
        message: "required string".into(),
    })
}

fn split_repo(repo: &str) -> Result<(&str, &str), ToolError> {
    repo.split_once('/').ok_or_else(|| ToolError::InvalidArgs {
        field: "repo".into(),
        message: "must be owner/repo format".into(),
    })
}

// ── github_api.create_branch ─────────────────────────────────────────────────

pub struct GhApiCreateBranchTool {
    provider: Arc<GitHubClientProvider>,
}

impl GhApiCreateBranchTool {
    pub fn new(provider: Arc<GitHubClientProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl ToolHandler for GhApiCreateBranchTool {
    fn name(&self) -> &str {
        "github_api.create_branch"
    }
    fn tier(&self) -> ToolTier {
        ToolTier::Deferred
    }
    fn tool_effect(&self) -> ToolEffect {
        ToolEffect::External
    }
    fn retry_safety(&self) -> RetrySafety {
        RetrySafety::IdempotentSafe
    }
    fn description(&self) -> &str {
        "Create a new branch in a GitHub repository from the default branch HEAD."
    }
    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["repo", "branch_name"],
            "properties": {
                "repo":        { "type": "string", "description": "owner/repo" },
                "branch_name": { "type": "string", "description": "New branch name" },
                "from_branch": { "type": "string", "description": "Source branch (default: repo default branch)" }
            }
        })
    }
    fn execution_class(&self) -> ExecutionClass {
        ExecutionClass::Sensitive
    }
    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::Execute
    }
    fn category(&self) -> ToolCategory {
        ToolCategory::Orchestration
    }

    async fn execute(&self, _project: &ProjectKey, args: Value) -> Result<ToolResult, ToolError> {
        let client = self.provider.get().await?;
        let repo = require_str(&args, "repo")?;
        let branch_name = require_str(&args, "branch_name")?;
        let (owner, repo_name) = split_repo(repo)?;

        let from_branch = match args["from_branch"].as_str() {
            Some(b) => b.to_owned(),
            None => {
                let repo_info = client
                    .get_repo(owner, repo_name)
                    .await
                    .map_err(|e| ToolError::Transient(e.to_string()))?;
                repo_info.default_branch
            }
        };

        let base_ref = client
            .get_ref(owner, repo_name, &from_branch)
            .await
            .map_err(|e| ToolError::Transient(e.to_string()))?;

        let new_ref = client
            .create_branch(owner, repo_name, branch_name, &base_ref.object.sha)
            .await
            .map_err(|e| ToolError::Transient(e.to_string()))?;

        Ok(ToolResult::ok(serde_json::json!({
            "branch": branch_name,
            "sha": new_ref.object.sha,
            "from": from_branch,
        })))
    }
}

// ── github_api.write_file ────────────────────────────────────────────────────

pub struct GhApiWriteFileTool {
    provider: Arc<GitHubClientProvider>,
}

impl GhApiWriteFileTool {
    pub fn new(provider: Arc<GitHubClientProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl ToolHandler for GhApiWriteFileTool {
    fn name(&self) -> &str {
        "github_api.write_file"
    }
    fn tier(&self) -> ToolTier {
        ToolTier::Deferred
    }
    fn tool_effect(&self) -> ToolEffect {
        ToolEffect::External
    }
    fn retry_safety(&self) -> RetrySafety {
        RetrySafety::DangerousPause
    }
    fn description(&self) -> &str {
        "Create or update a file in a GitHub repository. Commits directly to the specified branch."
    }
    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["repo", "path", "content", "message", "branch"],
            "properties": {
                "repo":    { "type": "string", "description": "owner/repo" },
                "path":    { "type": "string", "description": "File path in the repo" },
                "content": { "type": "string", "description": "File content (UTF-8)" },
                "message": { "type": "string", "description": "Commit message" },
                "branch":  { "type": "string", "description": "Target branch" }
            }
        })
    }
    fn execution_class(&self) -> ExecutionClass {
        ExecutionClass::Sensitive
    }
    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::Execute
    }
    fn category(&self) -> ToolCategory {
        ToolCategory::Orchestration
    }

    async fn execute(&self, _project: &ProjectKey, args: Value) -> Result<ToolResult, ToolError> {
        let client = self.provider.get().await?;
        let repo = require_str(&args, "repo")?;
        let path = require_str(&args, "path")?;
        let content = require_str(&args, "content")?;
        let message = require_str(&args, "message")?;
        let branch = require_str(&args, "branch")?;
        let (owner, repo_name) = split_repo(repo)?;

        // Check if file exists to get its SHA (needed for updates).
        let existing_sha = match client.get_file(owner, repo_name, path, Some(branch)).await {
            Ok(f) => Some(f.sha),
            Err(_) => None,
        };

        let result = client
            .put_file(
                owner,
                repo_name,
                path,
                content,
                message,
                branch,
                existing_sha.as_deref(),
            )
            .await
            .map_err(|e| ToolError::Transient(e.to_string()))?;

        Ok(ToolResult::ok(serde_json::json!({
            "path": result.content.path,
            "sha": result.commit.sha,
            "message": result.commit.message,
        })))
    }
}

// ── github_api.read_file ─────────────────────────────────────────────────────

pub struct GhApiReadFileTool {
    provider: Arc<GitHubClientProvider>,
}

impl GhApiReadFileTool {
    pub fn new(provider: Arc<GitHubClientProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl ToolHandler for GhApiReadFileTool {
    fn name(&self) -> &str {
        "github_api.read_file"
    }
    fn tier(&self) -> ToolTier {
        ToolTier::Deferred
    }
    fn tool_effect(&self) -> ToolEffect {
        ToolEffect::Observational
    }
    fn retry_safety(&self) -> RetrySafety {
        RetrySafety::IdempotentSafe
    }
    fn description(&self) -> &str {
        "Read a file from a GitHub repository. Returns decoded UTF-8 content."
    }
    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["repo", "path"],
            "properties": {
                "repo":   { "type": "string", "description": "owner/repo" },
                "path":   { "type": "string", "description": "File path in the repo" },
                "branch": { "type": "string", "description": "Branch (default: repo default)" }
            }
        })
    }
    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::ReadOnly
    }
    fn category(&self) -> ToolCategory {
        ToolCategory::Orchestration
    }

    async fn execute(&self, _project: &ProjectKey, args: Value) -> Result<ToolResult, ToolError> {
        let client = self.provider.get().await?;
        let repo = require_str(&args, "repo")?;
        let path = require_str(&args, "path")?;
        let branch = args["branch"].as_str();
        let (owner, repo_name) = split_repo(repo)?;

        let file = client
            .get_file(owner, repo_name, path, branch)
            .await
            .map_err(|e| ToolError::Transient(e.to_string()))?;

        let content = file
            .decode_content()
            .unwrap_or_else(|| "[binary or empty file]".to_owned());

        // Truncate for LLM context.
        let truncated = if content.len() > 8000 {
            format!("{}...[truncated at 8000 chars]", &content[..8000])
        } else {
            content
        };

        Ok(ToolResult::ok(serde_json::json!({
            "path": file.path,
            "sha": file.sha,
            "content": truncated,
        })))
    }
}

// ── github_api.create_pr ─────────────────────────────────────────────────────

pub struct GhApiCreatePrTool {
    provider: Arc<GitHubClientProvider>,
}

impl GhApiCreatePrTool {
    pub fn new(provider: Arc<GitHubClientProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl ToolHandler for GhApiCreatePrTool {
    fn name(&self) -> &str {
        "github_api.create_pr"
    }
    fn tier(&self) -> ToolTier {
        ToolTier::Deferred
    }
    fn tool_effect(&self) -> ToolEffect {
        ToolEffect::External
    }
    fn retry_safety(&self) -> RetrySafety {
        RetrySafety::DangerousPause
    }
    fn description(&self) -> &str {
        "Create a pull request in a GitHub repository."
    }
    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["repo", "title", "head", "base"],
            "properties": {
                "repo":  { "type": "string", "description": "owner/repo" },
                "title": { "type": "string", "description": "PR title" },
                "body":  { "type": "string", "description": "PR description (markdown)" },
                "head":  { "type": "string", "description": "Head branch (with changes)" },
                "base":  { "type": "string", "description": "Base branch to merge into" }
            }
        })
    }
    fn execution_class(&self) -> ExecutionClass {
        ExecutionClass::Sensitive
    }
    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::Execute
    }
    fn category(&self) -> ToolCategory {
        ToolCategory::Orchestration
    }

    async fn execute(&self, _project: &ProjectKey, args: Value) -> Result<ToolResult, ToolError> {
        let client = self.provider.get().await?;
        let repo = require_str(&args, "repo")?;
        let title = require_str(&args, "title")?;
        let body = args["body"].as_str().unwrap_or("");
        let head = require_str(&args, "head")?;
        let base = require_str(&args, "base")?;
        let (owner, repo_name) = split_repo(repo)?;

        let pr = client
            .create_pull_request(owner, repo_name, title, body, head, base)
            .await
            .map_err(|e| ToolError::Transient(e.to_string()))?;

        Ok(ToolResult::ok(serde_json::json!({
            "number": pr.number,
            "title": pr.title,
            "url": pr.html_url,
            "state": pr.state,
        })))
    }
}

// ── github_api.review_pr ─────────────────────────────────────────────────────
//
// Post one pull-request review — a top-level summary body plus any
// number of inline line-anchored comments — via one API call. Maps
// directly to `POST /repos/:o/:r/pulls/:n/reviews`. The event kind
// (`COMMENT` / `REQUEST_CHANGES` / `APPROVE`) controls whether the
// review blocks merge or is advisory.
//
// Motivation: agent reviewers need to emit "here's my overall take
// plus N specific inline findings" in a single atomic review thread.
// Posting N separate inline comments fragments the GitHub UI and
// loses the "one review" affordance.

pub struct GhApiReviewPrTool {
    provider: Arc<GitHubClientProvider>,
}

impl GhApiReviewPrTool {
    pub fn new(provider: Arc<GitHubClientProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl ToolHandler for GhApiReviewPrTool {
    fn name(&self) -> &str {
        "github_api.review_pr"
    }
    fn tier(&self) -> ToolTier {
        ToolTier::Deferred
    }
    fn tool_effect(&self) -> ToolEffect {
        ToolEffect::External
    }
    fn retry_safety(&self) -> RetrySafety {
        // Duplicate reviews *are* idempotent-ish (GitHub dedupes
        // identical bodies in the UI), but a retry after a
        // network-layer timeout would post twice. Mark as
        // DangerousPause to surface the choice to the operator.
        RetrySafety::DangerousPause
    }
    fn description(&self) -> &str {
        "Post a pull request review with a summary body and any number \
         of inline file:line comments in one API call. Use for batch \
         review feedback from an agent reviewer."
    }
    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["repo", "pr_number"],
            "properties": {
                "repo": {
                    "type": "string",
                    "description": "owner/repo"
                },
                "pr_number": {
                    "type": "integer",
                    "description": "PR number"
                },
                "commit_id": {
                    "type": "string",
                    "description": "Commit SHA to target (default: PR head). \
                                    Inline comments anchor against this commit."
                },
                "body": {
                    "type": "string",
                    "description": "Top-level review body (markdown). Optional \
                                    only when posting inline comments without a \
                                    summary."
                },
                "event": {
                    "type": "string",
                    "enum": ["COMMENT", "REQUEST_CHANGES", "APPROVE"],
                    "description": "Review event kind. COMMENT is advisory; \
                                    REQUEST_CHANGES blocks merge; APPROVE \
                                    unblocks. Defaults to COMMENT."
                },
                "comments": {
                    "type": "array",
                    "description": "Inline comments (optional). Each carries \
                                    path+line+body.",
                    "items": {
                        "type": "object",
                        "required": ["path", "body"],
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "Repo-relative file path"
                            },
                            "line": {
                                "type": "integer",
                                "description": "Line number in the new version \
                                                (RIGHT side). Required for \
                                                line-anchored comments; omit \
                                                for file-level comments + set \
                                                subject_type=file."
                            },
                            "body": {
                                "type": "string",
                                "description": "Comment markdown"
                            },
                            "side": {
                                "type": "string",
                                "enum": ["LEFT", "RIGHT"],
                                "description": "Defaults to RIGHT"
                            },
                            "start_line": {
                                "type": "integer",
                                "description": "Multi-line range start (pair \
                                                with `line`)"
                            },
                            "start_side": {
                                "type": "string",
                                "enum": ["LEFT", "RIGHT"]
                            },
                            "subject_type": {
                                "type": "string",
                                "enum": ["line", "file"],
                                "description": "Defaults to `line`"
                            }
                        }
                    }
                }
            }
        })
    }
    fn execution_class(&self) -> ExecutionClass {
        ExecutionClass::Sensitive
    }
    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::Execute
    }
    fn category(&self) -> ToolCategory {
        ToolCategory::Orchestration
    }

    async fn execute(
        &self,
        _project: &ProjectKey,
        mut args: Value,
    ) -> Result<ToolResult, ToolError> {
        // Validate arguments up-front so a malformed call surfaces
        // as InvalidArgs without waiting for the GitHub client to
        // initialize. Keeps the error contract clean for callers.
        let repo = require_str(&args, "repo")?.to_owned();
        let (owner, repo_name) = split_repo(&repo)?;
        let owner = owner.to_owned();
        let repo_name = repo_name.to_owned();
        let pr_number = args
            .get("pr_number")
            .and_then(Value::as_u64)
            .ok_or_else(|| ToolError::InvalidArgs {
                field: "pr_number".into(),
                message: "missing or not an integer".into(),
            })?;

        // Take the body/commit_id/event strings out of `args` instead
        // of borrowing them — a per-review body can be multi-kB of
        // markdown, and we'd otherwise clone it when handing to the
        // request struct below.
        // Take owned strings out of `args` so we don't clone kB-sized
        // review bodies. `Value::take` leaves `Value::Null` behind;
        // `if let Value::String(s) = …` keeps the match zero-copy.
        let commit_id = match args.get_mut("commit_id").map(Value::take) {
            Some(Value::String(s)) => Some(s),
            _ => None,
        };
        let body = match args.get_mut("body").map(Value::take) {
            Some(Value::String(s)) => Some(s),
            _ => None,
        };
        let event = args
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or("COMMENT")
            .to_owned();

        // Own the inline-comment strings so the borrows in
        // ReviewCommentInput<'_> live through the request build. The
        // GitHub client's request type borrows from the caller — it
        // never copies — so we keep everything in a local vec.
        #[derive(Default)]
        struct OwnedInline {
            path: String,
            body: String,
            line: Option<u32>,
            side: Option<String>,
            start_line: Option<u32>,
            start_side: Option<String>,
            subject_type: Option<String>,
        }

        // `Value::take` moves the inner Vec out of `args`, so the
        // comment strings can be moved into OwnedInline instead of
        // cloned. On PRs with many long inline bodies this avoids
        // O(N * body_bytes) of extra allocation.
        let comments_vec: Vec<Value> = match args.get_mut("comments").map(Value::take) {
            Some(Value::Array(v)) => v,
            _ => Vec::new(),
        };
        let mut owned_comments: Vec<OwnedInline> = Vec::with_capacity(comments_vec.len());
        for mut c in comments_vec {
            let path = match c.get_mut("path").map(Value::take) {
                Some(Value::String(s)) => s,
                _ => {
                    return Err(ToolError::InvalidArgs {
                        field: "comments[].path".into(),
                        message: "required, missing".into(),
                    });
                }
            };
            let cbody = match c.get_mut("body").map(Value::take) {
                Some(Value::String(s)) => s,
                _ => {
                    return Err(ToolError::InvalidArgs {
                        field: "comments[].body".into(),
                        message: "required, missing".into(),
                    });
                }
            };
            let take_string = |c: &mut Value, k: &str| -> Option<String> {
                match c.get_mut(k).map(Value::take) {
                    Some(Value::String(s)) => Some(s),
                    _ => None,
                }
            };
            owned_comments.push(OwnedInline {
                path,
                body: cbody,
                line: c.get("line").and_then(Value::as_u64).map(|v| v as u32),
                side: take_string(&mut c, "side"),
                start_line: c
                    .get("start_line")
                    .and_then(Value::as_u64)
                    .map(|v| v as u32),
                start_side: take_string(&mut c, "start_side"),
                subject_type: take_string(&mut c, "subject_type"),
            });
        }

        let comment_inputs: Vec<cairn_github::ReviewCommentInput<'_>> = owned_comments
            .iter()
            .map(|c| cairn_github::ReviewCommentInput {
                path: &c.path,
                body: &c.body,
                line: c.line,
                side: c.side.as_deref(),
                start_line: c.start_line,
                start_side: c.start_side.as_deref(),
                subject_type: c.subject_type.as_deref(),
            })
            .collect();

        let req = cairn_github::CreatePullRequestReviewRequest {
            commit_id: commit_id.as_deref(),
            body: body.as_deref(),
            event: Some(&event),
            comments: comment_inputs,
        };

        let client = self.provider.get().await?;
        let review = client
            .create_pull_request_review(&owner, &repo_name, pr_number, &req)
            .await
            .map_err(|e| ToolError::Transient(e.to_string()))?;

        Ok(ToolResult::ok(serde_json::json!({
            "review_id": review.id,
            "state": review.state,
            "url": review.html_url,
            "comments_posted": owned_comments.len(),
        })))
    }
}

// ── github_api.merge_pr ──────────────────────────────────────────────────────

pub struct GhApiMergePrTool {
    provider: Arc<GitHubClientProvider>,
}

impl GhApiMergePrTool {
    pub fn new(provider: Arc<GitHubClientProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl ToolHandler for GhApiMergePrTool {
    fn name(&self) -> &str {
        "github_api.merge_pr"
    }
    fn tier(&self) -> ToolTier {
        ToolTier::Deferred
    }
    fn tool_effect(&self) -> ToolEffect {
        ToolEffect::External
    }
    fn retry_safety(&self) -> RetrySafety {
        RetrySafety::DangerousPause
    }
    fn description(&self) -> &str {
        "Merge a pull request. SENSITIVE — should only be called after operator approval."
    }
    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["repo", "number"],
            "properties": {
                "repo":         { "type": "string", "description": "owner/repo" },
                "number":       { "type": "integer", "description": "PR number" },
                "merge_method": { "type": "string", "enum": ["merge", "squash", "rebase"], "default": "squash" }
            }
        })
    }
    fn execution_class(&self) -> ExecutionClass {
        ExecutionClass::Sensitive
    }
    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::Execute
    }
    fn category(&self) -> ToolCategory {
        ToolCategory::Orchestration
    }

    async fn execute(&self, _project: &ProjectKey, args: Value) -> Result<ToolResult, ToolError> {
        let client = self.provider.get().await?;
        let repo = require_str(&args, "repo")?;
        let number = args["number"]
            .as_u64()
            .ok_or_else(|| ToolError::InvalidArgs {
                field: "number".into(),
                message: "required integer".into(),
            })?;
        let merge_method = args["merge_method"].as_str().unwrap_or("squash");
        let (owner, repo_name) = split_repo(repo)?;

        let result = client
            .merge_pull_request(owner, repo_name, number, None, Some(merge_method))
            .await
            .map_err(|e| ToolError::Transient(e.to_string()))?;

        Ok(ToolResult::ok(serde_json::json!({
            "merged": result.merged,
            "sha": result.sha,
            "message": result.message,
        })))
    }
}

// ── github_api.list_contents ─────────────────────────────────────────────────

pub struct GhApiListContentsTool {
    provider: Arc<GitHubClientProvider>,
}

impl GhApiListContentsTool {
    pub fn new(provider: Arc<GitHubClientProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl ToolHandler for GhApiListContentsTool {
    fn name(&self) -> &str {
        "github_api.list_contents"
    }
    fn tier(&self) -> ToolTier {
        ToolTier::Deferred
    }
    fn tool_effect(&self) -> ToolEffect {
        ToolEffect::Observational
    }
    fn retry_safety(&self) -> RetrySafety {
        RetrySafety::IdempotentSafe
    }
    fn description(&self) -> &str {
        "List files and directories in a GitHub repository path."
    }
    fn parameters_schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "required": ["repo"],
            "properties": {
                "repo":   { "type": "string", "description": "owner/repo" },
                "path":   { "type": "string", "description": "Directory path (default: root)", "default": "" },
                "branch": { "type": "string", "description": "Branch (default: repo default)" }
            }
        })
    }
    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::ReadOnly
    }
    fn category(&self) -> ToolCategory {
        ToolCategory::Orchestration
    }

    async fn execute(&self, _project: &ProjectKey, args: Value) -> Result<ToolResult, ToolError> {
        let client = self.provider.get().await?;
        let repo = require_str(&args, "repo")?;
        let path = args["path"].as_str().unwrap_or("");
        let branch = args["branch"].as_str();
        let (owner, repo_name) = split_repo(repo)?;

        let entries = client
            .list_contents(owner, repo_name, path, branch)
            .await
            .map_err(|e| ToolError::Transient(e.to_string()))?;

        let items: Vec<Value> = entries
            .iter()
            .map(|e| {
                serde_json::json!({
                    "name": e.name,
                    "path": e.path,
                    "type": e.content_type,
                })
            })
            .collect();

        Ok(ToolResult::ok(serde_json::json!({
            "path": path,
            "entries": items,
            "count": items.len(),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_api_tools_are_deferred_tier() {
        let provider = Arc::new(GitHubClientProvider::new());
        assert_eq!(
            GhApiCreateBranchTool::new(provider.clone()).tier(),
            ToolTier::Deferred
        );
        assert_eq!(
            GhApiWriteFileTool::new(provider.clone()).tier(),
            ToolTier::Deferred
        );
        assert_eq!(
            GhApiReadFileTool::new(provider.clone()).tier(),
            ToolTier::Deferred
        );
        assert_eq!(
            GhApiCreatePrTool::new(provider.clone()).tier(),
            ToolTier::Deferred
        );
        assert_eq!(
            GhApiMergePrTool::new(provider.clone()).tier(),
            ToolTier::Deferred
        );
        assert_eq!(
            GhApiReviewPrTool::new(provider.clone()).tier(),
            ToolTier::Deferred
        );
        assert_eq!(
            GhApiListContentsTool::new(provider).tier(),
            ToolTier::Deferred
        );
    }

    #[test]
    fn write_tools_are_sensitive() {
        let provider = Arc::new(GitHubClientProvider::new());
        assert_eq!(
            GhApiCreateBranchTool::new(provider.clone()).execution_class(),
            ExecutionClass::Sensitive
        );
        assert_eq!(
            GhApiWriteFileTool::new(provider.clone()).execution_class(),
            ExecutionClass::Sensitive
        );
        assert_eq!(
            GhApiCreatePrTool::new(provider.clone()).execution_class(),
            ExecutionClass::Sensitive
        );
        assert_eq!(
            GhApiMergePrTool::new(provider.clone()).execution_class(),
            ExecutionClass::Sensitive
        );
        assert_eq!(
            GhApiReviewPrTool::new(provider).execution_class(),
            ExecutionClass::Sensitive
        );
    }

    #[tokio::test]
    async fn review_pr_rejects_missing_pr_number() {
        let tool = GhApiReviewPrTool::new(Arc::new(GitHubClientProvider::new()));
        let err = tool
            .execute(
                &ProjectKey::new("t", "w", "p"),
                serde_json::json!({"repo": "o/r", "body": "..."}),
            )
            .await
            .unwrap_err();
        match err {
            ToolError::InvalidArgs { field, .. } => {
                assert_eq!(field, "pr_number");
            }
            other => panic!("expected InvalidArgs, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn review_pr_rejects_comment_missing_path() {
        // Each inline comment must carry `path` + `body`. Missing
        // `path` must surface as InvalidArgs before the GitHub
        // client is touched — agents get a clean validation error,
        // not "App not configured".
        let tool = GhApiReviewPrTool::new(Arc::new(GitHubClientProvider::new()));
        let err = tool
            .execute(
                &ProjectKey::new("t", "w", "p"),
                serde_json::json!({
                    "repo": "o/r",
                    "pr_number": 1,
                    "comments": [{"body": "lgtm"}],
                }),
            )
            .await
            .unwrap_err();
        match err {
            ToolError::InvalidArgs { field, .. } => {
                assert_eq!(field, "comments[].path");
            }
            other => panic!("expected InvalidArgs, got {other:?}"),
        }
    }

    #[test]
    fn review_pr_tool_name_and_category() {
        let tool = GhApiReviewPrTool::new(Arc::new(GitHubClientProvider::new()));
        assert_eq!(tool.name(), "github_api.review_pr");
        assert_eq!(tool.category(), ToolCategory::Orchestration);
        assert_eq!(tool.permission_level(), PermissionLevel::Execute);
    }

    #[test]
    fn review_pr_schema_requires_repo_and_pr_number() {
        let tool = GhApiReviewPrTool::new(Arc::new(GitHubClientProvider::new()));
        let schema = tool.parameters_schema();
        let required = schema["required"].as_array().unwrap();
        assert!(required.iter().any(|v| v == "repo"));
        assert!(required.iter().any(|v| v == "pr_number"));
        // `body` is deliberately NOT required — agents may post
        // inline-only reviews without a top-level summary.
        assert!(!required.iter().any(|v| v == "body"));
    }

    #[test]
    fn read_tools_are_read_only() {
        let provider = Arc::new(GitHubClientProvider::new());
        assert_eq!(
            GhApiReadFileTool::new(provider.clone()).permission_level(),
            PermissionLevel::ReadOnly
        );
        assert_eq!(
            GhApiListContentsTool::new(provider).permission_level(),
            PermissionLevel::ReadOnly
        );
    }
}
