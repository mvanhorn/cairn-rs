//! GitHub REST API client for code operations.
//!
//! All operations use an installation access token (auto-refreshed).
//! The client is stateless — it doesn't cache repo state.

use serde::{Deserialize, Serialize};

use crate::auth::InstallationToken;
use crate::error::GitHubError;

const API_BASE: &str = "https://api.github.com";
const USER_AGENT: &str = "cairn-github/0.1";
const API_VERSION: &str = "2022-11-28";

/// GitHub REST API client authenticated via an installation access token.
#[derive(Clone, Debug)]
pub struct GitHubClient {
    token: InstallationToken,
    http: reqwest::Client,
    /// API base URL. Defaults to `https://api.github.com`. Override in tests
    /// via [`GitHubClient::with_base_url`] to point at an httpmock server.
    base_url: String,
}

impl GitHubClient {
    pub fn new(token: InstallationToken) -> Self {
        Self {
            http: reqwest::Client::new(),
            token,
            base_url: API_BASE.to_owned(),
        }
    }

    pub fn with_http(token: InstallationToken, http: reqwest::Client) -> Self {
        Self {
            http,
            token,
            base_url: API_BASE.to_owned(),
        }
    }

    /// Override the API base URL. Intended for tests only — redirects calls
    /// to an httpmock server without changing production behaviour.
    #[doc(hidden)]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    // ── Issues ──────────────────────────────────────────────────────────────

    /// Get a single issue by number.
    pub async fn get_issue(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
    ) -> Result<IssueResponse, GitHubError> {
        let url = self.url(format!("{API_BASE}/repos/{owner}/{repo}/issues/{number}"));
        self.get_json(&url).await
    }

    /// List issues for a repo with optional filters.
    pub async fn list_issues(
        &self,
        owner: &str,
        repo: &str,
        state: Option<&str>,
        labels: Option<&str>,
        per_page: u32,
    ) -> Result<Vec<IssueResponse>, GitHubError> {
        let mut url = self.url(format!(
            "{API_BASE}/repos/{owner}/{repo}/issues?per_page={per_page}"
        ));
        if let Some(state) = state {
            url.push_str(&format!("&state={state}"));
        }
        if let Some(labels) = labels {
            url.push_str(&format!("&labels={labels}"));
        }
        self.get_json(&url).await
    }

    /// Post a comment on an issue or pull request.
    pub async fn create_comment(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        body: &str,
    ) -> Result<CommentResponse, GitHubError> {
        let url = self.url(format!(
            "{API_BASE}/repos/{owner}/{repo}/issues/{issue_number}/comments"
        ));
        self.post_json(&url, &serde_json::json!({ "body": body }))
            .await
    }

    /// Add labels to an issue.
    pub async fn add_labels(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        labels: &[&str],
    ) -> Result<Vec<LabelResponse>, GitHubError> {
        let url = self.url(format!(
            "{API_BASE}/repos/{owner}/{repo}/issues/{issue_number}/labels"
        ));
        self.post_json(&url, &serde_json::json!({ "labels": labels }))
            .await
    }

    // ── Branches ────────────────────────────────────────────────────────────

    /// Get a branch reference (returns the SHA).
    pub async fn get_ref(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
    ) -> Result<GitRefResponse, GitHubError> {
        let url = self.url(format!(
            "{API_BASE}/repos/{owner}/{repo}/git/ref/heads/{branch}"
        ));
        self.get_json(&url).await
    }

    /// Create a new branch from a SHA.
    pub async fn create_branch(
        &self,
        owner: &str,
        repo: &str,
        branch_name: &str,
        from_sha: &str,
    ) -> Result<GitRefResponse, GitHubError> {
        let url = self.url(format!("{API_BASE}/repos/{owner}/{repo}/git/refs"));
        let body = serde_json::json!({
            "ref": format!("refs/heads/{branch_name}"),
            "sha": from_sha,
        });
        self.post_json(&url, &body).await
    }

    // ── Files ───────────────────────────────────────────────────────────────

    /// Get the contents of a file (base64-encoded for binary, UTF-8 for text).
    pub async fn get_file(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        branch: Option<&str>,
    ) -> Result<FileContentResponse, GitHubError> {
        let mut url = self.url(format!("{API_BASE}/repos/{owner}/{repo}/contents/{path}"));
        if let Some(branch) = branch {
            url.push_str(&format!("?ref={branch}"));
        }
        self.get_json(&url).await
    }

    /// Create or update a file in the repo.
    pub async fn put_file(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        content: &str,
        message: &str,
        branch: &str,
        sha: Option<&str>,
    ) -> Result<FileUpdateResponse, GitHubError> {
        let url = self.url(format!("{API_BASE}/repos/{owner}/{repo}/contents/{path}"));
        let encoded = base64_encode(content.as_bytes());
        let mut body = serde_json::json!({
            "message": message,
            "content": encoded,
            "branch": branch,
        });
        if let Some(sha) = sha {
            body["sha"] = serde_json::Value::String(sha.to_owned());
        }
        self.put_json(&url, &body).await
    }

    /// Delete a file in the repo.
    pub async fn delete_file(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        message: &str,
        sha: &str,
        branch: &str,
    ) -> Result<serde_json::Value, GitHubError> {
        let url = self.url(format!("{API_BASE}/repos/{owner}/{repo}/contents/{path}"));
        let body = serde_json::json!({
            "message": message,
            "sha": sha,
            "branch": branch,
        });
        let token = self.token.get().await?;
        let resp = self
            .http
            .delete(&url)
            .headers(self.default_headers(&token))
            .json(&body)
            .send()
            .await?;
        self.handle_response(resp).await
    }

    // ── Trees & Commits (batch file operations) ─────────────────────────────

    /// Create a tree with multiple file changes in one API call.
    pub async fn create_tree(
        &self,
        owner: &str,
        repo: &str,
        base_tree: &str,
        items: &[TreeItem],
    ) -> Result<TreeResponse, GitHubError> {
        let url = self.url(format!("{API_BASE}/repos/{owner}/{repo}/git/trees"));
        let body = serde_json::json!({
            "base_tree": base_tree,
            "tree": items,
        });
        self.post_json(&url, &body).await
    }

    /// Create a commit pointing to a tree.
    pub async fn create_commit(
        &self,
        owner: &str,
        repo: &str,
        message: &str,
        tree_sha: &str,
        parent_shas: &[&str],
    ) -> Result<CommitResponse, GitHubError> {
        let url = self.url(format!("{API_BASE}/repos/{owner}/{repo}/git/commits"));
        let body = serde_json::json!({
            "message": message,
            "tree": tree_sha,
            "parents": parent_shas,
        });
        self.post_json(&url, &body).await
    }

    /// Update a branch ref to point to a new commit.
    pub async fn update_ref(
        &self,
        owner: &str,
        repo: &str,
        branch: &str,
        sha: &str,
    ) -> Result<GitRefResponse, GitHubError> {
        let url = self.url(format!(
            "{API_BASE}/repos/{owner}/{repo}/git/refs/heads/{branch}"
        ));
        let body = serde_json::json!({ "sha": sha });
        self.patch_json(&url, &body).await
    }

    // ── Pull Requests ───────────────────────────────────────────────────────

    /// Get a pull request's metadata (merge state, head/base refs, etc.).
    pub async fn get_pull_request(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
    ) -> Result<PullRequestDetail, GitHubError> {
        let url = self.url(format!("{API_BASE}/repos/{owner}/{repo}/pulls/{number}"));
        self.get_json(&url).await
    }

    /// List a pull request's changed files with their patches.
    ///
    /// GitHub caps `per_page` at 100 and paginates via `?page=N`.
    /// Caller drives the page loop — most valkey PRs fit in a single
    /// 100-file page, so callers usually request `per_page=100, page=1`.
    pub async fn list_pull_request_files(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
        per_page: u32,
        page: u32,
    ) -> Result<Vec<PullRequestFile>, GitHubError> {
        let url = self.url(format!(
            "{API_BASE}/repos/{owner}/{repo}/pulls/{number}/files?per_page={per_page}&page={page}"
        ));
        self.get_json(&url).await
    }

    /// List inline review comments on a pull request (comments attached
    /// to specific diff lines, not top-level conversation).
    ///
    /// GitHub caps `per_page` at 100; caller drives the page loop.
    pub async fn list_pull_request_review_comments(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
        per_page: u32,
        page: u32,
    ) -> Result<Vec<PullRequestReviewComment>, GitHubError> {
        let url = self.url(format!(
            "{API_BASE}/repos/{owner}/{repo}/pulls/{number}/comments?per_page={per_page}&page={page}"
        ));
        self.get_json(&url).await
    }

    /// List submitted reviews on a pull request (approvals, change-requests,
    /// comments-only review bodies).
    ///
    /// GitHub caps `per_page` at 100; caller drives the page loop.
    pub async fn list_pull_request_reviews(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
        per_page: u32,
        page: u32,
    ) -> Result<Vec<PullRequestReview>, GitHubError> {
        let url = self.url(format!(
            "{API_BASE}/repos/{owner}/{repo}/pulls/{number}/reviews?per_page={per_page}&page={page}"
        ));
        self.get_json(&url).await
    }

    /// List top-level conversation comments on a pull request. These are
    /// the issue-style comments, distinct from inline review comments.
    ///
    /// GitHub caps `per_page` at 100; caller drives the page loop.
    pub async fn list_pull_request_issue_comments(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
        per_page: u32,
        page: u32,
    ) -> Result<Vec<CommentResponse>, GitHubError> {
        let url = self.url(format!(
            "{API_BASE}/repos/{owner}/{repo}/issues/{number}/comments?per_page={per_page}&page={page}"
        ));
        self.get_json(&url).await
    }

    /// Create a review on a pull request. This is the atomic post: a
    /// single call publishes both the summary (`body`) and all inline
    /// comments in one transaction. Use this for review-agent output so
    /// the reviewer sees one coherent review, not scattered comments.
    ///
    /// `event` is one of `APPROVE`, `REQUEST_CHANGES`, `COMMENT`, or
    /// omitted to leave the review as a pending draft.
    pub async fn create_pull_request_review(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
        request: &CreatePullRequestReviewRequest<'_>,
    ) -> Result<PullRequestReview, GitHubError> {
        let url = self.url(format!(
            "{API_BASE}/repos/{owner}/{repo}/pulls/{number}/reviews"
        ));
        let payload = serde_json::to_value(request).map_err(|e| GitHubError::Api {
            status: 0,
            body: format!("failed to serialize review request: {e}"),
        })?;
        self.post_json(&url, &payload).await
    }

    /// Post a single inline comment on a pull request diff line. Prefer
    /// [`Self::create_pull_request_review`] when posting many comments at
    /// once; use this for follow-up comments, replies to a reviewer (set
    /// [`CreateReviewCommentRequest::in_reply_to_id`]), or file-level
    /// comments (set `subject_type = "file"` and leave `line = None`).
    pub async fn create_pull_request_review_comment(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
        request: &CreateReviewCommentRequest<'_>,
    ) -> Result<PullRequestReviewComment, GitHubError> {
        let url = self.url(format!(
            "{API_BASE}/repos/{owner}/{repo}/pulls/{number}/comments"
        ));
        let payload = serde_json::to_value(request).map_err(|e| GitHubError::Api {
            status: 0,
            body: format!("failed to serialize review comment request: {e}"),
        })?;
        self.post_json(&url, &payload).await
    }

    /// Create a pull request.
    pub async fn create_pull_request(
        &self,
        owner: &str,
        repo: &str,
        title: &str,
        body: &str,
        head: &str,
        base: &str,
    ) -> Result<PullRequestResponse, GitHubError> {
        let url = self.url(format!("{API_BASE}/repos/{owner}/{repo}/pulls"));
        let payload = serde_json::json!({
            "title": title,
            "body": body,
            "head": head,
            "base": base,
        });
        self.post_json(&url, &payload).await
    }

    /// Merge a pull request.
    pub async fn merge_pull_request(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
        commit_title: Option<&str>,
        merge_method: Option<&str>,
    ) -> Result<MergeResponse, GitHubError> {
        let url = self.url(format!(
            "{API_BASE}/repos/{owner}/{repo}/pulls/{number}/merge"
        ));
        let mut body = serde_json::json!({});
        if let Some(title) = commit_title {
            body["commit_title"] = serde_json::Value::String(title.to_owned());
        }
        if let Some(method) = merge_method {
            body["merge_method"] = serde_json::Value::String(method.to_owned());
        }
        self.put_json(&url, &body).await
    }

    /// Close a pull request without merging.
    pub async fn close_pull_request(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
    ) -> Result<PullRequestResponse, GitHubError> {
        let url = self.url(format!("{API_BASE}/repos/{owner}/{repo}/pulls/{number}"));
        let body = serde_json::json!({ "state": "closed" });
        self.patch_json(&url, &body).await
    }

    // ── Repository info ─────────────────────────────────────────────────────

    /// Get repository metadata.
    pub async fn get_repo(&self, owner: &str, repo: &str) -> Result<RepoResponse, GitHubError> {
        let url = self.url(format!("{API_BASE}/repos/{owner}/{repo}"));
        self.get_json(&url).await
    }

    /// List repo directory contents.
    pub async fn list_contents(
        &self,
        owner: &str,
        repo: &str,
        path: &str,
        branch: Option<&str>,
    ) -> Result<Vec<ContentEntry>, GitHubError> {
        let mut url = self.url(format!("{API_BASE}/repos/{owner}/{repo}/contents/{path}"));
        if let Some(branch) = branch {
            url.push_str(&format!("?ref={branch}"));
        }
        self.get_json(&url).await
    }

    // ── HTTP helpers ────────────────────────────────────────────────────────

    fn default_headers(&self, token: &str) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Authorization", format!("Bearer {token}").parse().unwrap());
        headers.insert("Accept", "application/vnd.github+json".parse().unwrap());
        headers.insert("User-Agent", USER_AGENT.parse().unwrap());
        headers.insert("X-GitHub-Api-Version", API_VERSION.parse().unwrap());
        headers
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T, GitHubError> {
        let token = self.token.get().await?;
        let resp = self
            .http
            .get(url)
            .headers(self.default_headers(&token))
            .send()
            .await?;
        self.handle_response(resp).await
    }

    async fn post_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<T, GitHubError> {
        let token = self.token.get().await?;
        let resp = self
            .http
            .post(url)
            .headers(self.default_headers(&token))
            .json(body)
            .send()
            .await?;
        self.handle_response(resp).await
    }

    async fn put_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<T, GitHubError> {
        let token = self.token.get().await?;
        let resp = self
            .http
            .put(url)
            .headers(self.default_headers(&token))
            .json(body)
            .send()
            .await?;
        self.handle_response(resp).await
    }

    async fn patch_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<T, GitHubError> {
        let token = self.token.get().await?;
        let resp = self
            .http
            .patch(url)
            .headers(self.default_headers(&token))
            .json(body)
            .send()
            .await?;
        self.handle_response(resp).await
    }

    async fn handle_response<T: serde::de::DeserializeOwned>(
        &self,
        resp: reqwest::Response,
    ) -> Result<T, GitHubError> {
        let status = resp.status().as_u16();
        if status >= 400 {
            let body = resp.text().await.unwrap_or_default();
            return Err(GitHubError::Api { status, body });
        }
        Ok(resp.json().await?)
    }

    /// Rewrite a canonical `https://api.github.com/...` URL to use `self.base_url`.
    /// No-op in production (base_url == API_BASE). In tests this redirects calls
    /// to an httpmock server.
    fn url(&self, canonical: String) -> String {
        if self.base_url == API_BASE {
            canonical
        } else {
            canonical.replacen(API_BASE, &self.base_url, 1)
        }
    }
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

// ── Response types ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IssueResponse {
    pub number: u64,
    pub title: String,
    #[serde(default)]
    pub body: Option<String>,
    pub state: String,
    pub user: UserResponse,
    #[serde(default)]
    pub labels: Vec<LabelResponse>,
    pub html_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommentResponse {
    pub id: u64,
    pub body: String,
    pub html_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabelResponse {
    pub name: String,
    #[serde(default)]
    pub color: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GitRefResponse {
    #[serde(rename = "ref")]
    pub git_ref: String,
    pub object: GitObject,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GitObject {
    pub sha: String,
    #[serde(rename = "type")]
    pub object_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileContentResponse {
    pub name: String,
    pub path: String,
    pub sha: String,
    #[serde(default)]
    pub content: Option<String>,
    pub encoding: Option<String>,
    #[serde(rename = "type")]
    pub content_type: String,
}

impl FileContentResponse {
    /// Decode the base64 content to a UTF-8 string.
    pub fn decode_content(&self) -> Option<String> {
        use base64::Engine;
        let raw = self.content.as_ref()?;
        let cleaned: String = raw.chars().filter(|c| !c.is_whitespace()).collect();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&cleaned)
            .ok()?;
        String::from_utf8(bytes).ok()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileUpdateResponse {
    pub content: ContentEntry,
    pub commit: CommitSummary,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContentEntry {
    pub name: String,
    pub path: String,
    pub sha: String,
    #[serde(rename = "type")]
    pub content_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitSummary {
    pub sha: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TreeItem {
    pub path: String,
    pub mode: String,
    #[serde(rename = "type")]
    pub item_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

impl TreeItem {
    /// Create a tree item for a new/modified file with inline content.
    pub fn file(path: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            mode: "100644".to_owned(),
            item_type: "blob".to_owned(),
            sha: None,
            content: Some(content.into()),
        }
    }

    /// Create a tree item that deletes a file (null sha).
    pub fn delete(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            mode: "100644".to_owned(),
            item_type: "blob".to_owned(),
            sha: Some("null".to_owned()), // GitHub interprets this as deletion
            content: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TreeResponse {
    pub sha: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitResponse {
    pub sha: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullRequestResponse {
    pub number: u64,
    pub title: String,
    pub html_url: String,
    pub state: String,
    #[serde(default)]
    pub merged: Option<bool>,
}

/// Detailed pull-request metadata returned by `GET /repos/:o/:r/pulls/:n`.
///
/// Carries head/base refs with SHAs so a review agent can fetch the
/// exact commit being reviewed and scope comments to the right commit id.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullRequestDetail {
    pub number: u64,
    pub title: String,
    #[serde(default)]
    pub body: Option<String>,
    pub state: String,
    pub user: UserResponse,
    pub head: PullRequestRef,
    pub base: PullRequestRef,
    pub html_url: String,
    #[serde(default)]
    pub draft: bool,
    #[serde(default)]
    pub merged: Option<bool>,
    #[serde(default)]
    pub mergeable: Option<bool>,
    /// GitHub's `mergeable_state` enum: `clean`, `dirty`, `blocked`,
    /// `behind`, `unstable`, `unknown`. Useful for letting an agent skip
    /// review on PRs that are not yet in a reviewable state.
    #[serde(default)]
    pub mergeable_state: Option<String>,
    #[serde(default)]
    pub labels: Vec<LabelResponse>,
    /// Commit SHA created by the merge (populated only after merge).
    #[serde(default)]
    pub merge_commit_sha: Option<String>,
    #[serde(default)]
    pub merged_at: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub additions: Option<u64>,
    #[serde(default)]
    pub deletions: Option<u64>,
    #[serde(default)]
    pub changed_files: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullRequestRef {
    #[serde(rename = "ref")]
    pub ref_name: String,
    pub sha: String,
    #[serde(default)]
    pub repo: Option<PullRequestRepoRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullRequestRepoRef {
    pub id: u64,
    pub full_name: String,
    #[serde(default)]
    pub private: bool,
    #[serde(default)]
    pub fork: bool,
}

/// One changed file in a pull request. `patch` is the unified diff hunk
/// for this file; GitHub elides it for binary files and very large diffs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullRequestFile {
    #[serde(rename = "filename")]
    pub path: String,
    pub status: String,
    pub additions: u64,
    pub deletions: u64,
    pub changes: u64,
    #[serde(default)]
    pub patch: Option<String>,
    #[serde(default)]
    pub previous_filename: Option<String>,
    pub sha: String,
    pub blob_url: String,
    pub raw_url: String,
}

/// An inline review comment on a pull request diff line.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullRequestReviewComment {
    pub id: u64,
    #[serde(default)]
    pub pull_request_review_id: Option<u64>,
    pub path: String,
    #[serde(default)]
    pub line: Option<u32>,
    /// For multi-line range comments, the start of the range.
    #[serde(default)]
    pub start_line: Option<u32>,
    #[serde(default)]
    pub side: Option<String>,
    /// For multi-line range comments, the side of the diff the range
    /// starts on (`LEFT` or `RIGHT`).
    #[serde(default)]
    pub start_side: Option<String>,
    pub commit_id: String,
    pub body: String,
    /// The unified-diff hunk the comment is anchored to. Missing on
    /// file-level comments and on older comments predating GitHub's
    /// diff-hunk field; defaults to empty for those.
    #[serde(default)]
    pub diff_hunk: String,
    pub user: UserResponse,
    pub html_url: String,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub in_reply_to_id: Option<u64>,
}

/// A submitted pull-request review (approval, change-request, or comment).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PullRequestReview {
    pub id: u64,
    pub user: UserResponse,
    #[serde(default)]
    pub body: Option<String>,
    pub state: String,
    pub html_url: String,
    pub commit_id: String,
    #[serde(default)]
    pub submitted_at: Option<String>,
}

/// Request body for `POST /repos/:o/:r/pulls/:n/reviews`.
///
/// Set `event = None` to leave the review as a pending draft; set it to
/// `"COMMENT"` to publish comments without approving or requesting
/// changes. `comments` is the batch of inline comments posted as part of
/// this review.
#[derive(Clone, Debug, Serialize)]
pub struct CreatePullRequestReviewRequest<'a> {
    /// Commit SHA the comments target. When omitted, GitHub uses the
    /// most recent commit on the PR.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_id: Option<&'a str>,
    /// The review's summary body (rendered above inline comments).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<&'a str>,
    /// `APPROVE` / `REQUEST_CHANGES` / `COMMENT`. Omit for a pending draft.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<&'a str>,
    /// Inline comments included in the review.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<ReviewCommentInput<'a>>,
}

/// One inline comment inside a review-creation request.
///
/// For a single-line comment, set `line`. For a multi-line range, set
/// both `line` and `start_line` (optionally with `start_side`). For a
/// file-level comment, leave `line = None` and set `subject_type =
/// Some("file")`. `side` defaults to `RIGHT` (the new version of the
/// file) when omitted.
#[derive(Clone, Debug, Serialize)]
pub struct ReviewCommentInput<'a> {
    pub path: &'a str,
    pub body: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_side: Option<&'a str>,
    /// `"line"` (default) or `"file"`. Set to `"file"` for a comment
    /// that applies to the whole file rather than a specific line.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject_type: Option<&'a str>,
}

/// Request body for `POST /repos/:o/:r/pulls/:n/comments`.
///
/// Three flavours of single-comment post are encoded in one struct:
/// - **Top-level inline comment**: set `commit_id`, `path`, `line`,
///   optionally `side` and the `start_*` pair for a multi-line range.
/// - **Reply to an existing comment**: set `in_reply_to_id` to the
///   target comment's id; `path` / `line` / `commit_id` are redundant
///   and GitHub ignores them, but this struct leaves them available
///   for callers that want to attach the full context.
/// - **File-level comment**: set `subject_type = Some("file")` and
///   leave `line = None`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct CreateReviewCommentRequest<'a> {
    pub body: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_side: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject_type: Option<&'a str>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MergeResponse {
    pub sha: String,
    pub merged: bool,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepoResponse {
    pub full_name: String,
    pub default_branch: String,
    #[serde(default)]
    pub private: bool,
    pub html_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserResponse {
    pub login: String,
    pub id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn review_request_serializes_comments_and_omits_none_fields() {
        let req = CreatePullRequestReviewRequest {
            commit_id: Some("abc123"),
            body: Some("LGTM with one nit"),
            event: Some("COMMENT"),
            comments: vec![
                ReviewCommentInput {
                    path: "src/hashtable.c",
                    body: "This skips the server.dict_resizing guard.",
                    line: Some(633),
                    side: Some("RIGHT"),
                    start_line: None,
                    start_side: None,
                    subject_type: None,
                },
                ReviewCommentInput {
                    path: "src/evict.c",
                    body: "Multi-line comment range.",
                    line: Some(120),
                    side: Some("RIGHT"),
                    start_line: Some(115),
                    start_side: Some("RIGHT"),
                    subject_type: None,
                },
                // File-level comment: no line, subject_type = "file".
                ReviewCommentInput {
                    path: "README.md",
                    body: "Drive-by nit on whole file.",
                    line: None,
                    side: None,
                    start_line: None,
                    start_side: None,
                    subject_type: Some("file"),
                },
            ],
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v["commit_id"], "abc123");
        assert_eq!(v["event"], "COMMENT");
        assert_eq!(v["comments"].as_array().unwrap().len(), 3);
        assert_eq!(v["comments"][0]["path"], "src/hashtable.c");
        assert_eq!(v["comments"][0]["line"], 633);
        // Omitted optional fields must not appear in the wire payload.
        assert!(v["comments"][0].get("start_line").is_none());
        assert!(v["comments"][0].get("subject_type").is_none());
        assert_eq!(v["comments"][1]["start_line"], 115);
        // File-level comment: `line` and `side` omitted, `subject_type` present.
        assert!(v["comments"][2].get("line").is_none());
        assert!(v["comments"][2].get("side").is_none());
        assert_eq!(v["comments"][2]["subject_type"], "file");
    }

    #[test]
    fn review_request_omits_body_and_event_when_none() {
        let req = CreatePullRequestReviewRequest {
            commit_id: None,
            body: None,
            event: None,
            comments: vec![],
        };
        let v = serde_json::to_value(&req).unwrap();
        assert!(v.get("commit_id").is_none());
        assert!(v.get("body").is_none());
        assert!(v.get("event").is_none());
        // Empty comments vec omitted too.
        assert!(v.get("comments").is_none());
    }

    #[test]
    fn pull_request_detail_deserializes_github_fixture() {
        // Trimmed fixture modelled on `GET /repos/:o/:r/pulls/:n`.
        let body = serde_json::json!({
            "number": 2095,
            "title": "Allow shrinking hashtables in low memory situations",
            "body": "Motivation...",
            "state": "closed",
            "user": { "login": "Fusl", "id": 42 },
            "head": {
                "ref": "shrink-hashtables",
                "sha": "c16727d6400722f70dded23c2c896ca71384ce4c",
                "repo": {
                    "id": 1,
                    "full_name": "Fusl/valkey",
                    "private": false,
                    "fork": true
                }
            },
            "base": {
                "ref": "unstable",
                "sha": "deadbeef",
                "repo": null
            },
            "html_url": "https://github.com/valkey-io/valkey/pull/2095",
            "draft": false,
            "merged": true,
            "mergeable": null,
            "mergeable_state": "clean",
            "labels": [{"name": "backport-candidate"}, {"name": "ready-to-merge"}],
            "merge_commit_sha": "c16727d6400722f70dded23c2c896ca71384ce4c",
            "merged_at": "2025-06-25T18:14:46Z",
            "created_at": "2025-06-20T10:00:00Z",
            "updated_at": "2025-06-25T18:14:46Z",
            "additions": 50,
            "deletions": 20,
            "changed_files": 3
        });
        let pr: PullRequestDetail = serde_json::from_value(body).unwrap();
        assert_eq!(pr.number, 2095);
        assert_eq!(pr.head.ref_name, "shrink-hashtables");
        assert_eq!(pr.head.sha, "c16727d6400722f70dded23c2c896ca71384ce4c");
        assert_eq!(pr.head.repo.as_ref().unwrap().full_name, "Fusl/valkey");
        assert!(pr.head.repo.as_ref().unwrap().fork);
        assert!(pr.base.repo.is_none());
        assert_eq!(pr.merged, Some(true));
        assert_eq!(pr.changed_files, Some(3));
        assert_eq!(pr.mergeable_state.as_deref(), Some("clean"));
        assert_eq!(pr.labels.len(), 2);
        assert_eq!(pr.labels[0].name, "backport-candidate");
        assert_eq!(
            pr.merge_commit_sha.as_deref(),
            Some("c16727d6400722f70dded23c2c896ca71384ce4c")
        );
        assert_eq!(pr.created_at.as_deref(), Some("2025-06-20T10:00:00Z"));
    }

    #[test]
    fn pull_request_file_deserializes_with_patch_and_rename() {
        let body = serde_json::json!([{
            "filename": "src/hashtable.c",
            "previous_filename": "src/hash.c",
            "status": "renamed",
            "additions": 10,
            "deletions": 2,
            "changes": 12,
            "sha": "abc",
            "blob_url": "https://github.com/o/r/blob/sha/src/hashtable.c",
            "raw_url": "https://github.com/o/r/raw/sha/src/hashtable.c",
            "patch": "@@ -633,6 +625,14 @@ static int resize..."
        }, {
            "filename": "binary.bin",
            "status": "modified",
            "additions": 0,
            "deletions": 0,
            "changes": 0,
            "sha": "def",
            "blob_url": "x",
            "raw_url": "y"
        }]);
        let files: Vec<PullRequestFile> = serde_json::from_value(body).unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].path, "src/hashtable.c");
        assert_eq!(files[0].previous_filename.as_deref(), Some("src/hash.c"));
        assert!(files[0].patch.as_ref().unwrap().contains("@@"));
        // Binary file has no patch field → deserializes as None, not an error.
        assert!(files[1].patch.is_none());
    }

    #[test]
    fn pull_request_review_comment_deserializes_reply_chain() {
        let body = serde_json::json!({
            "id": 999,
            "pull_request_review_id": 555,
            "path": "src/hashtable.c",
            "line": 633,
            "start_line": 630,
            "side": "RIGHT",
            "start_side": "RIGHT",
            "commit_id": "c16727d6",
            "body": "I agree, let me change that.",
            "diff_hunk": "@@ -633,6 +625,14 @@ static int resize...",
            "user": { "login": "madolson", "id": 1 },
            "html_url": "https://github.com/o/r/pull/1#r999",
            "created_at": "2025-06-25T18:00:00Z",
            "updated_at": "2025-06-25T18:00:00Z",
            "in_reply_to_id": 997
        });
        let c: PullRequestReviewComment = serde_json::from_value(body).unwrap();
        assert_eq!(c.line, Some(633));
        assert_eq!(c.start_line, Some(630));
        assert_eq!(c.side.as_deref(), Some("RIGHT"));
        assert_eq!(c.start_side.as_deref(), Some("RIGHT"));
        assert!(c.diff_hunk.contains("@@"));
        assert_eq!(c.in_reply_to_id, Some(997));
        assert_eq!(c.pull_request_review_id, Some(555));
    }

    #[test]
    fn pull_request_review_comment_defaults_when_fields_missing() {
        // File-level comments and older review comments may arrive
        // without `diff_hunk` / `start_*` fields. Serde defaults must
        // let them deserialize cleanly instead of failing.
        let body = serde_json::json!({
            "id": 42,
            "path": "README.md",
            "commit_id": "abc",
            "body": "file-level note",
            "user": { "login": "a", "id": 1 },
            "html_url": "x",
            "created_at": "t0",
            "updated_at": "t0"
        });
        let c: PullRequestReviewComment = serde_json::from_value(body).unwrap();
        assert!(c.line.is_none());
        assert!(c.start_line.is_none());
        assert!(c.start_side.is_none());
        assert_eq!(c.diff_hunk, "");
    }

    #[test]
    fn pull_request_review_deserializes_without_body() {
        // A bare approval has no body; `body: null` must round-trip.
        let body = serde_json::json!({
            "id": 1,
            "user": { "login": "zuiderkwast", "id": 2 },
            "body": null,
            "state": "APPROVED",
            "html_url": "https://github.com/o/r/pull/1#pullrequestreview-1",
            "commit_id": "c16727d6",
            "submitted_at": "2025-06-25T19:00:00Z"
        });
        let r: PullRequestReview = serde_json::from_value(body).unwrap();
        assert_eq!(r.state, "APPROVED");
        assert!(r.body.is_none());
    }
}
