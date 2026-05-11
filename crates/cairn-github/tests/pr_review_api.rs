//! Integration tests for the PR-review surface added in feat/cairn-github-pr-review-api.
//!
//! Each test spins up an httpmock server, configures it to return a canned
//! fixture, and asserts that the client method:
//!   1. sends the correct HTTP verb + path + Authorization header, and
//!   2. deserializes the response into the right wire type.
//!
//! Token auth is bypassed via `InstallationToken::with_static_token("test-token")`
//! so tests never make real network calls.

use cairn_github::{
    CreatePullRequestReviewRequest, CreateReviewCommentRequest, GitHubClient, InstallationToken,
    ReviewCommentInput,
};
use httpmock::prelude::*;
use serde_json::json;

/// Build a `GitHubClient` that uses a static "test-token" and redirects all
/// API calls to `server`.
fn client_for(server: &MockServer) -> GitHubClient {
    let token = InstallationToken::with_static_token("test-token");
    let http = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("reqwest client");
    GitHubClient::with_http(token, http).with_base_url(server.base_url())
}

// ── get_pull_request ─────────────────────────────────────────────────────────

#[tokio::test]
async fn get_pull_request_returns_detail() {
    let server = MockServer::start();

    let m = server.mock(|when, then| {
        when.method(GET)
            .path("/repos/valkey-io/valkey/pulls/2095")
            .header("authorization", "Bearer test-token");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!({
                "number": 2095,
                "title": "Allow shrinking hashtables",
                "body": "Motivation",
                "state": "open",
                "user": { "login": "Fusl", "id": 42 },
                "head": { "ref": "shrink", "sha": "c16727d6", "repo": null },
                "base": { "ref": "unstable", "sha": "deadbeef", "repo": null },
                "html_url": "https://github.com/valkey-io/valkey/pull/2095",
                "draft": false,
                "additions": 50,
                "deletions": 20,
                "changed_files": 3
            }));
    });

    let pr = client_for(&server)
        .get_pull_request("valkey-io", "valkey", 2095)
        .await
        .expect("get_pull_request should succeed");

    assert_eq!(pr.number, 2095);
    assert_eq!(pr.head.ref_name, "shrink");
    assert_eq!(pr.additions, Some(50));
    assert_eq!(pr.changed_files, Some(3));
    assert!(pr.base.repo.is_none());
    m.assert();
}

// ── list_pull_request_files ───────────────────────────────────────────────────

#[tokio::test]
async fn list_pull_request_files_returns_file_list() {
    let server = MockServer::start();

    let m = server.mock(|when, then| {
        when.method(GET)
            .path("/repos/valkey-io/valkey/pulls/2095/files")
            .query_param("per_page", "100")
            .query_param("page", "1")
            .header("authorization", "Bearer test-token");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([
                {
                    "filename": "src/hashtable.c",
                    "status": "modified",
                    "additions": 10,
                    "deletions": 2,
                    "changes": 12,
                    "sha": "abc",
                    "blob_url": "https://github.com/o/r/blob/sha/src/hashtable.c",
                    "raw_url": "https://github.com/o/r/raw/sha/src/hashtable.c",
                    "patch": "@@ -633,6 +633,14 @@"
                },
                {
                    "filename": "binary.bin",
                    "status": "modified",
                    "additions": 0,
                    "deletions": 0,
                    "changes": 0,
                    "sha": "def",
                    "blob_url": "x",
                    "raw_url": "y"
                }
            ]));
    });

    let files = client_for(&server)
        .list_pull_request_files("valkey-io", "valkey", 2095, 100, 1)
        .await
        .expect("list_pull_request_files should succeed");

    assert_eq!(files.len(), 2);
    assert_eq!(files[0].path, "src/hashtable.c");
    assert_eq!(files[0].additions, 10);
    assert!(files[0].patch.as_ref().unwrap().contains("@@"));
    // Binary file — no patch field.
    assert!(files[1].patch.is_none());
    m.assert();
}

// ── list_pull_request_review_comments ────────────────────────────────────────

#[tokio::test]
async fn list_pull_request_review_comments_returns_comment_list() {
    let server = MockServer::start();

    let m = server.mock(|when, then| {
        when.method(GET)
            .path("/repos/valkey-io/valkey/pulls/2095/comments")
            .query_param("per_page", "50")
            .query_param("page", "1")
            .header("authorization", "Bearer test-token");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([{
                "id": 999,
                "pull_request_review_id": 555,
                "path": "src/hashtable.c",
                "line": 633,
                "side": "RIGHT",
                "commit_id": "c16727d6",
                "body": "I agree.",
                "user": { "login": "madolson", "id": 1 },
                "html_url": "https://github.com/o/r/pull/2095#r999",
                "created_at": "2025-06-25T18:00:00Z",
                "updated_at": "2025-06-25T18:00:00Z"
            }]));
    });

    let comments = client_for(&server)
        .list_pull_request_review_comments("valkey-io", "valkey", 2095, 50, 1)
        .await
        .expect("list_pull_request_review_comments should succeed");

    assert_eq!(comments.len(), 1);
    assert_eq!(comments[0].path, "src/hashtable.c");
    assert_eq!(comments[0].line, Some(633));
    assert_eq!(comments[0].pull_request_review_id, Some(555));
    m.assert();
}

// ── list_pull_request_reviews ─────────────────────────────────────────────────

#[tokio::test]
async fn list_pull_request_reviews_returns_review_list() {
    let server = MockServer::start();

    let m = server.mock(|when, then| {
        when.method(GET)
            .path("/repos/valkey-io/valkey/pulls/2095/reviews")
            .query_param("per_page", "50")
            .query_param("page", "1")
            .header("authorization", "Bearer test-token");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([{
                "id": 1,
                "user": { "login": "zuiderkwast", "id": 2 },
                "body": null,
                "state": "APPROVED",
                "html_url": "https://github.com/o/r/pull/2095#pullrequestreview-1",
                "commit_id": "c16727d6",
                "submitted_at": "2025-06-25T19:00:00Z"
            }]));
    });

    let reviews = client_for(&server)
        .list_pull_request_reviews("valkey-io", "valkey", 2095, 50, 1)
        .await
        .expect("list_pull_request_reviews should succeed");

    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].state, "APPROVED");
    assert!(reviews[0].body.is_none());
    m.assert();
}

// ── list_pull_request_issue_comments ─────────────────────────────────────────

#[tokio::test]
async fn list_pull_request_issue_comments_returns_comment_list() {
    let server = MockServer::start();

    let m = server.mock(|when, then| {
        when.method(GET)
            .path("/repos/valkey-io/valkey/issues/2095/comments")
            .query_param("per_page", "50")
            .query_param("page", "1")
            .header("authorization", "Bearer test-token");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([{
                "id": 100,
                "body": "LGTM!",
                "html_url": "https://github.com/o/r/pull/2095#issuecomment-100"
            }]));
    });

    let comments = client_for(&server)
        .list_pull_request_issue_comments("valkey-io", "valkey", 2095, 50, 1)
        .await
        .expect("list_pull_request_issue_comments should succeed");

    assert_eq!(comments.len(), 1);
    assert_eq!(comments[0].body, "LGTM!");
    m.assert();
}

// ── create_pull_request_review ────────────────────────────────────────────────

#[tokio::test]
async fn create_pull_request_review_posts_review_and_returns_result() {
    let server = MockServer::start();

    let m = server.mock(|when, then| {
        when.method(POST)
            .path("/repos/valkey-io/valkey/pulls/2095/reviews")
            .header("authorization", "Bearer test-token")
            .json_body_includes(r#"{"event":"COMMENT"}"#);
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!({
                "id": 77,
                "user": { "login": "cairn-bot", "id": 99 },
                "body": "Two nits.",
                "state": "COMMENTED",
                "html_url": "https://github.com/o/r/pull/2095#pullrequestreview-77",
                "commit_id": "c16727d6",
                "submitted_at": "2025-06-25T20:00:00Z"
            }));
    });

    let req = CreatePullRequestReviewRequest {
        commit_id: Some("c16727d6"),
        body: Some("Two nits."),
        event: Some("COMMENT"),
        comments: vec![ReviewCommentInput {
            path: "src/hashtable.c",
            body: "Missing guard.",
            line: Some(633),
            side: Some("RIGHT"),
            start_line: None,
            start_side: None,
            subject_type: None,
        }],
    };

    let review = client_for(&server)
        .create_pull_request_review("valkey-io", "valkey", 2095, &req)
        .await
        .expect("create_pull_request_review should succeed");

    assert_eq!(review.id, 77);
    assert_eq!(review.state, "COMMENTED");
    m.assert();
}

// ── create_pull_request_review_comment ───────────────────────────────────────

#[tokio::test]
async fn create_pull_request_review_comment_posts_single_comment() {
    let server = MockServer::start();

    let m = server.mock(|when, then| {
        when.method(POST)
            .path("/repos/valkey-io/valkey/pulls/2095/comments")
            .header("authorization", "Bearer test-token")
            .json_body_includes(r#"{"path":"src/hashtable.c"}"#)
            .json_body_includes(r#"{"line":633}"#)
            .json_body_includes(r#"{"side":"RIGHT"}"#);
        then.status(201)
            .header("content-type", "application/json")
            .json_body(json!({
                "id": 1001,
                "path": "src/hashtable.c",
                "line": 633,
                "side": "RIGHT",
                "commit_id": "c16727d6",
                "body": "This skips the guard.",
                "user": { "login": "cairn-bot", "id": 99 },
                "html_url": "https://github.com/o/r/pull/2095#r1001",
                "created_at": "2025-06-25T20:00:00Z",
                "updated_at": "2025-06-25T20:00:00Z"
            }));
    });

    let req = CreateReviewCommentRequest {
        body: "This skips the guard.",
        commit_id: Some("c16727d6"),
        path: Some("src/hashtable.c"),
        line: Some(633),
        side: Some("RIGHT"),
        start_line: None,
        start_side: None,
        in_reply_to_id: None,
        subject_type: None,
    };

    let comment = client_for(&server)
        .create_pull_request_review_comment("valkey-io", "valkey", 2095, &req)
        .await
        .expect("create_pull_request_review_comment should succeed");

    assert_eq!(comment.id, 1001);
    assert_eq!(comment.path, "src/hashtable.c");
    assert_eq!(comment.line, Some(633));
    m.assert();
}

#[tokio::test]
async fn create_pull_request_review_comment_posts_reply() {
    // Reply-to path: caller sets in_reply_to_id; GitHub accepts it with
    // only `body` + `in_reply_to_id` and ignores the diff-location
    // fields. The struct lets us send just those two.
    let server = MockServer::start();

    let m = server.mock(|when, then| {
        when.method(POST)
            .path("/repos/valkey-io/valkey/pulls/2095/comments")
            .json_body_includes(r#"{"in_reply_to_id":997}"#)
            .json_body_includes(r#"{"body":"Agreed, fix incoming."}"#);
        then.status(201)
            .header("content-type", "application/json")
            .json_body(json!({
                "id": 1002,
                "path": "src/hashtable.c",
                "line": 633,
                "commit_id": "c16727d6",
                "body": "Agreed, fix incoming.",
                "user": { "login": "cairn-bot", "id": 99 },
                "html_url": "x",
                "created_at": "t0",
                "updated_at": "t0",
                "in_reply_to_id": 997
            }));
    });

    let req = CreateReviewCommentRequest {
        body: "Agreed, fix incoming.",
        in_reply_to_id: Some(997),
        ..Default::default()
    };

    let comment = client_for(&server)
        .create_pull_request_review_comment("valkey-io", "valkey", 2095, &req)
        .await
        .expect("reply should succeed");

    assert_eq!(comment.id, 1002);
    assert_eq!(comment.in_reply_to_id, Some(997));
    m.assert();
}

// ── error path ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn get_pull_request_returns_api_error_on_404() {
    let server = MockServer::start();

    server.mock(|when, then| {
        when.method(GET).path("/repos/valkey-io/valkey/pulls/9999");
        then.status(404)
            .header("content-type", "application/json")
            .json_body(json!({ "message": "Not Found" }));
    });

    let err = client_for(&server)
        .get_pull_request("valkey-io", "valkey", 9999)
        .await
        .expect_err("should fail with 404");

    match err {
        cairn_github::GitHubError::Api { status, .. } => assert_eq!(status, 404),
        other => panic!("unexpected error variant: {other:?}"),
    }
}
