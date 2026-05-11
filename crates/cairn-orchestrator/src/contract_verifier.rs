//! RFC 032 PR-3: contract verifiers.
//!
//! [`verify_contract`] is the single dispatch entry point the gate
//! (PR-4) will call right before accepting `complete_run`. Each variant
//! of [`cairn_domain::CompletionContract`] has its own verifier that
//! returns either the extracted [`cairn_domain::ContractVerifiedOutput`]
//! (for structured propagation into `step_history.verified_output`) or
//! a typed rejection ([`VerifierRejection`]). The rejection's stable
//! [`cairn_domain::ContractRejectionCode`] is all that reaches
//! `step_history`; the `operator_trace` is a `tracing::warn!`-only
//! payload that never crosses the tenant boundary (RFC 032 §3.1).
//!
//! # Implemented invariants (RFC 032 §4)
//!
//! 1. **Tenant scoping FIRST.** The [`pull_request`] verifier asks
//!    [`ProjectRepoAccessQuery::contains_repo`] *before* any GitHub
//!    call. A repo outside the run's project allowlist rejects with
//!    [`ContractRejectionCode::PrNotInProjectAllowlist`]; no
//!    cross-tenant read is possible.
//! 2. **Layered reqwest timeouts.** The PR verifier wraps the entire
//!    GitHub call chain in `tokio::time::timeout(Duration::from_secs(5),
//!    …)`. The per-reqwest-client 3 s timeout is a property of the
//!    [`cairn_github::GitHubClient`] passed in by the caller (installed
//!    by the integration boot in PR-4); adding a mandatory new client
//!    here is scope creep — the 5 s wall-clock is the load-bearing
//!    safety bound.
//! 3. **File verifier uses a symlink-safe walk.** `fs::symlink_metadata`
//!    is called on *each* path component under the declared
//!    `working_dir`; a symlink anywhere in the chain rejects with
//!    [`ContractRejectionCode::FileSymlinkTraversal`]. `fs::canonicalize`
//!    is never called — it silently follows symlinks.
//! 4. **Diagnostic redaction.** [`VerifierRejection::operator_trace`]
//!    carries free-form context for a `tracing::warn!` call site; the
//!    LLM-facing surface only sees the stable snake_case code. The
//!    `operator_trace` is intentionally *not* equal to the `Display`
//!    form of the code.
//!
//! # Not-yet-implemented verifiers
//!
//! * [`CompletionContract::Structured`] — Phase 2. Variant is valid
//!   in the domain today but [`verify_contract`] returns
//!   [`ContractRejectionCode::NotImplemented`] when it sees one.
//! * [`CompletionContract::ExternalState`] — Phase 3 (webhook-first
//!   confirmation). Same "return `NotImplemented`" treatment.

use std::path::{Path, PathBuf};
use std::time::Duration;

use cairn_domain::{
    BoundedRegex, CompletionContract, ContractRejectionCode, ContractVerifiedOutput,
    FileRequirement, ProjectKey, RelPath, RunId,
};
use cairn_github::{GitHubClient, GitHubError};

// ── Public surface ────────────────────────────────────────────────────────────

/// Context every verifier consumes. All references borrowed from the
/// caller (the gate, PR-4). The verifier never mutates anything: the
/// verification step is pure "is this final_answer consistent with the
/// declared contract?"
pub struct VerifierContext<'a> {
    /// The `final_answer` text the LLM passed to `complete_run`. For
    /// the `Structured` verifier (Phase 2) this would be parsed as
    /// JSON; for every Phase 1 verifier it is treated as opaque text.
    pub final_answer: &'a str,
    /// The run whose `complete_run` is being gated. Only used for
    /// `tracing::warn!(run_id = %ctx.run_id)` call sites.
    pub run_id: &'a RunId,
    /// The run's project scope. Passed to
    /// [`ProjectRepoAccessQuery::contains_repo`] for the PR verifier's
    /// tenant-scope check.
    pub project: &'a ProjectKey,
    /// Absolute path to the run's resolved workspace. The file
    /// verifier joins each declared `RelPath` under this root and
    /// walks component-by-component with `fs::symlink_metadata`.
    pub working_dir: &'a Path,
    /// Query object the PR verifier consults *before* any GitHub call.
    /// Kept as a trait object so the gate (PR-4) can inject a thin
    /// adapter over the existing `ProjectRepoAccessService` without
    /// forcing this crate to take a `cairn-workspace` dependency.
    pub project_repo_access: &'a dyn ProjectRepoAccessQuery,
    /// GitHub App client, `None` when no App install is wired for the
    /// run's project. With the PR verifier active, a `None` client
    /// means we cannot check the PR; the verifier rejects with
    /// [`ContractRejectionCode::VerifierUnavailable`] so the run is
    /// never silently waved through without the declared check.
    pub github_client: Option<&'a GitHubClient>,
}

/// Synchronous, non-awaiting query the PR verifier uses to enforce
/// tenant scope before any external call. The implementation in PR-4
/// wraps the workspace crate's `ProjectRepoAccessService` (which
/// exposes an `async fn is_allowed` that doesn't actually await —
/// the wrapper keeps the verifier's signature sync so the dispatch
/// body stays easy to reason about).
pub trait ProjectRepoAccessQuery: Send + Sync {
    /// Returns `true` iff `repo` (`owner/name`) is on the project's
    /// allowlist. On *any* doubt (lock poisoned, repo parse fail, etc.)
    /// the implementation MUST return `false` — the verifier's
    /// contract is "default-deny on cross-tenant reads."
    fn contains_repo(&self, project: &ProjectKey, repo: &str) -> bool;
}

/// Rejection surfaced by a verifier.
///
/// The gate (PR-4) emits the `code` into step_history (wire-stable,
/// snake_case) and logs the `operator_trace` at `tracing::warn!` with
/// `run_id` + verifier name in scope. The two fields are structurally
/// distinct so an accidental log-the-code-as-operator-trace regression
/// shows up in the cross-check test (`operator_trace` is never the
/// `Debug`/`Display` form of the `code`).
#[derive(Debug, Clone)]
pub struct VerifierRejection {
    /// Wire-stable snake_case code. Reaches the LLM (via step_history)
    /// and operator dashboards. See [`ContractRejectionCode`] for the
    /// full variant list.
    pub code: ContractRejectionCode,
    /// Operator-only free-form context. Goes to `tracing::warn!`
    /// alongside `run_id`, never to step_history, never to the LLM.
    /// May carry filesystem paths, PR URLs, or numbers the gate does
    /// NOT want exposed across the tenant boundary (other tenants'
    /// run IDs could leak via ill-considered messages).
    pub operator_trace: String,
}

impl VerifierRejection {
    fn new(code: ContractRejectionCode, operator_trace: impl Into<String>) -> Self {
        Self {
            code,
            operator_trace: operator_trace.into(),
        }
    }
}

/// Single dispatch function the gate (PR-4) will call. Routes to the
/// per-variant verifier and propagates the typed `Ok` / `Err`.
pub async fn verify_contract(
    contract: &CompletionContract,
    ctx: &VerifierContext<'_>,
) -> Result<ContractVerifiedOutput, VerifierRejection> {
    match contract {
        CompletionContract::ProseNonEmpty => prose_non_empty(ctx),
        CompletionContract::Prose {
            min_chars,
            min_citations,
        } => prose(ctx, *min_chars, *min_citations),
        CompletionContract::File { paths } => file(ctx, paths).await,
        CompletionContract::PullRequest {
            expected_repo,
            expected_head_branch,
            must_be_open,
        } => {
            pull_request(
                ctx,
                expected_repo.as_deref(),
                expected_head_branch.as_ref(),
                *must_be_open,
            )
            .await
        }
        CompletionContract::Structured { .. } => Err(VerifierRejection::new(
            ContractRejectionCode::NotImplemented,
            "Structured verifier is Phase 2; not yet wired",
        )),
        CompletionContract::ExternalState { .. } => Err(VerifierRejection::new(
            ContractRejectionCode::NotImplemented,
            "ExternalState verifier is Phase 3; not yet wired",
        )),
    }
}

// ── ProseNonEmpty ────────────────────────────────────────────────────────────

fn prose_non_empty(ctx: &VerifierContext<'_>) -> Result<ContractVerifiedOutput, VerifierRejection> {
    if ctx.final_answer.chars().any(|c| !c.is_whitespace()) {
        Ok(ContractVerifiedOutput::ProseNonEmpty)
    } else {
        Err(VerifierRejection::new(
            ContractRejectionCode::ProseEmpty,
            "final_answer has no non-whitespace characters",
        ))
    }
}

// ── Prose ─────────────────────────────────────────────────────────────────────

fn prose(
    ctx: &VerifierContext<'_>,
    min_chars: u32,
    min_citations: u32,
) -> Result<ContractVerifiedOutput, VerifierRejection> {
    // Empty rejects first so operator traces stay specific.
    if !ctx.final_answer.chars().any(|c| !c.is_whitespace()) {
        return Err(VerifierRejection::new(
            ContractRejectionCode::ProseEmpty,
            "final_answer has no non-whitespace characters",
        ));
    }
    let actual_chars = ctx.final_answer.chars().count();
    if (actual_chars as u64) < min_chars as u64 {
        return Err(VerifierRejection::new(
            ContractRejectionCode::ProseTooShort,
            format!(
                "final_answer has {actual_chars} chars; contract requires at least {min_chars}"
            ),
        ));
    }
    let citations = count_http_urls(ctx.final_answer);
    if citations < min_citations {
        return Err(VerifierRejection::new(
            ContractRejectionCode::ProseInsufficientCitations,
            format!(
                "final_answer has {citations} URL citations; contract requires at least \
                 {min_citations}"
            ),
        ));
    }
    Ok(ContractVerifiedOutput::Prose {
        citations_resolved: citations,
    })
}

/// Count tokens that start with `http://` or `https://` at a word
/// boundary AND whose authority starts with an ASCII alphanumeric —
/// i.e. something at least superficially parseable as a URL. We
/// deliberately do NOT use the `regex` crate over arbitrary LLM
/// output here: the char-by-char scan matches #831's sentinel-scan
/// strategy (bounded per-char work, no backtrack or catastrophic
/// patterns) and is easier to audit.
///
/// Walks via `char_indices()` so all slice offsets land on UTF-8
/// code-point boundaries — a non-ASCII prefix like `"日本 http://x"`
/// must not panic the scanner.
///
/// PR-3 scope note: "resolvable" downgrades to "parseable as a URL"
/// per RFC 032 §4. The citation verifier does NOT issue HEAD requests
/// — doing so would require a reqwest client + per-citation timeout
/// budget + bounded concurrency which exceeds the 5 s wall clock.
/// Phase 2 may upgrade this to a live resolve under a per-citation
/// budget.
fn count_http_urls(text: &str) -> u32 {
    let mut count: u32 = 0;
    let mut iter = text.char_indices().peekable();
    let mut prev_char: Option<char> = None;
    while let Some(&(i, _)) = iter.peek() {
        // Word-boundary check: start of text OR previous char is NOT
        // an alphanumeric (unicode-aware — CJK + Cyrillic + … count
        // as word chars, matching Unicode's `\w` in the `regex`
        // crate). Keeps us from matching "nothttp://x" AND
        // "日本語http://x" as URL starts.
        let at_word_start = prev_char.is_none_or(|c| !c.is_alphanumeric());
        if !at_word_start {
            let (_, c) = iter.next().expect("peeked");
            prev_char = Some(c);
            continue;
        }
        let rest = &text[i..];
        let scheme_len = if rest.starts_with("https://") {
            8
        } else if rest.starts_with("http://") {
            7
        } else {
            let (_, c) = iter.next().expect("peeked");
            prev_char = Some(c);
            continue;
        };
        // Require the first authority char to be ASCII alphanumeric
        // so tokens like `https://)` or `https:// trailing` don't
        // inflate the count. RFC 3986 authority can also start with
        // digits or letters, never punctuation — this matches the
        // "parseable as URL" contract in count_http_urls's doc.
        let after_scheme = &rest[scheme_len..];
        let authority_ok = after_scheme
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric());
        if !authority_ok {
            // Skip past the scheme colon and continue — the `://`
            // cannot itself be a URL token.
            let new_byte_cursor = i + scheme_len;
            prev_char = Some(':');
            while let Some(&(j, _)) = iter.peek() {
                if j >= new_byte_cursor {
                    break;
                }
                iter.next();
            }
            continue;
        }
        count = count.saturating_add(1);
        // Advance past the URL body (up to next whitespace).
        let body_bytes: usize = after_scheme
            .char_indices()
            .find(|(_, c)| c.is_whitespace())
            .map(|(idx, _)| idx)
            .unwrap_or(after_scheme.len());
        let new_byte_cursor = i + scheme_len + body_bytes;
        // Fast-forward the peekable to new_byte_cursor, tracking the
        // last consumed char for the next word-boundary decision.
        while let Some(&(j, _)) = iter.peek() {
            if j >= new_byte_cursor {
                break;
            }
            let (_, c) = iter.next().expect("peeked");
            prev_char = Some(c);
        }
    }
    count
}

// ── File ──────────────────────────────────────────────────────────────────────

async fn file(
    ctx: &VerifierContext<'_>,
    paths: &[FileRequirement],
) -> Result<ContractVerifiedOutput, VerifierRejection> {
    let mut verified: Vec<String> = Vec::with_capacity(paths.len());
    for req in paths {
        let resolved = resolve_symlink_safe(ctx.working_dir, &req.path)?;
        // Re-stat via `symlink_metadata` (NOT `metadata`) so a
        // TOCTOU window between the walk and this stat cannot let a
        // swapped-in symlink escape `working_dir`. `metadata` follows
        // symlinks; we refuse that transparently-unsafe API on this
        // path.
        let md = std::fs::symlink_metadata(&resolved).map_err(|e| {
            VerifierRejection::new(
                ContractRejectionCode::FileMissing,
                format!("stat {}: {e}", resolved.display()),
            )
        })?;
        if md.file_type().is_symlink() {
            // A racing writer swapped the final component to a
            // symlink after the walk. Treat as a traversal attempt.
            return Err(VerifierRejection::new(
                ContractRejectionCode::FileSymlinkTraversal,
                format!(
                    "{} became a symlink between walk and stat (TOCTOU); refusing",
                    resolved.display()
                ),
            ));
        }
        if !md.is_file() {
            return Err(VerifierRejection::new(
                ContractRejectionCode::FileMissing,
                format!("{} is not a regular file", resolved.display()),
            ));
        }
        if let Some(max) = req.max_bytes {
            if md.len() > max {
                return Err(VerifierRejection::new(
                    ContractRejectionCode::FileExceedsMaxBytes,
                    format!(
                        "{} size {} > contract max {}",
                        resolved.display(),
                        md.len(),
                        max
                    ),
                ));
            }
        }
        if let Some(pattern) = &req.contains_regex {
            if !file_contains_regex(&resolved, pattern).await? {
                return Err(VerifierRejection::new(
                    ContractRejectionCode::FileRegexNoMatch,
                    format!(
                        "{} contents did not match regex `{}`",
                        resolved.display(),
                        pattern.as_str()
                    ),
                ));
            }
        }
        verified.push(req.path.to_string());
    }
    Ok(ContractVerifiedOutput::File { paths: verified })
}

/// Resolve `rel` under `root` with a component-by-component walk that
/// rejects on any symlink encountered in the chain. Returns the
/// resolved `PathBuf` on success.
///
/// This is the RFC 032 §4.2 "symlink-safe walk" in one function.
/// `fs::canonicalize` is intentionally NOT used — it silently follows
/// symlinks, defeating the confinement check. `std::fs` is used in
/// synchronous form here because (a) the operation is a handful of
/// stat syscalls, not megabyte IO, and (b) staying sync keeps the
/// error branch unambiguous (no `spawn_blocking` wrapper).
fn resolve_symlink_safe(root: &Path, rel: &RelPath) -> Result<PathBuf, VerifierRejection> {
    // Guard: `root` itself must not be a symlink or we'd unknowingly
    // escape the declared working_dir.
    match std::fs::symlink_metadata(root) {
        Ok(md) if md.file_type().is_symlink() => {
            return Err(VerifierRejection::new(
                ContractRejectionCode::FileSymlinkTraversal,
                format!("working_dir {} is itself a symlink", root.display()),
            ));
        }
        Ok(_) => {}
        Err(e) => {
            return Err(VerifierRejection::new(
                ContractRejectionCode::FileMissing,
                format!("stat working_dir {}: {e}", root.display()),
            ));
        }
    }

    let mut current = root.to_path_buf();
    for component in rel.components() {
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(md) if md.file_type().is_symlink() => {
                return Err(VerifierRejection::new(
                    ContractRejectionCode::FileSymlinkTraversal,
                    format!(
                        "component `{component}` under working_dir resolves to a symlink \
                         (path {})",
                        current.display()
                    ),
                ));
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(VerifierRejection::new(
                    ContractRejectionCode::FileMissing,
                    format!("{} does not exist", current.display()),
                ));
            }
            Err(e) => {
                return Err(VerifierRejection::new(
                    ContractRejectionCode::FileMissing,
                    format!("stat {}: {e}", current.display()),
                ));
            }
        }
    }
    Ok(current)
}

/// Stream a file in 16 KiB chunks, compile the regex once, and return
/// `true` as soon as a chunk matches. A single match anywhere in the
/// file satisfies the `contains_regex` check.
///
/// Chunk boundaries can split a multi-byte UTF-8 sequence or a regex
/// match — so we keep a sliding-tail of the previous chunk's last
/// `TAIL_OVERLAP` bytes (chosen ≥ any realistic regex match span
/// operators would set here). This gives the same semantics as a
/// "regex against whole file" on files small enough that no sliding
/// tail matters (≤ 16 KiB) and bounded false-negative risk otherwise:
/// the regex crate's DFA size_limit (256 KiB on `BoundedRegex`) means
/// a single match is strictly shorter than any plausible tail we pick.
async fn file_contains_regex(
    path: &Path,
    pattern: &BoundedRegex,
) -> Result<bool, VerifierRejection> {
    use tokio::io::AsyncReadExt as _;

    // Re-compile under the same caps `BoundedRegex::try_new` used.
    // `BoundedRegex` is validated at construction time so a failure
    // here is an internal error (verifier couldn't run), not a
    // content mismatch — reject with VerifierUnavailable so operators
    // can tell "no match" apart from "verifier broken."
    let compiled = regex::RegexBuilder::new(pattern.as_str())
        .size_limit(64 * 1024)
        .dfa_size_limit(256 * 1024)
        .build()
        .map_err(|e| {
            VerifierRejection::new(
                ContractRejectionCode::VerifierUnavailable,
                format!(
                    "regex re-compile at verifier time failed (should have been caught by \
                     BoundedRegex::try_new at accept time): {e}"
                ),
            )
        })?;

    // Open with `O_NOFOLLOW` on unix so a racing symlink-swap between
    // the walk and the open cannot escape `working_dir`. Without this
    // flag, `File::open` transparently follows symlinks — exactly the
    // TOCTOU escape path the walk was trying to close.
    let mut file = open_nofollow(path).await?;

    const CHUNK: usize = 16 * 1024;
    // Tail carry between reads — covers regex matches that straddle a
    // chunk boundary. `BOUNDED_REGEX_SOURCE_MAX` is 1 KiB; a 2 KiB tail
    // is an order-of-magnitude safety margin with modest memory cost.
    const TAIL_OVERLAP: usize = 2 * 1024;
    let mut buf: Vec<u8> = Vec::with_capacity(CHUNK + TAIL_OVERLAP);
    let mut tail: Vec<u8> = Vec::new();
    let mut read_buf = vec![0u8; CHUNK];

    loop {
        let n = file.read(&mut read_buf).await.map_err(|e| {
            // Read I/O errors mean we couldn't execute the check
            // (permission, transient I/O) — not "content didn't
            // match." Report as VerifierUnavailable.
            VerifierRejection::new(
                ContractRejectionCode::VerifierUnavailable,
                format!("read {}: {e}", path.display()),
            )
        })?;
        if n == 0 {
            break;
        }
        buf.clear();
        buf.extend_from_slice(&tail);
        buf.extend_from_slice(&read_buf[..n]);
        // `from_utf8_lossy` replaces invalid bytes with the replacement
        // char. Regex will then scan the lossy form; a contract that
        // requires an exact byte sequence through invalid UTF-8 is
        // out of scope for a text-oriented file verifier.
        let chunk_text = String::from_utf8_lossy(&buf);
        if compiled.is_match(&chunk_text) {
            return Ok(true);
        }
        // Carry the last TAIL_OVERLAP bytes so the next iteration can
        // match across the boundary.
        if buf.len() > TAIL_OVERLAP {
            tail.clear();
            tail.extend_from_slice(&buf[buf.len() - TAIL_OVERLAP..]);
        } else {
            tail.clear();
            tail.extend_from_slice(&buf);
        }
    }
    Ok(false)
}

/// Open `path` with `O_NOFOLLOW` on unix so a symlink swapped in
/// between the walk and the open cannot escape `working_dir`. On
/// non-unix platforms falls back to `tokio::fs::File::open`; the
/// verifier's primary runtime is linux and this is the load-bearing
/// platform.
#[cfg(unix)]
async fn open_nofollow(path: &Path) -> Result<tokio::fs::File, VerifierRejection> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let std_file = tokio::task::spawn_blocking({
        let path = path.to_path_buf();
        move || {
            std::fs::OpenOptions::new()
                .read(true)
                // `libc::O_NOFOLLOW`. Avoid pulling in the `libc`
                // crate — the constant is stable across linux libcs
                // and the nix/macos values match. 0x20000 (linux),
                // 0x100 (darwin). Use `libc` if we add it as a dep
                // later; for now, pull through the std constant.
                .custom_flags(o_nofollow_flag())
                .open(&path)
        }
    })
    .await
    .map_err(|e| {
        VerifierRejection::new(
            ContractRejectionCode::VerifierUnavailable,
            format!("spawn_blocking panic opening {}: {e}", path.display()),
        )
    })?
    .map_err(|e| {
        // On `ELOOP` (symlink encountered with O_NOFOLLOW) the kernel
        // returns `TooManyLinks` (linux) / `FilesystemLoop` in newer
        // Rust. Treat both as symlink traversal.
        if matches!(e.raw_os_error(), Some(40)) {
            return VerifierRejection::new(
                ContractRejectionCode::FileSymlinkTraversal,
                format!(
                    "{} is a symlink at open time (ELOOP with O_NOFOLLOW); refusing",
                    path.display()
                ),
            );
        }
        VerifierRejection::new(
            ContractRejectionCode::VerifierUnavailable,
            format!("open {}: {e}", path.display()),
        )
    })?;
    Ok(tokio::fs::File::from_std(std_file))
}

#[cfg(not(unix))]
async fn open_nofollow(path: &Path) -> Result<tokio::fs::File, VerifierRejection> {
    tokio::fs::File::open(path).await.map_err(|e| {
        VerifierRejection::new(
            ContractRejectionCode::VerifierUnavailable,
            format!("open {}: {e}", path.display()),
        )
    })
}

/// Platform-appropriate `O_NOFOLLOW` flag. Pulled through a named
/// helper so a future `libc` dep can replace the literals without
/// touching the call site.
#[cfg(unix)]
fn o_nofollow_flag() -> i32 {
    // Linux: 0x20000. Darwin/BSD: 0x0100. Both kernels surface
    // ELOOP (errno 40 on linux, 62 on darwin) when the final
    // component of the open path is a symlink.
    #[cfg(target_os = "linux")]
    {
        0x20000
    }
    #[cfg(not(target_os = "linux"))]
    {
        0x0100
    }
}

// ── PullRequest ──────────────────────────────────────────────────────────────

async fn pull_request(
    ctx: &VerifierContext<'_>,
    expected_repo: Option<&str>,
    expected_head_branch: Option<&BoundedRegex>,
    must_be_open: bool,
) -> Result<ContractVerifiedOutput, VerifierRejection> {
    // 1. Locate the PR URL in the final_answer. Prefer a structured
    //    `pr_url` field (operators can ship JSON payloads); fall back
    //    to a GitHub PR URL pattern extracted from free-form text.
    let pr_url = extract_pr_url(ctx.final_answer).ok_or_else(|| {
        VerifierRejection::new(
            ContractRejectionCode::PrUrlMissing,
            "no PR URL found in final_answer (neither `pr_url` JSON field nor \
             `https://github.com/<owner>/<repo>/pull/<n>` substring)",
        )
    })?;

    // 2. Parse owner / repo / number out of the URL. A non-matching
    //    URL shape is PrUrlMalformed so operators can distinguish
    //    "the model forgot the PR" from "the model pasted a garbled
    //    URL".
    let (owner, repo, number) = parse_github_pr_url(&pr_url).ok_or_else(|| {
        VerifierRejection::new(
            ContractRejectionCode::PrUrlMalformed,
            format!(
                "final_answer pr_url `{pr_url}` is not a github.com pull-request URL in the \
                 shape https://github.com/<owner>/<repo>/pull/<n>"
            ),
        )
    })?;
    let owner_repo = format!("{owner}/{repo}");

    // 3. RFC 032 INVARIANT 1: tenant-scope first. Reject BEFORE any
    //    GitHub call so a cross-tenant PR URL cannot trigger a read
    //    against a repo the run isn't authorised for.
    if !ctx
        .project_repo_access
        .contains_repo(ctx.project, &owner_repo)
    {
        return Err(VerifierRejection::new(
            ContractRejectionCode::PrNotInProjectAllowlist,
            format!(
                "PR {pr_url} targets `{owner_repo}` which is not in the project's repo \
                 allowlist; refusing any GitHub read"
            ),
        ));
    }

    // 4. If the operator pinned a specific expected_repo, enforce it.
    //    This is a tighter check than the allowlist (the project may
    //    own many repos) and reports a distinct code
    //    (`PrRepoMismatch`) so the model can tell "wrong repo" apart
    //    from both "cross-tenant attempt" (`PrNotInProjectAllowlist`)
    //    and "wrong branch" (`PrHeadBranchMismatch`).
    if let Some(expected) = expected_repo {
        if !eq_ignore_ascii_case_trim(expected, &owner_repo) {
            return Err(VerifierRejection::new(
                ContractRejectionCode::PrRepoMismatch,
                format!(
                    "contract expected_repo `{expected}` does not match PR repo `{owner_repo}`"
                ),
            ));
        }
    }

    // 5. Without a GitHub App client wired we cannot verify state; be
    //    explicit rather than silent-pass.
    let client = ctx.github_client.ok_or_else(|| {
        VerifierRejection::new(
            ContractRejectionCode::VerifierUnavailable,
            "PullRequest verifier requires an installed cairn-github client; none available \
             for this run",
        )
    })?;

    // 6. Single 5 s wall-clock covering DNS + TLS + GitHub response.
    let pr = tokio::time::timeout(
        Duration::from_secs(5),
        client.get_pull_request(owner, repo, number),
    )
    .await
    .map_err(|_| {
        VerifierRejection::new(
            ContractRejectionCode::VerifierTimeout,
            format!("GitHub get_pull_request({owner_repo} #{number}) exceeded 5 s wall clock"),
        )
    })?
    .map_err(|e| map_github_error(&owner_repo, number, e))?;

    if must_be_open && pr.state != "open" {
        return Err(VerifierRejection::new(
            ContractRejectionCode::PrNotOpen,
            format!(
                "PR {owner_repo}#{number} state=`{}` (must_be_open=true)",
                pr.state
            ),
        ));
    }

    if let Some(branch_regex) = expected_head_branch {
        // `BoundedRegex::try_new` enforces compile at contract-accept
        // time, so a failure here is an internal error (drift between
        // accept-time and verify-time caps, or a panic in the regex
        // engine). Report as VerifierUnavailable so operators can
        // distinguish "branch didn't match" from "verifier broken".
        let compiled = regex::RegexBuilder::new(branch_regex.as_str())
            .size_limit(64 * 1024)
            .dfa_size_limit(256 * 1024)
            .build()
            .map_err(|e| {
                VerifierRejection::new(
                    ContractRejectionCode::VerifierUnavailable,
                    format!(
                        "expected_head_branch regex re-compile at verifier time failed \
                         (should have been caught at accept time): {e}"
                    ),
                )
            })?;
        if !compiled.is_match(&pr.head.ref_name) {
            return Err(VerifierRejection::new(
                ContractRejectionCode::PrHeadBranchMismatch,
                format!(
                    "PR {owner_repo}#{number} head branch `{}` does not match regex `{}`",
                    pr.head.ref_name,
                    branch_regex.as_str()
                ),
            ));
        }
    }

    Ok(ContractVerifiedOutput::PullRequest {
        pr_url,
        head_sha: pr.head.sha,
    })
}

/// Extract a PR URL from `final_answer`. Strategy:
///
/// 1. If `final_answer` parses as JSON AND the top-level value has a
///    string field `pr_url`, return that. Operators shipping a
///    structured completion summary get precedence.
/// 2. Otherwise scan free-form text for the first
///    `https://github.com/<owner>/<repo>/pull/<n>` substring. The
///    scan uses a bounded regex with no backtracking, so an
///    adversarial `final_answer` can't trigger catastrophic matching.
fn extract_pr_url(final_answer: &str) -> Option<String> {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(final_answer) {
        if let Some(s) = v.get("pr_url").and_then(|f| f.as_str()) {
            if !s.is_empty() {
                return Some(s.to_owned());
            }
        }
    }
    // RE2-safe pattern: no lookaround, no backrefs, bounded alt.
    // Cached in a `OnceLock` so every `complete_run` gate call reuses
    // the compiled automaton instead of paying re-compile cost; the
    // pattern is a literal string with no operator-supplied input so
    // a single-shot compile at process start is sufficient.
    static PR_URL_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = PR_URL_RE.get_or_init(|| {
        regex::Regex::new(r"https://github\.com/[A-Za-z0-9_.\-]+/[A-Za-z0-9_.\-]+/pull/\d+")
            .expect("static PR URL regex must compile")
    });
    re.find(final_answer).map(|m| m.as_str().to_owned())
}

/// Parse the canonical GitHub PR URL shape into `(owner, repo, number)`.
/// Returns `None` on any deviation so callers can emit a dedicated
/// `PrUrlMalformed` rejection rather than a cryptic parse error.
fn parse_github_pr_url(url: &str) -> Option<(&str, &str, u64)> {
    let rest = url.strip_prefix("https://github.com/")?;
    let mut parts = rest.splitn(4, '/');
    let owner = parts.next()?;
    let repo = parts.next()?;
    if parts.next()? != "pull" {
        return None;
    }
    let num_and_trailing = parts.next()?;
    // Strip any `#discussion_r…`, `?…`, or trailing slash.
    let num_str = num_and_trailing.split(['/', '?', '#']).next()?;
    let number = num_str.parse::<u64>().ok()?;
    if owner.is_empty() || repo.is_empty() {
        return None;
    }
    Some((owner, repo, number))
}

fn eq_ignore_ascii_case_trim(a: &str, b: &str) -> bool {
    a.trim().eq_ignore_ascii_case(b.trim())
}

fn map_github_error(owner_repo: &str, number: u64, err: GitHubError) -> VerifierRejection {
    match err {
        GitHubError::Api { status: 404, body } => VerifierRejection::new(
            ContractRejectionCode::PrNotFound,
            format!("GitHub API 404 for {owner_repo}#{number}: {body}"),
        ),
        GitHubError::Api { status, body } => VerifierRejection::new(
            ContractRejectionCode::VerifierUnavailable,
            format!("GitHub API {status} for {owner_repo}#{number}: {body}"),
        ),
        other => VerifierRejection::new(
            ContractRejectionCode::VerifierUnavailable,
            format!("GitHub client error for {owner_repo}#{number}: {other}"),
        ),
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_domain::{ContractSchema, ExternalStateCheck, ProjectId, TenantId, WorkspaceId};
    use httpmock::prelude::*;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use tempfile::TempDir;

    // ── Harness ──────────────────────────────────────────────────────────

    fn test_project() -> ProjectKey {
        ProjectKey {
            tenant_id: TenantId::from("t1".to_owned()),
            workspace_id: WorkspaceId::from("w1".to_owned()),
            project_id: ProjectId::from("p1".to_owned()),
        }
    }

    fn test_run_id() -> RunId {
        RunId::from("run-verifier-test".to_owned())
    }

    /// Mock `ProjectRepoAccessQuery` backed by an in-memory allowlist.
    /// Tests control tenant-scope decisions by pre-populating the set.
    #[derive(Default)]
    struct StubAccess {
        allowed: Mutex<HashSet<(ProjectKey, String)>>,
    }

    impl StubAccess {
        fn allow(&self, project: &ProjectKey, repo: &str) {
            self.allowed
                .lock()
                .unwrap()
                .insert((project.clone(), repo.to_owned()));
        }
    }

    impl ProjectRepoAccessQuery for StubAccess {
        fn contains_repo(&self, project: &ProjectKey, repo: &str) -> bool {
            self.allowed
                .lock()
                .unwrap()
                .contains(&(project.clone(), repo.to_owned()))
        }
    }

    /// Spin up an `httpmock` + `GitHubClient` pair so PullRequest
    /// tests can drive live HTTP without touching github.com.
    fn client_for(server: &MockServer) -> GitHubClient {
        let token = cairn_github::InstallationToken::with_static_token("test-token");
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .expect("reqwest client");
        GitHubClient::with_http(token, http).with_base_url(server.base_url())
    }

    fn ctx<'a>(
        final_answer: &'a str,
        run_id: &'a RunId,
        project: &'a ProjectKey,
        working_dir: &'a Path,
        access: &'a dyn ProjectRepoAccessQuery,
        github: Option<&'a GitHubClient>,
    ) -> VerifierContext<'a> {
        VerifierContext {
            final_answer,
            run_id,
            project,
            working_dir,
            project_repo_access: access,
            github_client: github,
        }
    }

    // ── ProseNonEmpty ────────────────────────────────────────────────────

    #[tokio::test]
    async fn prose_non_empty_rejects_empty_string() {
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx("", &run, &project, dir.path(), &access, None);
        let err = verify_contract(&CompletionContract::ProseNonEmpty, &c)
            .await
            .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::ProseEmpty);
    }

    #[tokio::test]
    async fn prose_non_empty_rejects_whitespace_only() {
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx(" \t\n  ", &run, &project, dir.path(), &access, None);
        let err = verify_contract(&CompletionContract::ProseNonEmpty, &c)
            .await
            .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::ProseEmpty);
    }

    #[tokio::test]
    async fn prose_non_empty_accepts_single_character() {
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx("hello", &run, &project, dir.path(), &access, None);
        let out = verify_contract(&CompletionContract::ProseNonEmpty, &c)
            .await
            .unwrap();
        assert_eq!(out, ContractVerifiedOutput::ProseNonEmpty);
    }

    // ── Prose ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn prose_rejects_too_short() {
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx(
            "only a few chars",
            &run,
            &project,
            dir.path(),
            &access,
            None,
        );
        let err = verify_contract(
            &CompletionContract::Prose {
                min_chars: 500,
                min_citations: 0,
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::ProseTooShort);
    }

    #[tokio::test]
    async fn prose_rejects_insufficient_citations() {
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        // Plenty of chars, zero citations.
        let body = "a".repeat(600);
        let c = ctx(&body, &run, &project, dir.path(), &access, None);
        let err = verify_contract(
            &CompletionContract::Prose {
                min_chars: 500,
                min_citations: 2,
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::ProseInsufficientCitations);
    }

    #[tokio::test]
    async fn prose_accepts_and_reports_citation_count() {
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let body = format!(
            "{} see https://example.com/a and https://example.org/b plus http://foo.example/c",
            "a".repeat(600)
        );
        let c = ctx(&body, &run, &project, dir.path(), &access, None);
        let out = verify_contract(
            &CompletionContract::Prose {
                min_chars: 500,
                min_citations: 3,
            },
            &c,
        )
        .await
        .unwrap();
        assert_eq!(
            out,
            ContractVerifiedOutput::Prose {
                citations_resolved: 3
            }
        );
    }

    #[test]
    fn count_http_urls_does_not_match_inside_words() {
        // "nothttp://x" must not count — scheme must be at a
        // word-boundary to qualify as a citation token.
        assert_eq!(count_http_urls("prefixhttps://example.com"), 0);
        // Bare scheme without authority does not count.
        assert_eq!(count_http_urls("https:// trailing"), 0);
        // Authority must start with an alphanumeric — pure
        // punctuation authority is not a URL.
        assert_eq!(count_http_urls("https://) and https://."), 0);
        // Word-boundary match counts.
        assert_eq!(count_http_urls("see https://example.com/x here"), 1);
        // Punctuation is a valid word boundary.
        assert_eq!(count_http_urls("see(https://example.com/x)here"), 1);
    }

    #[test]
    fn count_http_urls_handles_non_ascii_prefix_without_panic() {
        // RFC 032 invariant: the scanner walks via `char_indices`, so
        // a non-ASCII codepoint before an `https://` URL must neither
        // panic (mid-codepoint slice) nor mis-count.
        let text = "日本語 see https://example.com/x あとで";
        assert_eq!(count_http_urls(text), 1);
        // Also verify no panic on a non-ASCII-only input.
        assert_eq!(count_http_urls("日本語のみ"), 0);
        // And a URL immediately after a non-ASCII word-boundary char.
        assert_eq!(count_http_urls("日本語https://example.com"), 0);
        assert_eq!(count_http_urls("日本語 https://example.com"), 1);
    }

    // ── File ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn file_rejects_missing_path() {
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx("done", &run, &project, dir.path(), &access, None);
        let err = verify_contract(
            &CompletionContract::File {
                paths: vec![FileRequirement {
                    path: RelPath::try_new("missing.txt").unwrap(),
                    contains_regex: None,
                    max_bytes: None,
                }],
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::FileMissing);
    }

    #[tokio::test]
    async fn file_accepts_existing_file() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("note.txt");
        std::fs::write(&p, b"hello").unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx("done", &run, &project, dir.path(), &access, None);
        let out = verify_contract(
            &CompletionContract::File {
                paths: vec![FileRequirement {
                    path: RelPath::try_new("note.txt").unwrap(),
                    contains_regex: None,
                    max_bytes: None,
                }],
            },
            &c,
        )
        .await
        .unwrap();
        match out {
            ContractVerifiedOutput::File { paths } => {
                assert_eq!(paths, vec!["note.txt".to_owned()]);
            }
            other => panic!("expected File variant; got {other:?}"),
        }
    }

    #[tokio::test]
    async fn file_rejects_symlink_component() {
        // Symlink layout:
        //   root/link  ->  root/real/
        //   root/real/target.txt
        // Contract asks for "link/target.txt" — must reject at the
        // symlink component, not canonicalize-and-follow.
        let dir = TempDir::new().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        std::fs::write(real.join("target.txt"), b"hi").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, dir.path().join("link")).unwrap();
        #[cfg(not(unix))]
        return; // symlink test requires unix symlink support

        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx("done", &run, &project, dir.path(), &access, None);
        let err = verify_contract(
            &CompletionContract::File {
                paths: vec![FileRequirement {
                    path: RelPath::try_new("link/target.txt").unwrap(),
                    contains_regex: None,
                    max_bytes: None,
                }],
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::FileSymlinkTraversal);
    }

    #[tokio::test]
    async fn file_rejects_over_max_bytes() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("big.bin"), vec![b'x'; 2048]).unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx("done", &run, &project, dir.path(), &access, None);
        let err = verify_contract(
            &CompletionContract::File {
                paths: vec![FileRequirement {
                    path: RelPath::try_new("big.bin").unwrap(),
                    contains_regex: None,
                    max_bytes: Some(1024),
                }],
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::FileExceedsMaxBytes);
    }

    #[tokio::test]
    async fn file_rejects_regex_no_match() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("src.rs"), b"fn not_main() {}").unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx("done", &run, &project, dir.path(), &access, None);
        let err = verify_contract(
            &CompletionContract::File {
                paths: vec![FileRequirement {
                    path: RelPath::try_new("src.rs").unwrap(),
                    contains_regex: Some(BoundedRegex::try_new(r"fn\s+main".to_owned()).unwrap()),
                    max_bytes: None,
                }],
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::FileRegexNoMatch);
    }

    #[tokio::test]
    async fn file_accepts_regex_match() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("src.rs"), b"fn main() { println!(); }").unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx("done", &run, &project, dir.path(), &access, None);
        let out = verify_contract(
            &CompletionContract::File {
                paths: vec![FileRequirement {
                    path: RelPath::try_new("src.rs").unwrap(),
                    contains_regex: Some(BoundedRegex::try_new(r"fn\s+main".to_owned()).unwrap()),
                    max_bytes: None,
                }],
            },
            &c,
        )
        .await
        .unwrap();
        match out {
            ContractVerifiedOutput::File { paths } => assert_eq!(paths, vec!["src.rs".to_owned()]),
            other => panic!("expected File variant; got {other:?}"),
        }
    }

    // ── PullRequest ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn pull_request_rejects_missing_url() {
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx("no pr url here", &run, &project, dir.path(), &access, None);
        let err = verify_contract(
            &CompletionContract::PullRequest {
                expected_repo: None,
                expected_head_branch: None,
                must_be_open: true,
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::PrUrlMissing);
    }

    #[tokio::test]
    async fn pull_request_rejects_non_github_url() {
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        // A structured `pr_url` is present but points at a non-github
        // URL. `extract_pr_url` returns it (JSON-field extraction
        // precedence), then `parse_github_pr_url` rejects it —
        // PrUrlMalformed. The code is distinct from PrUrlMissing so
        // operators can tell "model forgot to ship a URL" apart from
        // "model shipped garbage".
        let body = serde_json::json!({ "pr_url": "https://example.com/not/a/pr" }).to_string();
        let c = ctx(&body, &run, &project, dir.path(), &access, None);
        let err = verify_contract(
            &CompletionContract::PullRequest {
                expected_repo: None,
                expected_head_branch: None,
                must_be_open: true,
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::PrUrlMalformed);
    }

    #[tokio::test]
    async fn pull_request_rejects_cross_tenant_repo() {
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        // access is empty — no repo allowlisted for this project.
        let project = test_project();
        let run = test_run_id();
        let body = "see https://github.com/other-tenant/their-repo/pull/1";
        let c = ctx(body, &run, &project, dir.path(), &access, None);
        let err = verify_contract(
            &CompletionContract::PullRequest {
                expected_repo: None,
                expected_head_branch: None,
                must_be_open: true,
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::PrNotInProjectAllowlist);
    }

    #[tokio::test]
    async fn pull_request_rejects_when_github_client_unavailable() {
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        access.allow(&project, "owner/repo");
        let run = test_run_id();
        let body = "see https://github.com/owner/repo/pull/7";
        let c = ctx(body, &run, &project, dir.path(), &access, None);
        let err = verify_contract(
            &CompletionContract::PullRequest {
                expected_repo: None,
                expected_head_branch: None,
                must_be_open: true,
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::VerifierUnavailable);
    }

    #[tokio::test]
    async fn pull_request_rejects_404() {
        let server = MockServer::start();
        let gh = client_for(&server);
        let m = server.mock(|when, then| {
            when.method(GET).path("/repos/owner/repo/pulls/99");
            then.status(404).body(r#"{"message":"Not Found"}"#);
        });

        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        access.allow(&project, "owner/repo");
        let run = test_run_id();
        let body = "shipped at https://github.com/owner/repo/pull/99";
        let c = ctx(body, &run, &project, dir.path(), &access, Some(&gh));
        let err = verify_contract(
            &CompletionContract::PullRequest {
                expected_repo: None,
                expected_head_branch: None,
                must_be_open: true,
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::PrNotFound);
        m.assert();
    }

    #[tokio::test]
    async fn pull_request_rejects_closed_when_must_be_open() {
        let server = MockServer::start();
        let gh = client_for(&server);
        let m = server.mock(|when, then| {
            when.method(GET).path("/repos/owner/repo/pulls/5");
            then.status(200).json_body(serde_json::json!({
                "number": 5,
                "title": "closed pr",
                "state": "closed",
                "user": {"login": "a", "id": 1},
                "head": {"ref": "feat/x", "sha": "abc123", "repo": null},
                "base": {"ref": "main", "sha": "def", "repo": null},
                "html_url": "https://github.com/owner/repo/pull/5",
                "draft": false,
            }));
        });

        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        access.allow(&project, "owner/repo");
        let run = test_run_id();
        let body = "https://github.com/owner/repo/pull/5";
        let c = ctx(body, &run, &project, dir.path(), &access, Some(&gh));
        let err = verify_contract(
            &CompletionContract::PullRequest {
                expected_repo: None,
                expected_head_branch: None,
                must_be_open: true,
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::PrNotOpen);
        m.assert();
    }

    #[tokio::test]
    async fn pull_request_rejects_repo_mismatch() {
        // Both `owner/repo-a` and `owner/repo-b` are in the project's
        // allowlist, but the contract pinned `owner/repo-a` and the
        // model shipped a PR in `owner/repo-b`. Reject code MUST be
        // `PrRepoMismatch`, distinct from `PrNotInProjectAllowlist`
        // (no tenant violation) and from `PrHeadBranchMismatch` (the
        // branch is irrelevant here).
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        access.allow(&project, "owner/repo-a");
        access.allow(&project, "owner/repo-b");
        let run = test_run_id();
        let body = "https://github.com/owner/repo-b/pull/1";
        let c = ctx(body, &run, &project, dir.path(), &access, None);
        let err = verify_contract(
            &CompletionContract::PullRequest {
                expected_repo: Some("owner/repo-a".to_owned()),
                expected_head_branch: None,
                must_be_open: true,
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::PrRepoMismatch);
    }

    #[tokio::test]
    async fn pull_request_rejects_branch_mismatch() {
        let server = MockServer::start();
        let gh = client_for(&server);
        let m = server.mock(|when, then| {
            when.method(GET).path("/repos/owner/repo/pulls/7");
            then.status(200).json_body(serde_json::json!({
                "number": 7,
                "title": "wrong branch",
                "state": "open",
                "user": {"login": "a", "id": 1},
                "head": {"ref": "hotfix/emergency", "sha": "abc123", "repo": null},
                "base": {"ref": "main", "sha": "def", "repo": null},
                "html_url": "https://github.com/owner/repo/pull/7",
                "draft": false,
            }));
        });

        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        access.allow(&project, "owner/repo");
        let run = test_run_id();
        let body = "https://github.com/owner/repo/pull/7";
        let c = ctx(body, &run, &project, dir.path(), &access, Some(&gh));
        let err = verify_contract(
            &CompletionContract::PullRequest {
                expected_repo: Some("owner/repo".to_owned()),
                expected_head_branch: Some(BoundedRegex::try_new("^feat/.+".to_owned()).unwrap()),
                must_be_open: true,
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::PrHeadBranchMismatch);
        m.assert();
    }

    #[tokio::test]
    async fn pull_request_accepts_happy_path() {
        let server = MockServer::start();
        let gh = client_for(&server);
        let m = server.mock(|when, then| {
            when.method(GET).path("/repos/owner/repo/pulls/42");
            then.status(200).json_body(serde_json::json!({
                "number": 42,
                "title": "ship",
                "state": "open",
                "user": {"login": "a", "id": 1},
                "head": {"ref": "feat/ship-it", "sha": "c0ffee", "repo": null},
                "base": {"ref": "main", "sha": "def", "repo": null},
                "html_url": "https://github.com/owner/repo/pull/42",
                "draft": false,
            }));
        });

        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        access.allow(&project, "owner/repo");
        let run = test_run_id();
        let body = serde_json::json!({
            "summary": "shipped",
            "pr_url": "https://github.com/owner/repo/pull/42",
        })
        .to_string();
        let c = ctx(&body, &run, &project, dir.path(), &access, Some(&gh));
        let out = verify_contract(
            &CompletionContract::PullRequest {
                expected_repo: Some("owner/repo".to_owned()),
                expected_head_branch: Some(BoundedRegex::try_new("^feat/.+".to_owned()).unwrap()),
                must_be_open: true,
            },
            &c,
        )
        .await
        .unwrap();
        match out {
            ContractVerifiedOutput::PullRequest { pr_url, head_sha } => {
                assert_eq!(pr_url, "https://github.com/owner/repo/pull/42");
                assert_eq!(head_sha, "c0ffee");
            }
            other => panic!("expected PullRequest variant; got {other:?}"),
        }
        m.assert();
    }

    // ── Structured / ExternalState stubs ─────────────────────────────────

    #[tokio::test]
    async fn structured_returns_not_implemented() {
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx(r#"{}"#, &run, &project, dir.path(), &access, None);
        let schema = ContractSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
        let err = verify_contract(
            &CompletionContract::Structured {
                schema: Box::new(schema),
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::NotImplemented);
    }

    #[tokio::test]
    async fn external_state_returns_not_implemented() {
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx("done", &run, &project, dir.path(), &access, None);
        let err = verify_contract(
            &CompletionContract::ExternalState {
                check: ExternalStateCheck::GitHubPrMerged {
                    repo: "owner/repo".to_owned(),
                    number: 1,
                },
            },
            &c,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, ContractRejectionCode::NotImplemented);
    }

    // ── Redaction invariant ──────────────────────────────────────────────

    #[tokio::test]
    async fn operator_trace_is_distinct_from_rejection_code() {
        // RFC 032 §3.1: operator_trace is a tracing-only payload; it
        // MUST carry more context than the bare code. A regression
        // where the two become equal means we've accidentally leaked
        // (or lost) diagnostics. Pick one verifier with a rich
        // operator_trace — File's FileMissing — and assert the two
        // forms differ.
        let dir = TempDir::new().unwrap();
        let access = StubAccess::default();
        let project = test_project();
        let run = test_run_id();
        let c = ctx("done", &run, &project, dir.path(), &access, None);
        let err = verify_contract(
            &CompletionContract::File {
                paths: vec![FileRequirement {
                    path: RelPath::try_new("no-such-file.txt").unwrap(),
                    contains_regex: None,
                    max_bytes: None,
                }],
            },
            &c,
        )
        .await
        .unwrap_err();
        let code_json = serde_json::to_string(&err.code).unwrap();
        // operator_trace must NOT equal the wire form of the code, nor
        // a simple `Debug`/`Display` of the code. The code is
        // `file_missing`; the trace references `no-such-file.txt`.
        assert_ne!(err.operator_trace, code_json);
        assert_ne!(err.operator_trace, format!("{:?}", err.code));
        assert!(
            err.operator_trace.contains("no-such-file.txt"),
            "operator_trace should name the failing path; got {}",
            err.operator_trace
        );
    }
}
