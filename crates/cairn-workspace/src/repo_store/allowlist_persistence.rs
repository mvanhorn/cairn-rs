//! Persistence seam for `ProjectRepoAccessService` — closes #556.
//!
//! ## Why this is plugin-layer, not engine-layer
//!
//! The repo allowlist is **not a core runtime invariant** — it's a
//! project-scoped authorization list that a specific integration plugin
//! (today, the GitHub plugin) manages on behalf of operators. Persisting
//! it through the `RuntimeEvent` log would bind it to the engine's
//! event-sourced substrate (pg/sqlite projections, stream replay, SSE
//! contracts) — a substrate designed for *canonical runtime truth*
//! (runs, sessions, approvals, …), not for plugin-owned configuration
//! that is semantically equivalent to "the operator's list of
//! allowlisted repos."
//!
//! Instead, persistence lives *beside* the plugin that produces the
//! state. The GitHub plugin at startup installs a
//! `JsonFileAllowlistStore` at
//! `<CAIRN_PLUGIN_STATE_DIR>/github/allowlist.json`, loads the stored
//! entries into `ProjectRepoAccessService`, and registers a write-
//! through hook so every subsequent `allow`/`revoke` lands in the same
//! JSON file. No event-log involvement; no projection migration; no
//! backend-specific DDL.
//!
//! ## Correctness contract
//!
//! - `load_all` is called once, before the HTTP server accepts traffic.
//! - `record_allow` and `record_revoke` are called synchronously inside
//!   `ProjectRepoAccessService::allow`/`revoke`, *after* the in-memory
//!   map has been updated. If the persistence call fails, the operation
//!   fails — the in-memory update is rolled back so disk + memory stay
//!   consistent.
//! - Implementations must be crash-safe: a `record_*` call that is
//!   interrupted after the in-memory update but before the disk write
//!   completes must not leave the file partially written. The
//!   `JsonFileAllowlistStore` uses tmp-file + atomic rename.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use cairn_domain::ProjectKey;
use serde::{Deserialize, Serialize};

use crate::sandbox::RepoId;

/// Persistence seam consulted by `ProjectRepoAccessService` to survive
/// a cairn-app restart.
///
/// Implementations own the wire format + durability strategy. The
/// service only requires load-once + record-on-write semantics.
pub trait AllowlistPersistence: Send + Sync + std::fmt::Debug {
    /// Operator-facing location of the backing store — threaded
    /// through `RepoStoreError::Io::path` so diagnostic messages name
    /// the file (or endpoint) that failed. `None` for backends without
    /// a meaningful path (e.g. an in-memory test double).
    fn location(&self) -> Option<PathBuf>;

    /// Load every persisted `(project, repo)` entry. Called once at
    /// service construction, before any `allow`/`revoke`.
    ///
    /// Returns `Ok(empty)` on first boot (no file yet).
    ///
    /// Implementations must validate `RepoId` values loaded from disk
    /// and skip (with a `tracing::warn!`) any entry that fails
    /// validation — a hand-edited file must never surface an invalid
    /// `RepoId` that would later panic downstream consumers that
    /// assume the value round-tripped through `RepoId::parse`.
    fn load_all(&self) -> Result<HashMap<ProjectKey, HashSet<RepoId>>, AllowlistPersistenceError>;

    /// Persist an `allow(project, repo)` grant. Called after the
    /// in-memory update has succeeded.
    fn record_allow(
        &self,
        project: &ProjectKey,
        repo_id: &RepoId,
    ) -> Result<(), AllowlistPersistenceError>;

    /// Persist a `revoke(project, repo)` revocation. Called after the
    /// in-memory update has succeeded.
    fn record_revoke(
        &self,
        project: &ProjectKey,
        repo_id: &RepoId,
    ) -> Result<(), AllowlistPersistenceError>;
}

#[derive(Debug)]
pub enum AllowlistPersistenceError {
    Io(io::Error),
    Encoding(String),
}

impl std::fmt::Display for AllowlistPersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error on allowlist persistence: {e}"),
            Self::Encoding(msg) => write!(f, "failed to (de)serialize allowlist state: {msg}"),
        }
    }
}

impl std::error::Error for AllowlistPersistenceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Encoding(_) => None,
        }
    }
}

impl From<io::Error> for AllowlistPersistenceError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

// ── JSON file-backed implementation ──────────────────────────────────

/// On-disk wire format. Keyed by `<tenant>/<workspace>/<project>` triple
/// so the file is human-readable when an operator wants to inspect it.
#[derive(Debug, Default, Serialize, Deserialize)]
struct OnDiskAllowlist {
    /// Schema version for forward compatibility. Currently `1`.
    #[serde(default = "default_version")]
    version: u32,
    /// `"<tenant>/<workspace>/<project>"` → sorted list of repo ids.
    #[serde(default)]
    projects: BTreeMap<String, BTreeSet<String>>,
}

fn default_version() -> u32 {
    1
}

fn project_key_string(project: &ProjectKey) -> String {
    format!(
        "{}/{}/{}",
        project.tenant_id.as_str(),
        project.workspace_id.as_str(),
        project.project_id.as_str()
    )
}

fn parse_project_key(s: &str) -> Option<ProjectKey> {
    let mut parts = s.splitn(3, '/');
    let tenant = parts.next()?;
    let workspace = parts.next()?;
    let project = parts.next()?;
    if tenant.is_empty() || workspace.is_empty() || project.is_empty() {
        return None;
    }
    Some(ProjectKey::new(tenant, workspace, project))
}

/// Plugin-owned JSON file persistence for the repo allowlist.
///
/// The file is rewritten in full on every `record_*` call (allowlist is
/// tiny — O(projects) × O(repos-per-project), comfortably < 10 KiB for
/// realistic workloads). Writes are atomic (tmp + rename) so a kill
/// mid-flush cannot corrupt the file.
///
/// An in-memory snapshot is maintained alongside the file so we don't
/// re-read from disk on every write — `record_allow`/`record_revoke`
/// mutates the snapshot, then flushes the whole thing.
#[derive(Debug)]
pub struct JsonFileAllowlistStore {
    path: PathBuf,
    snapshot: Mutex<OnDiskAllowlist>,
}

impl JsonFileAllowlistStore {
    /// Open (or lazy-create) a JSON file-backed allowlist store at
    /// `path`.
    ///
    /// The parent directory is created with `mkdir -p` semantics. If the
    /// file exists but is malformed, returns `Encoding`.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, AllowlistPersistenceError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let snapshot = if path.exists() {
            let src = fs::read_to_string(&path)?;
            // Empty file = fresh boot; treat as default (not an error).
            if src.trim().is_empty() {
                OnDiskAllowlist::default()
            } else {
                serde_json::from_str::<OnDiskAllowlist>(&src)
                    .map_err(|e| AllowlistPersistenceError::Encoding(e.to_string()))?
            }
        } else {
            OnDiskAllowlist::default()
        };
        Ok(Self {
            path,
            snapshot: Mutex::new(snapshot),
        })
    }

    /// Path this store persists to. Operator-visible for diagnostics.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn flush(&self, snapshot: &OnDiskAllowlist) -> Result<(), AllowlistPersistenceError> {
        let content = serde_json::to_string_pretty(snapshot)
            .map_err(|e| AllowlistPersistenceError::Encoding(e.to_string()))?;
        // tmp + rename → atomic on POSIX. On Windows `rename` is atomic
        // when the target doesn't exist and replaces when it does on
        // NTFS; good enough for plugin-local state.
        let tmp = self.path.with_extension("json.tmp");
        fs::write(&tmp, &content)?;
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

impl AllowlistPersistence for JsonFileAllowlistStore {
    fn location(&self) -> Option<PathBuf> {
        Some(self.path.clone())
    }

    fn load_all(&self) -> Result<HashMap<ProjectKey, HashSet<RepoId>>, AllowlistPersistenceError> {
        let snapshot = self.snapshot.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = HashMap::with_capacity(snapshot.projects.len());
        for (key_str, repos) in &snapshot.projects {
            let Some(project) = parse_project_key(key_str) else {
                tracing::warn!(
                    key = %key_str,
                    path = %self.path.display(),
                    "skipping malformed project key in allowlist file"
                );
                continue;
            };
            let mut set = HashSet::with_capacity(repos.len());
            for repo in repos {
                // Validate every repo id — a hand-edited or corrupted
                // file must not smuggle a malformed `RepoId` into the
                // in-memory map where downstream consumers assume it
                // round-tripped through `RepoId::parse` (and would
                // panic on `expect` accesses).
                match RepoId::parse(repo.clone()) {
                    Ok(repo_id) => {
                        set.insert(repo_id);
                    }
                    Err(err) => {
                        tracing::warn!(
                            repo = %repo,
                            project = %key_str,
                            path = %self.path.display(),
                            error = %err,
                            "skipping malformed repo id in allowlist file"
                        );
                    }
                }
            }
            out.insert(project, set);
        }
        Ok(out)
    }

    fn record_allow(
        &self,
        project: &ProjectKey,
        repo_id: &RepoId,
    ) -> Result<(), AllowlistPersistenceError> {
        let mut snapshot = self.snapshot.lock().unwrap_or_else(|e| e.into_inner());
        snapshot.version = 1;
        snapshot
            .projects
            .entry(project_key_string(project))
            .or_default()
            .insert(repo_id.as_str().to_owned());
        self.flush(&snapshot)
    }

    fn record_revoke(
        &self,
        project: &ProjectKey,
        repo_id: &RepoId,
    ) -> Result<(), AllowlistPersistenceError> {
        let mut snapshot = self.snapshot.lock().unwrap_or_else(|e| e.into_inner());
        snapshot.version = 1;
        let key = project_key_string(project);
        if let Some(set) = snapshot.projects.get_mut(&key) {
            set.remove(repo_id.as_str());
            if set.is_empty() {
                snapshot.projects.remove(&key);
            }
        }
        self.flush(&snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn p(project_id: &str) -> ProjectKey {
        ProjectKey::new("tenant-a", "ws", project_id)
    }

    #[test]
    fn load_from_missing_file_returns_empty() {
        let dir = TempDir::new().unwrap();
        let store = JsonFileAllowlistStore::open(dir.path().join("allowlist.json")).unwrap();
        let loaded = store.load_all().unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn allow_then_reload_roundtrips() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("github/allowlist.json");

        {
            let store = JsonFileAllowlistStore::open(&path).unwrap();
            store
                .record_allow(&p("proj-a"), &RepoId::new("org/repo-1"))
                .unwrap();
            store
                .record_allow(&p("proj-a"), &RepoId::new("org/repo-2"))
                .unwrap();
            store
                .record_allow(&p("proj-b"), &RepoId::new("org/repo-3"))
                .unwrap();
        }

        // Fresh store — simulates restart.
        let store = JsonFileAllowlistStore::open(&path).unwrap();
        let loaded = store.load_all().unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded[&p("proj-a")],
            HashSet::from([RepoId::new("org/repo-1"), RepoId::new("org/repo-2")])
        );
        assert_eq!(
            loaded[&p("proj-b")],
            HashSet::from([RepoId::new("org/repo-3")])
        );
    }

    #[test]
    fn revoke_last_repo_removes_project_entry() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("allowlist.json");

        let store = JsonFileAllowlistStore::open(&path).unwrap();
        store
            .record_allow(&p("proj-a"), &RepoId::new("org/repo-1"))
            .unwrap();
        store
            .record_revoke(&p("proj-a"), &RepoId::new("org/repo-1"))
            .unwrap();

        let loaded = store.load_all().unwrap();
        assert!(loaded.is_empty());

        // The project key itself must be gone from the file, not merely
        // mapped to an empty set — otherwise `list_for_project` would
        // see a phantom "authoritative but empty" state after reload.
        let raw = fs::read_to_string(&path).unwrap();
        let parsed: OnDiskAllowlist = serde_json::from_str(&raw).unwrap();
        assert!(parsed.projects.is_empty());
    }

    #[test]
    fn revoke_non_last_repo_leaves_project_entry() {
        let dir = TempDir::new().unwrap();
        let store = JsonFileAllowlistStore::open(dir.path().join("allowlist.json")).unwrap();

        store
            .record_allow(&p("proj-a"), &RepoId::new("org/repo-1"))
            .unwrap();
        store
            .record_allow(&p("proj-a"), &RepoId::new("org/repo-2"))
            .unwrap();
        store
            .record_revoke(&p("proj-a"), &RepoId::new("org/repo-1"))
            .unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(
            loaded[&p("proj-a")],
            HashSet::from([RepoId::new("org/repo-2")])
        );
    }

    #[test]
    fn malformed_repo_id_is_skipped_with_warning() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("allowlist.json");

        // `../escape` fails `RepoId::parse` — must not leak into the
        // loaded map.
        let payload = serde_json::json!({
            "version": 1,
            "projects": {
                "tenant-a/ws/proj-a": ["org/legit-repo", "../escape", "also/legit"],
            }
        });
        fs::write(&path, serde_json::to_string_pretty(&payload).unwrap()).unwrap();

        let store = JsonFileAllowlistStore::open(&path).unwrap();
        let loaded = store.load_all().unwrap();

        let set = loaded.get(&p("proj-a")).expect("project must load");
        assert_eq!(set.len(), 2, "only the two valid repo ids must load");
        assert!(set.contains(&RepoId::new("org/legit-repo")));
        assert!(set.contains(&RepoId::new("also/legit")));
    }

    #[test]
    fn location_returns_canonical_path() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("allowlist.json");
        let store = JsonFileAllowlistStore::open(&path).unwrap();
        assert_eq!(store.location(), Some(path));
    }

    #[test]
    fn malformed_project_key_is_skipped_with_warning() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("allowlist.json");

        // Manually craft a file with a good row + a malformed row.
        let payload = serde_json::json!({
            "version": 1,
            "projects": {
                "tenant-a/ws/proj-a": ["org/repo-1"],
                "not-a-valid-project-key": ["org/repo-evil"],
            }
        });
        fs::write(&path, serde_json::to_string_pretty(&payload).unwrap()).unwrap();

        let store = JsonFileAllowlistStore::open(&path).unwrap();
        let loaded = store.load_all().unwrap();

        assert_eq!(loaded.len(), 1);
        assert!(loaded.contains_key(&p("proj-a")));
    }

    #[test]
    fn malformed_file_returns_encoding_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("allowlist.json");
        fs::write(&path, "this is not json{").unwrap();

        let err = JsonFileAllowlistStore::open(&path).unwrap_err();
        assert!(matches!(err, AllowlistPersistenceError::Encoding(_)));
    }

    #[test]
    fn empty_file_is_treated_as_fresh_boot() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("allowlist.json");
        fs::write(&path, "").unwrap();

        let store = JsonFileAllowlistStore::open(&path).unwrap();
        assert!(store.load_all().unwrap().is_empty());
    }

    #[test]
    fn parent_dir_is_created_on_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("does/not/exist/allowlist.json");
        assert!(!path.parent().unwrap().exists());

        JsonFileAllowlistStore::open(&path).unwrap();
        assert!(path.parent().unwrap().exists());
    }
}
