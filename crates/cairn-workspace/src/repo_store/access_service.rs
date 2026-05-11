use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use cairn_domain::{ActorRef, ProjectKey, RepoAccessContext};

use crate::error::RepoStoreError;
use crate::repo_store::allowlist_persistence::AllowlistPersistence;
use crate::sandbox::RepoId;

#[derive(Debug, Default)]
pub struct ProjectRepoAccessService {
    allowed: RwLock<HashMap<ProjectKey, HashSet<RepoId>>>,
    /// Optional plugin-owned persistence seam (closes #556).
    ///
    /// Installed post-construction by the GitHub (or other) integration
    /// plugin via [`Self::install_persistence`] so a shared
    /// `Arc<ProjectRepoAccessService>` reference (as held by `AppState`)
    /// can gain durability at plugin-wire time without a rebuild.
    ///
    /// When present, mutations are written through to the persistence
    /// layer synchronously inside `allow`/`revoke` *after* the in-
    /// memory map has been updated. If the write fails, the in-memory
    /// mutation is rolled back so memory + disk stay consistent.
    ///
    /// Left uninitialised in tests and lightweight bootstrap paths that
    /// don't need restart durability — in those cases the service
    /// behaves exactly as it did before #556.
    persistence: OnceLock<Arc<dyn AllowlistPersistence>>,
    /// Serialises the *combined* `(in-memory update + persistence
    /// write)` critical section so two concurrent `allow`/`revoke`
    /// calls on the same `(project, repo)` cannot interleave in a way
    /// that lets a rollback-on-persistence-failure re-insert (or
    /// re-remove) an entry another caller had just legitimately
    /// mutated. Held only for the duration of one mutation — reads
    /// continue to use the cheaper `RwLock` read-guard on `allowed`.
    ///
    /// Not a performance problem in practice: `allow`/`revoke` fire at
    /// operator-clickthrough rates (one per human decision), not at
    /// data-plane throughput.
    mutation_lock: Mutex<()>,
}

impl ProjectRepoAccessService {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install a plugin-owned persistence backend and rehydrate the
    /// in-memory allowlist from it. Called exactly once at plugin-wire
    /// time (before the HTTP server starts accepting traffic) so the
    /// first `POST /v1/projects/.../repos` already sees a durable
    /// backing store.
    ///
    /// Persistence is the GitHub (or any other) integration plugin's
    /// responsibility — the access service itself is a pure in-memory
    /// projection. See
    /// `crate::repo_store::allowlist_persistence` for the
    /// "why-not-event-sourced" rationale.
    ///
    /// Idempotency: returns an error if persistence has already been
    /// installed. Idempotent rehydration would silently drop prior
    /// in-memory state on a re-install, and that's always a bug.
    ///
    /// Failure modes are all observed *before* any in-memory state is
    /// mutated: the double-install guard fires first, then `load_all`,
    /// then the merge. On any error the `allowed` map is left exactly
    /// as the caller found it.
    pub fn install_persistence(
        &self,
        persistence: Arc<dyn AllowlistPersistence>,
    ) -> Result<(), RepoStoreError> {
        // Serialise install via `mutation_lock` so two racing callers
        // cannot both pass the double-install guard, both merge their
        // `load_all` contents, and then have the loser's contribution
        // linger in `allowed` after `OnceLock::set` rejects them. The
        // winner's load + merge + set all run atomically; the loser
        // sees `persistence` already occupied and fails before
        // touching any state.
        let _install_guard = self.mutation_lock.lock().unwrap_or_else(|e| e.into_inner());

        if self.persistence.get().is_some() {
            return Err(RepoStoreError::Io {
                action: "allowlist_install_persistence",
                path: persistence.location().unwrap_or_default(),
                message: "persistence already installed on this access service".into(),
            });
        }
        // Load before `set` so a failing backend (corrupt JSON, IO
        // error) rejects the install without ever claiming the
        // `OnceLock`. If `load_all` succeeds but the subsequent merge
        // panics (it can't — it's infallible `HashMap` ops — but even
        // so), the `OnceLock` stays empty so a follow-up retry can
        // install a different backend.
        let loaded = persistence.load_all().map_err(|e| RepoStoreError::Io {
            action: "allowlist_load",
            path: persistence.location().unwrap_or_default(),
            message: format!("load allowlist from persistence: {e}"),
        })?;
        // `OnceLock::set` is infallible here because:
        //   * we hold `mutation_lock` so no concurrent caller raced
        //     past the guard above;
        //   * the guard observed `self.persistence.get().is_none()`;
        //   * nothing between that observation and here calls `set`.
        // We still surface a defensive error if the invariant breaks
        // rather than panicking.
        self.persistence
            .set(persistence)
            .map_err(|value| RepoStoreError::Io {
                action: "allowlist_install_persistence",
                path: value.location().unwrap_or_default(),
                message: "persistence already installed on this access service".into(),
            })?;
        // Only mutate `allowed` *after* `set` succeeds so the error
        // path above never pollutes the in-memory map with a backend
        // we then rejected.
        let mut guard = self.allowed.write().expect("allowlist lock poisoned");
        // Merge loaded entries over whatever was already in memory.
        // In practice the service is freshly constructed and empty
        // here; explicit merge-rather-than-replace keeps the
        // behaviour honest if a future caller seeds state before
        // installing persistence.
        for (project, repos) in loaded {
            guard.entry(project).or_default().extend(repos);
        }
        Ok(())
    }

    /// Is this allowlist backed by durable persistence?
    ///
    /// Used by `SandboxService::recover_all`'s allowlist-revoked sweep
    /// to decide whether an empty `is_allowed` response is authoritative
    /// ("the operator has revoked everything") or an in-memory stub
    /// that wouldn't survive a plugin-wire failure. When `false`, the
    /// sweep falls back to the pre-#556 conservative semantics —
    /// repo-backed sandboxes aren't flagged just because nobody has
    /// called `allow` this process lifetime.
    pub fn is_authoritative(&self) -> bool {
        self.persistence.get().is_some()
    }

    pub async fn is_allowed(&self, ctx: &RepoAccessContext, repo_id: &RepoId) -> bool {
        if repo_id.validate().is_err() {
            return false;
        }
        self.allowed
            .read()
            .ok()
            .and_then(|map| map.get(&ctx.project).map(|repos| repos.contains(repo_id)))
            .unwrap_or(false)
    }

    pub async fn allow(
        &self,
        ctx: &RepoAccessContext,
        repo_id: &RepoId,
        _by: ActorRef,
    ) -> Result<(), RepoStoreError> {
        repo_id.validate()?;
        // Serialise with `mutation_lock` so concurrent allow/revoke
        // calls cannot interleave their `(in-memory update ↔
        // persistence write)` pairs. Without this, a rollback on a
        // failed persistence write could undo an in-memory update that
        // a racing sibling call had already performed and persisted
        // successfully — corrupting disk/memory consistency.
        let _guard = self.mutation_lock.lock().unwrap_or_else(|e| e.into_inner());

        let was_new = {
            let mut guard = self.allowed.write().expect("allowlist lock poisoned");
            guard
                .entry(ctx.project.clone())
                .or_default()
                .insert(repo_id.clone())
        };
        if let Some(persistence) = self.persistence.get() {
            if let Err(e) = persistence.record_allow(&ctx.project, repo_id) {
                // Roll back. `was_new` is safe because we hold
                // `mutation_lock` for the whole (insert + persist)
                // window — no other caller could have mutated the
                // (project, repo) slot in between.
                if was_new {
                    let mut guard = self.allowed.write().expect("allowlist lock poisoned");
                    if let Some(repos) = guard.get_mut(&ctx.project) {
                        repos.remove(repo_id);
                        if repos.is_empty() {
                            guard.remove(&ctx.project);
                        }
                    }
                }
                return Err(RepoStoreError::Io {
                    action: "allowlist_record_allow",
                    path: persistence.location().unwrap_or_default(),
                    message: format!("persist allowlist grant: {e}"),
                });
            }
        }
        Ok(())
    }

    pub async fn revoke(
        &self,
        ctx: &RepoAccessContext,
        repo_id: &RepoId,
        _by: ActorRef,
    ) -> Result<(), RepoStoreError> {
        repo_id.validate()?;
        // See `allow` above — `mutation_lock` serialises the
        // (in-memory ↔ persistence) pair so rollback is race-free.
        let _guard = self.mutation_lock.lock().unwrap_or_else(|e| e.into_inner());

        let was_present = {
            let mut guard = self.allowed.write().expect("allowlist lock poisoned");
            match guard.get_mut(&ctx.project) {
                Some(repos) => {
                    let present = repos.remove(repo_id);
                    if present && repos.is_empty() {
                        guard.remove(&ctx.project);
                    }
                    present
                }
                None => false,
            }
        };
        if let Some(persistence) = self.persistence.get() {
            if let Err(e) = persistence.record_revoke(&ctx.project, repo_id) {
                if was_present {
                    let mut guard = self.allowed.write().expect("allowlist lock poisoned");
                    guard
                        .entry(ctx.project.clone())
                        .or_default()
                        .insert(repo_id.clone());
                }
                return Err(RepoStoreError::Io {
                    action: "allowlist_record_revoke",
                    path: persistence.location().unwrap_or_default(),
                    message: format!("persist allowlist revoke: {e}"),
                });
            }
        }
        Ok(())
    }

    pub async fn list_for_project(&self, ctx: &RepoAccessContext) -> Vec<RepoId> {
        let mut repos = self
            .allowed
            .read()
            .ok()
            .and_then(|map| map.get(&ctx.project).cloned())
            .map(|repos| repos.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        repos.sort();
        repos
    }

    pub async fn list_all(&self) -> HashMap<ProjectKey, Vec<RepoId>> {
        self.allowed
            .read()
            .expect("allowlist lock poisoned")
            .iter()
            .map(|(project, repos)| {
                let mut repos = repos.iter().cloned().collect::<Vec<_>>();
                repos.sort();
                (project.clone(), repos)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::ProjectRepoAccessService;
    use crate::repo_store::allowlist_persistence::{
        AllowlistPersistence, AllowlistPersistenceError, JsonFileAllowlistStore,
    };
    use crate::sandbox::RepoId;
    use cairn_domain::{ActorRef, OperatorId, ProjectKey, RepoAccessContext};
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    fn ctx(project: &str) -> RepoAccessContext {
        RepoAccessContext {
            project: ProjectKey::new("tenant", "workspace", project),
        }
    }

    fn actor() -> ActorRef {
        ActorRef::Operator {
            operator_id: OperatorId::new("op"),
        }
    }

    #[tokio::test]
    async fn allowlist_is_project_scoped() {
        let service = ProjectRepoAccessService::new();
        let repo = RepoId::new("org/repo");

        service
            .allow(&ctx("project-a"), &repo, actor())
            .await
            .unwrap();

        assert!(service.is_allowed(&ctx("project-a"), &repo).await);
        assert!(!service.is_allowed(&ctx("project-b"), &repo).await);
    }

    #[tokio::test]
    async fn revoke_removes_last_repo_and_cleans_slot() {
        let service = ProjectRepoAccessService::new();
        let repo = RepoId::new("org/repo");
        let project_ctx = ctx("project-a");

        service.allow(&project_ctx, &repo, actor()).await.unwrap();
        service.revoke(&project_ctx, &repo, actor()).await.unwrap();

        assert!(!service.is_allowed(&project_ctx, &repo).await);
        assert!(service.list_all().await.is_empty());
    }

    #[tokio::test]
    async fn list_all_returns_hashmap_keyed_by_project() {
        let service = ProjectRepoAccessService::new();
        let repo_a = RepoId::new("org/repo-a");
        let repo_b = RepoId::new("org/repo-b");
        let project_a = ctx("project-a");
        let project_b = ctx("project-b");

        service.allow(&project_a, &repo_b, actor()).await.unwrap();
        service.allow(&project_a, &repo_a, actor()).await.unwrap();
        service.allow(&project_b, &repo_b, actor()).await.unwrap();

        let all = service.list_all().await;

        assert_eq!(
            all.get(&project_a.project),
            Some(&vec![repo_a, repo_b.clone()])
        );
        assert_eq!(all.get(&project_b.project), Some(&vec![repo_b]));
    }

    // ── Persistence integration ──────────────────────────────────────

    #[tokio::test]
    async fn install_persistence_rehydrates_from_disk() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("allowlist.json");

        // First boot — populate persistence via one service instance.
        {
            let store = Arc::new(JsonFileAllowlistStore::open(&path).unwrap());
            let service = ProjectRepoAccessService::new();
            service
                .install_persistence(store.clone() as Arc<dyn AllowlistPersistence>)
                .unwrap();
            service
                .allow(&ctx("proj-a"), &RepoId::new("org/repo-1"), actor())
                .await
                .unwrap();
            service
                .allow(&ctx("proj-a"), &RepoId::new("org/repo-2"), actor())
                .await
                .unwrap();
        }

        // Second boot — a fresh service instance attached to the same
        // persistence file sees every prior grant without any API
        // traffic.
        let store = Arc::new(JsonFileAllowlistStore::open(&path).unwrap());
        let service = ProjectRepoAccessService::new();
        service
            .install_persistence(store as Arc<dyn AllowlistPersistence>)
            .unwrap();

        assert!(
            service
                .is_allowed(&ctx("proj-a"), &RepoId::new("org/repo-1"))
                .await
        );
        assert!(
            service
                .is_allowed(&ctx("proj-a"), &RepoId::new("org/repo-2"))
                .await
        );
    }

    #[tokio::test]
    async fn revoke_persists_across_restart() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("allowlist.json");

        {
            let store = Arc::new(JsonFileAllowlistStore::open(&path).unwrap());
            let service = ProjectRepoAccessService::new();
            service
                .install_persistence(store as Arc<dyn AllowlistPersistence>)
                .unwrap();
            service
                .allow(&ctx("proj-a"), &RepoId::new("org/repo-1"), actor())
                .await
                .unwrap();
            service
                .revoke(&ctx("proj-a"), &RepoId::new("org/repo-1"), actor())
                .await
                .unwrap();
        }

        let store = Arc::new(JsonFileAllowlistStore::open(&path).unwrap());
        let service = ProjectRepoAccessService::new();
        service
            .install_persistence(store as Arc<dyn AllowlistPersistence>)
            .unwrap();

        assert!(service.list_all().await.is_empty());
    }

    #[tokio::test]
    async fn install_persistence_twice_is_rejected_without_mutating_state() {
        // Seed the map via an allow so we can detect any load_all merge
        // that slipped past the double-install guard.
        let dir = tempfile::TempDir::new().unwrap();
        let path1 = dir.path().join("allow-1.json");
        let store1 = Arc::new(JsonFileAllowlistStore::open(&path1).unwrap());

        let service = ProjectRepoAccessService::new();
        service
            .install_persistence(store1 as Arc<dyn AllowlistPersistence>)
            .unwrap();
        service
            .allow(&ctx("proj-a"), &RepoId::new("org/first"), actor())
            .await
            .unwrap();

        // Populate a second on-disk file with DIFFERENT contents, then
        // try to re-install. The rejection must happen before the
        // second file's contents are merged into `allowed`.
        let path2 = dir.path().join("allow-2.json");
        {
            // Pre-populate by writing directly, then opening.
            std::fs::write(
                &path2,
                serde_json::json!({
                    "version": 1,
                    "projects": {
                        "tenant/workspace/proj-a": ["org/should-not-merge"],
                    }
                })
                .to_string(),
            )
            .unwrap();
        }
        let store2 = Arc::new(JsonFileAllowlistStore::open(&path2).unwrap());
        let err = service
            .install_persistence(store2 as Arc<dyn AllowlistPersistence>)
            .unwrap_err();
        assert!(err.to_string().contains("already installed"));

        // `org/should-not-merge` must NOT have leaked into state.
        assert!(
            !service
                .is_allowed(&ctx("proj-a"), &RepoId::new("org/should-not-merge"))
                .await,
            "re-install failure must not merge the second backend's contents"
        );
        // `org/first` is still there.
        assert!(
            service
                .is_allowed(&ctx("proj-a"), &RepoId::new("org/first"))
                .await
        );
    }

    #[tokio::test]
    async fn concurrent_install_persistence_does_not_pollute_loser_state() {
        // Two callers race `install_persistence` with different
        // backends. The winner's entries must land; the loser's must
        // never leak into `allowed` (finding on PR #588 round 2).
        let dir = tempfile::TempDir::new().unwrap();

        // Backend A with one entry.
        let path_a = dir.path().join("a.json");
        std::fs::write(
            &path_a,
            serde_json::json!({
                "version": 1,
                "projects": {
                    "tenant/workspace/proj-a": ["org/from-backend-a"],
                }
            })
            .to_string(),
        )
        .unwrap();
        let store_a = Arc::new(JsonFileAllowlistStore::open(&path_a).unwrap());

        // Backend B with a different entry.
        let path_b = dir.path().join("b.json");
        std::fs::write(
            &path_b,
            serde_json::json!({
                "version": 1,
                "projects": {
                    "tenant/workspace/proj-a": ["org/from-backend-b"],
                }
            })
            .to_string(),
        )
        .unwrap();
        let store_b = Arc::new(JsonFileAllowlistStore::open(&path_b).unwrap());

        let service = Arc::new(ProjectRepoAccessService::new());

        let svc_a = service.clone();
        let store_a_dyn = store_a as Arc<dyn AllowlistPersistence>;
        let svc_b = service.clone();
        let store_b_dyn = store_b as Arc<dyn AllowlistPersistence>;

        // Fire both installs concurrently on blocking threads so they
        // actually race `mutation_lock`.
        let handle_a = tokio::task::spawn_blocking(move || svc_a.install_persistence(store_a_dyn));
        let handle_b = tokio::task::spawn_blocking(move || svc_b.install_persistence(store_b_dyn));
        let result_a = handle_a.await.unwrap();
        let result_b = handle_b.await.unwrap();

        // Exactly one wins, exactly one loses.
        let (winner_ok, loser_err) = match (result_a.is_ok(), result_b.is_ok()) {
            (true, false) => (true, result_b.unwrap_err()),
            (false, true) => (true, result_a.unwrap_err()),
            other => panic!("expected one winner, got {other:?}"),
        };
        assert!(winner_ok);
        assert!(loser_err.to_string().contains("already installed"));

        // Exactly one of the two backends' entries landed in `allowed`.
        let from_a = service
            .is_allowed(&ctx("proj-a"), &RepoId::new("org/from-backend-a"))
            .await;
        let from_b = service
            .is_allowed(&ctx("proj-a"), &RepoId::new("org/from-backend-b"))
            .await;
        assert!(
            from_a ^ from_b,
            "exactly one backend's entries must load, got from_a={from_a} from_b={from_b}"
        );
    }

    /// A persistence backend that fails the Nth call. Exercises rollback
    /// semantics without racing `std::fs`.
    #[derive(Debug)]
    struct FailingStore {
        fail_on: Mutex<u32>,
        allow_calls: Mutex<u32>,
        revoke_calls: Mutex<u32>,
    }

    impl FailingStore {
        fn new(fail_after_n: u32) -> Self {
            Self {
                fail_on: Mutex::new(fail_after_n),
                allow_calls: Mutex::new(0),
                revoke_calls: Mutex::new(0),
            }
        }
    }

    impl AllowlistPersistence for FailingStore {
        fn location(&self) -> Option<PathBuf> {
            Some(PathBuf::from("/test/failing-store"))
        }

        fn load_all(
            &self,
        ) -> Result<HashMap<ProjectKey, HashSet<RepoId>>, AllowlistPersistenceError> {
            Ok(HashMap::new())
        }

        fn record_allow(
            &self,
            _project: &ProjectKey,
            _repo_id: &RepoId,
        ) -> Result<(), AllowlistPersistenceError> {
            let mut count = self.allow_calls.lock().unwrap();
            *count += 1;
            let budget = *self.fail_on.lock().unwrap();
            if *count > budget {
                return Err(AllowlistPersistenceError::Encoding(
                    "injected failure".into(),
                ));
            }
            Ok(())
        }

        fn record_revoke(
            &self,
            _project: &ProjectKey,
            _repo_id: &RepoId,
        ) -> Result<(), AllowlistPersistenceError> {
            let mut count = self.revoke_calls.lock().unwrap();
            *count += 1;
            let budget = *self.fail_on.lock().unwrap();
            if *count > budget {
                return Err(AllowlistPersistenceError::Encoding(
                    "injected failure".into(),
                ));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn allow_rolls_back_in_memory_on_persistence_failure() {
        let store = Arc::new(FailingStore::new(0)); // fail on the very first allow
        let service = ProjectRepoAccessService::new();
        service
            .install_persistence(store as Arc<dyn AllowlistPersistence>)
            .unwrap();

        let err = service
            .allow(&ctx("proj-a"), &RepoId::new("org/repo-1"), actor())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("persist allowlist grant"));
        // Finding 4: actual persistence location threads through the
        // error, not an empty PathBuf.
        assert!(
            err.to_string().contains("/test/failing-store"),
            "expected error to include persistence location, got {err}",
        );

        // In-memory state must not have been left with the entry.
        assert!(
            !service
                .is_allowed(&ctx("proj-a"), &RepoId::new("org/repo-1"))
                .await
        );
        assert!(service.list_all().await.is_empty());
    }

    #[tokio::test]
    async fn revoke_rolls_back_in_memory_on_persistence_failure() {
        // Let the first allow succeed, then fail on the revoke.
        let store = Arc::new(FailingStore::new(u32::MAX));
        let service = ProjectRepoAccessService::new();
        service
            .install_persistence(store.clone() as Arc<dyn AllowlistPersistence>)
            .unwrap();

        service
            .allow(&ctx("proj-a"), &RepoId::new("org/repo-1"), actor())
            .await
            .unwrap();

        // Flip the budget to trigger rejection on the first revoke.
        *store.fail_on.lock().unwrap() = 0;

        let err = service
            .revoke(&ctx("proj-a"), &RepoId::new("org/repo-1"), actor())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("persist allowlist revoke"));

        // Memory should still report the repo as allowed.
        assert!(
            service
                .is_allowed(&ctx("proj-a"), &RepoId::new("org/repo-1"))
                .await
        );
    }

    #[tokio::test]
    async fn concurrent_allow_revoke_on_same_repo_is_serialised() {
        // Finding 1 / Finding 5 regression: the old rollback-on-
        // failure path was not safe under concurrent mutations on the
        // same (project, repo). With `mutation_lock` installed, we
        // expect either "repo ends up allowed" or "repo ends up
        // revoked" — never a torn state where the in-memory map
        // disagrees with the persistence file.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("allowlist.json");
        let store = Arc::new(JsonFileAllowlistStore::open(&path).unwrap());
        let service = Arc::new(ProjectRepoAccessService::new());
        service
            .install_persistence(store as Arc<dyn AllowlistPersistence>)
            .unwrap();

        let repo = RepoId::new("org/race");
        let project = ctx("proj-race");

        // Fan out 32 pairs of (allow, revoke) concurrently against
        // the same (project, repo).
        let mut handles = Vec::new();
        for _ in 0..32 {
            let svc = service.clone();
            let r = repo.clone();
            let p = project.clone();
            handles.push(tokio::spawn(async move {
                let _ = svc.allow(&p, &r, actor()).await;
            }));
            let svc = service.clone();
            let r = repo.clone();
            let p = project.clone();
            handles.push(tokio::spawn(async move {
                let _ = svc.revoke(&p, &r, actor()).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // The end-state must match: either present in both memory and
        // on disk, or absent from both. We don't care which — only
        // that they agree.
        let in_memory = service.is_allowed(&project, &repo).await;
        let disk_store = JsonFileAllowlistStore::open(&path).unwrap();
        let disk_map = disk_store.load_all().unwrap();
        let on_disk = disk_map
            .get(&project.project)
            .map(|s| s.contains(&repo))
            .unwrap_or(false);

        assert_eq!(
            in_memory, on_disk,
            "memory and disk must agree after concurrent allow/revoke — in_memory={in_memory}, on_disk={on_disk}"
        );
    }
}
