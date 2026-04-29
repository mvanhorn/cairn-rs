use std::collections::{hash_map::DefaultHasher, HashMap};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use cairn_domain::{
    CheckpointKind, DestroyReason, OnExhaustion, PreservationReason, ProjectKey, RepoAccessContext,
    ResourceDimension, RunId, TaskId,
};

use crate::error::WorkspaceError;
use crate::providers::SandboxProvider;
use crate::repo_store::access_service::ProjectRepoAccessService;
use crate::repo_store::clone_cache::RepoCloneCache;
use crate::sandbox::{
    DestroyResult, ProvisionedSandbox, RepoId, SandboxCheckpoint, SandboxErrorKind, SandboxEvent,
    SandboxMetadata, SandboxPolicy, SandboxState, SandboxStrategy, SandboxStrategyRequest,
};

pub trait Clock: Send + Sync + 'static {
    fn now_millis(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct SystemClock;

/// Fires the clock-before-epoch WARN at most once per process. `now_millis`
/// is called on hot paths (heartbeat, leak-detection sweep, overdue scan)
/// that can tick tens of times per second; without this latch a stuck RTC
/// would spam the log. The WARN still covers the transition edge (the
/// first bad tick after the clock goes bad), which is the operator signal
/// that matters. Reset-on-recovery is intentionally NOT attempted — a
/// clock that flaps across UNIX_EPOCH is a pathological scenario and
/// re-firing the WARN on every flap would re-introduce the spam.
static CLOCK_BEFORE_EPOCH_WARNED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

impl Clock for SystemClock {
    /// Returns the number of milliseconds since UNIX_EPOCH.
    ///
    /// # Clock-before-epoch fallback
    ///
    /// If the host clock reads *before* UNIX_EPOCH (1970-01-01T00:00:00Z),
    /// `duration_since(UNIX_EPOCH)` returns `Err(SystemTimeError)`. This
    /// is practically only reachable on hardware whose RTC battery dies
    /// and re-initialises to a pre-1970 value on reboot. We emit a WARN
    /// (once per process — see `CLOCK_BEFORE_EPOCH_WARNED`) and return
    /// `0` rather than panicking. Every sandbox event timestamp flows
    /// through this method; a panic here would take down the sandbox
    /// service at boot. A bogus `0` timestamp is distinctly visible in
    /// event logs but keeps the host alive and lets operators see the
    /// problem (closes #466). A clock stuck at exactly UNIX_EPOCH
    /// (equal, not before) does NOT trigger the fallback — it's a valid
    /// `Duration::ZERO`.
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_else(|err| {
                // Latch: WARN exactly once per process to avoid spamming
                // log aggregators when a stuck clock keeps this path hot
                // (heartbeat / sweep ticks call `now_millis` frequently).
                if !CLOCK_BEFORE_EPOCH_WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    tracing::warn!(
                        error = %err,
                        "system clock is before UNIX_EPOCH; returning 0 (sandbox events will be timestamped 0 until the clock recovers). This warning fires once per process."
                    );
                }
                0
            })
    }
}

pub trait SandboxEventSink: Send + Sync + 'static {
    fn publish(&self, event: SandboxEvent);
}

#[derive(Debug, Default)]
pub struct BufferedSandboxEventSink {
    events: Mutex<Vec<SandboxEvent>>,
}

impl BufferedSandboxEventSink {
    /// Poison-tolerant — a panic in a thread holding the lock must not
    /// cascade into the event-sink path. Losing observability because one
    /// thread panicked would be monumentally unhelpful, so we recover the
    /// inner value from the poisoned mutex and continue. Mirrors the
    /// approved pattern in `crate::sandbox::f65::BufferedF65EventSink`.
    pub fn drain(&self) -> Vec<SandboxEvent> {
        let mut guard = self.events.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *guard)
    }
}

impl SandboxEventSink for BufferedSandboxEventSink {
    fn publish(&self, event: SandboxEvent) {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(event);
    }
}

#[derive(Clone, Debug)]
struct SandboxSession {
    sandbox_id: crate::sandbox::SandboxId,
    run_id: RunId,
    task_id: Option<TaskId>,
    project: ProjectKey,
    policy: SandboxPolicy,
    state: SandboxState,
    sandbox: Option<ProvisionedSandbox>,
    metadata: Option<SandboxMetadata>,
}

impl SandboxSession {
    fn new(
        run_id: &RunId,
        task_id: Option<TaskId>,
        project: ProjectKey,
        policy: SandboxPolicy,
    ) -> Self {
        Self {
            sandbox_id: crate::sandbox::SandboxId::new(format!("sbx-{}", run_id.as_str())),
            run_id: run_id.clone(),
            task_id,
            project,
            policy,
            state: SandboxState::Initial,
            sandbox: None,
            metadata: None,
        }
    }
}

struct StrategyResolution {
    actual: SandboxStrategy,
    degraded_from: Option<SandboxStrategy>,
    degrade_reason: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SandboxRecoverySummary {
    pub reconnected: u32,
    pub preserved: u32,
    pub failed: u32,
    /// RFC 020 §"Run recovery matrix": sandboxes listed in the recovery
    /// registry whose on-disk root has gone missing between boots.
    /// `lost_runs` carries the `(run_id, project)` pairs so the run-level
    /// `RecoveryService` can transition each bound run to `failed` with
    /// `reason: sandbox_lost`. `lost` is the count for summary logging /
    /// readiness reporting and is always equal to `lost_runs.len()`.
    pub lost: u32,
    pub lost_runs: Vec<(RunId, ProjectKey)>,
    /// RFC 020 §"Run recovery matrix": registry entries for
    /// `SandboxBase::Repo` sandboxes whose bound repo is no longer on the
    /// project's allowlist at recovery time. `allowlist_revoked_runs`
    /// carries `(run_id, project, repo_id)` so the run-level
    /// `RecoveryService` can transition each bound run to
    /// `WaitingApproval` with a synthesized approval asking the operator
    /// to re-grant or cancel (per the `AllowlistRevoked` matrix row).
    /// `preserved_allowlist_revoked` is always equal to
    /// `allowlist_revoked_runs.len()`.
    pub preserved_allowlist_revoked: u32,
    pub allowlist_revoked_runs: Vec<(RunId, ProjectKey, RepoId)>,
    /// RFC 020 §"Run recovery matrix": registry entries whose on-disk
    /// root survived the crash and whose repo binding (if any) is still
    /// allowlisted. `reattached_runs` carries `(run_id, project)` pairs
    /// so the run-level `RecoveryService` can emit
    /// `RecoveryAttempted { reason: "sandbox_reattached" }` for the
    /// audit trail. No state transition happens — the run stays in its
    /// existing non-terminal state and the orchestrator resumes it on
    /// the next tick. `reattached` is always equal to
    /// `reattached_runs.len()`.
    pub reattached: u32,
    pub reattached_runs: Vec<(RunId, ProjectKey)>,
    /// RFC 020 §"Run recovery matrix": registry entries for
    /// `SandboxBase::Repo` overlay sandboxes whose stored `base_revision`
    /// no longer matches the locked clone's HEAD at recovery time — the
    /// operator called `RepoCloneCache::refresh()` (or equivalent) and
    /// moved the clone out from under the overlay's upper layer.
    /// `base_revision_drift_runs` carries `(run_id, project, repo_id)` so
    /// the run-level `RecoveryService` can transition each bound run to
    /// `WaitingApproval` with a synthesized approval asking the operator
    /// to re-provision against the new base or cancel the run (per the
    /// `BaseRevisionDrift` matrix row). Reflink sandboxes are exempt per
    /// RFC 016 — they are physically independent post-provision, so the
    /// upstream clone moving cannot corrupt their contents.
    /// `preserved_base_revision_drift` is always equal to
    /// `base_revision_drift_runs.len()`.
    pub preserved_base_revision_drift: u32,
    pub base_revision_drift_runs: Vec<(RunId, ProjectKey, RepoId)>,
}

/// Directory name (relative to `base_dir`) holding the recovery registry —
/// one sidecar JSON per provisioned sandbox that survives the sandbox
/// directory being deleted. Without this sidecar, a sandbox whose root
/// has been removed is indistinguishable from "never provisioned", and
/// `recover_all` cannot attribute a `sandbox_lost` failure to a run.
///
/// Leading dot keeps the registry out of provider `list()` sweeps — all
/// providers skip entries without a `meta.json` (registry entries have
/// a `registry.json` instead), and the leading dot is an additional
/// belt-and-braces filter against any future provider that enumerates
/// by prefix.
const RECOVERY_REGISTRY_DIRNAME: &str = ".registry";

/// Sidecar filename for a single registry entry.
const REGISTRY_ENTRY_FILENAME: &str = "registry.json";

/// One registry entry — the minimum information needed to attribute a
/// missing sandbox directory back to the run that owned it.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct RegistryEntry {
    sandbox_id: crate::sandbox::SandboxId,
    run_id: RunId,
    project: ProjectKey,
    strategy: SandboxStrategy,
    /// Expected on-disk root. At recovery time we check `path.exists()`
    /// — a missing path with a present registry entry means
    /// `SandboxLost`.
    path: PathBuf,
    /// Unix-ms timestamp for operator forensics. Not consumed by the
    /// recovery matrix.
    registered_at: u64,
    /// Bound repo for `SandboxBase::Repo` sandboxes. When `Some`, the
    /// allowlist-revoked recovery sweep looks this repo up in
    /// `ProjectRepoAccessService`; if the repo is no longer allowed
    /// under `project`, the sweep emits `SandboxAllowlistRevoked` and
    /// preserves the sandbox. `None` for `SandboxBase::Empty` and
    /// `SandboxBase::Directory` — those bases have no allowlist semantics.
    /// Backward-compat: older registry sidecars written before the
    /// allowlist sweep existed deserialize to `None`, which disables the
    /// check for them (indistinguishable from a non-repo base).
    #[serde(default)]
    repo_id: Option<RepoId>,
    /// Tombstone flag: `true` once the allowlist-revoked sweep has emitted
    /// `SandboxAllowlistRevoked` for this entry. Prevents re-emission
    /// across subsequent boots — the sandbox is `Preserved` and the
    /// operator-facing approval has been synthesized; firing the event
    /// again would duplicate that approval. Operator must re-grant (which
    /// happens via the HTTP repo-allow path and does not touch the
    /// registry) or delete the sandbox (which removes the registry entry).
    #[serde(default)]
    allowlist_revoked_handled: bool,
    /// Stored base revision for overlay-on-repo sandboxes. At recovery
    /// time the `base_revision_drift` sweep reads this, asks the
    /// `RepoCloneCache` for the current HEAD of the bound clone, and
    /// emits `SandboxBaseRevisionDrift` if they differ. `None` for bases
    /// with no upstream notion of HEAD (`SandboxBase::Empty`,
    /// `SandboxBase::Directory`) and for reflink sandboxes (physically
    /// independent per RFC 016 — the sweep skips them regardless).
    ///
    /// Backward-compat: sidecars written before the drift sweep existed
    /// deserialize to `None`, which disables the check for them — the
    /// drift sweep keys off `Some(stored) != Some(current)`, so a `None`
    /// stored value is indistinguishable from "no tracked revision".
    #[serde(default)]
    base_revision: Option<String>,
    /// Tombstone flag: `true` once the base-revision-drift sweep has
    /// emitted `SandboxBaseRevisionDrift` for this entry. Same rationale
    /// as `allowlist_revoked_handled` — the sandbox is `Preserved` and
    /// the operator-facing approval has been synthesized; a repeat sweep
    /// on the next boot would duplicate the approval.
    #[serde(default)]
    base_revision_drift_handled: bool,
}

/// Object-safe façade over [`SandboxService`] exposing exactly the methods
/// that consumers outside this crate (cairn-app runtime, GC sweeper,
/// integration-test seed helpers) call.
///
/// Introduced for issue #443 so cairn-app's `AppState` can hold
/// `Arc<dyn SandboxServiceApi>` instead of the concrete struct, which
/// (a) enables mock-based tests without standing up the full providers/
/// event-sink/clock stack and (b) keeps the upper layer from binding to
/// the private shape of `SandboxService`.
///
/// The trait is intentionally narrow: only methods reached from outside
/// `cairn-workspace` are on it. Internal helpers and builder setters stay
/// as inherent `impl SandboxService` methods.
#[async_trait::async_trait]
pub trait SandboxServiceApi: Send + Sync {
    /// Provision (or reconnect to an existing) sandbox bound to `run_id`.
    /// See [`SandboxService::provision_or_reconnect`] for the real docs.
    async fn provision_or_reconnect(
        &self,
        run_id: &RunId,
        task_id: Option<TaskId>,
        project: ProjectKey,
        policy: SandboxPolicy,
    ) -> Result<ProvisionedSandbox, WorkspaceError>;

    /// Mark a provisioned sandbox as active and return the active handle.
    async fn activate(
        &self,
        run_id: &RunId,
        pid: Option<u32>,
    ) -> Result<ProvisionedSandbox, WorkspaceError>;

    /// Root directory under which per-sandbox directories live.
    fn base_dir(&self) -> &PathBuf;

    /// Crash-recovery sweep. See [`SandboxService::recover_all`].
    async fn recover_all(&self) -> Result<SandboxRecoverySummary, WorkspaceError>;

    /// Reap a workspace snapshot directory. Returns `Ok(true)` when the
    /// directory was removed, `Ok(false)` when it did not exist or a
    /// resume is in flight against it. See
    /// [`SandboxService::reap_snapshot_dir`].
    fn reap_snapshot_dir(
        &self,
        snapshot_id: &cairn_domain::WorkspaceSnapshotId,
    ) -> Result<bool, WorkspaceError>;

    /// Integration-test helper. Registers a registry entry directly,
    /// bypassing provisioning. See
    /// [`SandboxService::seed_registry_entry_for_test`].
    fn seed_registry_entry_for_test(
        &self,
        sandbox_id: crate::sandbox::SandboxId,
        run_id: RunId,
        project: ProjectKey,
        strategy: SandboxStrategy,
        path: PathBuf,
    ) -> Result<(), WorkspaceError>;

    /// Integration-test helper. Variant of
    /// [`Self::seed_registry_entry_for_test`] that captures a bound
    /// `repo_id`.
    fn seed_registry_entry_for_test_with_repo(
        &self,
        sandbox_id: crate::sandbox::SandboxId,
        run_id: RunId,
        project: ProjectKey,
        strategy: SandboxStrategy,
        path: PathBuf,
        repo_id: Option<RepoId>,
    ) -> Result<(), WorkspaceError>;

    /// Integration-test helper. Extended variant that also captures the
    /// entry's stored `base_revision`.
    fn seed_registry_entry_for_test_full(
        &self,
        sandbox_id: crate::sandbox::SandboxId,
        run_id: RunId,
        project: ProjectKey,
        strategy: SandboxStrategy,
        path: PathBuf,
        repo_id: Option<RepoId>,
        base_revision: Option<String>,
    ) -> Result<(), WorkspaceError>;
}

pub struct SandboxService {
    providers: HashMap<SandboxStrategy, Box<dyn SandboxProvider>>,
    event_sink: Arc<dyn SandboxEventSink>,
    base_dir: PathBuf,
    clock: Arc<dyn Clock>,
    sessions: RwLock<HashMap<RunId, SandboxSession>>,
    /// Allowlist source consulted during `recover_all` to decide whether a
    /// `SandboxBase::Repo` sandbox's repo is still authorised under the
    /// owning project. `None` disables the allowlist-revoked sweep
    /// entirely (treat every recovered sandbox as still allowed) — used by
    /// the workspace-layer unit tests which exercise the sweep directly
    /// with seeded entries and would otherwise need to stand up the full
    /// access service.
    allowlist: Option<Arc<ProjectRepoAccessService>>,
    /// Source of truth for the locked clone's HEAD, consulted during
    /// `recover_all` to diff the registry entry's stored `base_revision`
    /// against the current upstream HEAD. `None` disables the
    /// base-revision-drift sweep entirely (useful for workspace-layer
    /// unit tests that exercise the sweep directly with seeded entries
    /// and would otherwise need to stand up the full clone cache).
    clone_cache: Option<Arc<RepoCloneCache>>,
    /// F65 PR-5: root directory for durable workspace snapshots
    /// (`~/.cairn/snapshots/<uuid>/`). `None` disables the F65 snapshot
    /// and resume path entirely — RFC 016 sandboxing still works via the
    /// classic `provision_or_reconnect` API. Set via
    /// [`Self::with_snapshot_dir`].
    snapshot_dir: Option<PathBuf>,
    /// F65 PR-5: emits the operator-visible session-scoped events
    /// (`SessionAttemptStarted`, `WorkspaceSnapshotCreated`,
    /// `WorkspaceBackendDegraded`, `SandboxCrashRecovered`). Defaults to
    /// `NoopF65EventSink` — cairn-app overrides it with an adapter that
    /// appends to the store event log.
    f65_event_sink: Arc<dyn crate::sandbox::f65::F65SandboxEventSink>,
    /// F65 PR-5: per-session dedupe flag for `WorkspaceBackendDegraded`.
    /// Mirrors the PR-4 `reflink_tree_with_fallback` primitive's
    /// `&AtomicBool` contract, scoped by `SessionId` so the flag survives
    /// across the terminate-then-resume-then-terminate cycle. The arch
    /// doc commitment is "once per session, across resume boundaries" —
    /// this map is what keeps the flip state.
    degraded_flag_by_session:
        Mutex<HashMap<cairn_domain::SessionId, Arc<std::sync::atomic::AtomicBool>>>,
    /// F65 PR-5: per-session emission dedupe for
    /// `WorkspaceBackendDegraded`. Separate from `degraded_flag_by_session`
    /// (which tracks "did reflink fall back this session") so the event
    /// fires at most once even across restarts within the same session.
    degraded_emitted_sessions: Mutex<std::collections::HashSet<cairn_domain::SessionId>>,
    /// F65 PR-5: active session sandboxes, keyed by session id. Each
    /// entry carries the paths the orchestrator + child agent need.
    /// Distinct from `sessions` (RunId-keyed, RFC 016/020 shape).
    session_sandboxes:
        RwLock<HashMap<cairn_domain::SessionId, crate::sandbox::f65::SessionSandbox>>,
    /// F65 PR-5: in-flight resume operations. The GC sweeper consults
    /// this set before reaping so it cannot pull the rug out from under
    /// a resume that has already started reflinking a snapshot.
    inflight_restores: Mutex<std::collections::HashSet<cairn_domain::WorkspaceSnapshotId>>,
    /// F65 PR-5: writer that back-fills the per-snapshot filesystem
    /// metadata (`bytes`, `reflink_used`, `snapshot_path`,
    /// `parent_snapshot_id`) on the existing `workspace_snapshots` row.
    /// Defaults to a no-op so unit tests and the RFC 016 path can ignore
    /// it; cairn-app wires in the real cairn-store-backed adapter via
    /// [`Self::with_snapshot_writer`].
    snapshot_writer: Arc<dyn crate::sandbox::f65::WorkspaceSnapshotWriter>,
}

impl SandboxService {
    pub fn new(
        providers: HashMap<SandboxStrategy, Box<dyn SandboxProvider>>,
        event_sink: Arc<dyn SandboxEventSink>,
        base_dir: impl Into<PathBuf>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            providers,
            event_sink,
            base_dir: base_dir.into(),
            clock,
            sessions: RwLock::new(HashMap::new()),
            allowlist: None,
            clone_cache: None,
            snapshot_dir: None,
            f65_event_sink: Arc::new(crate::sandbox::f65::NoopF65EventSink),
            degraded_flag_by_session: Mutex::new(HashMap::new()),
            degraded_emitted_sessions: Mutex::new(std::collections::HashSet::new()),
            session_sandboxes: RwLock::new(HashMap::new()),
            inflight_restores: Mutex::new(std::collections::HashSet::new()),
            snapshot_writer: Arc::new(crate::sandbox::f65::NoopWorkspaceSnapshotWriter),
        }
    }

    /// F65 PR-5: wire the root directory under which durable workspace
    /// snapshots are written as `<snapshot_dir>/<uuid>/…`. Without this,
    /// `terminate_for_session` fails with a descriptive error rather than
    /// silently skipping the snapshot — a silent-skip would be a data
    /// loss bug for resume.
    pub fn with_snapshot_dir(mut self, snapshot_dir: impl Into<PathBuf>) -> Self {
        self.snapshot_dir = Some(snapshot_dir.into());
        self
    }

    /// F65 PR-5: wire the event sink that adapts F65 events to cairn-app's
    /// event log. Default is `NoopF65EventSink`.
    pub fn with_f65_event_sink(
        mut self,
        sink: Arc<dyn crate::sandbox::f65::F65SandboxEventSink>,
    ) -> Self {
        self.f65_event_sink = sink;
        self
    }

    /// F65 PR-5: wire the writer that back-fills the snapshot row's
    /// filesystem metadata (bytes/reflink_used/snapshot_path/
    /// parent_snapshot_id) after the reflink copy runs.
    pub fn with_snapshot_writer(
        mut self,
        writer: Arc<dyn crate::sandbox::f65::WorkspaceSnapshotWriter>,
    ) -> Self {
        self.snapshot_writer = writer;
        self
    }

    /// F65 PR-5: read accessor for the snapshot root (used by the GC
    /// sweeper to enumerate on-disk snapshot directories).
    pub fn snapshot_dir(&self) -> Option<&PathBuf> {
        self.snapshot_dir.as_ref()
    }

    /// Wire in the project-scoped repo allowlist. When set, `recover_all`
    /// will emit `SandboxAllowlistRevoked` for any registry entry whose
    /// bound repo is no longer allowed under the sandbox's project.
    /// Without this, the allowlist-revoked sweep is a no-op.
    pub fn with_allowlist(mut self, allowlist: Arc<ProjectRepoAccessService>) -> Self {
        self.allowlist = Some(allowlist);
        self
    }

    /// Wire in the repo clone cache. When set, `recover_all` will emit
    /// `SandboxBaseRevisionDrift` for any overlay registry entry whose
    /// stored `base_revision` no longer matches the locked clone's HEAD.
    /// Reflink sandboxes are skipped (physically independent per RFC 016).
    /// Without this, the base-revision-drift sweep is a no-op.
    pub fn with_clone_cache(mut self, clone_cache: Arc<RepoCloneCache>) -> Self {
        self.clone_cache = Some(clone_cache);
        self
    }

    pub fn base_dir(&self) -> &PathBuf {
        &self.base_dir
    }

    /// Register a lost-sandbox entry directly without going through
    /// provisioning. Exposed for integration tests that need to simulate
    /// "operator deleted the sandbox root between boots" without wiring a
    /// full HTTP provisioning surface. `path` should be a path that does
    /// NOT exist on disk — the recovery sweep keys `SandboxLost` off
    /// `path.exists() == false`.
    pub fn seed_registry_entry_for_test(
        &self,
        sandbox_id: crate::sandbox::SandboxId,
        run_id: RunId,
        project: ProjectKey,
        strategy: SandboxStrategy,
        path: PathBuf,
    ) -> Result<(), WorkspaceError> {
        self.seed_registry_entry_for_test_with_repo(
            sandbox_id, run_id, project, strategy, path, None,
        )
    }

    /// Like `seed_registry_entry_for_test`, but captures the bound
    /// `repo_id`. Used by the RFC 020 `allowlist_revoked` integration
    /// test: the sandbox directory exists but the repo binding is
    /// what the allowlist-revoked sweep keys off.
    pub fn seed_registry_entry_for_test_with_repo(
        &self,
        sandbox_id: crate::sandbox::SandboxId,
        run_id: RunId,
        project: ProjectKey,
        strategy: SandboxStrategy,
        path: PathBuf,
        repo_id: Option<RepoId>,
    ) -> Result<(), WorkspaceError> {
        self.seed_registry_entry_for_test_full(
            sandbox_id, run_id, project, strategy, path, repo_id, None,
        )
    }

    /// Extended seed helper that also captures the entry's stored
    /// `base_revision`. Used by the RFC 020 `base_revision_drift`
    /// integration test so the recovery sweep can compare the seeded
    /// value against the clone cache's current HEAD and emit drift.
    pub fn seed_registry_entry_for_test_full(
        &self,
        sandbox_id: crate::sandbox::SandboxId,
        run_id: RunId,
        project: ProjectKey,
        strategy: SandboxStrategy,
        path: PathBuf,
        repo_id: Option<RepoId>,
        base_revision: Option<String>,
    ) -> Result<(), WorkspaceError> {
        let entry = RegistryEntry {
            sandbox_id,
            run_id,
            project,
            strategy,
            path,
            registered_at: self.clock.now_millis(),
            repo_id,
            allowlist_revoked_handled: false,
            base_revision,
            base_revision_drift_handled: false,
        };
        self.write_registry_entry(&entry)
    }

    // ── Lock-poison recovery (issue #463) ───────────────────────────────
    //
    // Every guard in this impl uses `.unwrap_or_else(|e| e.into_inner())`
    // rather than `.expect(…)`. F65 PR-4 put sandbox confinement on the
    // critical path of every orchestrator session: a single panicking
    // thread holding one of these guards would poison the lock and every
    // subsequent sandbox op would panic on the unwrap, cascading into a
    // full-process DoS.
    //
    // We recover the inner value and continue. The inner state is a
    // `HashMap` keyed by `RunId` / `SessionId` / `WorkspaceSnapshotId` —
    // per-key independent, so a partial insert from a panicking writer
    // leaves at most one half-written entry, never a broken invariant.
    // Mirrors the approved pattern in
    // `crate::sandbox::f65::BufferedF65EventSink`.
    pub fn state_for(&self, run_id: &RunId) -> Option<SandboxState> {
        self.sessions
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(run_id)
            .map(|session| session.state)
    }

    pub fn metadata_for(&self, run_id: &RunId) -> Option<SandboxMetadata> {
        self.sessions
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(run_id)
            .and_then(|session| session.metadata.clone())
    }

    pub async fn provision_or_reconnect(
        &self,
        run_id: &RunId,
        task_id: Option<TaskId>,
        project: ProjectKey,
        policy: SandboxPolicy,
    ) -> Result<ProvisionedSandbox, WorkspaceError> {
        if let Some(existing) = self.existing_sandbox(run_id)? {
            return Ok(existing);
        }

        let resolution = self.resolve_strategy(&policy.strategy)?;
        let started_at = self.clock.now_millis();
        let (sandbox_id, run_id_owned) = {
            let mut sessions = self.sessions.write().unwrap_or_else(|e| e.into_inner());
            let session = sessions.entry(run_id.clone()).or_insert_with(|| {
                SandboxSession::new(run_id, task_id.clone(), project.clone(), policy.clone())
            });
            if !session.state.can_transition_to(SandboxState::Provisioning) {
                return Err(WorkspaceError::InvalidSandboxStateTransition {
                    run_id: run_id.clone(),
                    from: session.state,
                    to: SandboxState::Provisioning,
                });
            }
            session.task_id = task_id.clone();
            session.project = project.clone();
            session.policy = policy.clone();
            session.state = SandboxState::Provisioning;
            (session.sandbox_id.clone(), session.run_id.clone())
        };

        if let Some(requested) = resolution.degraded_from {
            self.event_sink
                .publish(SandboxEvent::SandboxPolicyDegraded {
                    sandbox_id: sandbox_id.clone(),
                    run_id: run_id_owned.clone(),
                    requested,
                    actual: resolution.actual,
                    reason: resolution
                        .degrade_reason
                        .clone()
                        .unwrap_or_else(|| "preferred strategy unavailable".to_string()),
                    degraded_at: started_at,
                });
        }

        let provider = self.provider(resolution.actual)?;
        match provider.provision(run_id, &project, &policy).await {
            Ok(sandbox) => {
                let provisioned_at = self.clock.now_millis();
                let metadata = SandboxMetadata {
                    sandbox_id: sandbox.sandbox_id.clone(),
                    run_id: run_id.clone(),
                    task_id,
                    project: project.clone(),
                    strategy: sandbox.strategy,
                    state: SandboxState::Ready,
                    base_rev: sandbox.base_revision.clone(),
                    repo_id: repo_id_for(&policy),
                    path: sandbox.path.clone(),
                    pid: None,
                    created_at: provisioned_at,
                    heartbeat_at: provisioned_at,
                    policy_hash: policy_hash(&policy),
                };

                {
                    let mut sessions = self.sessions.write().unwrap_or_else(|e| e.into_inner());
                    let session = sessions
                        .get_mut(run_id)
                        .expect("sandbox session must exist after provisioning");
                    session.sandbox_id = sandbox.sandbox_id.clone();
                    session.state = SandboxState::Ready;
                    session.sandbox = Some(sandbox.clone());
                    session.metadata = Some(metadata.clone());
                }
                // Write the recovery registry sidecar *before* persisting
                // the full metadata. If persisting fails and we never reach
                // the metadata write, the registry entry still attributes
                // the half-provisioned sandbox to its run — better than
                // silently losing track of it. On the happy path both
                // succeed and recover_all sees the sandbox via the
                // provider's meta.json anyway.
                let registry_entry = RegistryEntry {
                    sandbox_id: sandbox.sandbox_id.clone(),
                    run_id: run_id.clone(),
                    project: project.clone(),
                    strategy: sandbox.strategy,
                    path: sandbox.path.clone(),
                    registered_at: provisioned_at,
                    repo_id: repo_id_for(&policy),
                    allowlist_revoked_handled: false,
                    base_revision: sandbox.base_revision.clone(),
                    base_revision_drift_handled: false,
                };
                if let Err(error) = self.write_registry_entry(&registry_entry) {
                    let failed_at = self.clock.now_millis();
                    {
                        let mut sessions = self.sessions.write().unwrap_or_else(|e| e.into_inner());
                        if let Some(session) = sessions.get_mut(run_id) {
                            session.state = SandboxState::Failed;
                        }
                    }
                    self.event_sink
                        .publish(SandboxEvent::SandboxProvisioningFailed {
                            sandbox_id: sandbox.sandbox_id.clone(),
                            run_id: run_id.clone(),
                            error_kind: SandboxErrorKind::Filesystem,
                            error: error.to_string(),
                            failed_at,
                        });
                    return Err(error);
                }

                if let Err(error) = self.persist_metadata(&metadata) {
                    let failed_at = self.clock.now_millis();
                    {
                        let mut sessions = self.sessions.write().unwrap_or_else(|e| e.into_inner());
                        if let Some(session) = sessions.get_mut(run_id) {
                            session.state = SandboxState::Failed;
                        }
                    }
                    self.event_sink
                        .publish(SandboxEvent::SandboxProvisioningFailed {
                            sandbox_id: sandbox.sandbox_id.clone(),
                            run_id: run_id.clone(),
                            error_kind: SandboxErrorKind::Filesystem,
                            error: error.to_string(),
                            failed_at,
                        });
                    return Err(error);
                }

                self.event_sink.publish(SandboxEvent::SandboxProvisioned {
                    sandbox_id: sandbox.sandbox_id.clone(),
                    run_id: run_id.clone(),
                    task_id: metadata.task_id.clone(),
                    project,
                    strategy: sandbox.strategy,
                    base_revision: sandbox.base_revision.clone(),
                    policy,
                    path: sandbox.path.clone(),
                    duration_ms: provisioned_at.saturating_sub(started_at),
                    provisioned_at,
                });

                Ok(sandbox)
            }
            Err(error) => {
                let failed_at = self.clock.now_millis();
                {
                    let mut sessions = self.sessions.write().unwrap_or_else(|e| e.into_inner());
                    if let Some(session) = sessions.get_mut(run_id) {
                        session.state = SandboxState::Failed;
                    }
                }

                self.event_sink
                    .publish(SandboxEvent::SandboxProvisioningFailed {
                        sandbox_id,
                        run_id: run_id_owned,
                        error_kind: SandboxErrorKind::Filesystem,
                        error: error.to_string(),
                        failed_at,
                    });

                Err(error)
            }
        }
    }

    pub async fn activate(
        &self,
        run_id: &RunId,
        pid: Option<u32>,
    ) -> Result<ProvisionedSandbox, WorkspaceError> {
        let activated_at = self.clock.now_millis();
        let (sandbox_id, sandbox, metadata) = {
            let mut sessions = self.sessions.write().unwrap_or_else(|e| e.into_inner());
            let session = self.session_mut(&mut sessions, run_id)?;
            let resumed = matches!(
                session.state,
                SandboxState::Checkpointed | SandboxState::Preserved | SandboxState::Active
            );
            self.transition(session, SandboxState::Active)?;
            let sandbox =
                session
                    .sandbox
                    .as_mut()
                    .ok_or_else(|| WorkspaceError::SandboxNotFound {
                        run_id: run_id.clone(),
                    })?;
            sandbox.is_resumed = resumed;
            if let Some(metadata) = session.metadata.as_mut() {
                metadata.state = SandboxState::Active;
                metadata.pid = pid;
                metadata.heartbeat_at = activated_at;
            }
            (
                session.sandbox_id.clone(),
                sandbox.clone(),
                session.metadata.clone(),
            )
        };
        if let Some(metadata) = metadata.as_ref() {
            self.persist_metadata(metadata)?;
        }

        self.event_sink.publish(SandboxEvent::SandboxActivated {
            sandbox_id,
            run_id: run_id.clone(),
            pid,
            activated_at,
        });

        Ok(sandbox)
    }

    pub async fn heartbeat(&self, run_id: &RunId) -> Result<(), WorkspaceError> {
        let strategy = {
            let sessions = self.sessions.read().unwrap_or_else(|e| e.into_inner());
            let session = sessions
                .get(run_id)
                .ok_or_else(|| WorkspaceError::SandboxNotFound {
                    run_id: run_id.clone(),
                })?;
            if session.state != SandboxState::Active {
                return Err(WorkspaceError::InvalidSandboxStateTransition {
                    run_id: run_id.clone(),
                    from: session.state,
                    to: SandboxState::Active,
                });
            }
            session
                .sandbox
                .as_ref()
                .ok_or_else(|| WorkspaceError::SandboxNotFound {
                    run_id: run_id.clone(),
                })?
                .strategy
        };

        self.provider(strategy)?.heartbeat(run_id).await?;

        let heartbeat_at = self.clock.now_millis();
        let (sandbox_id, metadata) = {
            let mut sessions = self.sessions.write().unwrap_or_else(|e| e.into_inner());
            let session = self.session_mut(&mut sessions, run_id)?;
            if let Some(metadata) = session.metadata.as_mut() {
                metadata.heartbeat_at = heartbeat_at;
            }
            (session.sandbox_id.clone(), session.metadata.clone())
        };
        if let Some(metadata) = metadata.as_ref() {
            self.persist_metadata(metadata)?;
        }

        self.event_sink.publish(SandboxEvent::SandboxHeartbeat {
            sandbox_id,
            run_id: run_id.clone(),
            heartbeat_at,
        });

        Ok(())
    }

    pub async fn checkpoint(
        &self,
        run_id: &RunId,
        kind: CheckpointKind,
    ) -> Result<SandboxCheckpoint, WorkspaceError> {
        let strategy = self.sandbox_strategy(run_id)?;
        let checkpoint = self.provider(strategy)?.checkpoint(run_id, kind).await?;
        let checkpointed_at = self.clock.now_millis();

        let metadata = {
            let mut sessions = self.sessions.write().unwrap_or_else(|e| e.into_inner());
            let session = self.session_mut(&mut sessions, run_id)?;
            self.transition(session, SandboxState::Checkpointed)?;
            if let Some(metadata) = session.metadata.as_mut() {
                metadata.state = SandboxState::Checkpointed;
            }
            session.metadata.clone()
        };
        if let Some(metadata) = metadata.as_ref() {
            self.persist_metadata(metadata)?;
        }

        self.event_sink.publish(SandboxEvent::SandboxCheckpointed {
            sandbox_id: checkpoint.sandbox_id.clone(),
            run_id: checkpoint.run_id.clone(),
            checkpoint_kind: checkpoint.kind,
            rescue_ref: checkpoint.rescue_ref.clone(),
            upper_snapshot: checkpoint.upper_snapshot.clone(),
            checkpointed_at,
        });

        Ok(checkpoint)
    }

    pub fn preserve(
        &self,
        run_id: &RunId,
        reason: PreservationReason,
    ) -> Result<(), WorkspaceError> {
        let preserved_at = self.clock.now_millis();
        let (sandbox_id, metadata) = {
            let mut sessions = self.sessions.write().unwrap_or_else(|e| e.into_inner());
            let session = self.session_mut(&mut sessions, run_id)?;
            self.transition(session, SandboxState::Preserved)?;
            if let Some(metadata) = session.metadata.as_mut() {
                metadata.state = SandboxState::Preserved;
                metadata.pid = None;
            }
            (session.sandbox_id.clone(), session.metadata.clone())
        };
        if let Some(metadata) = metadata.as_ref() {
            self.persist_metadata(metadata)?;
        }

        self.event_sink.publish(SandboxEvent::SandboxPreserved {
            sandbox_id,
            run_id: run_id.clone(),
            reason,
            preserved_at,
        });

        Ok(())
    }

    pub async fn destroy(
        &self,
        run_id: &RunId,
        preserve: bool,
        reason: DestroyReason,
    ) -> Result<DestroyResult, WorkspaceError> {
        let strategy = self.sandbox_strategy(run_id)?;
        {
            let mut sessions = self.sessions.write().unwrap_or_else(|e| e.into_inner());
            let session = self.session_mut(&mut sessions, run_id)?;
            self.transition(session, SandboxState::Destroying)?;
            if let Some(metadata) = session.metadata.as_mut() {
                metadata.state = SandboxState::Destroying;
                metadata.pid = None;
            }
        }

        let result = self.provider(strategy)?.destroy(run_id, preserve).await?;
        let destroyed_at = self.clock.now_millis();

        // Drop the recovery registry entry on a full (non-preserve) destroy.
        // Preserved sandboxes still exist on disk and must stay attributable
        // at the next boot; destroyed ones intentionally must not look
        // `sandbox_lost` when they are picked up again by recovery.
        if !preserve {
            self.remove_registry_entry(&result.sandbox_id)?;
        }

        {
            let mut sessions = self.sessions.write().unwrap_or_else(|e| e.into_inner());
            let session = self.session_mut(&mut sessions, run_id)?;
            self.transition(session, SandboxState::Destroyed)?;
            if let Some(metadata) = session.metadata.as_mut() {
                metadata.state = SandboxState::Destroyed;
            }
        }

        self.event_sink.publish(SandboxEvent::SandboxDestroyed {
            sandbox_id: result.sandbox_id.clone(),
            run_id: run_id.clone(),
            files_changed: result.files_changed,
            bytes_written: result.bytes_written,
            reason,
            destroyed_at,
        });

        Ok(result)
    }

    pub async fn observe_resource_limit(
        &self,
        run_id: &RunId,
        dimension: ResourceDimension,
        observed: u64,
    ) -> Result<(), WorkspaceError> {
        let (sandbox_id, policy) = {
            let sessions = self.sessions.read().unwrap_or_else(|e| e.into_inner());
            let session = sessions
                .get(run_id)
                .ok_or_else(|| WorkspaceError::SandboxNotFound {
                    run_id: run_id.clone(),
                })?;
            (session.sandbox_id.clone(), session.policy.clone())
        };
        let limit =
            limit_for(&policy, dimension).ok_or_else(|| WorkspaceError::ResourceLimitMissing {
                run_id: run_id.clone(),
                dimension,
            })?;
        let at = self.clock.now_millis();

        self.event_sink
            .publish(SandboxEvent::SandboxResourceLimitExceeded {
                sandbox_id,
                run_id: run_id.clone(),
                dimension,
                limit,
                observed,
                at,
            });

        match policy.on_resource_exhaustion {
            OnExhaustion::Destroy => {
                self.destroy(
                    run_id,
                    false,
                    DestroyReason::ResourceLimitExceeded {
                        dimension,
                        limit,
                        observed,
                    },
                )
                .await?;
            }
            OnExhaustion::PauseAwaitOperator => {
                self.preserve(
                    run_id,
                    PreservationReason::AwaitingResourceRaise {
                        dimension,
                        limit,
                        observed,
                    },
                )?;
            }
            OnExhaustion::ReportOnly => {}
        }

        Ok(())
    }

    // ─── F65 PR-5: session-scoped provision + terminate + resume ──────────

    /// F65 PR-5: provision a session-scoped sandbox.
    ///
    /// Delegates to [`Self::provision_or_reconnect`] for the mount/upper/
    /// work/lower work, then builds a [`SessionSandbox`] carrying the
    /// paths the orchestrator + confined child need. Emits
    /// `SessionAttemptStarted` on the F65 event sink once the mount is
    /// live + the registry entry is durable (Q6: atomic "sandbox ready →
    /// event").
    ///
    /// If `spec.base_snapshot_id` is `Some`, call
    /// [`Self::restore_from_snapshot`] instead — this method is the
    /// fresh-provision entrypoint only.
    pub async fn provision_for_session(
        &self,
        spec: crate::sandbox::f65::SessionProvisionSpec,
    ) -> Result<crate::sandbox::f65::SessionSandbox, WorkspaceError> {
        if spec.base_snapshot_id.is_some() {
            return Err(WorkspaceError::unimplemented(
                "provision_for_session: spec carries base_snapshot_id; callers \
                 must use restore_from_snapshot for resume paths",
            ));
        }
        let run_id = spec.root_run_id.clone();
        let project = spec.project.clone();
        let provisioned = self
            .provision_or_reconnect(&run_id, None, project.clone(), spec.policy.clone())
            .await?;
        let session_sandbox = self.build_session_sandbox(&spec, &provisioned, None)?;

        {
            let mut map = self
                .session_sandboxes
                .write()
                .unwrap_or_else(|e| e.into_inner());
            map.insert(spec.session_id.clone(), session_sandbox.clone());
        }

        self.f65_event_sink.publish(
            crate::sandbox::f65::F65SandboxEvent::SessionAttemptStarted {
                project,
                session_id: spec.session_id.clone(),
                root_run_id: spec.root_run_id.clone(),
                attempt_number: spec.attempt_number,
                max_attempts: spec.max_attempts,
            },
        );

        Ok(session_sandbox)
    }

    /// F65 PR-5: terminate the session-scoped sandbox for `session_id`,
    /// reflink the RW upperdir into a durable snapshot, and reap the
    /// overlay teardown state.
    ///
    /// Returns the new `WorkspaceSnapshotId`. Emits
    /// `WorkspaceSnapshotCreated` after the metadata stamp lands
    /// (write-before-event: readers who receive the event see a fully
    /// populated projection row). Emits `WorkspaceBackendDegraded` at
    /// most once per session if `reflink_tree_with_fallback` fell back
    /// to byte-copy.
    pub async fn terminate_for_session(
        &self,
        session_id: &cairn_domain::SessionId,
        _reason: crate::sandbox::f65::TerminationReason,
    ) -> Result<cairn_domain::WorkspaceSnapshotId, WorkspaceError> {
        let session_sandbox = {
            let map = self
                .session_sandboxes
                .read()
                .unwrap_or_else(|e| e.into_inner());
            map.get(session_id).cloned().ok_or_else(|| {
                WorkspaceError::sandbox_op(
                    &RunId::new(session_id.as_str()),
                    "terminate_for_session.unknown_session",
                    "no active session sandbox",
                )
            })?
        };
        let run_id = session_sandbox.root_run_id.clone();
        let snapshot_root = self.snapshot_dir.as_ref().ok_or_else(|| {
            WorkspaceError::sandbox_op(
                &run_id,
                "terminate_for_session",
                "snapshot_dir not configured on SandboxService; call with_snapshot_dir(..)",
            )
        })?;

        // Snapshot id + on-disk path.
        let snapshot_id = cairn_domain::WorkspaceSnapshotId::new(new_snapshot_ulid());
        let snapshot_path = snapshot_root.join(snapshot_id.as_str());
        fs::create_dir_all(&snapshot_path)
            .map_err(|error| WorkspaceError::sandbox_op(&run_id, "create_snapshot_dir", error))?;

        // Unmount via the existing destroy path (preserve=false reaps the
        // overlay dirs + drops the registry entry); we do the reflink
        // BEFORE destroy consumes the upper. That ordering is
        // load-bearing: arch §4.3.3 mandates umount-first-then-reflink to
        // avoid racing the child agent's fsync. The existing
        // `maybe_unmount` in the overlay provider runs inside `destroy`,
        // so we bypass it: read the upper path off the F65 record (which
        // the previous `provision_for_session` populated) and reflink
        // from that onto the snapshot dir. `destroy` then reaps the
        // overlay itself.
        let degraded_flag = self.degraded_flag_for_session(session_id);
        let outcome = crate::providers::reflink_tree_with_fallback(
            &session_sandbox.upper,
            &snapshot_path,
            &degraded_flag,
        )
        .map_err(|error| WorkspaceError::sandbox_op(&run_id, "reflink_upper_to_snapshot", error))?;

        // Emit the one-shot degraded event. `compare_exchange` above
        // guarantees only the first flip lands here; across resumes the
        // same session flag stays `true` so a second terminate is a
        // no-op.
        if !outcome.reflink_used {
            // Only emit on the very first FS-level fallback for this
            // session. The atomic already flipped in
            // reflink_tree_with_fallback; we need a separate "already
            // emitted" marker to dedupe the event itself.
            self.maybe_emit_degraded_once(session_id, &session_sandbox.project);
        }

        // Stamp metadata on the (pre-emitted) snapshot row. The cairn-app
        // event sink may have already fired the PR-2 insert for
        // `WorkspaceSnapshotCreated` — but we emit AFTER the stamp here,
        // so readers see the fully-populated row. This is the "stamp
        // before event" invariant in the plan §2.4.
        if let Err(err) = self
            .snapshot_writer
            .stamp_metadata(
                &snapshot_id,
                &snapshot_path.display().to_string(),
                outcome.bytes_copied,
                outcome.reflink_used,
                session_sandbox.base_snapshot_id.as_ref(),
            )
            .await
        {
            return Err(WorkspaceError::sandbox_op(
                &run_id,
                "stamp_snapshot_metadata",
                err,
            ));
        }

        // Emit WorkspaceSnapshotCreated. cairn-app translates this to the
        // domain event + projection insert. #482: carry bytes /
        // reflink_used / parent_snapshot_id on the event itself so a
        // fresh log replay rebuilds the projection row with the same
        // metadata the live writer produced.
        self.f65_event_sink.publish(
            crate::sandbox::f65::F65SandboxEvent::WorkspaceSnapshotCreated {
                project: session_sandbox.project.clone(),
                snapshot_id: snapshot_id.clone(),
                workspace_id: session_sandbox.workspace_id.clone(),
                session_id: session_id.clone(),
                bytes: outcome.bytes_copied,
                reflink_used: outcome.reflink_used,
                parent_snapshot_id: session_sandbox.base_snapshot_id.clone(),
            },
        );

        // Reap overlay + drop the registry entry.
        let _ = self
            .destroy(&run_id, false, cairn_domain::DestroyReason::Completed)
            .await?;

        // Remove the session-scoped sandbox record.
        {
            let mut map = self
                .session_sandboxes
                .write()
                .unwrap_or_else(|e| e.into_inner());
            map.remove(session_id);
        }

        Ok(snapshot_id)
    }

    /// F65 PR-5: resume a session from a previously-snapshotted upperdir.
    ///
    /// Reflinks `~/.cairn/snapshots/<base_snapshot_id>/` into a fresh
    /// upperdir-seed directory, mounts a new overlay over it, allocates
    /// a fresh `RunId`, and returns the new [`SessionSandbox`] plus the
    /// resumed root run id. Takes + holds an in-flight-restore lease on
    /// `base_snapshot_id` so the GC sweeper cannot reap the snapshot
    /// mid-resume.
    ///
    /// PR-5 boundary: returns only the sandbox; the caller is responsible
    /// for loading `F65CheckpointRecord.body` separately (PR-6 wires the
    /// LLM-context restore). The plan intentionally keeps resume-side
    /// surface minimal so PR-5 can ship without the summarizer.
    pub async fn restore_from_snapshot(
        &self,
        spec: crate::sandbox::f65::SessionProvisionSpec,
    ) -> Result<crate::sandbox::f65::SessionSandbox, WorkspaceError> {
        let base_snapshot_id = spec.base_snapshot_id.clone().ok_or_else(|| {
            WorkspaceError::sandbox_op(
                &spec.root_run_id,
                "restore_from_snapshot",
                "spec.base_snapshot_id is required",
            )
        })?;
        let snapshot_root = self.snapshot_dir.as_ref().ok_or_else(|| {
            WorkspaceError::sandbox_op(
                &spec.root_run_id,
                "restore_from_snapshot",
                "snapshot_dir not configured",
            )
        })?;
        let snapshot_src = snapshot_root.join(base_snapshot_id.as_str());
        if !snapshot_src.exists() {
            return Err(WorkspaceError::sandbox_op(
                &spec.root_run_id,
                "restore_from_snapshot",
                format!("snapshot {} not found", snapshot_src.display()),
            ));
        }

        // Acquire in-flight-restore lease to hold off the GC sweeper.
        {
            let mut set = self
                .inflight_restores
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            set.insert(base_snapshot_id.clone());
        }
        // RAII-like drop guard to release the lease on every return path.
        // Matches the #463 poison-recovery policy: recover the inner
        // set even across a poisoned lock so the release still lands.
        // A silently-skipped release (the pre-#463 `if let Ok(..)`
        // pattern) would leave the set holding a stale snapshot id
        // forever, blocking the GC sweeper from reaping a snapshot
        // that no thread is actually walking.
        struct InflightGuard<'a> {
            svc: &'a SandboxService,
            snapshot_id: cairn_domain::WorkspaceSnapshotId,
        }
        impl<'a> Drop for InflightGuard<'a> {
            fn drop(&mut self) {
                let mut set = self
                    .svc
                    .inflight_restores
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                set.remove(&self.snapshot_id);
            }
        }
        let _guard = InflightGuard {
            svc: self,
            snapshot_id: base_snapshot_id.clone(),
        };

        // PR-5 shape: provision a fresh overlay for the new root-Run,
        // then reflink the snapshot contents into the new upperdir so the
        // child agent sees the prior session's state on first read. This
        // is a pragmatic placement — overlay provision creates an empty
        // upper; we fill it with the snapshot before the child is
        // spawned. A dedicated `OverlayProvider::restore` path (plan
        // §2.3) is a PR-6/7 refactor; PR-5 exercises the contract with
        // this minimal approach.
        let run_id = spec.root_run_id.clone();
        let project = spec.project.clone();
        let provisioned = self
            .provision_or_reconnect(&run_id, None, project.clone(), spec.policy.clone())
            .await?;

        let session_sandbox =
            self.build_session_sandbox(&spec, &provisioned, Some(base_snapshot_id.clone()))?;

        // Seed the upperdir from the snapshot AFTER the overlay is
        // mounted so the merged view reflects the restored state.
        // reflink_tree_with_fallback is idempotent across a non-empty
        // destination only when the destination is empty; the fresh
        // upper is guaranteed empty by the provision path above.
        let degraded_flag = self.degraded_flag_for_session(&spec.session_id);
        crate::providers::reflink_tree_with_fallback(
            &snapshot_src,
            &session_sandbox.upper,
            &degraded_flag,
        )
        .map_err(|error| WorkspaceError::sandbox_op(&run_id, "reflink_snapshot_to_upper", error))?;

        {
            let mut map = self
                .session_sandboxes
                .write()
                .unwrap_or_else(|e| e.into_inner());
            map.insert(spec.session_id.clone(), session_sandbox.clone());
        }

        self.f65_event_sink.publish(
            crate::sandbox::f65::F65SandboxEvent::SessionAttemptStarted {
                project,
                session_id: spec.session_id.clone(),
                root_run_id: spec.root_run_id.clone(),
                attempt_number: spec.attempt_number,
                max_attempts: spec.max_attempts,
            },
        );

        Ok(session_sandbox)
    }

    /// F65 PR-5 (#359): return a snapshot of the currently-provisioned
    /// session sandboxes, keyed by `SessionId`. Used by the metrics
    /// gauge + the GC sweeper debug path.
    pub fn live_session_count(&self) -> usize {
        self.session_sandboxes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// F65 PR-5: clear an individual snapshot directory from disk. Called
    /// by the GC sweeper + the admin reap endpoint. Returns `Ok(true)` if
    /// the dir existed + was removed, `Ok(false)` if already gone. Errors
    /// on other failures (IO).
    pub fn reap_snapshot_dir(
        &self,
        snapshot_id: &cairn_domain::WorkspaceSnapshotId,
    ) -> Result<bool, WorkspaceError> {
        // Respect in-flight-restore leases: if a resume is walking the
        // snapshot right now, defer the reap. Next sweep tick picks it
        // up. For operator-driven reap, the caller can retry after the
        // resume completes.
        {
            let set = self
                .inflight_restores
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if set.contains(snapshot_id) {
                return Ok(false);
            }
        }

        let Some(snapshot_root) = self.snapshot_dir.as_ref() else {
            return Ok(false);
        };
        let path = snapshot_root.join(snapshot_id.as_str());
        match fs::remove_dir_all(&path) {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(WorkspaceError::sandbox_op(
                &RunId::new(snapshot_id.as_str()),
                "reap_snapshot_dir",
                err,
            )),
        }
    }

    fn build_session_sandbox(
        &self,
        spec: &crate::sandbox::f65::SessionProvisionSpec,
        provisioned: &ProvisionedSandbox,
        base_snapshot_id: Option<cairn_domain::WorkspaceSnapshotId>,
    ) -> Result<crate::sandbox::f65::SessionSandbox, WorkspaceError> {
        // Derive the overlay-internal paths from the ProvisionedSandbox.
        // For overlay: .path == <root>/merged.
        // For reflink: .path == <root>/root.
        // The F65 SessionSandbox stores merged/upper/work/lower which
        // only make sense for overlay. Reflink sessions still get a
        // SessionSandbox but the non-merged fields point to stable
        // stand-ins so the caller can tell them apart.
        let workspace_id =
            cairn_domain::WorkspaceId::new(format!("ws-{}", spec.root_run_id.as_str()));
        let (merged, upper, work, lower) = match provisioned.strategy {
            SandboxStrategy::Overlay => {
                let merged = provisioned.path.clone();
                let root = merged.parent().map(|p| p.to_path_buf()).ok_or_else(|| {
                    WorkspaceError::sandbox_op(
                        &spec.root_run_id,
                        "build_session_sandbox",
                        "overlay provisioned path has no parent directory",
                    )
                })?;
                (
                    merged,
                    root.join("upper"),
                    root.join("work"),
                    root.join("empty"),
                )
            }
            SandboxStrategy::Reflink => {
                let merged = provisioned.path.clone();
                (merged.clone(), merged.clone(), merged.clone(), merged)
            }
        };

        let confinement =
            crate::sandbox::SandboxConfinement::production(merged.clone(), Vec::new());
        Ok(crate::sandbox::f65::SessionSandbox {
            workspace_id,
            session_id: spec.session_id.clone(),
            root_run_id: spec.root_run_id.clone(),
            project: spec.project.clone(),
            merged,
            upper,
            work,
            lower,
            confinement,
            network: spec.network,
            base_snapshot_id,
        })
    }

    fn degraded_flag_for_session(
        &self,
        session_id: &cairn_domain::SessionId,
    ) -> Arc<std::sync::atomic::AtomicBool> {
        let mut map = self
            .degraded_flag_by_session
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        map.entry(session_id.clone())
            .or_insert_with(|| Arc::new(std::sync::atomic::AtomicBool::new(false)))
            .clone()
    }

    /// F65 PR-5 (#359): unmount any `type=overlay` entry in
    /// `/proc/self/mounts` whose merged path lives under this service's
    /// base_dir AND which the registry knows about.
    ///
    /// The "registry knows about it" gate is load-bearing: a co-tenant
    /// cairn-app may also have registered overlay mounts under a sibling
    /// path; we only touch ours. On non-Linux hosts this is a no-op.
    async fn sweep_orphan_overlays(&self) -> Result<(), WorkspaceError> {
        #[cfg(target_os = "linux")]
        {
            let mounts = match fs::read_to_string("/proc/self/mounts") {
                Ok(s) => s,
                Err(_) => return Ok(()),
            };
            let base_dir = self
                .base_dir
                .canonicalize()
                .unwrap_or_else(|_| self.base_dir.clone());
            let registry = self.list_registry_entries()?;
            let registry_paths: HashMap<PathBuf, RegistryEntry> =
                registry.into_iter().map(|e| (e.path.clone(), e)).collect();
            for line in mounts.lines() {
                // Lines are: `device mountpoint fstype opts freq passno`
                let mut parts = line.split_whitespace();
                let _device = parts.next();
                let Some(mountpoint) = parts.next() else {
                    continue;
                };
                let Some(fstype) = parts.next() else {
                    continue;
                };
                if fstype != "overlay" {
                    continue;
                }
                let mountpoint_path = PathBuf::from(mountpoint);
                // Is this mountpoint under our base dir?
                if !mountpoint_path.starts_with(&base_dir) {
                    continue;
                }
                // Find the owning registry entry. The registry's stored
                // path is the `ProvisionedSandbox.path` which for overlay
                // is the merged directory. Match that.
                let Some(entry) = registry_paths.get(&mountpoint_path) else {
                    continue;
                };

                use nix::mount::{umount2, MntFlags};
                if let Err(err) = umount2(&mountpoint_path, MntFlags::MNT_DETACH) {
                    eprintln!(
                        "sweep_orphan_overlays: umount2(MNT_DETACH, {}) failed: {err}; leaving \
                         mount in place (host will eventually GC after PID reuse)",
                        mountpoint_path.display(),
                    );
                    continue;
                }
                eprintln!(
                    "sweep_orphan_overlays: unmounted dangling overlay {} (sidecar sandbox_id={})",
                    mountpoint_path.display(),
                    entry.sandbox_id.as_str(),
                );
                // Emit the crash-recovery event. The session_id is not
                // persisted in the registry sidecar today — we use the
                // run_id as a best-effort placeholder. PR-6+ may extend
                // the sidecar to carry session_id directly; for PR-5
                // the run-id-as-session fallback keeps the event shape
                // addressable from the run-level recovery service.
                self.f65_event_sink.publish(
                    crate::sandbox::f65::F65SandboxEvent::SandboxCrashRecovered {
                        project: entry.project.clone(),
                        session_id: cairn_domain::SessionId::new(entry.run_id.as_str()),
                        run_id: entry.run_id.clone(),
                    },
                );
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            // No overlay mount concept on non-Linux.
        }
        Ok(())
    }

    /// Emit `WorkspaceBackendDegraded` at most once per session. Uses a
    /// dedicated emission-tracking set (separate from the AtomicBool
    /// that reflink_tree_with_fallback flips) so the event fires on the
    /// first observed FS fallback for a session and never again — even
    /// across terminate-resume-terminate cycles.
    fn maybe_emit_degraded_once(&self, session_id: &cairn_domain::SessionId, project: &ProjectKey) {
        let newly_inserted = self
            .degraded_emitted_sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(session_id.clone());
        if !newly_inserted {
            return;
        }
        self.f65_event_sink.publish(
            crate::sandbox::f65::F65SandboxEvent::WorkspaceBackendDegraded {
                project: project.clone(),
                session_id: session_id.clone(),
                backend: "ext4_copy".to_string(),
                reason: "reflink_unsupported_fs".to_string(),
            },
        );
    }

    pub async fn recover_all(&self) -> Result<SandboxRecoverySummary, WorkspaceError> {
        // F65 PR-5 (#359): sweep dangling overlay mounts left by a crashed
        // cairn-app before we enumerate providers. `/proc/self/mounts`
        // lists every mount inherited from our parent's ns; we
        // unmount any `type=overlay` whose merged dir falls under
        // base_dir/<sandbox_id>/merged and for which a registry
        // sidecar exists. Emits `SandboxCrashRecovered` per successful
        // umount so operators see the repair.
        self.sweep_orphan_overlays().await?;

        let mut handles = Vec::new();
        for provider in self.providers.values() {
            handles.extend(provider.list().await?);
        }
        handles.sort_by(|left, right| {
            left.metadata
                .sandbox_id
                .as_str()
                .cmp(right.metadata.sandbox_id.as_str())
        });

        // Track every sandbox_id that the providers surfaced so we can
        // diff against the recovery registry below. Anything in the
        // registry that the providers did NOT list is a candidate for
        // `sandbox_lost`.
        let mut observed: std::collections::HashSet<String> = std::collections::HashSet::new();
        for handle in &handles {
            observed.insert(handle.metadata.sandbox_id.as_str().to_owned());
        }

        let mut summary = SandboxRecoverySummary::default();
        for handle in handles {
            let mut metadata = handle.metadata;
            if matches!(
                metadata.state,
                SandboxState::Destroyed | SandboxState::Failed
            ) {
                continue;
            }

            let run_id = metadata.run_id.clone();
            match self.provider(metadata.strategy)?.reconnect(&run_id).await {
                Ok(Some(sandbox)) => {
                    metadata.state = recovered_state(metadata.state);
                    metadata.path = sandbox.path.clone();
                    metadata.base_rev = sandbox.base_revision.clone();
                    metadata.pid = None;
                    metadata.heartbeat_at = self.clock.now_millis();
                    self.persist_metadata(&metadata)?;
                    self.remember_recovered_session(
                        metadata.clone(),
                        Some(sandbox),
                        recovery_policy(&metadata),
                    );
                    summary.reconnected += 1;
                }
                Ok(None) => {}
                Err(WorkspaceError::BaseRevisionDrift {
                    expected, actual, ..
                }) => {
                    let detected_at = self.clock.now_millis();
                    if let Some(repo_id) = metadata.repo_id.clone() {
                        self.event_sink
                            .publish(SandboxEvent::SandboxBaseRevisionDrift {
                                sandbox_id: metadata.sandbox_id.clone(),
                                run_id: run_id.clone(),
                                project: metadata.project.clone(),
                                repo_id,
                                expected: expected.clone(),
                                actual: actual.clone(),
                                detected_at,
                            });
                    }

                    metadata.state = SandboxState::Preserved;
                    metadata.pid = None;
                    metadata.heartbeat_at = detected_at;
                    self.persist_metadata(&metadata)?;
                    self.remember_recovered_session(
                        metadata.clone(),
                        None,
                        recovery_policy(&metadata),
                    );
                    self.event_sink.publish(SandboxEvent::SandboxPreserved {
                        sandbox_id: metadata.sandbox_id.clone(),
                        run_id: run_id.clone(),
                        reason: PreservationReason::BaseRevisionDrift { expected, actual },
                        preserved_at: detected_at,
                    });
                    summary.preserved += 1;
                }
                Err(error) => {
                    let failed_at = self.clock.now_millis();
                    metadata.state = SandboxState::Failed;
                    metadata.pid = None;
                    self.persist_metadata(&metadata)?;
                    self.remember_recovered_session(
                        metadata.clone(),
                        None,
                        recovery_policy(&metadata),
                    );
                    self.event_sink
                        .publish(SandboxEvent::SandboxProvisioningFailed {
                            sandbox_id: metadata.sandbox_id.clone(),
                            run_id: run_id.clone(),
                            error_kind: SandboxErrorKind::Recovery,
                            error: error.to_string(),
                            failed_at,
                        });
                    summary.failed += 1;
                }
            }
        }

        // RFC 020 §"Run recovery matrix": for every registry entry whose
        // provider did not surface a live handle, check the filesystem.
        // Absent root → emit `SandboxLost` and record the (run, project)
        // pair for the run-level recovery service to transition to `failed`
        // with `reason: sandbox_lost`. Present root with no provider handle
        // means the provider's own metadata is corrupt — leave it alone;
        // the provider's list() path is responsible for that case and we
        // would double-report otherwise.
        let mut registry = self.list_registry_entries()?;
        registry.sort_by(|a, b| a.sandbox_id.as_str().cmp(b.sandbox_id.as_str()));
        for entry in registry {
            if observed.contains(entry.sandbox_id.as_str()) {
                continue;
            }
            if entry.path.exists() {
                continue;
            }
            let detected_at = self.clock.now_millis();
            self.event_sink.publish(SandboxEvent::SandboxLost {
                sandbox_id: entry.sandbox_id.clone(),
                run_id: entry.run_id.clone(),
                project: entry.project.clone(),
                sandbox_path: entry.path.clone(),
                detected_at,
            });
            // Drop the registry entry so subsequent boots don't re-emit
            // the same `SandboxLost` event for a run that has already
            // been transitioned to terminal state by the run-recovery
            // service.
            self.remove_registry_entry(&entry.sandbox_id)?;
            summary.lost_runs.push((entry.run_id, entry.project));
        }
        summary.lost = summary.lost_runs.len() as u32;

        // RFC 020 §"Run recovery matrix" — `AllowlistRevoked` row. For
        // every registry entry that survived the lost-sweep above and
        // carries a bound `repo_id`, ask the project allowlist whether
        // the repo is still authorised. If not → emit
        // `SandboxAllowlistRevoked` and record the `(run, project, repo)`
        // triple so the run-level recovery service can transition the
        // bound run to `WaitingApproval` with a synthesized approval.
        // Mark the entry as `allowlist_revoked_handled` so subsequent
        // boots do not re-emit the same event / re-create the approval.
        //
        // No allowlist wired (`None`) → skip entirely. Workspace-layer
        // unit tests exercise the sweep directly with seeded entries and
        // would otherwise need to stand up the full access service.
        //
        // **Authoritative-allowlist gate (Cursor Bugbot high-1):**
        // `ProjectRepoAccessService` is an in-memory `RwLock<HashMap>`
        // populated via HTTP `POST /v1/projects/.../repos/...` — it is
        // NOT replayed from the event log on boot. A freshly-started
        // cairn-app therefore sees an empty allowlist for every project
        // until the operator (or an external controller) re-asserts
        // entries. Treating that empty state as "all repos revoked"
        // would flag every repo-backed sandbox as `AllowlistRevoked` on
        // every restart — a catastrophic false positive that would
        // freeze unrelated runs until an operator resolved the flood of
        // synthesized approvals.
        //
        // Until the allowlist persists across restarts (whether via an
        // event replay, a sidecar, or a projection), the sweep is only
        // sound for projects with *at least one* allowlisted repo at
        // recovery time. Projects with zero entries are treated as
        // "not authoritative yet" and skipped; the sweep picks them up
        // on the next `recover_all` call once the operator has re-
        // asserted the allowlist. This is strictly an under-approximation
        // (false negatives, no false positives) — the exact opposite of
        // the failure mode the bug report flagged.
        //
        // TODO(#556, RFC 016 persistence): when the allowlist gains
        // durable storage, remove the "non-empty project" gate and
        // rely on the allowlist's own authoritative semantics.
        if let Some(allowlist) = self.allowlist.clone() {
            let entries = self.list_registry_entries()?;
            // Cache per-project "is the allowlist authoritative?" answers
            // so we don't re-query `list_for_project` for every registry
            // entry in the same project.
            let mut project_authoritative: HashMap<ProjectKey, bool> = HashMap::new();
            for mut entry in entries {
                if entry.allowlist_revoked_handled {
                    continue;
                }
                let Some(repo_id) = entry.repo_id.clone() else {
                    continue;
                };
                let ctx = RepoAccessContext {
                    project: entry.project.clone(),
                };
                let authoritative = match project_authoritative.get(&entry.project).copied() {
                    Some(v) => v,
                    None => {
                        let v = !allowlist.list_for_project(&ctx).await.is_empty();
                        project_authoritative.insert(entry.project.clone(), v);
                        v
                    }
                };
                if !authoritative {
                    continue;
                }
                if allowlist.is_allowed(&ctx, &repo_id).await {
                    continue;
                }
                let detected_at = self.clock.now_millis();
                self.event_sink
                    .publish(SandboxEvent::SandboxAllowlistRevoked {
                        sandbox_id: entry.sandbox_id.clone(),
                        run_id: entry.run_id.clone(),
                        project: entry.project.clone(),
                        repo_id: repo_id.clone(),
                        revoked_at: detected_at,
                        detected_at,
                    });
                entry.allowlist_revoked_handled = true;
                self.write_registry_entry(&entry)?;
                summary
                    .allowlist_revoked_runs
                    .push((entry.run_id, entry.project, repo_id));
            }
            summary.preserved_allowlist_revoked = summary.allowlist_revoked_runs.len() as u32;
        }

        // RFC 020 §"Run recovery matrix" — `BaseRevisionDrift` row. For
        // every surviving overlay-on-repo registry entry, compare the
        // stored `base_revision` against the locked clone's current
        // HEAD. Mismatch → emit `SandboxBaseRevisionDrift` + record the
        // `(run, project, repo)` triple so the run-level recovery
        // service can transition the bound run to `WaitingApproval`
        // with a synthesized re-provision approval.
        //
        // Reflink sandboxes are exempt (RFC 016 §"Sandbox provider
        // selection"): a reflink copy is physically independent of the
        // upstream clone, so the clone's HEAD moving cannot corrupt
        // the sandbox's contents. Only overlay sandboxes compose their
        // upper layer against a live lower layer and therefore depend
        // on the clone's HEAD remaining pinned between provisioning
        // and recovery.
        //
        // `clone_cache == None` → no-op, mirroring the allowlist sweep's
        // `None` behaviour. Entries lacking a stored `base_revision` or
        // `repo_id` are skipped (nothing to diff against).
        //
        // Idempotency: `base_revision_drift_handled` is set after the
        // first emission and persisted back to the sidecar so a second
        // boot does not re-fire the approval. The tombstone is cleared
        // only when the operator destroys the sandbox (which removes
        // the registry entry) — a manual re-provision is expected to
        // write a fresh entry with the new revision.
        if let Some(clone_cache) = self.clone_cache.clone() {
            let entries = self.list_registry_entries()?;
            // Cache `(tenant, repo)` → current HEAD lookups so a project
            // with N runs against the same repo makes one filesystem read
            // instead of N.
            let mut head_cache: HashMap<(cairn_domain::TenantId, RepoId), Option<String>> =
                HashMap::new();
            for mut entry in entries {
                if entry.base_revision_drift_handled {
                    continue;
                }
                // Reflink: physically independent, skip.
                if matches!(entry.strategy, SandboxStrategy::Reflink) {
                    continue;
                }
                let Some(repo_id) = entry.repo_id.clone() else {
                    continue;
                };
                let Some(stored) = entry.base_revision.clone() else {
                    continue;
                };
                let tenant = entry.project.tenant_id.clone();
                let key = (tenant.clone(), repo_id.clone());
                let current = match head_cache.get(&key).cloned() {
                    Some(v) => v,
                    None => {
                        let v = clone_cache
                            .current_head(&tenant, &repo_id)
                            .await
                            .map_err(|e| {
                                WorkspaceError::sandbox_op(&entry.run_id, "read_clone_head", e)
                            })?;
                        head_cache.insert(key, v.clone());
                        v
                    }
                };
                let Some(current) = current else {
                    // Clone missing — distinct failure mode from drift.
                    // SandboxLost-style remediation is the wrong fit here
                    // (the sandbox root still exists), but there's also no
                    // drift to report. Leave the entry untouched; if the
                    // operator re-creates the clone with the original
                    // revision the next sweep sees no drift, and if they
                    // re-clone at a new HEAD the next sweep fires.
                    continue;
                };
                if current == stored {
                    continue;
                }
                let detected_at = self.clock.now_millis();
                self.event_sink
                    .publish(SandboxEvent::SandboxBaseRevisionDrift {
                        sandbox_id: entry.sandbox_id.clone(),
                        run_id: entry.run_id.clone(),
                        project: entry.project.clone(),
                        repo_id: repo_id.clone(),
                        expected: stored.clone(),
                        actual: current.clone(),
                        detected_at,
                    });
                self.event_sink.publish(SandboxEvent::SandboxPreserved {
                    sandbox_id: entry.sandbox_id.clone(),
                    run_id: entry.run_id.clone(),
                    reason: PreservationReason::BaseRevisionDrift {
                        expected: stored,
                        actual: current,
                    },
                    preserved_at: detected_at,
                });
                entry.base_revision_drift_handled = true;
                self.write_registry_entry(&entry)?;
                summary
                    .base_revision_drift_runs
                    .push((entry.run_id, entry.project, repo_id));
            }
            summary.preserved_base_revision_drift = summary.base_revision_drift_runs.len() as u32;
        }

        // RFC 020 §"Run recovery matrix" — healthy-reattach row. Any
        // registry entry that survived the lost-sweep (path exists), the
        // allowlist-revoked sweep (repo still allowlisted, or no bound
        // repo), AND the base-revision-drift sweep (overlay clone HEAD
        // still pinned) represents a sandbox that cleanly survived the
        // crash. Emit `SandboxReattached` so the run-level recovery
        // service can record an audit trail entry
        // (`RecoveryAttempted{reason:"sandbox_reattached"}`) without
        // transitioning state — the run stays in its existing
        // non-terminal state and the orchestrator resumes it on its
        // next tick.
        //
        // Entries already surfaced by a provider via `list()` are
        // already counted under `reconnected`/`preserved`/`failed` and
        // must not be double-reported here. Entries whose
        // `allowlist_revoked_handled` or `base_revision_drift_handled`
        // flag is set are owned by those sweeps. Everything else that
        // has a present path is healthy.
        let reattach_entries = self.list_registry_entries()?;
        for entry in reattach_entries {
            if observed.contains(entry.sandbox_id.as_str()) {
                continue;
            }
            if !entry.path.exists() {
                continue;
            }
            if entry.allowlist_revoked_handled || entry.base_revision_drift_handled {
                continue;
            }
            let reattached_at = self.clock.now_millis();
            self.event_sink.publish(SandboxEvent::SandboxReattached {
                sandbox_id: entry.sandbox_id.clone(),
                run_id: entry.run_id.clone(),
                project: entry.project.clone(),
                sandbox_path: entry.path.clone(),
                reattached_at,
            });
            summary.reattached_runs.push((entry.run_id, entry.project));
        }
        summary.reattached = summary.reattached_runs.len() as u32;

        Ok(summary)
    }

    fn provider(&self, strategy: SandboxStrategy) -> Result<&dyn SandboxProvider, WorkspaceError> {
        self.providers
            .get(&strategy)
            .map(|provider| provider.as_ref())
            .ok_or(WorkspaceError::ProviderUnavailable { strategy })
    }

    fn remember_recovered_session(
        &self,
        metadata: SandboxMetadata,
        sandbox: Option<ProvisionedSandbox>,
        policy: SandboxPolicy,
    ) {
        self.sessions
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                metadata.run_id.clone(),
                SandboxSession {
                    sandbox_id: metadata.sandbox_id.clone(),
                    run_id: metadata.run_id.clone(),
                    task_id: metadata.task_id.clone(),
                    project: metadata.project.clone(),
                    policy,
                    state: metadata.state,
                    sandbox,
                    metadata: Some(metadata),
                },
            );
    }

    fn resolve_strategy(
        &self,
        request: &SandboxStrategyRequest,
    ) -> Result<StrategyResolution, WorkspaceError> {
        match *request {
            SandboxStrategyRequest::Force(strategy) => {
                self.provider(strategy)?;
                Ok(StrategyResolution {
                    actual: strategy,
                    degraded_from: None,
                    degrade_reason: None,
                })
            }
            SandboxStrategyRequest::Preferred(strategy) => {
                if self.providers.contains_key(&strategy) {
                    return Ok(StrategyResolution {
                        actual: strategy,
                        degraded_from: None,
                        degrade_reason: None,
                    });
                }

                let fallback = fallback_strategy(strategy);
                if self.providers.contains_key(&fallback) {
                    Ok(StrategyResolution {
                        actual: fallback,
                        degraded_from: Some(strategy),
                        degrade_reason: Some(format!(
                            "preferred strategy {strategy:?} unavailable; fell back to {fallback:?}"
                        )),
                    })
                } else {
                    Err(WorkspaceError::ProviderUnavailable { strategy })
                }
            }
        }
    }

    fn existing_sandbox(
        &self,
        run_id: &RunId,
    ) -> Result<Option<ProvisionedSandbox>, WorkspaceError> {
        let sessions = self.sessions.read().unwrap_or_else(|e| e.into_inner());
        let Some(session) = sessions.get(run_id) else {
            return Ok(None);
        };

        if session.state.is_terminal() {
            return Err(WorkspaceError::InvalidSandboxStateTransition {
                run_id: run_id.clone(),
                from: session.state,
                to: SandboxState::Provisioning,
            });
        }

        Ok(session.sandbox.as_ref().map(|sandbox| {
            let mut resumed = sandbox.clone();
            resumed.is_resumed = true;
            resumed
        }))
    }

    fn sandbox_strategy(&self, run_id: &RunId) -> Result<SandboxStrategy, WorkspaceError> {
        self.sessions
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(run_id)
            .and_then(|session| session.sandbox.as_ref().map(|sandbox| sandbox.strategy))
            .ok_or_else(|| WorkspaceError::SandboxNotFound {
                run_id: run_id.clone(),
            })
    }

    fn session_mut<'a>(
        &self,
        sessions: &'a mut HashMap<RunId, SandboxSession>,
        run_id: &RunId,
    ) -> Result<&'a mut SandboxSession, WorkspaceError> {
        sessions
            .get_mut(run_id)
            .ok_or_else(|| WorkspaceError::SandboxNotFound {
                run_id: run_id.clone(),
            })
    }

    fn transition(
        &self,
        session: &mut SandboxSession,
        next: SandboxState,
    ) -> Result<(), WorkspaceError> {
        if session.state == next {
            return Ok(());
        }
        if !session.state.can_transition_to(next) {
            return Err(WorkspaceError::InvalidSandboxStateTransition {
                run_id: session.run_id.clone(),
                from: session.state,
                to: next,
            });
        }
        session.state = next;
        Ok(())
    }

    /// Directory where recovery registry sidecars are kept. Lives under
    /// `base_dir/.registry/<sandbox_id>/registry.json` — deliberately
    /// separate from the sandbox root so that `rm -rf <sandbox_root>`
    /// does not take the registry entry with it.
    fn registry_dir(&self) -> PathBuf {
        self.base_dir.join(RECOVERY_REGISTRY_DIRNAME)
    }

    fn registry_entry_path(&self, sandbox_id: &crate::sandbox::SandboxId) -> PathBuf {
        self.registry_dir()
            .join(sandbox_id.as_str())
            .join(REGISTRY_ENTRY_FILENAME)
    }

    fn write_registry_entry(&self, entry: &RegistryEntry) -> Result<(), WorkspaceError> {
        let path = self.registry_entry_path(&entry.sandbox_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                WorkspaceError::sandbox_op(&entry.run_id, "create_registry_dir", error)
            })?;
        }
        let encoded = serde_json::to_vec_pretty(entry).map_err(|error| {
            WorkspaceError::sandbox_op(&entry.run_id, "serialize_registry_entry", error)
        })?;
        fs::write(&path, encoded)
            .map_err(|error| WorkspaceError::sandbox_op(&entry.run_id, "write_registry", error))
    }

    fn remove_registry_entry(
        &self,
        sandbox_id: &crate::sandbox::SandboxId,
    ) -> Result<(), WorkspaceError> {
        let entry_dir = self.registry_dir().join(sandbox_id.as_str());
        match fs::remove_dir_all(&entry_dir) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(WorkspaceError::sandbox_op(
                &RunId::new(sandbox_id.as_str()),
                "remove_registry_entry",
                error,
            )),
        }
    }

    fn list_registry_entries(&self) -> Result<Vec<RegistryEntry>, WorkspaceError> {
        let dir = self.registry_dir();
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(WorkspaceError::sandbox_op(
                    &RunId::new("_registry"),
                    "read_registry_dir",
                    error,
                ))
            }
        };
        let mut out = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| {
                WorkspaceError::sandbox_op(&RunId::new("_registry"), "read_registry_entry", error)
            })?;
            let path = entry.path().join(REGISTRY_ENTRY_FILENAME);
            if !path.is_file() {
                continue;
            }
            let bytes = fs::read(&path).map_err(|error| {
                WorkspaceError::sandbox_op(&RunId::new("_registry"), "read_registry_body", error)
            })?;
            match serde_json::from_slice::<RegistryEntry>(&bytes) {
                Ok(parsed) => out.push(parsed),
                // A corrupt sidecar must not break recovery — it just means
                // we cannot attribute a lost sandbox to a run. Skip and
                // continue so the rest of the sweep proceeds; operators see
                // the broken file in the base dir.
                Err(_) => continue,
            }
        }
        Ok(out)
    }

    fn persist_metadata(&self, metadata: &SandboxMetadata) -> Result<(), WorkspaceError> {
        let sandbox_dir = self.base_dir.join(metadata.sandbox_id.as_str());
        fs::create_dir_all(&sandbox_dir).map_err(|error| {
            WorkspaceError::sandbox_op(&metadata.run_id, "create_metadata_dir", error)
        })?;
        let metadata_path = sandbox_dir.join("meta.json");
        let encoded = serde_json::to_vec_pretty(metadata).map_err(|error| {
            WorkspaceError::sandbox_op(&metadata.run_id, "serialize_metadata", error)
        })?;

        // Atomic publish via stage + fsync + rename + directory fsync.
        // Invariant: a concurrent reader of `meta.json` observes
        // either the previous committed contents or the new committed
        // contents, never an empty, torn, or partially-written file.
        // Crash invariant: once this function returns `Ok`, a
        // subsequent boot sees the new contents (the payload is on
        // stable storage and the directory entry update is durable).
        let tmp_name = format!(
            "meta.json.tmp.{}.{:?}",
            std::process::id(),
            std::thread::current().id(),
        );
        let tmp_path = sandbox_dir.join(tmp_name);

        // Stage the new payload into a per-thread sibling file.
        // Wrapped in a helper so every error path removes the orphan
        // tmp file — if we leak tmp files on partial failure they
        // accumulate in the sandbox dir and eventually shadow
        // legitimate recovery reads.
        let stage = || -> std::io::Result<()> {
            use std::io::Write;
            let mut file = fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp_path)?;
            file.write_all(&encoded)?;
            file.sync_all()
        };
        if let Err(error) = stage() {
            // Best-effort cleanup for every pre-rename failure mode:
            // open, write_all, sync_all. Ignore the remove error —
            // surfacing the stage error is what matters.
            let _ = fs::remove_file(&tmp_path);
            return Err(WorkspaceError::sandbox_op(
                &metadata.run_id,
                "stage_metadata_tmp",
                error,
            ));
        }

        if let Err(error) = fs::rename(&tmp_path, &metadata_path) {
            let _ = fs::remove_file(&tmp_path);
            return Err(WorkspaceError::sandbox_op(
                &metadata.run_id,
                "rename_metadata",
                error,
            ));
        }

        // POSIX `rename(2)` publishes the new inode for `meta.json`
        // atomically — any reader now sees the new contents. The
        // directory entry change itself, however, is not crash-durable
        // until the containing directory is fsynced. Without this a
        // crash between `rename` returning and the kernel flushing the
        // dentry to the journal could surface the old meta.json (or no
        // meta.json) on next boot. Best-effort: a failure here doesn't
        // roll back the rename — the payload is published, we just
        // couldn't prove it's durable.
        if let Ok(dir) = fs::File::open(&sandbox_dir) {
            let _ = dir.sync_all();
        }

        Ok(())
    }
}

fn fallback_strategy(strategy: SandboxStrategy) -> SandboxStrategy {
    match strategy {
        SandboxStrategy::Overlay => SandboxStrategy::Reflink,
        SandboxStrategy::Reflink => SandboxStrategy::Overlay,
    }
}

/// F65 PR-5: generate a fresh snapshot identifier. We don't pull the
/// `ulid` crate in for this — the id format is not load-bearing (it's
/// an opaque string the projection reads back). `snap-<unix-ms>-<rand>`
/// is unique enough for operational use and sorts chronologically.
fn new_snapshot_ulid() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default();
    // 64-bit entropy is plenty — the actual uniqueness invariant is per-
    // host per-millisecond, and we add 16 hex chars worth.
    let mut hasher = DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    now.hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    let suffix = hasher.finish();
    format!("snap-{now:016x}-{suffix:016x}")
}

fn limit_for(policy: &SandboxPolicy, dimension: ResourceDimension) -> Option<u64> {
    match dimension {
        ResourceDimension::DiskBytes => policy.disk_quota_bytes,
        ResourceDimension::MemoryBytes => policy.memory_limit_bytes,
        ResourceDimension::WallClockMs => policy
            .wall_clock_limit
            .map(|duration| duration.as_millis() as u64),
    }
}

fn repo_id_for(policy: &SandboxPolicy) -> Option<crate::sandbox::RepoId> {
    match &policy.base {
        crate::sandbox::SandboxBase::Repo { repo_id, .. } => Some(repo_id.clone()),
        _ => None,
    }
}

fn policy_hash(policy: &SandboxPolicy) -> String {
    let mut hasher = DefaultHasher::new();
    format!("{policy:?}").hash(&mut hasher);
    format!("policy:{:016x}", hasher.finish())
}

fn recovered_state(state: SandboxState) -> SandboxState {
    match state {
        SandboxState::Initial | SandboxState::Provisioning | SandboxState::Active => {
            SandboxState::Ready
        }
        other => other,
    }
}

fn recovery_policy(metadata: &SandboxMetadata) -> SandboxPolicy {
    let base = match &metadata.repo_id {
        Some(repo_id) => crate::sandbox::SandboxBase::Repo {
            repo_id: repo_id.clone(),
            starting_ref: metadata.base_rev.clone(),
        },
        None => crate::sandbox::SandboxBase::Empty,
    };

    SandboxPolicy {
        strategy: crate::sandbox::SandboxStrategyRequest::Force(metadata.strategy),
        base,
        credentials: Vec::new(),
        network_egress: None,
        memory_limit_bytes: None,
        cpu_weight: None,
        disk_quota_bytes: None,
        wall_clock_limit: None,
        on_resource_exhaustion: OnExhaustion::Destroy,
        preserve_on_failure: true,
        required_host_caps: crate::sandbox::HostCapabilityRequirements::default(),
    }
}

// ── SandboxServiceApi impl (issue #443) ──────────────────────────────────
//
// Each arm forwards to the inherent method of the same name so the
// behaviour is unchanged. The inherent methods stay on `impl SandboxService`
// so the test suite and the builder-style `with_*` setters keep working
// without any call-site churn.

#[async_trait::async_trait]
impl SandboxServiceApi for SandboxService {
    async fn provision_or_reconnect(
        &self,
        run_id: &RunId,
        task_id: Option<TaskId>,
        project: ProjectKey,
        policy: SandboxPolicy,
    ) -> Result<ProvisionedSandbox, WorkspaceError> {
        SandboxService::provision_or_reconnect(self, run_id, task_id, project, policy).await
    }

    async fn activate(
        &self,
        run_id: &RunId,
        pid: Option<u32>,
    ) -> Result<ProvisionedSandbox, WorkspaceError> {
        SandboxService::activate(self, run_id, pid).await
    }

    fn base_dir(&self) -> &PathBuf {
        SandboxService::base_dir(self)
    }

    async fn recover_all(&self) -> Result<SandboxRecoverySummary, WorkspaceError> {
        SandboxService::recover_all(self).await
    }

    fn reap_snapshot_dir(
        &self,
        snapshot_id: &cairn_domain::WorkspaceSnapshotId,
    ) -> Result<bool, WorkspaceError> {
        SandboxService::reap_snapshot_dir(self, snapshot_id)
    }

    fn seed_registry_entry_for_test(
        &self,
        sandbox_id: crate::sandbox::SandboxId,
        run_id: RunId,
        project: ProjectKey,
        strategy: SandboxStrategy,
        path: PathBuf,
    ) -> Result<(), WorkspaceError> {
        SandboxService::seed_registry_entry_for_test(
            self, sandbox_id, run_id, project, strategy, path,
        )
    }

    fn seed_registry_entry_for_test_with_repo(
        &self,
        sandbox_id: crate::sandbox::SandboxId,
        run_id: RunId,
        project: ProjectKey,
        strategy: SandboxStrategy,
        path: PathBuf,
        repo_id: Option<RepoId>,
    ) -> Result<(), WorkspaceError> {
        SandboxService::seed_registry_entry_for_test_with_repo(
            self, sandbox_id, run_id, project, strategy, path, repo_id,
        )
    }

    fn seed_registry_entry_for_test_full(
        &self,
        sandbox_id: crate::sandbox::SandboxId,
        run_id: RunId,
        project: ProjectKey,
        strategy: SandboxStrategy,
        path: PathBuf,
        repo_id: Option<RepoId>,
        base_revision: Option<String>,
    ) -> Result<(), WorkspaceError> {
        SandboxService::seed_registry_entry_for_test_full(
            self,
            sandbox_id,
            run_id,
            project,
            strategy,
            path,
            repo_id,
            base_revision,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use cairn_domain::{
        CheckpointKind, DestroyReason, OnExhaustion, PreservationReason, ProjectKey,
        ResourceDimension, RunId,
    };

    use super::{BufferedSandboxEventSink, Clock, SandboxEventSink, SandboxService};
    use crate::error::WorkspaceError;
    use crate::providers::SandboxProvider;
    use crate::sandbox::{
        DestroyResult, HostCapabilityRequirements, ProvisionedSandbox, SandboxBase,
        SandboxCheckpoint, SandboxEvent, SandboxHandle, SandboxMetadata, SandboxPolicy,
        SandboxState, SandboxStrategy, SandboxStrategyRequest,
    };

    #[derive(Debug)]
    struct FixedClock {
        now: Mutex<u64>,
    }

    impl FixedClock {
        fn new(start: u64) -> Self {
            Self {
                now: Mutex::new(start),
            }
        }
    }

    impl Clock for FixedClock {
        fn now_millis(&self) -> u64 {
            let mut guard = self.now.lock().expect("fixed clock poisoned");
            *guard += 10;
            *guard
        }
    }

    // ── #466: SystemClock no-panic regression ──────────────────────────────
    //
    // The clock-before-epoch path is not reachable on any live production
    // host (it would require an RTC reset to before 1970-01-01 mid-run),
    // but the `unwrap_or_else` fallback is still observable via the public
    // contract: `now_millis` must return `0` — not panic — on a
    // `SystemTimeError`. A full runtime-level reproduction would need us
    // to travel the real clock backwards, which rustc/tokio don't permit.
    // These two tests together keep the public contract honest:
    //
    //   * `system_clock_now_millis_happy_path` asserts the live path
    //     returns a value after 2020-01-01 (a sanity check that the
    //     conversion isn't silently broken).
    //   * `system_clock_fallback_path_returns_zero` constructs a
    //     synthetic `SystemTimeError` via a known-good recipe
    //     (`earlier.duration_since(later)`) and asserts the same code
    //     shape the production impl uses returns `0`.

    #[test]
    fn system_clock_now_millis_happy_path() {
        use super::SystemClock;
        use std::time::{SystemTime, UNIX_EPOCH};

        // Only assert when `SystemTime::now()` is actually after UNIX_EPOCH
        // on this host — dev/CI containers with a skewed clock are still
        // a valid test environment and shouldn't make this unit fail.
        // Per Copilot review comment on PR #560: an absolute "post-2020"
        // threshold coupled this test to wall-clock correctness. What
        // matters is: the method returns a value and doesn't panic, and
        // if the underlying call says we're after UNIX_EPOCH, `now_millis`
        // returns a non-zero reflection of that.
        if SystemTime::now().duration_since(UNIX_EPOCH).is_ok() {
            let clock = SystemClock;
            let now = clock.now_millis();
            assert!(
                now > 0,
                "SystemClock must return a non-zero timestamp when the host clock is past UNIX_EPOCH; got {now}",
            );
        }
    }

    #[test]
    fn system_clock_fallback_path_returns_zero() {
        // Replay the production fallback shape against a synthetic
        // `SystemTimeError`. We construct the error by asking
        // `duration_since` about an instant in the future — the only
        // portable way to produce a real `SystemTimeError` without
        // mucking with the system clock.
        use std::time::{Duration, UNIX_EPOCH};

        let future = UNIX_EPOCH + Duration::from_secs(60);
        let err = UNIX_EPOCH
            .duration_since(future)
            .expect_err("duration_since a future instant must error");

        // Same shape as `SystemClock::now_millis` — if this shape ever
        // panics, the production call site panics too.
        let result: u64 = Err::<Duration, _>(err)
            .map(|d| d.as_millis() as u64)
            .unwrap_or_else(|e| {
                // In production this is a `tracing::warn!`. Under the
                // test harness `tracing` emits to the registered
                // subscriber (or nothing); we just mirror the sink
                // path to prove it compiles.
                let _ = format!("{e}");
                0
            });

        assert_eq!(result, 0, "fallback path must return 0, got {result}");
    }

    #[derive(Debug)]
    struct TestProvider {
        strategy: SandboxStrategy,
        provision_calls: Mutex<u32>,
        heartbeats: Mutex<u32>,
    }

    impl TestProvider {
        fn new(strategy: SandboxStrategy) -> Self {
            Self {
                strategy,
                provision_calls: Mutex::new(0),
                heartbeats: Mutex::new(0),
            }
        }
    }

    #[derive(Debug)]
    struct RecoveryProvider {
        strategy: SandboxStrategy,
        handles: Vec<SandboxHandle>,
        reconnect_results:
            Mutex<HashMap<String, Result<Option<ProvisionedSandbox>, WorkspaceError>>>,
    }

    impl RecoveryProvider {
        fn new(
            strategy: SandboxStrategy,
            handles: Vec<SandboxHandle>,
            reconnect_results: HashMap<String, Result<Option<ProvisionedSandbox>, WorkspaceError>>,
        ) -> Self {
            Self {
                strategy,
                handles,
                reconnect_results: Mutex::new(reconnect_results),
            }
        }
    }

    #[async_trait]
    impl SandboxProvider for TestProvider {
        fn strategy(&self) -> SandboxStrategy {
            self.strategy
        }

        async fn provision(
            &self,
            run_id: &RunId,
            _project: &ProjectKey,
            policy: &SandboxPolicy,
        ) -> Result<ProvisionedSandbox, WorkspaceError> {
            *self
                .provision_calls
                .lock()
                .expect("provision call counter poisoned") += 1;

            Ok(ProvisionedSandbox {
                sandbox_id: crate::sandbox::SandboxId::new(format!("sbx-{}", run_id.as_str())),
                run_id: run_id.clone(),
                path: PathBuf::from(format!("/tmp/{}", run_id.as_str())),
                base: policy.base.clone(),
                strategy: self.strategy,
                base_revision: match &policy.base {
                    SandboxBase::Repo { starting_ref, .. } => {
                        Some(starting_ref.clone().unwrap_or_else(|| "head-1".to_string()))
                    }
                    _ => None,
                },
                branch: Some("main".to_string()),
                is_resumed: false,
                env: HashMap::from([("GIT_TERMINAL_PROMPT".to_string(), "0".to_string())]),
            })
        }

        async fn reconnect(
            &self,
            _run_id: &RunId,
        ) -> Result<Option<ProvisionedSandbox>, WorkspaceError> {
            Ok(None)
        }

        async fn checkpoint(
            &self,
            run_id: &RunId,
            kind: CheckpointKind,
        ) -> Result<SandboxCheckpoint, WorkspaceError> {
            Ok(SandboxCheckpoint {
                sandbox_id: crate::sandbox::SandboxId::new(format!("sbx-{}", run_id.as_str())),
                run_id: run_id.clone(),
                kind,
                rescue_ref: Some("refs/cairn/checkpoint".to_string()),
                upper_snapshot: Some(PathBuf::from(format!(
                    "/tmp/{}/upper.prev.0",
                    run_id.as_str()
                ))),
            })
        }

        async fn restore(
            &self,
            _from_checkpoint: &SandboxCheckpoint,
            _new_run_id: &RunId,
            _project: &ProjectKey,
            _policy: &SandboxPolicy,
        ) -> Result<ProvisionedSandbox, WorkspaceError> {
            Err(WorkspaceError::unimplemented(
                "restore not used in step 3 tests",
            ))
        }

        async fn destroy(
            &self,
            run_id: &RunId,
            _preserve: bool,
        ) -> Result<DestroyResult, WorkspaceError> {
            Ok(DestroyResult {
                sandbox_id: crate::sandbox::SandboxId::new(format!("sbx-{}", run_id.as_str())),
                files_changed: 4,
                bytes_written: 512,
            })
        }

        async fn list(&self) -> Result<Vec<SandboxHandle>, WorkspaceError> {
            Ok(Vec::new())
        }

        async fn heartbeat(&self, _run_id: &RunId) -> Result<(), WorkspaceError> {
            *self.heartbeats.lock().expect("heartbeat counter poisoned") += 1;
            Ok(())
        }
    }

    #[async_trait]
    impl SandboxProvider for RecoveryProvider {
        fn strategy(&self) -> SandboxStrategy {
            self.strategy
        }

        async fn provision(
            &self,
            _run_id: &RunId,
            _project: &ProjectKey,
            _policy: &SandboxPolicy,
        ) -> Result<ProvisionedSandbox, WorkspaceError> {
            Err(WorkspaceError::unimplemented(
                "recovery provider does not provision in tests",
            ))
        }

        async fn reconnect(
            &self,
            run_id: &RunId,
        ) -> Result<Option<ProvisionedSandbox>, WorkspaceError> {
            self.reconnect_results
                .lock()
                .expect("reconnect results lock poisoned")
                .get(run_id.as_str())
                .cloned()
                .unwrap_or(Ok(None))
        }

        async fn checkpoint(
            &self,
            _run_id: &RunId,
            _kind: CheckpointKind,
        ) -> Result<SandboxCheckpoint, WorkspaceError> {
            Err(WorkspaceError::unimplemented(
                "recovery provider does not checkpoint in tests",
            ))
        }

        async fn restore(
            &self,
            _from_checkpoint: &SandboxCheckpoint,
            _new_run_id: &RunId,
            _project: &ProjectKey,
            _policy: &SandboxPolicy,
        ) -> Result<ProvisionedSandbox, WorkspaceError> {
            Err(WorkspaceError::unimplemented(
                "recovery provider does not restore in tests",
            ))
        }

        async fn destroy(
            &self,
            _run_id: &RunId,
            _preserve: bool,
        ) -> Result<DestroyResult, WorkspaceError> {
            Err(WorkspaceError::unimplemented(
                "recovery provider does not destroy in tests",
            ))
        }

        async fn list(&self) -> Result<Vec<SandboxHandle>, WorkspaceError> {
            Ok(self.handles.clone())
        }

        async fn heartbeat(&self, _run_id: &RunId) -> Result<(), WorkspaceError> {
            Ok(())
        }
    }

    fn policy(
        strategy: SandboxStrategyRequest,
        on_resource_exhaustion: OnExhaustion,
    ) -> SandboxPolicy {
        SandboxPolicy {
            strategy,
            base: SandboxBase::Repo {
                repo_id: "octocat/hello".into(),
                starting_ref: Some("abc123".to_string()),
            },
            credentials: vec![crate::sandbox::CredentialReference::Named(
                "github_installation".to_string(),
            )],
            network_egress: None,
            memory_limit_bytes: Some(256),
            cpu_weight: Some(100),
            disk_quota_bytes: Some(128),
            wall_clock_limit: Some(Duration::from_secs(5)),
            on_resource_exhaustion,
            preserve_on_failure: true,
            required_host_caps: HostCapabilityRequirements::default(),
        }
    }

    fn service_with_providers(
        providers: Vec<(SandboxStrategy, Box<dyn SandboxProvider>)>,
    ) -> (SandboxService, Arc<BufferedSandboxEventSink>) {
        let sink = Arc::new(BufferedSandboxEventSink::default());
        let base_dir = unique_test_dir("service");
        let service = SandboxService::new(
            HashMap::from_iter(providers),
            sink.clone(),
            base_dir,
            Arc::new(FixedClock::new(1_000)),
        );
        (service, sink)
    }

    fn unique_test_dir(label: &str) -> PathBuf {
        // `mkdtemp(3)` creates the directory atomically with O_EXCL —
        // parallel tests cannot collide regardless of clock
        // resolution. Footgun: do NOT replace this with a
        // timestamp-named path; `SystemTime::now().as_nanos()` is not
        // unique across concurrent threads.
        //
        // `.keep()` detaches the `TempDir` drop guard so the returned
        // `PathBuf` survives past this function. The directory is
        // intentionally not cleaned up — the service owns what it
        // writes there and individual tests would need an anchor
        // otherwise.
        tempfile::Builder::new()
            .prefix(&format!("cairn-workspace-{label}-"))
            .tempdir()
            .expect("test temp dir should be creatable")
            .keep()
    }

    fn run_id() -> RunId {
        RunId::new("run-1")
    }

    fn project() -> ProjectKey {
        ProjectKey::new("tenant-a", "workspace-a", "project-a")
    }

    fn recovery_metadata(run_id: &RunId, state: SandboxState) -> SandboxMetadata {
        SandboxMetadata {
            sandbox_id: crate::sandbox::SandboxId::new(format!("sbx-{}", run_id.as_str())),
            run_id: run_id.clone(),
            task_id: None,
            project: project(),
            strategy: SandboxStrategy::Overlay,
            state,
            base_rev: Some("abc123".to_string()),
            repo_id: Some(crate::sandbox::RepoId::new("octocat/hello")),
            path: PathBuf::from(format!("/tmp/{}", run_id.as_str())),
            pid: Some(42),
            created_at: 1_000,
            heartbeat_at: 1_010,
            policy_hash: "policy:test".to_string(),
        }
    }

    fn reconnected_sandbox(run_id: &RunId) -> ProvisionedSandbox {
        ProvisionedSandbox {
            sandbox_id: crate::sandbox::SandboxId::new(format!("sbx-{}", run_id.as_str())),
            run_id: run_id.clone(),
            path: PathBuf::from(format!("/tmp/{}/merged", run_id.as_str())),
            base: SandboxBase::Repo {
                repo_id: crate::sandbox::RepoId::new("octocat/hello"),
                starting_ref: Some("abc123".to_string()),
            },
            strategy: SandboxStrategy::Overlay,
            base_revision: Some("abc123".to_string()),
            branch: Some("main".to_string()),
            is_resumed: true,
            env: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn preferred_strategy_falls_back_and_emits_degraded_event() {
        let (service, sink) = service_with_providers(vec![(
            SandboxStrategy::Reflink,
            Box::new(TestProvider::new(SandboxStrategy::Reflink)),
        )]);

        let sandbox = service
            .provision_or_reconnect(
                &run_id(),
                None,
                project(),
                policy(
                    SandboxStrategyRequest::Preferred(SandboxStrategy::Overlay),
                    OnExhaustion::Destroy,
                ),
            )
            .await
            .unwrap();

        assert_eq!(sandbox.strategy, SandboxStrategy::Reflink);
        assert_eq!(service.state_for(&run_id()), Some(SandboxState::Ready));

        let events = sink.drain();
        assert!(matches!(
            &events[0],
            SandboxEvent::SandboxPolicyDegraded {
                requested: SandboxStrategy::Overlay,
                actual: SandboxStrategy::Reflink,
                ..
            }
        ));
        assert!(matches!(
            &events[1],
            SandboxEvent::SandboxProvisioned {
                strategy: SandboxStrategy::Reflink,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn provision_reconnect_activate_checkpoint_and_destroy_flow() {
        let (service, sink) = service_with_providers(vec![(
            SandboxStrategy::Overlay,
            Box::new(TestProvider::new(SandboxStrategy::Overlay)),
        )]);
        let run = run_id();

        let provisioned = service
            .provision_or_reconnect(
                &run,
                Some(cairn_domain::TaskId::new("task-1")),
                project(),
                policy(
                    SandboxStrategyRequest::Force(SandboxStrategy::Overlay),
                    OnExhaustion::Destroy,
                ),
            )
            .await
            .unwrap();
        let reconnected = service
            .provision_or_reconnect(
                &run,
                Some(cairn_domain::TaskId::new("task-1")),
                project(),
                policy(
                    SandboxStrategyRequest::Force(SandboxStrategy::Overlay),
                    OnExhaustion::Destroy,
                ),
            )
            .await
            .unwrap();
        assert!(reconnected.is_resumed);
        assert_eq!(reconnected.sandbox_id, provisioned.sandbox_id);

        service.activate(&run, Some(42)).await.unwrap();
        service.heartbeat(&run).await.unwrap();
        service
            .checkpoint(&run, CheckpointKind::Intent)
            .await
            .unwrap();
        service
            .destroy(&run, false, DestroyReason::Completed)
            .await
            .unwrap();

        assert_eq!(service.state_for(&run), Some(SandboxState::Destroyed));
        let metadata = service.metadata_for(&run).unwrap();
        assert_eq!(metadata.task_id, Some(cairn_domain::TaskId::new("task-1")));

        let events = sink.drain();
        assert!(matches!(events[0], SandboxEvent::SandboxProvisioned { .. }));
        assert!(matches!(
            events[1],
            SandboxEvent::SandboxActivated { pid: Some(42), .. }
        ));
        assert!(matches!(events[2], SandboxEvent::SandboxHeartbeat { .. }));
        assert!(matches!(
            events[3],
            SandboxEvent::SandboxCheckpointed {
                checkpoint_kind: CheckpointKind::Intent,
                ..
            }
        ));
        assert!(matches!(
            events[4],
            SandboxEvent::SandboxDestroyed {
                reason: DestroyReason::Completed,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn resource_limit_destroy_mode_emits_limit_and_destroyed() {
        let (service, sink) = service_with_providers(vec![(
            SandboxStrategy::Overlay,
            Box::new(TestProvider::new(SandboxStrategy::Overlay)),
        )]);
        let run = run_id();

        service
            .provision_or_reconnect(
                &run,
                None,
                project(),
                policy(
                    SandboxStrategyRequest::Force(SandboxStrategy::Overlay),
                    OnExhaustion::Destroy,
                ),
            )
            .await
            .unwrap();
        service.activate(&run, Some(7)).await.unwrap();
        service
            .observe_resource_limit(&run, ResourceDimension::DiskBytes, 256)
            .await
            .unwrap();

        assert_eq!(service.state_for(&run), Some(SandboxState::Destroyed));
        let events = sink.drain();
        assert!(matches!(
            events[2],
            SandboxEvent::SandboxResourceLimitExceeded {
                dimension: ResourceDimension::DiskBytes,
                limit: 128,
                observed: 256,
                ..
            }
        ));
        assert!(matches!(
            events[3],
            SandboxEvent::SandboxDestroyed {
                reason: DestroyReason::ResourceLimitExceeded {
                    dimension: ResourceDimension::DiskBytes,
                    limit: 128,
                    observed: 256,
                },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn resource_limit_pause_mode_preserves_sandbox() {
        let (service, sink) = service_with_providers(vec![(
            SandboxStrategy::Overlay,
            Box::new(TestProvider::new(SandboxStrategy::Overlay)),
        )]);
        let run = run_id();

        service
            .provision_or_reconnect(
                &run,
                None,
                project(),
                policy(
                    SandboxStrategyRequest::Force(SandboxStrategy::Overlay),
                    OnExhaustion::PauseAwaitOperator,
                ),
            )
            .await
            .unwrap();
        service.activate(&run, Some(8)).await.unwrap();
        service
            .observe_resource_limit(&run, ResourceDimension::MemoryBytes, 300)
            .await
            .unwrap();

        assert_eq!(service.state_for(&run), Some(SandboxState::Preserved));
        let events = sink.drain();
        assert!(matches!(
            events[3],
            SandboxEvent::SandboxPreserved {
                reason: PreservationReason::AwaitingResourceRaise {
                    dimension: ResourceDimension::MemoryBytes,
                    limit: 256,
                    observed: 300,
                },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn resource_limit_report_only_keeps_sandbox_active() {
        let (service, sink) = service_with_providers(vec![(
            SandboxStrategy::Overlay,
            Box::new(TestProvider::new(SandboxStrategy::Overlay)),
        )]);
        let run = run_id();

        service
            .provision_or_reconnect(
                &run,
                None,
                project(),
                policy(
                    SandboxStrategyRequest::Force(SandboxStrategy::Overlay),
                    OnExhaustion::ReportOnly,
                ),
            )
            .await
            .unwrap();
        service.activate(&run, Some(9)).await.unwrap();
        service
            .observe_resource_limit(&run, ResourceDimension::WallClockMs, 7_500)
            .await
            .unwrap();

        assert_eq!(service.state_for(&run), Some(SandboxState::Active));
        let events = sink.drain();
        assert_eq!(events.len(), 3);
        assert!(matches!(
            events[2],
            SandboxEvent::SandboxResourceLimitExceeded {
                dimension: ResourceDimension::WallClockMs,
                limit: 5_000,
                observed: 7_500,
                ..
            }
        ));
    }

    #[test]
    fn metadata_captures_task_id_and_policy_hash() {
        let metadata = SandboxMetadata {
            sandbox_id: crate::sandbox::SandboxId::new("sbx-run"),
            run_id: run_id(),
            task_id: Some(cairn_domain::TaskId::new("task-1")),
            project: project(),
            strategy: SandboxStrategy::Overlay,
            state: SandboxState::Ready,
            base_rev: Some("abc123".to_string()),
            repo_id: Some("octocat/hello".into()),
            path: PathBuf::from("/tmp/run-1"),
            pid: None,
            created_at: 10,
            heartbeat_at: 20,
            policy_hash: "policy:abc".to_string(),
        };

        assert_eq!(metadata.task_id, Some(cairn_domain::TaskId::new("task-1")));
        assert_eq!(metadata.policy_hash, "policy:abc");
    }

    #[tokio::test]
    async fn service_persists_metadata_for_recovery() {
        let (service, _sink) = service_with_providers(vec![(
            SandboxStrategy::Overlay,
            Box::new(TestProvider::new(SandboxStrategy::Overlay)),
        )]);
        let run = run_id();

        service
            .provision_or_reconnect(
                &run,
                Some(cairn_domain::TaskId::new("task-9")),
                project(),
                policy(
                    SandboxStrategyRequest::Force(SandboxStrategy::Overlay),
                    OnExhaustion::Destroy,
                ),
            )
            .await
            .unwrap();

        let metadata_path = service.base_dir().join("sbx-run-1").join("meta.json");
        let metadata: SandboxMetadata =
            serde_json::from_slice(&fs::read(&metadata_path).unwrap()).unwrap();
        assert_eq!(metadata.state, SandboxState::Ready);
        assert_eq!(metadata.task_id, Some(cairn_domain::TaskId::new("task-9")));

        service.activate(&run, Some(77)).await.unwrap();
        let metadata: SandboxMetadata =
            serde_json::from_slice(&fs::read(&metadata_path).unwrap()).unwrap();
        assert_eq!(metadata.state, SandboxState::Active);
        assert_eq!(metadata.pid, Some(77));

        service
            .checkpoint(&run, CheckpointKind::Result)
            .await
            .unwrap();
        let metadata: SandboxMetadata =
            serde_json::from_slice(&fs::read(&metadata_path).unwrap()).unwrap();
        assert_eq!(metadata.state, SandboxState::Checkpointed);

        service
            .preserve(&run, PreservationReason::ControlPlaneRestart)
            .unwrap();
        let metadata: SandboxMetadata =
            serde_json::from_slice(&fs::read(&metadata_path).unwrap()).unwrap();
        assert_eq!(metadata.state, SandboxState::Preserved);
        assert_eq!(metadata.pid, None);
    }

    #[tokio::test]
    async fn persist_metadata_is_atomic_under_concurrent_readers() {
        // Regression for #341. The old `fs::write`-based persist
        // truncated `meta.json` to 0 bytes before writing, so a
        // reader that landed mid-write saw empty or torn bytes and
        // `serde_json::from_slice` failed with "EOF while parsing a
        // value". This exercise runs a writer thread calling
        // `persist_metadata` in a tight loop concurrently with a
        // reader thread, and asserts every read either succeeds
        // (valid `SandboxMetadata`) or sees ENOENT before the first
        // write — never a parse error, never a zero-byte read. With
        // the old `fs::write` this test failed on the first few
        // iterations (empty reads dominate); with the atomic
        // stage+rename it passes.
        let (service, _sink) = service_with_providers(vec![(
            SandboxStrategy::Overlay,
            Box::new(TestProvider::new(SandboxStrategy::Overlay)),
        )]);
        let run = run_id();
        service
            .provision_or_reconnect(
                &run,
                None,
                project(),
                policy(
                    SandboxStrategyRequest::Force(SandboxStrategy::Overlay),
                    OnExhaustion::Destroy,
                ),
            )
            .await
            .unwrap();

        let metadata_path = service.base_dir().join("sbx-run-1").join("meta.json");
        let service = Arc::new(service);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        const ITERS: u32 = 2_000;

        // Writer: repeatedly persist with alternating fields so
        // every call rewrites meta.json. Uses the private helper
        // directly (avoids the async state machine, which would
        // throttle the race window).
        let writer_service = service.clone();
        let writer_stop = stop.clone();
        let writer_run = run.clone();
        let writer = std::thread::spawn(move || {
            let mut i = 0u32;
            while i < ITERS && !writer_stop.load(std::sync::atomic::Ordering::Relaxed) {
                let meta = SandboxMetadata {
                    sandbox_id: crate::sandbox::SandboxId::new("sbx-run-1"),
                    run_id: writer_run.clone(),
                    task_id: None,
                    project: project(),
                    strategy: SandboxStrategy::Overlay,
                    state: if i % 2 == 0 {
                        SandboxState::Active
                    } else {
                        SandboxState::Preserved
                    },
                    base_rev: Some(format!("rev-{i}")),
                    repo_id: None,
                    path: PathBuf::from("/tmp/run-1"),
                    pid: if i % 2 == 0 { Some(i) } else { None },
                    created_at: 1_000,
                    heartbeat_at: 1_000 + i as u64,
                    policy_hash: "policy:test".to_string(),
                };
                writer_service.persist_metadata(&meta).expect("persist");
                i += 1;
            }
        });

        // Reader: read + parse until the writer stops. Every
        // successful read must parse as SandboxMetadata — no empty
        // reads, no torn JSON.
        let reader_path = metadata_path.clone();
        let reader_stop = stop.clone();
        let reader = std::thread::spawn(move || {
            let mut reads = 0u32;
            let mut oks = 0u32;
            while !reader_stop.load(std::sync::atomic::Ordering::Relaxed) {
                reads += 1;
                match fs::read(&reader_path) {
                    Ok(bytes) => {
                        assert!(
                            !bytes.is_empty(),
                            "persist_metadata must never expose a zero-byte file",
                        );
                        serde_json::from_slice::<SandboxMetadata>(&bytes).unwrap_or_else(|e| {
                            panic!(
                                "persist_metadata must never expose a torn file: \
                                 {e} (bytes={} head={:?})",
                                bytes.len(),
                                &bytes[..bytes.len().min(64)],
                            )
                        });
                        oks += 1;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        // Acceptable only before the first write.
                    }
                    Err(e) => panic!("unexpected read error: {e}"),
                }
            }
            (reads, oks)
        });

        writer.join().expect("writer panicked");
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let (reads, oks) = reader.join().expect("reader panicked");
        assert!(
            oks > 0,
            "reader should have observed at least one durable meta.json (reads={reads})"
        );
    }

    #[tokio::test]
    async fn recover_all_reconnects_sandbox_and_normalizes_active_state() {
        let run = RunId::new("run-recover-ok");
        let provider = RecoveryProvider::new(
            SandboxStrategy::Overlay,
            vec![SandboxHandle {
                metadata: recovery_metadata(&run, SandboxState::Active),
            }],
            HashMap::from([(run.to_string(), Ok(Some(reconnected_sandbox(&run))))]),
        );
        let (service, _sink) =
            service_with_providers(vec![(SandboxStrategy::Overlay, Box::new(provider))]);

        let summary = service.recover_all().await.unwrap();

        assert_eq!(summary.reconnected, 1);
        assert_eq!(summary.preserved, 0);
        assert_eq!(summary.failed, 0);
        assert_eq!(service.state_for(&run), Some(SandboxState::Ready));

        let recovered = service.metadata_for(&run).unwrap();
        assert_eq!(recovered.state, SandboxState::Ready);
        assert_eq!(recovered.pid, None);
        assert_eq!(recovered.base_rev.as_deref(), Some("abc123"));
        assert_eq!(recovered.path, PathBuf::from("/tmp/run-recover-ok/merged"));
    }

    #[tokio::test]
    async fn recover_all_emits_sandbox_lost_when_registry_entry_has_no_directory() {
        // RFC 020 §"Run recovery matrix": a registry sidecar whose
        // sandbox directory is missing at recovery must surface as
        // `SandboxLost` + `summary.lost_runs` so the run-level service
        // can transition the bound run to `failed` with
        // `reason: sandbox_lost`.
        let run = RunId::new("run-lost");
        let provider = RecoveryProvider::new(SandboxStrategy::Overlay, Vec::new(), HashMap::new());
        let (service, sink) =
            service_with_providers(vec![(SandboxStrategy::Overlay, Box::new(provider))]);

        // Seed a registry entry pointing at a path that deliberately
        // doesn't exist. `base_dir()` is the live service base dir so
        // the registry sidecar writer picks it up.
        let entry = super::RegistryEntry {
            sandbox_id: crate::sandbox::SandboxId::new(format!("sbx-{}", run.as_str())),
            run_id: run.clone(),
            project: project(),
            strategy: SandboxStrategy::Overlay,
            path: service.base_dir().join("sbx-run-lost-missing-root"),
            registered_at: 1_000,
            repo_id: None,
            allowlist_revoked_handled: false,
            base_revision: None,
            base_revision_drift_handled: false,
        };
        service
            .write_registry_entry(&entry)
            .expect("write registry entry");

        let summary = service.recover_all().await.unwrap();

        assert_eq!(summary.reconnected, 0);
        assert_eq!(summary.preserved, 0);
        assert_eq!(summary.failed, 0);
        assert_eq!(summary.lost, 1);
        assert_eq!(summary.lost_runs.len(), 1);
        assert_eq!(summary.lost_runs[0].0, run);
        assert_eq!(summary.lost_runs[0].1, project());

        let events = sink.drain();
        assert_eq!(events.len(), 1, "expected exactly one SandboxLost event");
        match &events[0] {
            SandboxEvent::SandboxLost {
                run_id, project: p, ..
            } => {
                assert_eq!(run_id, &run);
                assert_eq!(p, &project());
            }
            other => panic!("expected SandboxLost, got {other:?}"),
        }

        // Idempotency: a second recovery sweep must not re-emit the
        // event — the entry was cleared after the first detection.
        let second = service.recover_all().await.unwrap();
        assert_eq!(second.lost, 0);
        assert!(second.lost_runs.is_empty());
    }

    #[tokio::test]
    async fn recover_all_emits_allowlist_revoked_when_repo_not_allowlisted() {
        // RFC 020 §"Run recovery matrix" — AllowlistRevoked row.
        // Seed a registry entry for a `SandboxBase::Repo` sandbox
        // whose path exists on disk (so the lost-sweep skips it) but
        // whose bound repo is NOT in the project allowlist. Recovery
        // must emit `SandboxAllowlistRevoked` and surface the triple
        // on `summary.allowlist_revoked_runs`.
        use crate::repo_store::access_service::ProjectRepoAccessService;

        use cairn_domain::{ActorRef, OperatorId, RepoAccessContext};

        let run = RunId::new("run-revoked");
        let repo_id = crate::sandbox::RepoId::new("octocat/hello");
        let provider = RecoveryProvider::new(SandboxStrategy::Overlay, Vec::new(), HashMap::new());
        let allowlist = Arc::new(ProjectRepoAccessService::new());
        // Seed a sentinel repo so the project is "authoritative" under
        // the sweep gate (an empty allowlist is treated as
        // not-yet-replayed and skipped — Bugbot high-1).
        allowlist
            .allow(
                &RepoAccessContext { project: project() },
                &crate::sandbox::RepoId::new("other/sentinel"),
                ActorRef::Operator {
                    operator_id: OperatorId::new("test"),
                },
            )
            .await
            .expect("seed sentinel");
        let sink = Arc::new(BufferedSandboxEventSink::default());
        let base_dir = unique_test_dir("allowlist-revoked");
        let service = SandboxService::new(
            HashMap::from([(
                SandboxStrategy::Overlay,
                Box::new(provider) as Box<dyn crate::providers::SandboxProvider>,
            )]),
            sink.clone(),
            base_dir.clone(),
            Arc::new(FixedClock::new(1_000)),
        )
        .with_allowlist(allowlist);

        // Sandbox path exists → lost-sweep skips. Repo not in
        // allowlist → allowlist-revoked sweep fires.
        let sandbox_path = base_dir.join("sbx-run-revoked");
        fs::create_dir_all(&sandbox_path).expect("create stub sandbox dir");
        service
            .seed_registry_entry_for_test_with_repo(
                crate::sandbox::SandboxId::new("sbx-run-revoked"),
                run.clone(),
                project(),
                SandboxStrategy::Overlay,
                sandbox_path,
                Some(repo_id.clone()),
            )
            .expect("seed registry entry");

        let summary = service.recover_all().await.unwrap();

        assert_eq!(summary.lost, 0);
        assert_eq!(summary.preserved_allowlist_revoked, 1);
        assert_eq!(summary.allowlist_revoked_runs.len(), 1);
        assert_eq!(summary.allowlist_revoked_runs[0].0, run);
        assert_eq!(summary.allowlist_revoked_runs[0].1, project());
        assert_eq!(summary.allowlist_revoked_runs[0].2, repo_id);

        let events = sink.drain();
        assert_eq!(events.len(), 1, "expected exactly one event");
        match &events[0] {
            SandboxEvent::SandboxAllowlistRevoked {
                run_id,
                project: p,
                repo_id: r,
                ..
            } => {
                assert_eq!(run_id, &run);
                assert_eq!(p, &project());
                assert_eq!(r, &repo_id);
            }
            other => panic!("expected SandboxAllowlistRevoked, got {other:?}"),
        }

        // Idempotency: second sweep must not re-emit.
        let second = service.recover_all().await.unwrap();
        assert_eq!(second.preserved_allowlist_revoked, 0);
        assert!(second.allowlist_revoked_runs.is_empty());
        // The registry entry has `allowlist_revoked_handled=true` after
        // boot 1, so the reattach sweep also skips it on boot 2. Sink
        // stays empty.
        assert!(sink.drain().is_empty());
    }

    #[tokio::test]
    async fn recover_all_skips_allowlist_revoked_sweep_when_project_allowlist_empty() {
        // Bugbot high-1 gate: an empty allowlist for a project means
        // "not yet replayed / re-asserted this boot", NOT "all repos
        // revoked". The sweep must skip such projects so a freshly-
        // started cairn-app does not flood operators with approvals.
        use crate::repo_store::access_service::ProjectRepoAccessService;

        let run = RunId::new("run-empty-allowlist");
        let repo_id = crate::sandbox::RepoId::new("octocat/hello");
        let provider = RecoveryProvider::new(SandboxStrategy::Overlay, Vec::new(), HashMap::new());
        let allowlist = Arc::new(ProjectRepoAccessService::new());
        // Do NOT seed any entries. The project is "not authoritative".
        let sink = Arc::new(BufferedSandboxEventSink::default());
        let base_dir = unique_test_dir("allowlist-empty");
        let service = SandboxService::new(
            HashMap::from([(
                SandboxStrategy::Overlay,
                Box::new(provider) as Box<dyn crate::providers::SandboxProvider>,
            )]),
            sink.clone(),
            base_dir.clone(),
            Arc::new(FixedClock::new(1_000)),
        )
        .with_allowlist(allowlist);

        let sandbox_path = base_dir.join("sbx-run-empty-allowlist");
        fs::create_dir_all(&sandbox_path).expect("create stub sandbox dir");
        service
            .seed_registry_entry_for_test_with_repo(
                crate::sandbox::SandboxId::new("sbx-run-empty-allowlist"),
                run.clone(),
                project(),
                SandboxStrategy::Overlay,
                sandbox_path,
                Some(repo_id),
            )
            .expect("seed registry entry");

        let summary = service.recover_all().await.unwrap();
        assert_eq!(
            summary.preserved_allowlist_revoked, 0,
            "empty allowlist must be treated as not-yet-authoritative, not as all-revoked",
        );
        assert!(summary.allowlist_revoked_runs.is_empty());
        // The entry has a present path and no allowlist-revoke decision
        // this boot, so the healthy-reattach sweep fires and surfaces it.
        assert_eq!(summary.reattached, 1);
        assert_eq!(summary.reattached_runs[0].0, run);
        let events = sink.drain();
        assert_eq!(
            events.len(),
            1,
            "expected exactly one SandboxReattached event"
        );
        assert!(matches!(&events[0], SandboxEvent::SandboxReattached { .. }));
    }

    #[tokio::test]
    async fn recover_all_skips_allowlist_revoked_when_repo_still_allowed() {
        // Allowlisted repo → no emission.
        use crate::repo_store::access_service::ProjectRepoAccessService;
        use cairn_domain::{ActorRef, OperatorId, RepoAccessContext};

        let run = RunId::new("run-still-allowed");
        let repo_id = crate::sandbox::RepoId::new("octocat/hello");
        let provider = RecoveryProvider::new(SandboxStrategy::Overlay, Vec::new(), HashMap::new());
        let allowlist = Arc::new(ProjectRepoAccessService::new());
        allowlist
            .allow(
                &RepoAccessContext { project: project() },
                &repo_id,
                ActorRef::Operator {
                    operator_id: OperatorId::new("op"),
                },
            )
            .await
            .expect("allow repo");
        let sink = Arc::new(BufferedSandboxEventSink::default());
        let base_dir = unique_test_dir("allowlist-still-allowed");
        let service = SandboxService::new(
            HashMap::from([(
                SandboxStrategy::Overlay,
                Box::new(provider) as Box<dyn crate::providers::SandboxProvider>,
            )]),
            sink.clone(),
            base_dir.clone(),
            Arc::new(FixedClock::new(1_000)),
        )
        .with_allowlist(allowlist);

        let sandbox_path = base_dir.join("sbx-run-still-allowed");
        fs::create_dir_all(&sandbox_path).expect("create stub sandbox dir");
        service
            .seed_registry_entry_for_test_with_repo(
                crate::sandbox::SandboxId::new("sbx-run-still-allowed"),
                run.clone(),
                project(),
                SandboxStrategy::Overlay,
                sandbox_path,
                Some(repo_id),
            )
            .expect("seed registry entry");

        let summary = service.recover_all().await.unwrap();
        assert_eq!(summary.preserved_allowlist_revoked, 0);
        assert!(summary.allowlist_revoked_runs.is_empty());
        // Repo still allowlisted + path present → healthy reattach.
        assert_eq!(summary.reattached, 1);
        assert_eq!(summary.reattached_runs[0].0, run);
        let events = sink.drain();
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], SandboxEvent::SandboxReattached { .. }));
    }

    #[tokio::test]
    async fn recover_all_preserves_sandbox_on_base_revision_drift() {
        let run = RunId::new("run-recover-drift");
        let provider = RecoveryProvider::new(
            SandboxStrategy::Overlay,
            vec![SandboxHandle {
                metadata: recovery_metadata(&run, SandboxState::Ready),
            }],
            HashMap::from([(
                run.to_string(),
                Err(WorkspaceError::BaseRevisionDrift {
                    run_id: run.clone(),
                    expected: "abc123".to_string(),
                    actual: "def456".to_string(),
                }),
            )]),
        );
        let (service, sink) =
            service_with_providers(vec![(SandboxStrategy::Overlay, Box::new(provider))]);

        let summary = service.recover_all().await.unwrap();

        assert_eq!(summary.reconnected, 0);
        assert_eq!(summary.preserved, 1);
        assert_eq!(summary.failed, 0);
        assert_eq!(service.state_for(&run), Some(SandboxState::Preserved));

        let events = sink.drain();
        assert!(matches!(
            &events[0],
            SandboxEvent::SandboxBaseRevisionDrift {
                expected,
                actual,
                ..
            } if expected == "abc123" && actual == "def456"
        ));
        assert!(matches!(
            &events[1],
            SandboxEvent::SandboxPreserved {
                reason: PreservationReason::BaseRevisionDrift { expected, actual },
                ..
            } if expected == "abc123" && actual == "def456"
        ));
    }

    #[tokio::test]
    async fn recover_all_emits_sandbox_reattached_for_healthy_entry() {
        // RFC 020 §"Run recovery matrix" — healthy-reattach row. A
        // registry entry whose on-disk root exists and whose repo
        // binding (if any) is still allowlisted — or which carries no
        // repo binding at all — must surface as `SandboxReattached`
        // with the `(run, project)` pair on
        // `summary.reattached_runs` so the run-level recovery service
        // can emit the audit-trail triple.
        let run = RunId::new("run-reattach-healthy");
        let provider = RecoveryProvider::new(SandboxStrategy::Overlay, Vec::new(), HashMap::new());
        let (service, sink) =
            service_with_providers(vec![(SandboxStrategy::Overlay, Box::new(provider))]);

        let sandbox_path = service.base_dir().join("sbx-run-reattach-healthy");
        fs::create_dir_all(&sandbox_path).expect("create stub sandbox dir");
        service
            .seed_registry_entry_for_test(
                crate::sandbox::SandboxId::new("sbx-run-reattach-healthy"),
                run.clone(),
                project(),
                SandboxStrategy::Overlay,
                sandbox_path,
            )
            .expect("seed registry entry");

        let summary = service.recover_all().await.unwrap();
        assert_eq!(summary.reattached, 1);
        assert_eq!(summary.reattached_runs.len(), 1);
        assert_eq!(summary.reattached_runs[0].0, run);
        assert_eq!(summary.reattached_runs[0].1, project());
        assert_eq!(summary.lost, 0);
        assert_eq!(summary.preserved_allowlist_revoked, 0);

        let events = sink.drain();
        assert_eq!(events.len(), 1);
        match &events[0] {
            SandboxEvent::SandboxReattached {
                run_id, project: p, ..
            } => {
                assert_eq!(run_id, &run);
                assert_eq!(p, &project());
            }
            other => panic!("expected SandboxReattached, got {other:?}"),
        }

        // Idempotency: the second sweep re-emits because the registry
        // entry is stable across boots — recovery is advisory only.
        // The run-level service de-duplicates on the consuming side
        // via `RecoveryAttempted/Completed` events keyed by boot_id.
        let second = service.recover_all().await.unwrap();
        assert_eq!(second.reattached, 1);
    }

    #[tokio::test]
    async fn recover_all_emits_base_revision_drift_on_clone_head_mismatch() {
        // RFC 020 §"Run recovery matrix" — BaseRevisionDrift row.
        // Seed an overlay registry entry whose path exists on disk (lost-
        // sweep skips it) and whose stored `base_revision` differs from
        // the clone cache's live HEAD. Recovery must emit
        // `SandboxBaseRevisionDrift` + `SandboxPreserved{BaseRevisionDrift}`
        // and surface the triple on `summary.base_revision_drift_runs`.
        use cairn_domain::TenantId;

        use crate::repo_store::clone_cache::RepoCloneCache;

        let run = RunId::new("run-clone-drift");
        let repo_id = crate::sandbox::RepoId::new("octocat/hello");
        let provider = RecoveryProvider::new(SandboxStrategy::Overlay, Vec::new(), HashMap::new());
        let sink = Arc::new(BufferedSandboxEventSink::default());
        let base_dir = unique_test_dir("drift-sweep");
        let clone_dir = unique_test_dir("drift-sweep-clones");
        let clone_cache = Arc::new(RepoCloneCache::new(clone_dir.clone()));
        // Create the clone so `current_head()` returns `Some(init-*-<now>)`.
        clone_cache
            .ensure_cloned(&TenantId::new("tenant-a"), &repo_id)
            .await
            .expect("ensure clone");

        let service = SandboxService::new(
            HashMap::from([(
                SandboxStrategy::Overlay,
                Box::new(provider) as Box<dyn crate::providers::SandboxProvider>,
            )]),
            sink.clone(),
            base_dir.clone(),
            Arc::new(FixedClock::new(1_000)),
        )
        .with_clone_cache(clone_cache);

        // Path exists → lost-sweep skips. Stored base_revision differs
        // from clone HEAD → drift sweep fires.
        let sandbox_path = base_dir.join("sbx-run-clone-drift");
        fs::create_dir_all(&sandbox_path).expect("create stub sandbox dir");
        service
            .seed_registry_entry_for_test_full(
                crate::sandbox::SandboxId::new("sbx-run-clone-drift"),
                run.clone(),
                project(),
                SandboxStrategy::Overlay,
                sandbox_path,
                Some(repo_id.clone()),
                Some("seed-fake-rev".to_owned()),
            )
            .expect("seed registry entry");

        let summary = service.recover_all().await.unwrap();

        assert_eq!(summary.lost, 0);
        assert_eq!(summary.preserved_base_revision_drift, 1);
        assert_eq!(summary.base_revision_drift_runs.len(), 1);
        assert_eq!(summary.base_revision_drift_runs[0].0, run);
        assert_eq!(summary.base_revision_drift_runs[0].1, project());
        assert_eq!(summary.base_revision_drift_runs[0].2, repo_id);

        let events = sink.drain();
        assert_eq!(events.len(), 2, "expected drift + preserved pair");
        assert!(matches!(
            &events[0],
            SandboxEvent::SandboxBaseRevisionDrift {
                run_id,
                project: p,
                repo_id: r,
                expected,
                ..
            } if run_id == &run
                && p == &project()
                && r == &repo_id
                && expected == "seed-fake-rev"
        ));
        assert!(matches!(
            &events[1],
            SandboxEvent::SandboxPreserved {
                reason: PreservationReason::BaseRevisionDrift { expected, .. },
                ..
            } if expected == "seed-fake-rev"
        ));

        // Idempotency: second sweep must not re-emit.
        let second = service.recover_all().await.unwrap();
        assert_eq!(second.preserved_base_revision_drift, 0);
        assert!(second.base_revision_drift_runs.is_empty());
        assert!(sink.drain().is_empty());
    }

    #[tokio::test]
    async fn recover_all_skips_base_revision_drift_for_reflink_sandbox() {
        // RFC 016 §"Sandbox provider selection": reflink sandboxes are
        // physically independent post-provision. The drift sweep MUST
        // skip them even when the clone HEAD has moved.
        use cairn_domain::TenantId;

        use crate::repo_store::clone_cache::RepoCloneCache;

        let run = RunId::new("run-reflink-drift");
        let repo_id = crate::sandbox::RepoId::new("octocat/hello");
        let provider = RecoveryProvider::new(SandboxStrategy::Reflink, Vec::new(), HashMap::new());
        let sink = Arc::new(BufferedSandboxEventSink::default());
        let base_dir = unique_test_dir("reflink-drift-sweep");
        let clone_dir = unique_test_dir("reflink-drift-sweep-clones");
        let clone_cache = Arc::new(RepoCloneCache::new(clone_dir.clone()));
        clone_cache
            .ensure_cloned(&TenantId::new("tenant-a"), &repo_id)
            .await
            .expect("ensure clone");

        let service = SandboxService::new(
            HashMap::from([(
                SandboxStrategy::Reflink,
                Box::new(provider) as Box<dyn crate::providers::SandboxProvider>,
            )]),
            sink.clone(),
            base_dir.clone(),
            Arc::new(FixedClock::new(1_000)),
        )
        .with_clone_cache(clone_cache);

        let sandbox_path = base_dir.join("sbx-run-reflink-drift");
        fs::create_dir_all(&sandbox_path).expect("create stub sandbox dir");
        service
            .seed_registry_entry_for_test_full(
                crate::sandbox::SandboxId::new("sbx-run-reflink-drift"),
                run.clone(),
                project(),
                SandboxStrategy::Reflink,
                sandbox_path,
                Some(repo_id.clone()),
                // Sentinel that would cause drift if the sweep ran —
                // the assertion below is that it does not.
                Some("seed-fake-rev".to_owned()),
            )
            .expect("seed registry entry");

        let summary = service.recover_all().await.unwrap();
        assert_eq!(
            summary.preserved_base_revision_drift, 0,
            "reflink sandboxes are physically independent per RFC 016 \
             and MUST NOT emit BaseRevisionDrift",
        );
        assert!(summary.base_revision_drift_runs.is_empty());
        // After PR #88 the reattach sweep may emit `SandboxReattached`
        // for this entry (path exists, no drift-handled tombstone, no
        // allowlist-revoked tombstone) — that's a separate, correct
        // contract. Here we assert only that no drift event fires.
        let events = sink.drain();
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, SandboxEvent::SandboxBaseRevisionDrift { .. })),
            "reflink sandbox must not emit SandboxBaseRevisionDrift; got {events:?}",
        );
    }

    #[tokio::test]
    async fn recover_all_skips_base_revision_drift_when_clone_missing() {
        // Clone never created → `current_head` returns `Ok(None)` →
        // the drift sweep skips silently. This is a distinct failure
        // mode from drift (operator cleanup of the clone directory);
        // firing drift against a missing clone would ask the operator
        // to approve a re-provision against a nonexistent base.
        use crate::repo_store::clone_cache::RepoCloneCache;

        let run = RunId::new("run-clone-missing");
        let repo_id = crate::sandbox::RepoId::new("octocat/hello");
        let provider = RecoveryProvider::new(SandboxStrategy::Overlay, Vec::new(), HashMap::new());
        let sink = Arc::new(BufferedSandboxEventSink::default());
        let base_dir = unique_test_dir("clone-missing-drift");
        let clone_dir = unique_test_dir("clone-missing-drift-clones");
        let clone_cache = Arc::new(RepoCloneCache::new(clone_dir));
        // Deliberately do NOT call `ensure_cloned`.

        let service = SandboxService::new(
            HashMap::from([(
                SandboxStrategy::Overlay,
                Box::new(provider) as Box<dyn crate::providers::SandboxProvider>,
            )]),
            sink.clone(),
            base_dir.clone(),
            Arc::new(FixedClock::new(1_000)),
        )
        .with_clone_cache(clone_cache);

        let sandbox_path = base_dir.join("sbx-run-clone-missing");
        fs::create_dir_all(&sandbox_path).expect("create stub sandbox dir");
        service
            .seed_registry_entry_for_test_full(
                crate::sandbox::SandboxId::new("sbx-run-clone-missing"),
                run,
                project(),
                SandboxStrategy::Overlay,
                sandbox_path,
                Some(repo_id),
                Some("seed-fake-rev".to_owned()),
            )
            .expect("seed registry entry");

        let summary = service.recover_all().await.unwrap();
        assert_eq!(summary.preserved_base_revision_drift, 0);
        assert!(summary.base_revision_drift_runs.is_empty());
        // Reattach sweep (PR #88) may emit `SandboxReattached` — it is
        // the drift absence we're locking in here, not overall silence.
        let events = sink.drain();
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, SandboxEvent::SandboxBaseRevisionDrift { .. })),
            "clone-missing must not emit SandboxBaseRevisionDrift; got {events:?}",
        );
    }

    // ── Lock-poison recovery (issue #463) ────────────────────────────────
    //
    // Before the fix, `SandboxService` had 18 `.expect("… lock poisoned")`
    // call sites across `sessions`, `session_sandboxes`, `inflight_restores`,
    // `degraded_flag_by_session`, `degraded_emitted_sessions`, and the
    // `BufferedSandboxEventSink::events` buffer. A panic holding any of
    // those guards (e.g. an OOM inside `HashMap::insert`, a `policy_hash`
    // hit overflow, a failing path canonicalisation in nested helpers)
    // would poison the lock and take down every subsequent sandbox op —
    // and since F65 PR-4 put sandbox confinement on the critical path of
    // every orchestrator session, this was a full-process DoS vector.
    //
    // The tests below spawn real threads that panic WHILE HOLDING the
    // write guard and assert that every reader/writer path still serves
    // afterwards. They exercise the production service (not a mock)
    // through its public API; the panic is induced by reaching into the
    // private lock field, which is visible inside this `#[cfg(test)]`
    // module by design.

    #[test]
    fn sessions_rwlock_survives_writer_panic() {
        let (service, _sink) = service_with_providers(vec![]);
        let service = Arc::new(service);

        // Seed a session the readers will look up post-poison. Use the
        // test-only `remember_recovered_session` helper (private but
        // in-module) to avoid needing a real provider — we just need a
        // present entry.
        let rid = run_id();
        let metadata = recovery_metadata(&rid, SandboxState::Preserved);
        service.remember_recovered_session(
            metadata,
            None,
            policy(
                SandboxStrategyRequest::Preferred(SandboxStrategy::Overlay),
                OnExhaustion::Destroy,
            ),
        );

        // Poison the `sessions` RwLock from a dedicated writer thread.
        let svc_clone = service.clone();
        let writer = std::thread::spawn(move || {
            let _guard = svc_clone
                .sessions
                .write()
                .unwrap_or_else(|e| e.into_inner());
            panic!("test-induced panic while holding sessions write guard");
        });
        assert!(writer.join().is_err(), "writer thread should have panicked");

        // After the poison, every `sessions` reader path must still serve.
        // Before #463 these `.read().expect("sandbox session lock poisoned")`
        // calls would themselves panic on the poisoned guard.
        assert_eq!(service.state_for(&rid), Some(SandboxState::Preserved));
        assert!(service.metadata_for(&rid).is_some());

        // A subsequent write must still land.
        let rid2 = RunId::new("run-2");
        let metadata2 = recovery_metadata(&rid2, SandboxState::Ready);
        service.remember_recovered_session(
            metadata2,
            None,
            policy(
                SandboxStrategyRequest::Preferred(SandboxStrategy::Overlay),
                OnExhaustion::Destroy,
            ),
        );
        assert_eq!(service.state_for(&rid2), Some(SandboxState::Ready));
    }

    #[test]
    fn session_sandboxes_rwlock_survives_writer_panic() {
        let (service, _sink) = service_with_providers(vec![]);
        let service = Arc::new(service);

        let svc_clone = service.clone();
        let writer = std::thread::spawn(move || {
            let _guard = svc_clone
                .session_sandboxes
                .write()
                .unwrap_or_else(|e| e.into_inner());
            panic!("test-induced panic while holding session_sandboxes write guard");
        });
        assert!(writer.join().is_err(), "writer thread should have panicked");

        // `live_session_count` reads `session_sandboxes`. Before #463
        // this would panic on the poisoned lock.
        assert_eq!(service.live_session_count(), 0);
    }

    #[test]
    fn inflight_restores_mutex_survives_writer_panic() {
        let (service, _sink) = service_with_providers(vec![]);
        let service = Arc::new(service);

        let svc_clone = service.clone();
        let writer = std::thread::spawn(move || {
            let mut guard = svc_clone
                .inflight_restores
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.insert(cairn_domain::WorkspaceSnapshotId::new("snap-poison"));
            panic!("test-induced panic while holding inflight_restores guard");
        });
        assert!(writer.join().is_err());

        // `reap_snapshot_dir` consults `inflight_restores`; it must not
        // panic on the poisoned guard. The insert from the panicking
        // writer IS observable — it defers the reap. That is the correct
        // outcome: a partial insert from a panicking writer leaves the
        // set consistent, and our poison-recovery pattern preserves it.
        let reaped = service
            .reap_snapshot_dir(&cairn_domain::WorkspaceSnapshotId::new("snap-poison"))
            .expect("reap_snapshot_dir must survive inflight_restores poison");
        assert!(!reaped, "in-flight snapshot must defer reap");

        // A snapshot NOT held in-flight still reaps (noop for missing
        // dir + no snapshot_dir configured → returns Ok(false)).
        let reaped_other = service
            .reap_snapshot_dir(&cairn_domain::WorkspaceSnapshotId::new("snap-other"))
            .expect("reap_snapshot_dir must survive inflight_restores poison");
        assert!(!reaped_other);
    }

    #[test]
    fn sandbox_event_buffer_survives_writer_panic() {
        let sink = Arc::new(BufferedSandboxEventSink::default());

        // Publish one event so `drain` has content to return.
        sink.publish(SandboxEvent::SandboxHeartbeat {
            sandbox_id: crate::sandbox::SandboxId::new("sbx-x"),
            run_id: RunId::new("run-x"),
            heartbeat_at: 1,
        });

        // Poison the `events` mutex from a writer thread.
        let sink_clone = sink.clone();
        let writer = std::thread::spawn(move || {
            let _guard = sink_clone.events.lock().unwrap_or_else(|e| e.into_inner());
            panic!("test-induced panic while holding events mutex");
        });
        assert!(writer.join().is_err());

        // Both public methods must survive. Before #463 they were
        // `.expect("sandbox event buffer poisoned")`.
        sink.publish(SandboxEvent::SandboxHeartbeat {
            sandbox_id: crate::sandbox::SandboxId::new("sbx-y"),
            run_id: RunId::new("run-y"),
            heartbeat_at: 2,
        });
        let events = sink.drain();
        assert_eq!(events.len(), 2);
    }

    /// Concurrent readers + a panicking writer on the real sandbox
    /// service. Models the production failure mode: many orchestrator
    /// threads reading sandbox state in parallel, one panicking writer,
    /// survivors must keep serving.
    #[test]
    fn concurrent_readers_survive_sandbox_writer_panic() {
        let (service, _sink) = service_with_providers(vec![]);
        let service = Arc::new(service);

        let rid = run_id();
        service.remember_recovered_session(
            recovery_metadata(&rid, SandboxState::Preserved),
            None,
            policy(
                SandboxStrategyRequest::Preferred(SandboxStrategy::Overlay),
                OnExhaustion::Destroy,
            ),
        );

        let readers: Vec<_> = (0..6)
            .map(|_| {
                let svc = service.clone();
                let rid = rid.clone();
                std::thread::spawn(move || {
                    for _ in 0..32 {
                        let _ = svc.state_for(&rid);
                        let _ = svc.metadata_for(&rid);
                        let _ = svc.live_session_count();
                    }
                })
            })
            .collect();

        let svc_clone = service.clone();
        let writer = std::thread::spawn(move || {
            let _guard = svc_clone
                .sessions
                .write()
                .unwrap_or_else(|e| e.into_inner());
            panic!("poison sessions under reader load");
        });
        assert!(writer.join().is_err());

        for (i, r) in readers.into_iter().enumerate() {
            r.join()
                .unwrap_or_else(|_| panic!("reader {i} panicked after lock poison"));
        }

        assert_eq!(service.state_for(&rid), Some(SandboxState::Preserved));
    }
}
