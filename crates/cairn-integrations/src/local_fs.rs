//! Local-filesystem integration plugin for Cairn.
//!
//! Treats a directory on disk as a pseudo-repo for issue-sync and
//! PR-proposal flows. No webhook, no network auth — the operator hands
//! over a path, and Cairn reads the directory as if it were a git host.
//!
//! This is the minimum surface needed to dogfood the UI's multi-host
//! selector against something that works without a real external
//! service. Writes happen via the regular file/git tools, not through
//! this plugin.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    EventAction, EventActionMapping, Integration, IntegrationError, IntegrationEvent, QueueStats,
    WorkItem,
};

/// Local-filesystem integration configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalFsConfig {
    /// Absolute path to the directory that should be exposed as a
    /// pseudo-repo. Must exist at configuration time.
    pub path: String,
    /// Optional human-readable label shown in the UI. Defaults to the
    /// basename of `path`.
    #[serde(default)]
    pub display_name: Option<String>,
}

/// A minimal read-only integration backed by a local directory.
pub struct LocalFsPlugin {
    id: String,
    display_name: String,
    path: String,
}

/// Validate a `local_fs.path` against an explicit `CAIRN_LOCAL_FS_BASE`
/// value. Pure (no env reads) so unit tests can exercise the policy
/// without racing on the process-wide env block — `set_var` is
/// `unsafe` under edition 2024 and `unsafe_code = "forbid"` is set at
/// the workspace level, so deterministic tests must drive the policy
/// through this seam.
///
/// Returns the canonicalised path on success. Mirrors the rules in
/// `cairn-app::repo_routes::add_local_fs_path` so both entry points
/// (legacy `POST /v1/repos/:project_key/local_fs` and
/// `POST /v1/integrations` with `type=local_fs`) reject the same set
/// of inputs.
fn validate_local_fs_path(
    raw_path: &str,
    base_raw: Option<&str>,
) -> Result<std::path::PathBuf, IntegrationError> {
    if raw_path.trim().is_empty() {
        return Err(IntegrationError::ConfigInvalid(
            "local_fs.path must not be empty".into(),
        ));
    }
    let p = std::path::Path::new(raw_path);
    if !p.is_absolute() {
        return Err(IntegrationError::ConfigInvalid(
            "local_fs.path must be an absolute path".into(),
        ));
    }
    if p.components().any(|c| {
        matches!(
            c,
            std::path::Component::ParentDir | std::path::Component::CurDir
        )
    }) {
        return Err(IntegrationError::ConfigInvalid(
            "local_fs.path must not contain `.` or `..` components".into(),
        ));
    }
    if !p.exists() {
        return Err(IntegrationError::ConfigInvalid(format!(
            "local_fs.path does not exist: {raw_path}"
        )));
    }
    if !p.is_dir() {
        return Err(IntegrationError::ConfigInvalid(format!(
            "local_fs.path is not a directory: {raw_path}"
        )));
    }
    let base_raw = base_raw.ok_or_else(|| {
        IntegrationError::ConfigInvalid(
            "local_fs access is disabled until CAIRN_LOCAL_FS_BASE is configured".into(),
        )
    })?;
    if base_raw.trim().is_empty() {
        return Err(IntegrationError::ConfigInvalid(
            "CAIRN_LOCAL_FS_BASE must not be empty".into(),
        ));
    }
    let base = std::path::PathBuf::from(base_raw);
    match (p.canonicalize(), base.canonicalize()) {
        (Ok(canon), Ok(base_canon)) if canon.starts_with(&base_canon) => Ok(canon),
        (Ok(_), Ok(_)) => Err(IntegrationError::ConfigInvalid(
            "local_fs.path is outside the configured CAIRN_LOCAL_FS_BASE".into(),
        )),
        _ => Err(IntegrationError::ConfigInvalid(
            "local_fs.path could not be canonicalised for base-dir check".into(),
        )),
    }
}

impl LocalFsPlugin {
    pub fn new(id: &str, config: LocalFsConfig) -> Result<Self, IntegrationError> {
        // Read the env once at the production entry point; tests drive
        // `validate_local_fs_path` (or `new_with_base`) directly to
        // avoid racing on the process-wide env block.
        let base_owned = std::env::var("CAIRN_LOCAL_FS_BASE").ok();
        Self::new_with_base(id, config, base_owned.as_deref())
    }

    /// Test seam: construct a `LocalFsPlugin` with the base directory
    /// supplied explicitly instead of read from `CAIRN_LOCAL_FS_BASE`.
    /// Production code goes through `new`.
    pub(crate) fn new_with_base(
        id: &str,
        config: LocalFsConfig,
        base_raw: Option<&str>,
    ) -> Result<Self, IntegrationError> {
        let canon = validate_local_fs_path(&config.path, base_raw)?;
        let canon_str = canon.to_string_lossy().into_owned();
        let display_name = config.display_name.unwrap_or_else(|| {
            canon
                .file_name()
                .and_then(|n| n.to_str())
                .map(str::to_owned)
                .unwrap_or_else(|| canon_str.clone())
        });
        Ok(Self {
            id: id.to_owned(),
            display_name,
            path: canon_str,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

#[async_trait]
impl Integration for LocalFsPlugin {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn display_name(&self) -> &str {
        &self.display_name
    }

    fn is_configured(&self) -> bool {
        std::path::Path::new(&self.path).is_dir()
    }

    fn default_agent_prompt(&self) -> &str {
        "You are an autonomous agent working on a task rooted in a local \
         directory. Use file and shell tools to read, modify, and verify \
         changes. There is no external issue tracker — the operator \
         describes the task directly."
    }

    fn default_event_actions(&self) -> Vec<EventActionMapping> {
        // local_fs has no webhook surface; event mappings are inert but
        // non-empty so overrides can still attach.
        vec![EventActionMapping {
            event_pattern: "*".into(),
            label_filter: None,
            repo_filter: None,
            action: EventAction::Ignore,
        }]
    }

    async fn verify_webhook(
        &self,
        _headers: &http::HeaderMap,
        _body: &[u8],
    ) -> Result<(), IntegrationError> {
        // No webhook surface for a local directory.
        Err(IntegrationError::VerificationFailed(
            "local_fs integration does not accept webhooks".into(),
        ))
    }

    async fn parse_event(
        &self,
        _headers: &http::HeaderMap,
        _body: &[u8],
    ) -> Result<IntegrationEvent, IntegrationError> {
        Err(IntegrationError::ParseError(
            "local_fs integration has no event stream".into(),
        ))
    }

    async fn build_goal(&self, item: &WorkItem) -> Result<String, IntegrationError> {
        Ok(format!(
            "Work on task `{}` rooted at local path `{}`.\n\nDescription:\n{}",
            item.title, self.path, item.body,
        ))
    }

    async fn prepare_tool_registry(
        &self,
        base: &cairn_tools::BuiltinToolRegistry,
        _item: &WorkItem,
    ) -> Arc<cairn_tools::BuiltinToolRegistry> {
        Arc::new(cairn_tools::BuiltinToolRegistry::from_existing(base))
    }

    fn auth_exempt_paths(&self) -> Vec<String> {
        Vec::new()
    }

    async fn queue_stats(&self) -> QueueStats {
        QueueStats::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_str(d: &tempfile::TempDir) -> Option<String> {
        Some(d.path().to_string_lossy().into_owned())
    }

    #[test]
    fn rejects_missing_path() {
        let base = tempfile::tempdir().expect("base tempdir");
        let res = validate_local_fs_path(
            "/nonexistent/path/that/really/does/not/exist",
            base_str(&base).as_deref(),
        );
        assert!(
            matches!(res, Err(IntegrationError::ConfigInvalid(m)) if m.contains("does not exist"))
        );
    }

    #[test]
    fn rejects_empty_path() {
        let base = tempfile::tempdir().expect("base tempdir");
        let res = validate_local_fs_path("   ", base_str(&base).as_deref());
        assert!(
            matches!(res, Err(IntegrationError::ConfigInvalid(m)) if m.contains("must not be empty"))
        );
    }

    #[test]
    fn rejects_when_base_env_unset() {
        let inside = tempfile::tempdir().expect("tempdir");
        let res = validate_local_fs_path(&inside.path().to_string_lossy(), None);
        let msg = match res {
            Err(IntegrationError::ConfigInvalid(m)) => m,
            other => panic!("expected ConfigInvalid, got {other:?}"),
        };
        assert!(
            msg.contains("CAIRN_LOCAL_FS_BASE"),
            "error must name the missing env var: {msg}"
        );
    }

    #[test]
    fn rejects_when_base_env_empty() {
        let inside = tempfile::tempdir().expect("tempdir");
        let res = validate_local_fs_path(&inside.path().to_string_lossy(), Some(""));
        assert!(
            matches!(res, Err(IntegrationError::ConfigInvalid(m)) if m.contains("must not be empty")),
            "empty CAIRN_LOCAL_FS_BASE should not silently succeed"
        );
    }

    #[test]
    fn rejects_path_outside_base() {
        let base = tempfile::tempdir().expect("base tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let res = validate_local_fs_path(
            &outside.path().to_string_lossy(),
            base_str(&base).as_deref(),
        );
        assert!(
            matches!(res, Err(IntegrationError::ConfigInvalid(m)) if m.contains("outside the configured CAIRN_LOCAL_FS_BASE")),
            "paths outside the fence must be rejected"
        );
    }

    #[test]
    fn rejects_relative_path() {
        let base = tempfile::tempdir().expect("base tempdir");
        let res = validate_local_fs_path("relative/dir", base_str(&base).as_deref());
        assert!(matches!(res, Err(IntegrationError::ConfigInvalid(m)) if m.contains("absolute")));
    }

    #[test]
    fn rejects_traversal_components() {
        let base = tempfile::tempdir().expect("base tempdir");
        let traversal = format!("{}/../etc", base.path().display());
        let res = validate_local_fs_path(&traversal, base_str(&base).as_deref());
        assert!(
            matches!(res, Err(IntegrationError::ConfigInvalid(m)) if m.contains("`.` or `..`"))
        );
    }

    #[test]
    fn accepts_existing_directory_inside_base() {
        let base = tempfile::tempdir().expect("base tempdir");
        let inside = tempfile::tempdir_in(base.path()).expect("inside tempdir");
        let plugin = LocalFsPlugin::new_with_base(
            "lfs",
            LocalFsConfig {
                path: inside.path().to_string_lossy().into_owned(),
                display_name: Some("Temp".into()),
            },
            Some(&base.path().to_string_lossy()),
        )
        .expect("path inside the fence should be accepted");
        assert_eq!(plugin.display_name(), "Temp");
        assert!(plugin.is_configured());
    }

    #[test]
    fn default_display_name_uses_basename() {
        let base = tempfile::tempdir().expect("base tempdir");
        let inside = tempfile::tempdir_in(base.path()).expect("inside tempdir");
        let plugin = LocalFsPlugin::new_with_base(
            "lfs",
            LocalFsConfig {
                path: inside.path().to_string_lossy().into_owned(),
                display_name: None,
            },
            Some(&base.path().to_string_lossy()),
        )
        .expect("path inside the fence should be accepted");
        let basename = inside
            .path()
            .canonicalize()
            .expect("canonicalise inside")
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap()
            .to_owned();
        assert_eq!(plugin.display_name(), basename);
    }

    /// Symlink-TOCTOU regression: even if a caller presents a symlink
    /// that resolves to a path inside the fence, the stored `path`
    /// must be the canonicalised target. Otherwise an attacker who
    /// later points the same symlink at `/etc` could escape the jail
    /// at runtime.
    #[cfg(unix)]
    #[test]
    fn stores_canonicalised_path_not_symlink() {
        use std::os::unix::fs::symlink;
        let base = tempfile::tempdir().expect("base tempdir");
        let real = tempfile::tempdir_in(base.path()).expect("real tempdir");
        let link = base.path().join("link");
        symlink(real.path(), &link).expect("symlink");
        let plugin = LocalFsPlugin::new_with_base(
            "lfs",
            LocalFsConfig {
                path: link.to_string_lossy().into_owned(),
                display_name: None,
            },
            Some(&base.path().to_string_lossy()),
        )
        .expect("symlinked path inside fence is accepted");
        let real_canon = real.path().canonicalize().expect("canonicalise real");
        assert_eq!(
            std::path::Path::new(plugin.path()),
            real_canon.as_path(),
            "plugin must persist the canonical target, not the symlink"
        );
    }
}
