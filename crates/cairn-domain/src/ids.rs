use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! define_id {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        // Intentionally no `impl Default`: an empty-string ID has no
        // meaningful referent and would silently match nothing in a
        // query (the hazard the audit called out — a test fixture
        // using `..Default::default()` on a struct with an ID field
        // flows an empty ID into store queries that then return
        // empty results, masking the test bug).
        //
        // Call sites that genuinely need a placeholder must spell it
        // — `$name::new("")` (e.g. `TenantId::new("")`) for
        // schema-migration of an older event shape (the payload
        // predates the field), or a named sentinel such as
        // `$name::new("unknown")` for operational fallbacks.
        //
        // Separately, serde back-compat for missing ID fields is
        // handled per-field via
        // `#[serde(default = "crate::ids::empty_<kind>_id")]`. That
        // attribute only affects deserialisation of older payloads
        // where the field was absent; it does not reinstate Rust's
        // `Default` trait on the ID type itself, so
        // `..Default::default()` on a struct carrying an ID still
        // fails to compile. Audit: #473.
    };
}

define_id!(ApprovalId);
define_id!(ChannelId);
define_id!(CheckpointId);
define_id!(ChunkId);
define_id!(CommandId);
define_id!(CorrelationId);
define_id!(CredentialId);
define_id!(DecisionId);
define_id!(EvalRunId);
define_id!(EventId);
define_id!(IngestJobId);
// RFC 030: family-neutral document ID used across memory and knowledge wire
// types. `KnowledgeDocumentId` is retained as a back-compat alias so event
// variants + persisted types continue to compile and serialize identically.
define_id!(DocumentId);
pub type KnowledgeDocumentId = DocumentId;
define_id!(KnowledgePackId);
define_id!(MailboxMessageId);
define_id!(OperatorId);
define_id!(OutcomeId);
define_id!(PolicyId);
define_id!(ProjectId);
// RFC 029: reference to a configured knowledge provider. Format is either
// "cairn-default" for the in-process reference implementation, or
// "plugin:<plugin_id>" for an external provider plugin registered via
// RFC 007. Stored on `project_knowledge_providers`.
define_id!(ProviderRef);
define_id!(PromptAssetId);
define_id!(PromptReleaseId);
define_id!(PromptVersionId);
define_id!(ProviderBindingId);
define_id!(ProviderConnectionId);
define_id!(ProviderCallId);
define_id!(ProviderModelId);
define_id!(ProviderRouteTemplateId);
define_id!(ReleaseActionId);
define_id!(RouteAttemptId);
define_id!(RouteDecisionId);
define_id!(RunId);
define_id!(RunTemplateId);
define_id!(SessionId);
define_id!(SignalId);
define_id!(ScheduledTaskId);
define_id!(SourceId);
define_id!(TaskId);
define_id!(TenantId);
define_id!(ToolCallId);
define_id!(ToolInvocationId);
define_id!(TriggerId);
define_id!(WorkerId);
define_id!(WorkspaceId);
// F65: workspace-filesystem snapshots used by the orchestrator session redesign.
define_id!(WorkspaceSnapshotId);

/// Issue #670: stable prefix for LLM-initiated subagent child runs.
///
/// The orchestrator's `spawn_subagent` execute branch derives the
/// child run id from the child task id (itself a fresh uuid) — the
/// prefix is applied via [`RunId::new_subagent_for_task`]. The
/// `FabricTaskServiceAdapter::spawn_subagent` override's fallback
/// path (when callers pass `child_run_id: None` — test fakes and
/// pre-G3 callers) uses [`RunId::new_subagent_for_parent`]. Keeping
/// the prefix constant here stops the two sites from drifting.
pub const SUBAGENT_RUN_ID_PREFIX: &str = "run_subagent_";

impl RunId {
    /// Mint a child-run id derived from the child task id. Used on
    /// the LLM-initiated spawn path (`execute_impl`): the task id is
    /// a fresh uuid minted earlier in the same execute phase, so
    /// pairing `run_subagent_<task>` gives a stable, traceable link
    /// between the audit row and the child run.
    pub fn new_subagent_for_task(child_task_id: &TaskId) -> Self {
        Self::new(format!(
            "{SUBAGENT_RUN_ID_PREFIX}{}",
            child_task_id.as_str()
        ))
    }

    /// Mint a fallback child-run id derived from the parent run id.
    /// Used on the adapter-level default-impl fallback path when
    /// callers (test fakes, pre-G3 code) pass `child_run_id: None` —
    /// matches the pre-G3 behaviour of
    /// `RunService::spawn_subagent`'s default impl so the fallback
    /// id shape is unchanged across the G1→G3 boundary.
    pub fn new_subagent_for_parent(parent_run_id: &RunId) -> Self {
        Self::new(format!("subagent_{}", parent_run_id.as_str()))
    }
}

// ── Serde migration helpers ───────────────────────────────────────────────
//
// Per audit #473, the ID newtypes no longer implement `Default`.
// A small set of event payloads embed bare-typed IDs with
// `#[serde(default)]` so that older event-log entries (written
// before the field existed) still deserialise. For each such field
// we expose a named helper here so the declaration site can opt in
// via `#[serde(default = "empty_<kind>_id")]` — the empty-string
// value is still the payload shape, but the call site reads as a
// deliberate schema-migration choice rather than a silent Default.
//
// When an older event-log entry rehydrates with one of these empty
// IDs, projections treat it as "attribute was absent at write time";
// the surrounding payload carries the canonical identity via
// `ProjectKey` (tenant/workspace/project) so no cross-tenant leak
// is introduced.

pub(crate) fn empty_task_id() -> TaskId {
    TaskId::new("")
}

pub(crate) fn empty_tenant_id() -> TenantId {
    TenantId::new("")
}

pub(crate) fn empty_workspace_id() -> WorkspaceId {
    WorkspaceId::new("")
}

pub(crate) fn empty_credential_id() -> CredentialId {
    CredentialId::new("")
}

pub(crate) fn empty_provider_connection_id() -> ProviderConnectionId {
    ProviderConnectionId::new("")
}

#[cfg(test)]
mod tests {
    use super::{ProjectId, TenantId};

    #[test]
    fn ids_preserve_string_representation() {
        let tenant_id = TenantId::new("tenant_acme");
        let project_id = ProjectId::from("project_support");

        assert_eq!(tenant_id.as_str(), "tenant_acme");
        assert_eq!(project_id.to_string(), "project_support");
    }
}
