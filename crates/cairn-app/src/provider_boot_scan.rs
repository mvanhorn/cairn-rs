//! Boot-time provider-slot backfill + family-mismatch scan.
//!
//! Two responsibilities, both run exactly once per process start:
//!
//! 1. **Backfill sweep**: every project that pre-dates the
//!    `ProjectCreated`-driven dual bootstrap emission should still
//!    have a `cairn-default` binding visible in both the memory and
//!    knowledge slots. The sweep walks the event log, identifies
//!    projects with no `*ProviderConfigured` event for either family,
//!    and emits a single `*ProviderConfigured(is_bootstrap=true)` per
//!    missing slot. Idempotent — re-running is a no-op on any
//!    project that already has the relevant event.
//!
//! 2. **Family-mismatch scan**: for each project with a
//!    `plugin:<id>` ref on either slot, check whether the plugin's
//!    most recent handshake declared the matching capability family.
//!    A mismatch (e.g. mem0 on the knowledge slot) emits a one-shot
//!    audit event (`KnowledgeProviderFamilyMismatch` /
//!    `MemoryProviderFamilyMismatch`) the operator dashboard
//!    subscribes to via SSE.
//!
//! Both passes are best-effort. Failures log at WARN and do not block
//! boot — the runtime continues with degraded visibility rather than
//! refusing to start for operators whose projection backends are
//! transiently broken.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cairn_domain::events::{
    KnowledgeProviderConfigured, KnowledgeProviderFamilyMismatch, MemoryProviderConfigured,
    MemoryProviderFamilyMismatch,
};
use cairn_domain::{OperatorId, ProjectKey, ProviderRef, RuntimeEvent};
use cairn_plugin_proto::CapabilityFamily;
use cairn_store::EventLog;
use tracing::{debug, info, warn};

use cairn_runtime::services::event_helpers::make_envelope;

/// Bootstrap actor used for auto-emitted events (backfill + scan).
/// Mirrors the sentinel `ProjectServiceImpl::create` uses for the
/// `ProjectCreated`-driven bootstrap.
const BOOTSTRAP_ACTOR: &str = "system:bootstrap";

/// Page size for event-log stream scans. Matches cairn-memory's
/// resolver default.
const PAGE_SIZE: usize = 1_000;

/// Max events per `EventLog::append` call during backfill. Some event
/// log implementations (pg with conservative max_bind_params, in-mem
/// stores bounded on test harnesses) cap the size of a single batch;
/// chunking keeps the backfill reliable across backends even when a
/// very large number of projects need both slots emitted.
const APPEND_CHUNK_SIZE: usize = 500;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Run the backfill sweep + family-mismatch scan against the event
/// log. Returns a summary so the caller can log + metrics-surface the
/// counts.
///
/// `plugin_family_for_id` is consulted during the scan: given a
/// `plugin_id` (the part after `plugin:`), it must return the
/// capability family the plugin declared at its most recent
/// handshake. `None` means the plugin either hasn't handshaked yet
/// (still Spawning) or isn't registered — the scan skips those
/// projects rather than emitting spurious mismatches.
pub async fn run_provider_boot_scan<S, F>(
    store: Arc<S>,
    plugin_family_for_id: F,
) -> ProviderBootScanSummary
where
    S: EventLog + 'static,
    F: Fn(&str) -> Option<CapabilityFamily>,
{
    let mut summary = ProviderBootScanSummary::default();
    let scan = match scan_provider_state(store.as_ref()).await {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "provider boot scan failed to read event log — skipping");
            return summary;
        }
    };

    // ── Backfill pass ──────────────────────────────────────────────
    let backfill_events = build_backfill_events(&scan);
    summary.backfilled_knowledge =
        backfill_events
            .iter()
            .filter(|e| matches!(&e.payload, RuntimeEvent::KnowledgeProviderConfigured(k) if k.is_bootstrap))
            .count();
    summary.backfilled_memory = backfill_events
        .iter()
        .filter(
            |e| matches!(&e.payload, RuntimeEvent::MemoryProviderConfigured(m) if m.is_bootstrap),
        )
        .count();
    if !backfill_events.is_empty() {
        let mut append_failed = false;
        for chunk in backfill_events.chunks(APPEND_CHUNK_SIZE) {
            if let Err(e) = store.append(chunk).await {
                warn!(
                    error = %e,
                    knowledge = summary.backfilled_knowledge,
                    memory = summary.backfilled_memory,
                    "provider-slot backfill emission failed — downstream reads will still fall through to cairn-default via resolver fallback"
                );
                append_failed = true;
                break;
            }
        }
        if append_failed {
            // Partial-write recovery: re-running the scan on next boot
            // is idempotent on any projects whose chunks did land
            // (they now have the relevant event), so the caller will
            // converge without a manual recovery step.
            summary.backfilled_knowledge = 0;
            summary.backfilled_memory = 0;
        } else {
            info!(
                knowledge = summary.backfilled_knowledge,
                memory = summary.backfilled_memory,
                "provider-slot backfill: emitted cairn-default bootstrap bindings for projects missing one or both family bindings"
            );
        }
    }

    // ── Family-mismatch pass ──────────────────────────────────────
    let mismatch_events = build_family_mismatch_events(&scan, &plugin_family_for_id);
    summary.knowledge_mismatches = mismatch_events
        .iter()
        .filter(|e| matches!(e.payload, RuntimeEvent::KnowledgeProviderFamilyMismatch(_)))
        .count();
    summary.memory_mismatches = mismatch_events
        .iter()
        .filter(|e| matches!(e.payload, RuntimeEvent::MemoryProviderFamilyMismatch(_)))
        .count();
    if !mismatch_events.is_empty() {
        let mut append_failed = false;
        for chunk in mismatch_events.chunks(APPEND_CHUNK_SIZE) {
            if let Err(e) = store.append(chunk).await {
                warn!(
                    error = %e,
                    knowledge = summary.knowledge_mismatches,
                    memory = summary.memory_mismatches,
                    "provider family-mismatch emission failed — operator dashboard will not see these audits this boot"
                );
                append_failed = true;
                break;
            }
        }
        if append_failed {
            summary.knowledge_mismatches = 0;
            summary.memory_mismatches = 0;
        } else {
            warn!(
                knowledge = summary.knowledge_mismatches,
                memory = summary.memory_mismatches,
                "provider family-mismatch: detected misconfigured plugin slots; operator must reconfigure via the correct PUT endpoint"
            );
        }
    } else {
        debug!("provider family-mismatch scan: no mismatches detected");
    }

    summary
}

/// Outcome of one bootstrap run. Surfaced to callers so they can log
/// + populate metrics.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct ProviderBootScanSummary {
    pub backfilled_knowledge: usize,
    pub backfilled_memory: usize,
    pub knowledge_mismatches: usize,
    pub memory_mismatches: usize,
}

/// Snapshot of the current provider state per-project, derived from
/// the event log. Used by both the backfill pass (which needs the
/// "has any event?" bit per family) and the mismatch scan (which
/// needs the current provider_ref).
#[derive(Default)]
struct ProviderScan {
    /// Projects that have received any `ProjectCreated` event, in the
    /// order the log first mentioned them. Needed so the backfill
    /// pass only emits bootstrap bindings for actual projects, not
    /// for sentinel/_system keys.
    projects: Vec<ProjectKey>,
    /// Latest `configured` provider_ref per project on the
    /// knowledge slot. Missing key = no event ever observed.
    knowledge_refs: HashMap<ProjectKey, ProviderRef>,
    /// Same for memory slot.
    memory_refs: HashMap<ProjectKey, ProviderRef>,
}

async fn scan_provider_state<S: EventLog + ?Sized>(store: &S) -> Result<ProviderScan, String> {
    let mut scan = ProviderScan::default();
    let mut seen: HashSet<ProjectKey> = HashSet::new();
    let mut after = None;
    loop {
        let page = store
            .read_stream(after, PAGE_SIZE)
            .await
            .map_err(|e| e.to_string())?;
        if page.is_empty() {
            break;
        }
        for stored in &page {
            match &stored.envelope.payload {
                RuntimeEvent::ProjectCreated(pc) => {
                    // Only `ProjectCreated` qualifies a key as an
                    // actual project. Configure events without a
                    // matching creation (shouldn't happen in a
                    // consistent event log, but defensive against
                    // sentinel keys or hand-crafted test harness
                    // input) are ignored for backfill purposes.
                    remember_project(&mut scan, &mut seen, &pc.project);
                }
                RuntimeEvent::KnowledgeProviderConfigured(e) => {
                    scan.knowledge_refs
                        .insert(e.project.clone(), e.provider_ref.clone());
                }
                RuntimeEvent::MemoryProviderConfigured(e) => {
                    scan.memory_refs
                        .insert(e.project.clone(), e.provider_ref.clone());
                }
                _ => {}
            }
        }
        after = page.last().map(|s| s.position);
    }
    Ok(scan)
}

/// Record a project's first appearance in the event stream, preserving
/// insertion order so the backfill pass walks projects in the same
/// order they were created. No-op when the project has already been
/// seen.
fn remember_project(scan: &mut ProviderScan, seen: &mut HashSet<ProjectKey>, project: &ProjectKey) {
    if seen.insert(project.clone()) {
        scan.projects.push(project.clone());
    }
}

fn build_backfill_events(scan: &ProviderScan) -> Vec<cairn_domain::EventEnvelope<RuntimeEvent>> {
    let ts = now_ms();
    let actor = OperatorId::new(BOOTSTRAP_ACTOR);
    let default_ref = ProviderRef::new("cairn-default");
    let mut events = Vec::new();
    for project in &scan.projects {
        if !scan.knowledge_refs.contains_key(project) {
            events.push(make_envelope(RuntimeEvent::KnowledgeProviderConfigured(
                KnowledgeProviderConfigured {
                    project: project.clone(),
                    provider_ref: default_ref.clone(),
                    configured_by: actor.clone(),
                    is_bootstrap: true,
                    at_ms: ts,
                },
            )));
        }
        if !scan.memory_refs.contains_key(project) {
            events.push(make_envelope(RuntimeEvent::MemoryProviderConfigured(
                MemoryProviderConfigured {
                    project: project.clone(),
                    provider_ref: default_ref.clone(),
                    configured_by: actor.clone(),
                    is_bootstrap: true,
                    at_ms: ts,
                },
            )));
        }
    }
    events
}

fn build_family_mismatch_events<F>(
    scan: &ProviderScan,
    plugin_family_for_id: &F,
) -> Vec<cairn_domain::EventEnvelope<RuntimeEvent>>
where
    F: Fn(&str) -> Option<CapabilityFamily>,
{
    let ts = now_ms();
    let mut events = Vec::new();
    for (project, pref) in &scan.knowledge_refs {
        if let Some(plugin_id) = plugin_id_from_ref(pref) {
            if let Some(family) = plugin_family_for_id(plugin_id) {
                if family == CapabilityFamily::MemoryProvider {
                    events.push(make_envelope(
                        RuntimeEvent::KnowledgeProviderFamilyMismatch(
                            KnowledgeProviderFamilyMismatch {
                                project: project.clone(),
                                provider_ref: pref.clone(),
                                observed_family: family.as_str().to_owned(),
                                configured_slot: CapabilityFamily::KnowledgeProvider
                                    .as_str()
                                    .to_owned(),
                                at_ms: ts,
                            },
                        ),
                    ));
                }
            }
        }
    }
    for (project, pref) in &scan.memory_refs {
        if let Some(plugin_id) = plugin_id_from_ref(pref) {
            if let Some(family) = plugin_family_for_id(plugin_id) {
                if family == CapabilityFamily::KnowledgeProvider {
                    events.push(make_envelope(RuntimeEvent::MemoryProviderFamilyMismatch(
                        MemoryProviderFamilyMismatch {
                            project: project.clone(),
                            provider_ref: pref.clone(),
                            observed_family: family.as_str().to_owned(),
                            configured_slot: CapabilityFamily::MemoryProvider.as_str().to_owned(),
                            at_ms: ts,
                        },
                    )));
                }
            }
        }
    }
    events
}

fn plugin_id_from_ref(pref: &ProviderRef) -> Option<&str> {
    pref.as_str()
        .strip_prefix("plugin:")
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_domain::events::ProjectCreated;
    use cairn_store::InMemoryStore;

    fn project(id: &str) -> ProjectKey {
        ProjectKey::new("t", "w", id)
    }

    fn created(p: ProjectKey) -> cairn_domain::EventEnvelope<RuntimeEvent> {
        make_envelope(RuntimeEvent::ProjectCreated(ProjectCreated {
            project: p,
            name: "t".to_owned(),
            created_at: 1,
        }))
    }

    fn configured_knowledge(
        p: ProjectKey,
        pref: &str,
    ) -> cairn_domain::EventEnvelope<RuntimeEvent> {
        make_envelope(RuntimeEvent::KnowledgeProviderConfigured(
            KnowledgeProviderConfigured {
                project: p,
                provider_ref: ProviderRef::new(pref),
                configured_by: OperatorId::new("op"),
                is_bootstrap: false,
                at_ms: 2,
            },
        ))
    }

    fn configured_memory(p: ProjectKey, pref: &str) -> cairn_domain::EventEnvelope<RuntimeEvent> {
        make_envelope(RuntimeEvent::MemoryProviderConfigured(
            MemoryProviderConfigured {
                project: p,
                provider_ref: ProviderRef::new(pref),
                configured_by: OperatorId::new("op"),
                is_bootstrap: false,
                at_ms: 2,
            },
        ))
    }

    #[tokio::test]
    async fn empty_log_is_no_op() {
        let store = Arc::new(InMemoryStore::new());
        let out = run_provider_boot_scan(store, |_| None).await;
        assert_eq!(out, ProviderBootScanSummary::default());
    }

    #[tokio::test]
    async fn project_missing_both_bindings_gets_dual_backfill() {
        // A project that only has `ProjectCreated` (no bootstrap
        // emission) is the legacy shape — predates the dual-family
        // bootstrap on project creation.
        let store = Arc::new(InMemoryStore::new());
        store.append(&[created(project("alpha"))]).await.unwrap();
        let out = run_provider_boot_scan(store.clone(), |_| None).await;
        assert_eq!(out.backfilled_knowledge, 1);
        assert_eq!(out.backfilled_memory, 1);

        // Re-running is idempotent.
        let out2 = run_provider_boot_scan(store, |_| None).await;
        assert_eq!(out2.backfilled_knowledge, 0);
        assert_eq!(out2.backfilled_memory, 0);
    }

    #[tokio::test]
    async fn projects_with_existing_bindings_are_left_alone() {
        // ProjectCreated + both bootstrap emissions — the current
        // canonical shape. No backfill needed.
        let store = Arc::new(InMemoryStore::new());
        store
            .append(&[
                created(project("bravo")),
                configured_knowledge(project("bravo"), "cairn-default"),
                configured_memory(project("bravo"), "cairn-default"),
            ])
            .await
            .unwrap();
        let out = run_provider_boot_scan(store, |_| None).await;
        assert_eq!(out.backfilled_knowledge, 0);
        assert_eq!(out.backfilled_memory, 0);
    }

    #[tokio::test]
    async fn partial_project_gets_only_missing_slot_backfilled() {
        // Project has knowledge binding but no memory binding —
        // backfill fills the memory slot only.
        let store = Arc::new(InMemoryStore::new());
        store
            .append(&[
                created(project("charlie")),
                configured_knowledge(project("charlie"), "plugin:bedrock-kb"),
            ])
            .await
            .unwrap();
        let out = run_provider_boot_scan(store, |_| None).await;
        assert_eq!(out.backfilled_knowledge, 0);
        assert_eq!(out.backfilled_memory, 1);
    }

    #[tokio::test]
    async fn memory_plugin_on_knowledge_slot_emits_mismatch() {
        let store = Arc::new(InMemoryStore::new());
        store
            .append(&[
                created(project("delta")),
                configured_knowledge(project("delta"), "plugin:mem0"),
                configured_memory(project("delta"), "cairn-default"),
            ])
            .await
            .unwrap();
        // mem0's handshake declares memory_provider.
        let lookup = |id: &str| {
            if id == "mem0" {
                Some(CapabilityFamily::MemoryProvider)
            } else {
                None
            }
        };
        let out = run_provider_boot_scan(store, lookup).await;
        assert_eq!(
            out.knowledge_mismatches, 1,
            "mem0 on knowledge slot must emit a knowledge mismatch"
        );
        assert_eq!(out.memory_mismatches, 0);
    }

    #[tokio::test]
    async fn knowledge_plugin_on_memory_slot_emits_mismatch() {
        let store = Arc::new(InMemoryStore::new());
        store
            .append(&[
                created(project("echo")),
                configured_memory(project("echo"), "plugin:bedrock-kb"),
                configured_knowledge(project("echo"), "cairn-default"),
            ])
            .await
            .unwrap();
        let lookup = |id: &str| {
            if id == "bedrock-kb" {
                Some(CapabilityFamily::KnowledgeProvider)
            } else {
                None
            }
        };
        let out = run_provider_boot_scan(store, lookup).await;
        assert_eq!(out.knowledge_mismatches, 0);
        assert_eq!(out.memory_mismatches, 1);
    }

    #[tokio::test]
    async fn correct_family_on_correct_slot_is_silent() {
        let store = Arc::new(InMemoryStore::new());
        store
            .append(&[
                created(project("foxtrot")),
                configured_knowledge(project("foxtrot"), "plugin:bedrock-kb"),
                configured_memory(project("foxtrot"), "plugin:mem0"),
            ])
            .await
            .unwrap();
        let lookup = |id: &str| match id {
            "mem0" => Some(CapabilityFamily::MemoryProvider),
            "bedrock-kb" => Some(CapabilityFamily::KnowledgeProvider),
            _ => None,
        };
        let out = run_provider_boot_scan(store, lookup).await;
        assert_eq!(out.knowledge_mismatches, 0);
        assert_eq!(out.memory_mismatches, 0);
    }

    #[tokio::test]
    async fn plugin_without_declared_family_is_skipped() {
        // Lookup returning `None` (plugin not yet handshaked) must
        // not produce a mismatch — the scan waits for next boot.
        let store = Arc::new(InMemoryStore::new());
        store
            .append(&[
                created(project("golf")),
                configured_knowledge(project("golf"), "plugin:unknown"),
            ])
            .await
            .unwrap();
        let out = run_provider_boot_scan(store, |_| None).await;
        assert_eq!(out.knowledge_mismatches, 0);
        assert_eq!(out.memory_mismatches, 0);
    }

    #[tokio::test]
    async fn backfill_emissions_show_up_on_second_scan() {
        // After the first pass backfills, subsequent resolver reads
        // see the events. Second pass should be a no-op.
        let store = Arc::new(InMemoryStore::new());
        store.append(&[created(project("hotel"))]).await.unwrap();
        let out1 = run_provider_boot_scan(store.clone(), |_| None).await;
        assert_eq!(out1.backfilled_knowledge, 1);
        assert_eq!(out1.backfilled_memory, 1);

        let out2 = run_provider_boot_scan(store, |_| None).await;
        assert_eq!(out2, ProviderBootScanSummary::default());
    }
}
