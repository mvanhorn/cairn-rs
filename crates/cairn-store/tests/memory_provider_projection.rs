//! RFC 030 PR-B: memory-provider projection integration tests.
//!
//! Exercises the `project_memory_providers` + `memory_ingest_jobs` tables
//! on sqlite end-to-end: events appended to the log must land in the
//! projection tables in the right shape, honouring the PK + ON CONFLICT
//! rules the pg mirror uses.
//!
//! Companion to the `KnowledgeProvider*` projection code shipped in RFC
//! 029 PR-B1, which never had a dedicated integration test. PR-B closes
//! that gap for the memory side; a parallel test for knowledge is
//! included so the retroactive V072 wiring for the knowledge pg tables
//! is covered too.

use cairn_domain::{
    DocumentId, EventEnvelope, EventId, EventSource, KnowledgeDocumentId, KnowledgeIngestRejected,
    KnowledgeIngestSubmitted, KnowledgeProviderConfigured, MemoryIngestRejected,
    MemoryIngestStatusUpdated, MemoryIngestSubmitted, MemoryProviderCapabilityChanged,
    MemoryProviderConfigured, MemoryProviderUnavailable, OperatorId, ProjectId, ProjectKey,
    ProviderRef, ResolvedProviderSnapshot, RuntimeEvent, TenantId, WorkspaceId,
};
use cairn_store::{sqlite::SqliteAdapter, EventLog};

fn evt(id: &str, payload: RuntimeEvent) -> EventEnvelope<RuntimeEvent> {
    EventEnvelope::for_runtime_event(EventId::new(id), EventSource::Runtime, payload)
}

fn project(tag: &str) -> ProjectKey {
    ProjectKey {
        tenant_id: TenantId::new(format!("t_{tag}")),
        workspace_id: WorkspaceId::new("w"),
        project_id: ProjectId::new(format!("p_{tag}")),
    }
}

// ── memory_provider_configured → project_memory_providers ─────────────────

#[tokio::test]
async fn memory_provider_configured_lands_in_project_memory_providers() {
    let adapter = SqliteAdapter::in_memory().await.unwrap();
    let log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());
    let p = project("mp_cfg");

    log.append(&[evt(
        "e1",
        RuntimeEvent::MemoryProviderConfigured(MemoryProviderConfigured {
            project: p.clone(),
            provider_ref: ProviderRef::new("plugin:mem0"),
            configured_by: OperatorId::new("op_alice"),
            is_bootstrap: false,
            at_ms: 1_000,
        }),
    )])
    .await
    .unwrap();

    let (provider_ref, kind, configured_by, is_bootstrap): (String, String, String, bool) =
        sqlx::query_as(
            "SELECT provider_ref, kind, configured_by, is_bootstrap
               FROM project_memory_providers
              WHERE tenant_id = ? AND workspace_id = ? AND project_id = ?",
        )
        .bind(p.tenant_id.as_str())
        .bind(p.workspace_id.as_str())
        .bind(p.project_id.as_str())
        .fetch_one(adapter.pool())
        .await
        .unwrap();
    assert_eq!(provider_ref, "plugin:mem0");
    assert_eq!(kind, "configured");
    assert_eq!(configured_by, "op_alice");
    assert!(!is_bootstrap);
}

#[tokio::test]
async fn memory_provider_configured_records_is_bootstrap_flag() {
    let adapter = SqliteAdapter::in_memory().await.unwrap();
    let log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());
    let p = project("mp_boot");

    log.append(&[evt(
        "e1",
        RuntimeEvent::MemoryProviderConfigured(MemoryProviderConfigured {
            project: p.clone(),
            provider_ref: ProviderRef::new("cairn-default"),
            configured_by: OperatorId::new("system"),
            is_bootstrap: true,
            at_ms: 1_000,
        }),
    )])
    .await
    .unwrap();

    let is_bootstrap: bool = sqlx::query_scalar(
        "SELECT is_bootstrap FROM project_memory_providers
          WHERE tenant_id = ? AND workspace_id = ? AND project_id = ?",
    )
    .bind(p.tenant_id.as_str())
    .bind(p.workspace_id.as_str())
    .bind(p.project_id.as_str())
    .fetch_one(adapter.pool())
    .await
    .unwrap();
    assert!(is_bootstrap, "bootstrap binding must persist the flag");
}

#[tokio::test]
async fn memory_provider_unavailable_inserts_audit_row_not_overwrite() {
    let adapter = SqliteAdapter::in_memory().await.unwrap();
    let log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());
    let p = project("mp_unavail");

    log.append(&[
        evt(
            "e1",
            RuntimeEvent::MemoryProviderConfigured(MemoryProviderConfigured {
                project: p.clone(),
                provider_ref: ProviderRef::new("plugin:mem0"),
                configured_by: OperatorId::new("op"),
                is_bootstrap: false,
                at_ms: 1_000,
            }),
        ),
        evt(
            "e2",
            RuntimeEvent::MemoryProviderUnavailable(MemoryProviderUnavailable {
                project: p.clone(),
                provider_ref: ProviderRef::new("plugin:mem0"),
                reason: "handshake timeout".to_owned(),
                at_ms: 2_000,
            }),
        ),
    ])
    .await
    .unwrap();

    let rows: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT kind, configured_by, reason FROM project_memory_providers
          WHERE tenant_id = ? AND workspace_id = ? AND project_id = ? ORDER BY at_ms",
    )
    .bind(p.tenant_id.as_str())
    .bind(p.workspace_id.as_str())
    .bind(p.project_id.as_str())
    .fetch_all(adapter.pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), 2, "configured + unavailable must both persist");
    assert_eq!(rows[0].0, "configured");
    assert_eq!(rows[0].1.as_deref(), Some("op"));
    assert_eq!(rows[1].0, "unavailable");
    assert_eq!(rows[1].2.as_deref(), Some("handshake timeout"));
}

#[tokio::test]
async fn memory_provider_capability_changed_writes_prior_and_current_json() {
    let adapter = SqliteAdapter::in_memory().await.unwrap();
    let log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());
    let p = project("mp_cap");

    let prior = ResolvedProviderSnapshot {
        provider_id: "mem0".into(),
        ingest_capable: true,
        retrieval_modes: vec!["vector_only".into()],
        scoring_dimensions_surfaced: vec!["semantic_relevance".into()],
    };
    let current = ResolvedProviderSnapshot {
        provider_id: "mem0".into(),
        ingest_capable: false,
        retrieval_modes: vec!["vector_only".into()],
        scoring_dimensions_surfaced: vec!["semantic_relevance".into()],
    };
    log.append(&[evt(
        "e1",
        RuntimeEvent::MemoryProviderCapabilityChanged(MemoryProviderCapabilityChanged {
            project: p.clone(),
            provider_ref: ProviderRef::new("plugin:mem0"),
            prior: prior.clone(),
            current: current.clone(),
            at_ms: 1_000,
        }),
    )])
    .await
    .unwrap();

    let (kind, prior_json, current_json): (String, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT kind, prior_snapshot_json, current_snapshot_json
               FROM project_memory_providers
              WHERE tenant_id = ? AND workspace_id = ? AND project_id = ?",
        )
        .bind(p.tenant_id.as_str())
        .bind(p.workspace_id.as_str())
        .bind(p.project_id.as_str())
        .fetch_one(adapter.pool())
        .await
        .unwrap();
    assert_eq!(kind, "capability_changed");
    let decoded_prior: ResolvedProviderSnapshot =
        serde_json::from_str(prior_json.as_deref().unwrap()).unwrap();
    let decoded_current: ResolvedProviderSnapshot =
        serde_json::from_str(current_json.as_deref().unwrap()).unwrap();
    assert_eq!(decoded_prior, prior);
    assert_eq!(decoded_current, current);
}

// ── memory_ingest_jobs projection ─────────────────────────────────────────

#[tokio::test]
async fn memory_ingest_submitted_and_status_updated_flow() {
    let adapter = SqliteAdapter::in_memory().await.unwrap();
    let log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());
    let p = project("mi");

    log.append(&[
        evt(
            "e1",
            RuntimeEvent::MemoryIngestSubmitted(MemoryIngestSubmitted {
                project: p.clone(),
                provider_ref: ProviderRef::new("plugin:mem0"),
                document_id: DocumentId::new("mem_42"),
                source_type: "plain_text".to_owned(),
                at_ms: 1_000,
            }),
        ),
        evt(
            "e2",
            RuntimeEvent::MemoryIngestStatusUpdated(MemoryIngestStatusUpdated {
                project: p.clone(),
                provider_ref: ProviderRef::new("plugin:mem0"),
                document_id: DocumentId::new("mem_42"),
                status: "completed".to_owned(),
                at_ms: 2_000,
            }),
        ),
    ])
    .await
    .unwrap();

    let (status, source_type, submitted, updated): (String, Option<String>, i64, i64) =
        sqlx::query_as(
            "SELECT status, source_type, submitted_at_ms, updated_at_ms
               FROM memory_ingest_jobs
              WHERE tenant_id = ? AND workspace_id = ? AND project_id = ? AND document_id = ?",
        )
        .bind(p.tenant_id.as_str())
        .bind(p.workspace_id.as_str())
        .bind(p.project_id.as_str())
        .bind("mem_42")
        .fetch_one(adapter.pool())
        .await
        .unwrap();
    assert_eq!(status, "completed");
    assert_eq!(source_type.as_deref(), Some("plain_text"));
    assert_eq!(submitted, 1_000);
    assert_eq!(updated, 2_000);
}

#[tokio::test]
async fn memory_ingest_rejected_uses_synthetic_document_id() {
    // Two rejections in the same millisecond must not collide in the
    // projection table — the mirror uses a synthetic document_id keyed on
    // the envelope's event_id. Regression guard for the race the
    // knowledge-side mirror already handles.
    let adapter = SqliteAdapter::in_memory().await.unwrap();
    let log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());
    let p = project("mi_rej");

    log.append(&[
        evt(
            "e1",
            RuntimeEvent::MemoryIngestRejected(MemoryIngestRejected {
                project: p.clone(),
                provider_ref: ProviderRef::new("plugin:mem0"),
                reason: "auto_extract provider".to_owned(),
                at_ms: 5_000,
            }),
        ),
        evt(
            "e2",
            RuntimeEvent::MemoryIngestRejected(MemoryIngestRejected {
                project: p.clone(),
                provider_ref: ProviderRef::new("plugin:mem0"),
                reason: "auto_extract provider".to_owned(),
                at_ms: 5_000,
            }),
        ),
    ])
    .await
    .unwrap();

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM memory_ingest_jobs
          WHERE tenant_id = ? AND workspace_id = ? AND project_id = ?
            AND status = 'rejected'",
    )
    .bind(p.tenant_id.as_str())
    .bind(p.workspace_id.as_str())
    .bind(p.project_id.as_str())
    .fetch_one(adapter.pool())
    .await
    .unwrap();
    assert_eq!(
        count, 2,
        "distinct event_ids must produce distinct synthetic document_ids"
    );
}

// ── v_all_ingest_jobs view covers both families ───────────────────────────

#[tokio::test]
async fn v_all_ingest_jobs_unions_knowledge_and_memory_rows() {
    let adapter = SqliteAdapter::in_memory().await.unwrap();
    let log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());
    let p = project("vall");

    log.append(&[
        evt(
            "ek",
            RuntimeEvent::KnowledgeIngestSubmitted(KnowledgeIngestSubmitted {
                project: p.clone(),
                provider_ref: ProviderRef::new("cairn-default"),
                document_id: KnowledgeDocumentId::new("k_1"),
                source_type: "markdown".to_owned(),
                at_ms: 1_000,
            }),
        ),
        evt(
            "em",
            RuntimeEvent::MemoryIngestSubmitted(MemoryIngestSubmitted {
                project: p.clone(),
                provider_ref: ProviderRef::new("plugin:mem0"),
                document_id: DocumentId::new("m_1"),
                source_type: "plain_text".to_owned(),
                at_ms: 2_000,
            }),
        ),
    ])
    .await
    .unwrap();

    // Both rows should surface through the unified view, tagged by family.
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT family, document_id FROM v_all_ingest_jobs
          WHERE tenant_id = ? AND workspace_id = ? AND project_id = ?
          ORDER BY submitted_at_ms",
    )
    .bind(p.tenant_id.as_str())
    .bind(p.workspace_id.as_str())
    .bind(p.project_id.as_str())
    .fetch_all(adapter.pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("knowledge".to_owned(), "k_1".to_owned()));
    assert_eq!(rows[1], ("memory".to_owned(), "m_1".to_owned()));
}

// ── Knowledge side — retroactive coverage for the pre-existing gap ────────

#[tokio::test]
async fn knowledge_provider_projection_also_works_on_sqlite() {
    // RFC 029 PR-B1 landed knowledge projection code + sqlite schema
    // mirror but never wired `migrations/V018__create_knowledge_providers.sql`
    // into the pg migration runner. V072 retroactively does so, and
    // sqlite already had the DDL inline. This test pins down that fresh
    // adapters boot with working knowledge-side projection — and that
    // the RFC 030 `is_bootstrap` column is present + written end-to-end
    // on the knowledge side too.
    let adapter = SqliteAdapter::in_memory().await.unwrap();
    let log = cairn_store::sqlite::SqliteEventLog::new(adapter.pool().clone());
    let p = project("kp");

    log.append(&[
        evt(
            "e1",
            RuntimeEvent::KnowledgeProviderConfigured(KnowledgeProviderConfigured {
                project: p.clone(),
                provider_ref: ProviderRef::new("cairn-default"),
                configured_by: OperatorId::new("op"),
                is_bootstrap: true,
                at_ms: 1_000,
            }),
        ),
        evt(
            "e2",
            RuntimeEvent::KnowledgeIngestRejected(KnowledgeIngestRejected {
                project: p.clone(),
                provider_ref: ProviderRef::new("cairn-default"),
                reason: "ingest_capable=false".to_owned(),
                at_ms: 2_000,
            }),
        ),
    ])
    .await
    .unwrap();

    let (provider_count, bootstrap_count): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*),
                SUM(CASE WHEN is_bootstrap THEN 1 ELSE 0 END)
           FROM project_knowledge_providers
          WHERE tenant_id = ? AND project_id = ?",
    )
    .bind(p.tenant_id.as_str())
    .bind(p.project_id.as_str())
    .fetch_one(adapter.pool())
    .await
    .unwrap();
    let ingest_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM knowledge_ingest_jobs
          WHERE tenant_id = ? AND project_id = ? AND status = 'rejected'",
    )
    .bind(p.tenant_id.as_str())
    .bind(p.project_id.as_str())
    .fetch_one(adapter.pool())
    .await
    .unwrap();
    assert_eq!(provider_count, 1);
    assert_eq!(
        bootstrap_count, 1,
        "is_bootstrap must propagate to project_knowledge_providers column"
    );
    assert_eq!(ingest_count, 1);
}
