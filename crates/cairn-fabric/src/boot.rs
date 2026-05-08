use std::sync::Arc;

use ferriskey::Client;
use flowfabric::core::backend::ScannerFilter;
use flowfabric::core::capability::Capabilities;
use flowfabric::core::completion_backend::CompletionBackend;
use flowfabric::core::contracts::SeedWaitpointHmacSecretArgs;
use flowfabric::core::engine_backend::EngineBackend;
use flowfabric::core::partition::PartitionConfig;
use flowfabric::engine::{Engine, EngineConfig};
use flowfabric::valkey::ValkeyBackend;

use crate::config::FabricConfig;
use crate::error::FabricError;

pub struct FabricRuntime {
    pub client: Client,
    pub engine: Engine,
    pub partition_config: PartitionConfig,
    pub config: Arc<FabricConfig>,
    /// Shared FF observability handle. Constructed once at
    /// `FabricRuntime::start` and cloned into the `Engine` — so the
    /// counters/histograms the engine records into at runtime are the
    /// same ones rendered here. `Metrics` is internally `Arc`-based; the
    /// explicit `Arc` here matches the engine's expected ownership and
    /// lets cairn-app append FF's Prometheus text to its `/metrics`
    /// response without threading a second handle through startup.
    pub ff_metrics: Arc<ff_observability::Metrics>,
    /// FF 0.10 (FF#277) flat capabilities struct, computed once at
    /// boot and cached. Consumers inspect this to grey-render features
    /// a backend does not support without round-tripping into Valkey.
    /// CG-a wires the startup call + getter; CJ-2 consumes it from the
    /// UI greyrender. CG-c migrated the BTreeMap to the flat `Supports`
    /// struct per the v0.9→v0.10 consumer migration guide.
    pub capabilities: Capabilities,
    /// Typed handle to the backend. Held for the post-boot surface
    /// (`restore_frames` stream reads, etc.) that now takes
    /// `&dyn EngineBackend`. Kept alongside the raw `client` for CG-a
    /// — CG-b drops the raw client once `subscribe_lease_history` is
    /// trait-based (FF#282).
    pub backend: Arc<dyn EngineBackend>,
}

const CONNECT_MAX_ATTEMPTS: u32 = 3;
const CONNECT_BACKOFF_MS: [u64; 3] = [1_000, 2_000, 4_000];

impl FabricRuntime {
    // Steady-state reconnect after transient disconnects is handled by
    // ferriskey's internal connection pool — it re-establishes transparently
    // on the next command. This retry loop only covers initial startup.
    pub async fn start(config: FabricConfig) -> Result<Self, FabricError> {
        // Pull the ValkeyConnection once; non-Valkey backends fail loud
        // here rather than deeper into ferriskey.
        let vk = config.valkey_connection()?.clone();
        tracing::info!(
            host = %vk.host,
            port = vk.port,
            tls = vk.tls,
            cluster = vk.cluster,
            "connecting to valkey"
        );

        let mut last_err = String::new();
        let mut client = None;
        for attempt in 0..CONNECT_MAX_ATTEMPTS {
            // Rebuild each attempt: `ClientBuilder::build` consumes self.
            let result = config.client_builder()?.build().await;
            match result {
                Ok(c) => {
                    client = Some(c);
                    break;
                }
                Err(e) => {
                    last_err = e.to_string();
                    if attempt + 1 < CONNECT_MAX_ATTEMPTS {
                        let backoff = CONNECT_BACKOFF_MS[attempt as usize];
                        tracing::warn!(
                            attempt = attempt + 1,
                            error = %last_err,
                            backoff_ms = backoff,
                            "valkey connect failed, retrying"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    }
                }
            }
        }
        let client = client.ok_or(FabricError::Valkey(last_err))?;

        // Verify the connected Valkey has the Functions API (>= 7.0),
        // with a boot-time WARN when the detected major is below 8.0.
        // 60s retry budget tolerates rolling upgrades. See
        // `version_check` module docs for the full rationale.
        crate::version_check::verify_valkey_version(&client).await?;

        let partition_config = PartitionConfig::default();

        // PERF#5/#6: operators need to see the partition counts on boot
        // so a mis-set cluster (e.g. half the nodes at 64, half at 256)
        // is obvious before any ExecutionId is minted. ExecutionId
        // stability depends on keeping `num_flow_partitions` fixed across
        // the cluster lifetime (RFC-011 bumped the default from 64 → 256).
        tracing::info!(
            num_flow_partitions = partition_config.num_flow_partitions,
            num_budget_partitions = partition_config.num_budget_partitions,
            num_quota_partitions = partition_config.num_quota_partitions,
            "fabric: initialised with partition config (default changed \
             64→256 in RFC-011; ExecutionId stability depends on keeping \
             this count fixed across the cluster lifetime)"
        );

        // Build the per-consumer `ScannerFilter` (FF PR #127 / issue #122).
        //
        // Scope choice — **instance_tag only, namespace is `None`**.
        //
        // FF's `ScannerFilter` has two axes: `namespace` (matched against
        // `exec_core.namespace`) and `instance_tag` (matched against the
        // `cairn.instance_id` entry on the execution's tags hash).
        //
        // Cairn's `exec_core.namespace` is written per-*tenant*
        // (`id_map::tenant_to_namespace(project.tenant_id)`, see
        // `services/run_service.rs::namespace` and the session-service
        // sibling). A single cairn-fabric process serves many tenants,
        // so wiring `ScannerFilter.namespace` to any one of them would
        // collapse scanner scope to just that tenant's executions.
        // Tenant scoping is enforced at the cairn-store projection
        // layer, not via FF scanners — so we leave this axis unset.
        //
        // `instance_tag`, by contrast, is written per-*cairn-app
        // instance* (task_service + run_service `HSET cairn.instance_id
        // <worker_instance_id>`). It's the exact axis the cross-instance
        // isolation invariant needs — two cairn-apps sharing a Valkey
        // must each see only their own executions' scanner cycles and
        // completion frames.
        //
        // Supersedes cairn's PR #106 client-side filter in
        // `LeaseHistorySubscriber::fetch_entity_context` — the upstream
        // backend filter now drops foreign frames before they hit the
        // cairn subscriber, and the matching predicate on the
        // subscribe_completions_filtered stream keeps this engine's
        // DAG dispatch loop blind to foreign completions.
        // `ScannerFilter` is `#[non_exhaustive]` on the FF side
        // (future dimensions like `lane_id` / `worker_instance` can
        // land additively), so we can't use a bare struct literal or
        // struct-update syntax. Mutate after `default()` instead.
        let mut scanner_filter = ScannerFilter::default();
        scanner_filter.instance_tag = Some((
            "cairn.instance_id".to_owned(),
            config.worker_instance_id.as_str().to_owned(),
        ));

        // Construct a ValkeyBackend around the already-dialed client so
        // the completion subscriber can reuse a single connection
        // topology and open its dedicated RESP3 subscriber from the
        // retained `ValkeyConnection`. This replaces FF 0.3.0's
        // `CompletionListenerConfig` — PR #127 removed the implicit
        // listener field on EngineConfig in favour of an explicit
        // stream handed to `Engine::start_with_completions`.
        let backend = ValkeyBackend::from_client_partitions_and_connection(
            client.clone(),
            partition_config,
            vk.clone(),
        );

        // FF 0.9 (FF#281): backend.prepare() replaces the hand-rolled
        // `ff_script::loader::ensure_library` retry loop. The Valkey
        // impl runs FUNCTION LOAD REPLACE and is idempotent — safe to
        // call on every boot. Fails loud with EngineError::Backend on
        // transport errors; cairn propagates as FabricError::Engine.
        let prepare_outcome = backend
            .prepare()
            .await
            .map_err(|e| FabricError::Engine(Box::new(e)))?;
        tracing::info!(outcome = ?prepare_outcome, "backend prepared (FF#281)");

        // FF 0.9 (FF#280): seed the waitpoint HMAC secret via the
        // trait method. Replaces cairn's per-partition HSET loop.
        // Idempotent upstream — operators can call it every boot and
        // observe `SeedOutcome::AlreadySeeded` after the first.
        let (secret, kid) = match (
            config.waitpoint_hmac_secret.as_deref(),
            config.resolved_waitpoint_hmac_kid(),
        ) {
            (Some(s), Some(k)) => (s, k),
            _ => {
                return Err(FabricError::Config(
                    "CAIRN_FABRIC_WAITPOINT_HMAC_SECRET is required — boot refuses \
                     to ship a runtime that would reject every ff_suspend_execution \
                     with hmac_secret_not_initialized. Set the secret (64 hex chars) \
                     plus CAIRN_FABRIC_WAITPOINT_HMAC_KID."
                        .to_owned(),
                ));
            }
        };
        let seed_outcome = backend
            .seed_waitpoint_hmac_secret(SeedWaitpointHmacSecretArgs::new(kid, secret))
            .await
            .map_err(|e| reshape_seed_error(e, kid))?;
        tracing::info!(kid = %kid, outcome = ?seed_outcome, "waitpoint HMAC secret seeded (FF#280)");

        // FF 0.10 (FF#277 flat reshape): snapshot the backend's
        // capabilities once at boot so consumers (cairn-app `/v1/status`,
        // UI grey-rendering) can reason about backend-parity gaps without
        // per-request RTT. CJ-2 consumes this via AppState.
        let capabilities = backend.capabilities();
        tracing::info!(
            backend = %capabilities.identity.family,
            version = ?capabilities.identity.version,
            "backend capabilities captured (FF#277)"
        );

        // Open the filtered completion stream. The backend applies the
        // filter at push time (one HGET on the exec's tags hash per
        // frame when `instance_tag` is set), so foreign completions
        // never reach the dispatch loop. This closes the
        // cross-instance leak that cairn's PR #106 addressed
        // client-side and the FF#122 data-plane audit flagged in the
        // DAG dispatch path.
        let completion_stream = backend
            .subscribe_completions_filtered(&scanner_filter)
            .await
            .map_err(|e| FabricError::Valkey(format!("subscribe_completions_filtered: {e}")))?;

        let engine_config = EngineConfig {
            partition_config,
            lanes: vec![config.lane_id.clone()],
            scanner_filter: scanner_filter.clone(),
            ..EngineConfig::default()
        };

        // `Engine::start_with_completions` is the PR #127 replacement
        // for the old `completion_listener: Some(_)` field. Supplying
        // the stream here wires push-based DAG dispatch; scanners also
        // honour `scanner_filter` so every execution-shaped scan
        // (lease_expiry, attempt_timeout, etc.) skips foreign
        // candidates before the scanner FCALL hot path.
        // Build the shared FF metrics registry once. The engine
        // records into this same `Arc<Metrics>` the /metrics handler
        // later renders — cloning the Arc is how FF's own crates share
        // the registry across threads (see ff-observability 0.3.2
        // `real.rs`: every instrument handle is itself `Arc`-backed).
        //
        // FF 0.13 (cairn #436, PR-7b): the engine now accepts
        // `Arc<dyn EngineBackend>` instead of `ferriskey::Client`. The
        // Valkey scanner spawn path inside ff-engine still extracts
        // the embedded `ferriskey::Client` via `as_any().downcast_ref::
        // <ValkeyBackend>`, so behaviour is unchanged — the engine
        // just no longer panics on non-Valkey backends. Coerce our
        // `Arc<ValkeyBackend>` to the trait object before the call so
        // the same handle powers both the engine and the post-boot
        // `restore_frames` / lease-history subscriber surface.
        let backend: Arc<dyn EngineBackend> = backend;

        let ff_metrics = std::sync::Arc::new(ff_observability::Metrics::new());
        let engine = Engine::start_with_completions(
            engine_config,
            backend.clone(),
            std::sync::Arc::clone(&ff_metrics),
            completion_stream,
        );
        tracing::info!("fabric runtime started");

        Ok(Self {
            client,
            engine,
            partition_config,
            config: Arc::new(config),
            ff_metrics,
            capabilities,
            backend,
        })
    }

    /// Expose the cached capability matrix. Stable snapshot captured at
    /// boot; callers MUST treat it as read-only (FF#277).
    pub fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// Expose the backend handle for post-boot consumers that need the
    /// typed trait surface (stream reads, future lease-history
    /// subscriber in CG-b).
    pub fn backend(&self) -> &Arc<dyn EngineBackend> {
        &self.backend
    }

    /// Dispatch an FCALL against FF's registered Lua library.
    ///
    /// Takes `&[String]` rather than `&[&str]` so the ~20 internal call
    /// sites no longer rebuild two transient `Vec<&str>` per dispatch —
    /// each FCALL previously paid two allocations just to re-borrow the
    /// owned `String`s the builders already produced (#501). Both
    /// `ferriskey::Client::fcall` (`&[impl ToArgs]`) and
    /// `crate::fcall::verify_builder_counts` (`&[String]`) accept the
    /// owned slice directly, so the transformation is a pure de-allocation
    /// without changing the wire format.
    pub async fn fcall(
        &self,
        function: &str,
        keys: &[String],
        args: &[String],
    ) -> Result<ferriskey::Value, FabricError> {
        if cfg!(debug_assertions) {
            crate::fcall::verify_builder_counts(function, keys, args)?;
        }
        let timeout = std::time::Duration::from_millis(self.config.fcall_timeout_ms);
        match tokio::time::timeout(timeout, self.client.fcall(function, keys, args)).await {
            Ok(Ok(val)) => Ok(val),
            Ok(Err(e)) => Err(FabricError::Valkey(format!("{function}: {e}"))),
            Err(_) => Err(FabricError::Valkey(format!(
                "{function}: timeout after {}ms",
                self.config.fcall_timeout_ms
            ))),
        }
    }

    pub async fn shutdown(self) {
        tracing::info!("shutting down fabric runtime");
        self.engine.shutdown().await;
    }
}

// FF 0.9 (FF#280) adopted: `seed_waitpoint_hmac_secret_if_configured`
// (~45 LOC of per-partition HSET loops) was deleted in favour of
// `EngineBackend::seed_waitpoint_hmac_secret`. Call site lives in
// `FabricRuntime::start` above.

// PR-C4c: impl the backend-agnostic `FabricRuntimeHandle` trait so
// services can hold `Arc<dyn FabricRuntimeHandle>` instead of the
// concrete Valkey-typed `Arc<FabricRuntime>`. All accessors
// delegate to the existing fields; `valkey_client` returns `Some`
// (this IS the Valkey runtime) and `fcall` delegates to
// [`FabricRuntime::fcall`].
#[async_trait::async_trait]
impl crate::runtime_handle::FabricRuntimeHandle for FabricRuntime {
    fn partition_config(&self) -> &PartitionConfig {
        &self.partition_config
    }

    fn worker_instance_id(&self) -> &flowfabric::core::types::WorkerInstanceId {
        &self.config.worker_instance_id
    }

    fn lease_ttl_ms(&self) -> u64 {
        self.config.lease_ttl_ms
    }

    fn signal_dedup_ttl_ms(&self) -> u64 {
        self.config.signal_dedup_ttl_ms
    }

    fn worker_capabilities(&self) -> &std::collections::BTreeSet<String> {
        &self.config.worker_capabilities
    }

    fn backend(&self) -> &Arc<dyn EngineBackend> {
        &self.backend
    }

    fn valkey_client(&self) -> Option<&ferriskey::Client> {
        Some(&self.client)
    }

    async fn fcall(
        &self,
        function: &str,
        keys: &[String],
        args: &[String],
    ) -> Result<ferriskey::Value, crate::error::FabricError> {
        // Delegate to the existing inherent method so debug-mode
        // arg verification + the `fcall_timeout_ms` timeout both
        // fire exactly once.
        FabricRuntime::fcall(self, function, keys, args).await
    }
}

/// #743: reshape FF's seed_waitpoint_hmac_secret kid-mismatch error
/// into operator-actionable guidance.
///
/// Stock FF error: `validation: InvalidInput: seed_waitpoint_hmac_secret:
/// stored current_kid \"x\" differs from supplied kid \"y\"; use
/// rotate_waitpoint_hmac_secret_all to change kid`.
///
/// That tells an operator (a) the wrong env var is set OR (b) the
/// Valkey state was carried over from a prior boot — but the only
/// recovery hint is an FF-internal FCALL name. Cairn surfaces it as
/// a `FabricError::Config` with the actual kid + remediation.
///
/// Other FF errors pass through unchanged as `FabricError::Engine`.
fn reshape_seed_error(
    err: flowfabric::core::engine_error::EngineError,
    supplied_kid: &str,
) -> crate::error::FabricError {
    use flowfabric::core::engine_error::{EngineError, ValidationKind};

    if let EngineError::Validation {
        kind: ValidationKind::InvalidInput,
        detail,
    } = &err
    {
        if detail.contains("differs from supplied kid") {
            // Try to pull the stored kid out of the FF detail string.
            // Format: `... stored current_kid "X" differs from ...`
            // Match on the surrounding literal so cairn's wrapper
            // stays robust to incidental wording changes; if the
            // exact `"X"` extraction fails, we still surface a useful
            // message naming the supplied kid.
            let stored = detail
                .split("stored current_kid ")
                .nth(1)
                .and_then(|s| s.split(" differs").next())
                .unwrap_or("<unknown>")
                .trim_matches('"');
            return crate::error::FabricError::Config(format!(
                "waitpoint HMAC kid mismatch on boot: Valkey has \
                 current_kid={stored:?}, env supplies CAIRN_FABRIC_WAITPOINT_HMAC_KID={supplied_kid:?}.\n\
                 \n\
                 This typically happens after one of:\n\
                 \n\
                 (a) the kid env var was rotated but Valkey was not flushed,\n\
                 (b) Valkey state was restored from a snapshot keyed under \
                     a different kid, or\n\
                 (c) two cairn deployments are sharing the same Valkey but \
                     advertising different kids.\n\
                 \n\
                 To resolve:\n\
                 \n\
                 - Set CAIRN_FABRIC_WAITPOINT_HMAC_KID={stored} (matching the \
                   stored kid) AND supply the original secret bytes to keep \
                   existing waitpoint tokens valid, OR\n\
                 - Operator-pace a rotate via the FF \
                   `rotate_waitpoint_hmac_secret_all` admin path (drains live \
                   tokens with a grace window), OR\n\
                 - For a destructive reset (acceptable on dev / fresh deploys \
                   only), FLUSHDB the Valkey backend and restart cairn-app."
            ));
        }
    }
    crate::error::FabricError::Engine(Box::new(err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flowfabric::core::engine_error::{EngineError, ValidationKind};

    #[test]
    fn reshape_seed_error_translates_kid_mismatch_to_actionable_config_error() {
        let err = EngineError::Validation {
            kind: ValidationKind::InvalidInput,
            detail: "seed_waitpoint_hmac_secret: stored current_kid \"dogfood-r6\" \
                     differs from supplied kid \"k1\"; use \
                     rotate_waitpoint_hmac_secret_all to change kid"
                .to_owned(),
        };
        let reshaped = reshape_seed_error(err, "k1");
        let msg = reshaped.to_string();
        // Names BOTH kid values so the operator can do something
        // about it without re-grepping FF source.
        assert!(msg.contains("dogfood-r6"), "must surface stored kid: {msg}");
        assert!(msg.contains("\"k1\""), "must surface supplied kid: {msg}");
        // Names the env var the operator must touch — not the
        // FF-internal FCALL name.
        assert!(
            msg.contains("CAIRN_FABRIC_WAITPOINT_HMAC_KID"),
            "must name env var: {msg}"
        );
        // Lists at least one concrete recovery action.
        assert!(
            msg.contains("FLUSHDB") || msg.contains("rotate") || msg.contains("matching"),
            "must include remediation: {msg}"
        );
    }

    #[test]
    fn reshape_seed_error_passes_other_validation_errors_through_unchanged() {
        let err = EngineError::Validation {
            kind: ValidationKind::InvalidInput,
            detail: "some unrelated validation error".to_owned(),
        };
        let reshaped = reshape_seed_error(err, "k1");
        // Pass-through wraps in FabricError::Engine, NOT
        // FabricError::Config.
        match reshaped {
            crate::error::FabricError::Engine(_) => {}
            other => panic!("expected Engine pass-through, got {other:?}"),
        }
    }

    #[test]
    fn reshape_seed_error_passes_non_validation_errors_through_unchanged() {
        let err = EngineError::Unavailable {
            op: "seed_waitpoint_hmac_secret",
        };
        let reshaped = reshape_seed_error(err, "k1");
        match reshaped {
            crate::error::FabricError::Engine(_) => {}
            other => panic!("expected Engine pass-through, got {other:?}"),
        }
    }

    /// Defensive: even if FF subtly changes the detail wording so
    /// the kid-extraction regex misses, the catch-all still surfaces
    /// a usable message naming the supplied kid + env var.
    #[test]
    fn reshape_seed_error_handles_unparseable_detail_gracefully() {
        let err = EngineError::Validation {
            kind: ValidationKind::InvalidInput,
            detail: "differs from supplied kid (no quotes)".to_owned(),
        };
        let reshaped = reshape_seed_error(err, "k1");
        let msg = reshaped.to_string();
        assert!(
            msg.contains("CAIRN_FABRIC_WAITPOINT_HMAC_KID"),
            "must still name env var even when FF detail wording changes: {msg}"
        );
    }
}
