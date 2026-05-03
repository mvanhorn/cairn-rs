use std::collections::BTreeSet;

use crate::error::FabricError;
use flowfabric::core::backend::{BackendConfig, BackendConnection, ValkeyConnection};
use flowfabric::core::types::{LaneId, Namespace, WorkerId, WorkerInstanceId};

#[derive(Clone, Debug)]
pub struct FabricConfig {
    /// Backend connection config. Single source of truth for the
    /// Valkey host/port/TLS/cluster knobs (replaces the four flat
    /// fields that pre-dated FF's `BackendConfig` reshape). Populated
    /// from the `CAIRN_FABRIC_URL` env var via [`Self::from_env`] —
    /// see [`parse_fabric_url`] for the scheme table.
    pub backend: BackendConfig,
    pub lane_id: LaneId,
    pub worker_id: WorkerId,
    pub worker_instance_id: WorkerInstanceId,
    pub namespace: Namespace,
    /// Lease TTL in milliseconds applied to every FF execution lease
    /// cairn claims. Overridable via `CAIRN_FABRIC_LEASE_TTL_MS`.
    ///
    /// **Default: `180_000` (3 minutes).**
    ///
    /// # Tradeoff
    ///
    /// * **Shorter TTL** → faster stuck-run recovery. When a worker
    ///   genuinely crashes, the lease expires sooner and FF's
    ///   `LeaseExpiryScanner` promotes the execution back to
    ///   `eligible` for re-claim. Recovery latency ≈ TTL.
    /// * **Longer TTL** → fewer spurious `lease_expired` failures on
    ///   pull-mode drivers. `POST /v1/runs/:id/orchestrate` runs one
    ///   iteration per HTTP call and returns; between calls nobody
    ///   renews the lease. Operator-paced approval flows + LLM tail
    ///   latency + tool execution routinely exceed short TTLs.
    ///
    /// # Why 180_000 (3 min)
    ///
    /// F63 dogfood (2026-04-27) on the F62 binary showed the previous
    /// `30_000` default routinely expired between orchestrate calls:
    /// LLM tail (~30 s) + operator approval think-time (~60 s) + tool
    /// exec (~30 s) easily exceeded 30 s. Every expiry tripped F62's
    /// `TerminalWriteDeadlock` path and lost the LLM's productive work.
    ///
    /// `180_000` covers the typical end-to-end iteration (LLM plus
    /// human plus tools) with headroom, while keeping recovery latency
    /// on the rare actual-crash path bounded (3 min vs the 600 s
    /// workaround previously reverted in F43 triage — that one 20×'d
    /// zombie recovery and bloated the `worker_leases` index).
    ///
    /// FF's dual-door deadlock (the upstream root cause this default
    /// mitigates) is tracked at
    /// <https://github.com/avifenesh/FlowFabric/issues/371>. Once FF
    /// ships the fix we can revisit this default downward.
    pub lease_ttl_ms: u64,
    pub grant_ttl_ms: u64,
    pub max_concurrent_tasks: usize,
    pub signal_dedup_ttl_ms: u64,
    pub fcall_timeout_ms: u64,
    /// Capabilities this worker advertises. Threaded into FF's
    /// `ff_issue_claim_grant` via `flowfabric::scheduler::Scheduler::claim_for_worker`;
    /// FF skips executions whose `required_capabilities` are not a subset.
    /// BTreeSet guarantees the CSV FF builds is deterministically ordered.
    /// Empty set = "no capabilities advertised" (FF accepts, matches only
    /// executions that require nothing).
    pub worker_capabilities: BTreeSet<String>,
    /// Hex-encoded 32-byte HMAC secret used to sign waitpoint tokens (RFC-004
    /// §Waitpoint Security). FF mints a token for every waitpoint via
    /// `ff_suspend_execution` and validates it on every `ff_deliver_signal`;
    /// a missing secret causes every suspend to fail with
    /// `hmac_secret_not_initialized`.
    ///
    /// **Security**
    /// - MUST be 32 random bytes (64 hex characters, case-insensitive).
    /// - This secret controls waitpoint signal authentication. Leaking it
    ///   lets an attacker forge signals into any waitpoint on the server —
    ///   approvals, subagent completions, tool results, operator resumes.
    ///   Treat with the same care as a JWT signing key.
    /// - Keep it out of logs. `read_waitpoint_token` + `WaitpointToken`'s
    ///   Debug/Display both redact; the raw secret only lives in this field
    ///   and in Valkey's `hmac_secrets` hash.
    ///
    /// **Rotation** (not in this round) — FF exposes per-kid expiry via
    /// `rotate_waitpoint_hmac_secret` / validate_waitpoint_token's multi-kid
    /// scan. Cairn will wire a rotate endpoint in a later round.
    ///
    /// `None` = no seeding on boot. ff_suspend_execution will fail fast with
    /// the FF-side error code; operators can seed post-boot via the FF
    /// admin path or a dedicated cairn tool.
    pub waitpoint_hmac_secret: Option<String>,
    /// Key identifier paired with `waitpoint_hmac_secret`. Defaults to `"k1"`
    /// when a secret is configured but no kid is supplied. Must be non-empty
    /// when `waitpoint_hmac_secret` is `Some`. Arbitrary operator-chosen
    /// string; FF uses it only as a lookup key in its secrets hash.
    pub waitpoint_hmac_kid: Option<String>,
    /// Which storage backend the FabricServices aggregate should bring
    /// up. Parsed from the `CAIRN_FABRIC_BACKEND` env var by
    /// [`Self::from_env`] (default: [`BackendKind::Valkey`]).
    ///
    /// Always-compiled (not behind any Cargo feature) so the field is
    /// visible to operators regardless of which backend features the
    /// binary was built with. A mismatch between the requested kind
    /// and the enabled Cargo features is caught in
    /// [`Self::validate`] with an actionable error message.
    ///
    /// PR-C3 lands the field + parser; the runtime dispatch arm that
    /// consumes it (`FabricServices::start` matching on `backend_kind`)
    /// stubs the Postgres branch until PR-C4 wires
    /// `PostgresFabricRuntime::start`.
    pub backend_kind: BackendKind,
}

/// Which storage backend cairn-fabric should bring up.
///
/// Orthogonal to the [`BackendConnection`] carried by
/// [`FabricConfig::backend`]: `BackendConnection` describes the *wire*
/// (Valkey host/port, Postgres URL) while `BackendKind` selects which
/// cairn-fabric runtime (valkey / postgres) dispatches the services
/// on top. Today they agree 1:1 (Valkey wire → Valkey runtime), but
/// the separate enum lets PR-C4 wire a Postgres runtime without
/// refactoring the wire-config enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    Valkey,
    Postgres,
}

impl BackendKind {
    /// Human-readable token used in env-var parsing and error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Valkey => "valkey",
            Self::Postgres => "postgres",
        }
    }
}

impl std::str::FromStr for BackendKind {
    type Err = FabricError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Accept lowercase ASCII only. The env-var parser in
        // `FabricConfig::from_env` trims + lowercases before calling us.
        match s {
            "valkey" => Ok(Self::Valkey),
            "postgres" => Ok(Self::Postgres),
            other => Err(FabricError::Config(format!(
                "CAIRN_FABRIC_BACKEND must be one of [valkey, postgres], got '{other}'"
            ))),
        }
    }
}

impl FabricConfig {
    pub fn from_env() -> Result<Self, FabricError> {
        // Parse `CAIRN_FABRIC_URL`; if unset, fall back to the default
        // Valkey endpoint (`valkey://localhost:6379`). There is no
        // legacy env-var fallback — `CAIRN_FABRIC_HOST/PORT/TLS/CLUSTER`
        // were removed when cairn-rs migrated to `BackendConfig`.
        let backend = match std::env::var("CAIRN_FABRIC_URL") {
            Ok(url) if !url.is_empty() => parse_fabric_url(&url)?,
            _ => BackendConfig::valkey("localhost", 6379),
        };

        let lane_id =
            LaneId::new(std::env::var("CAIRN_FABRIC_LANE").unwrap_or_else(|_| "cairn".into()));
        let worker_id = WorkerId::new(
            std::env::var("CAIRN_FABRIC_WORKER_ID").unwrap_or_else(|_| "cairn-worker".into()),
        );
        let worker_instance_id = WorkerInstanceId::new(
            std::env::var("CAIRN_FABRIC_INSTANCE_ID")
                .unwrap_or_else(|_| load_or_generate_instance_id()),
        );
        let namespace = Namespace::new(
            std::env::var("CAIRN_FABRIC_NAMESPACE").unwrap_or_else(|_| "cairn".into()),
        );
        // Default 180_000 (3 min) — see `lease_ttl_ms` field docs for
        // the F63 rationale and the upstream FF#371 cross-reference.
        let lease_ttl_ms = std::env::var("CAIRN_FABRIC_LEASE_TTL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(180_000);
        let grant_ttl_ms = std::env::var("CAIRN_FABRIC_GRANT_TTL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5_000);
        let max_concurrent_tasks = std::env::var("CAIRN_FABRIC_MAX_TASKS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4);
        let signal_dedup_ttl_ms = std::env::var("CAIRN_FABRIC_SIGNAL_DEDUP_TTL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(86_400_000);
        let fcall_timeout_ms = std::env::var("CAIRN_FABRIC_FCALL_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5_000);
        // Comma-separated capability tokens. Empty / unset = no capabilities.
        // FF validates tokens server-side (no commas, no whitespace/control,
        // CAPS_MAX_TOKENS cap); fail-loud validation lives in
        // flowfabric::scheduler::Scheduler::claim_for_worker.
        let worker_capabilities: BTreeSet<String> =
            std::env::var("CAIRN_FABRIC_WORKER_CAPABILITIES")
                .ok()
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_owned())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default();
        // HMAC secret: hex-encoded 32-byte key. No default — operators must
        // opt in explicitly. Validation enforces shape in `validate()`.
        let waitpoint_hmac_secret = std::env::var("CAIRN_FABRIC_WAITPOINT_HMAC_SECRET")
            .ok()
            .filter(|s| !s.is_empty());
        let waitpoint_hmac_kid = std::env::var("CAIRN_FABRIC_WAITPOINT_HMAC_KID")
            .ok()
            .filter(|s| !s.is_empty());

        // `CAIRN_FABRIC_BACKEND` (optional). Default to Valkey for
        // backwards compat with every deployment predating PR-C3.
        // Whitespace trimmed, ASCII-lowercased, then parsed.
        let backend_kind = match std::env::var("CAIRN_FABRIC_BACKEND") {
            Ok(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    BackendKind::Valkey
                } else {
                    trimmed.to_ascii_lowercase().parse::<BackendKind>()?
                }
            }
            Err(_) => BackendKind::Valkey,
        };

        let config = Self {
            backend,
            lane_id,
            worker_id,
            worker_instance_id,
            namespace,
            lease_ttl_ms,
            grant_ttl_ms,
            max_concurrent_tasks,
            signal_dedup_ttl_ms,
            fcall_timeout_ms,
            worker_capabilities,
            waitpoint_hmac_secret,
            waitpoint_hmac_kid,
            backend_kind,
        };
        config.validate()?;
        Ok(config)
    }

    /// Borrow the `ValkeyConnection` out of `backend.connection`, or
    /// return a typed error when the backend is non-Valkey. Used by
    /// the host/port-shaped log lines in `FabricRuntime::start` and
    /// the ferriskey client-builder construction below.
    pub fn valkey_connection(&self) -> Result<&ValkeyConnection, FabricError> {
        match &self.backend.connection {
            BackendConnection::Valkey(vk) => Ok(vk),
            // Format only the backend *kind* — a `{other:?}` dump
            // would splice the full `BackendConnection::Postgres`
            // value (including the Postgres connection URL) into the
            // error string and thence into boot logs.
            other => Err(FabricError::Config(format!(
                "expected Valkey backend, got {}",
                backend_kind(other)
            ))),
        }
    }

    /// Resolve the HMAC kid to seed with, falling back to `"k1"` when the
    /// operator sets a secret without specifying a kid. Returns `None` if
    /// no secret is configured (no seeding will run).
    pub fn resolved_waitpoint_hmac_kid(&self) -> Option<&str> {
        self.waitpoint_hmac_secret.as_ref()?;
        Some(
            self.waitpoint_hmac_kid
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("k1"),
        )
    }

    pub fn validate(&self) -> Result<(), FabricError> {
        // Cross-check the backend-kind selector against the wire-config
        // variant. An operator who sets `CAIRN_FABRIC_BACKEND=postgres`
        // but leaves `CAIRN_FABRIC_URL=valkey://...` (or vice versa) is
        // in an impossible state — the runtime-dispatch arm selects a
        // runtime that can't consume the configured connection. Fail
        // loud here with an actionable message pointing at both env vars.
        // (Copilot review, PR #600.)
        //
        // `BackendConnection` is `#[non_exhaustive]` upstream — we
        // deliberately list every known variant so a future FF addition
        // fails the build rather than silently defaulting.
        match (self.backend_kind, &self.backend.connection) {
            (BackendKind::Valkey, BackendConnection::Valkey(_)) => {}
            (BackendKind::Postgres, BackendConnection::Postgres(_)) => {}
            (kind, conn) => {
                return Err(FabricError::Config(format!(
                    "CAIRN_FABRIC_BACKEND={} does not match CAIRN_FABRIC_URL scheme \
                     (resolved to {}). Align the two env vars: either set \
                     CAIRN_FABRIC_BACKEND={} to match the URL, or reset \
                     CAIRN_FABRIC_URL to a {} scheme \
                     (e.g. `valkey://localhost:6379` or `postgres://user:pw@host/db`).",
                    kind.as_str(),
                    backend_kind(conn),
                    backend_kind(conn),
                    kind.as_str(),
                )));
            }
        }

        // Cross-check the backend-kind selector against the compiled
        // Cargo features. Fails loud at boot with an actionable message
        // when an operator sets `CAIRN_FABRIC_BACKEND=postgres` on a
        // binary built without the `fabric-postgres` feature (or the
        // symmetric case). The runtime dispatch arm in
        // `FabricServices::start` depends on this check to rule out the
        // "feature disabled" case before matching.
        match self.backend_kind {
            BackendKind::Valkey => {
                #[cfg(not(feature = "fabric-valkey"))]
                {
                    return Err(FabricError::Config(
                        "CAIRN_FABRIC_BACKEND=valkey requires the `fabric-valkey` Cargo feature \
                         (enabled by default); this binary was built with \
                         `--no-default-features` and without `--features fabric-valkey`."
                            .into(),
                    ));
                }
            }
            BackendKind::Postgres => {
                #[cfg(not(feature = "fabric-postgres"))]
                {
                    return Err(FabricError::Config(
                        "CAIRN_FABRIC_BACKEND=postgres requires the `fabric-postgres` Cargo \
                         feature; this binary was built without it. Rebuild with \
                         `--features fabric-postgres` (or rely on the default `fabric-valkey` \
                         backend)."
                            .into(),
                    ));
                }
            }
        }
        // `BackendConnection` is `#[non_exhaustive]` upstream — keep a
        // catch-all arm so a future FF variant cairn doesn't know about
        // fails loud at boot instead of silently defaulting.
        match &self.backend.connection {
            BackendConnection::Valkey(vk) => {
                if vk.port == 0 {
                    return Err(FabricError::Config(
                        "CAIRN_FABRIC_URL has port=0; set a non-zero TCP port \
                         (e.g. `valkey://localhost:6379`)."
                            .into(),
                    ));
                }
            }
            BackendConnection::Postgres(pg) => {
                if pg.url.is_empty() {
                    return Err(FabricError::Config(
                        "CAIRN_FABRIC_URL is empty for a Postgres backend; \
                         set it to a `postgres://user:pass@host:5432/db` URL."
                            .into(),
                    ));
                }
            }
            other => {
                return Err(FabricError::Config(format!(
                    "unsupported backend variant: {}",
                    backend_kind(other)
                )));
            }
        }
        if self.lease_ttl_ms < 1000 {
            return Err(FabricError::Config(format!(
                "CAIRN_FABRIC_LEASE_TTL_MS must be >= 1000 (got {}); \
                 set it to a millisecond value of 1000 or greater, \
                 or unset it to accept the 180000 default.",
                self.lease_ttl_ms
            )));
        }
        if self.max_concurrent_tasks < 1 {
            return Err(FabricError::Config(
                "CAIRN_FABRIC_MAX_TASKS must be >= 1; \
                 set it to a positive integer (e.g. `4`) or unset it to \
                 accept the 4 default."
                    .into(),
            ));
        }
        if self.grant_ttl_ms == 0 {
            return Err(FabricError::Config(
                "CAIRN_FABRIC_GRANT_TTL_MS must be > 0; \
                 set it to a positive millisecond value (e.g. `5000`) or \
                 unset it to accept the 5000 default."
                    .into(),
            ));
        }
        if self.fcall_timeout_ms == 0 {
            return Err(FabricError::Config(
                "CAIRN_FABRIC_FCALL_TIMEOUT_MS must be > 0; \
                 set it to a positive millisecond value (e.g. `5000`) or \
                 unset it to accept the 5000 default."
                    .into(),
            ));
        }
        if self.signal_dedup_ttl_ms == 0 {
            return Err(FabricError::Config(
                "CAIRN_FABRIC_SIGNAL_DEDUP_TTL_MS must be > 0; \
                 set it to a positive millisecond value (e.g. `86400000`) or \
                 unset it to accept the 86400000 default."
                    .into(),
            ));
        }
        // HMAC secret: if supplied, MUST be exactly 64 hex chars (256-bit
        // key). Fail loud — a truncated or mis-encoded secret produces an
        // opaque HMAC failure at runtime that's painful to diagnose.
        if let Some(secret) = &self.waitpoint_hmac_secret {
            if secret.len() != 64 {
                return Err(FabricError::Config(format!(
                    "CAIRN_FABRIC_WAITPOINT_HMAC_SECRET must be 64 hex chars (32 bytes), got {}. \
                     Generate a fresh secret with `openssl rand -hex 32`.",
                    secret.len()
                )));
            }
            if !secret.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(FabricError::Config(
                    "CAIRN_FABRIC_WAITPOINT_HMAC_SECRET must be hex-encoded (0-9, a-f, A-F only). \
                     Generate a fresh secret with `openssl rand -hex 32`."
                        .into(),
                ));
            }
            // Kid must be present and non-empty iff secret is set. An empty
            // kid would produce HSET `secret:` (no suffix) which the Lua
            // loader treats as a malformed record.
            if let Some(kid) = &self.waitpoint_hmac_kid {
                if kid.is_empty() {
                    return Err(FabricError::Config(
                        "CAIRN_FABRIC_WAITPOINT_HMAC_KID must not be empty when \
                         CAIRN_FABRIC_WAITPOINT_HMAC_SECRET is set. \
                         Set it to a stable operator-chosen identifier (e.g. `k1`) or \
                         unset it to accept the `k1` default."
                            .into(),
                    ));
                }
                // Defensive: FF Lua builds the field name `secret:<kid>` and
                // `expires_at:<kid>`; a kid containing `:` would split the
                // hash-field parser (ff lua/helpers.lua:180-188). Reject here
                // so operator typos fail loud instead of silently corrupting
                // the validation path.
                if kid.contains(':') {
                    return Err(FabricError::Config(format!(
                        "CAIRN_FABRIC_WAITPOINT_HMAC_KID must not contain ':' \
                         (FF reserves ':' as a hash-field delimiter): {kid:?}. \
                         Pick a colon-free identifier (e.g. `k1`, `k-2026-05`)."
                    )));
                }
            }
        } else if self.waitpoint_hmac_kid.is_some() {
            return Err(FabricError::Config(
                "CAIRN_FABRIC_WAITPOINT_HMAC_KID is set, but \
                 CAIRN_FABRIC_WAITPOINT_HMAC_SECRET is missing. \
                 Generate a 32-byte hex secret with `openssl rand -hex 32` and export it, \
                 or unset CAIRN_FABRIC_WAITPOINT_HMAC_KID."
                    .into(),
            ));
        }
        Ok(())
    }

    /// Construct a ferriskey [`ClientBuilder`] pre-configured with this
    /// fabric's host/port/TLS/cluster settings. Callers call `.build().await`
    /// to get a connected `Client`.
    ///
    /// Only valid for Valkey-backed configs — returns
    /// [`FabricError::Config`] when the backend is not Valkey. A real
    /// backend-agnostic builder lands with the runtime dispatch in PR-C.
    ///
    /// This replaces the previous `valkey_url()` URL-string path. The
    /// `redis://` scheme was redundant (we never parse a URL — we build one
    /// only to hand it back to ferriskey, which re-parses it) and would
    /// break on non-Redis-cloud hosts that reject the `redis` scheme
    /// prefix. The builder accepts a bare host + port and applies TLS as
    /// an explicit flag, matching the ferriskey 0.2 public API.
    ///
    /// Gated behind `fabric-valkey` because ferriskey is not linked
    /// under `--no-default-features`.
    #[cfg(feature = "fabric-valkey")]
    pub fn client_builder(&self) -> Result<ferriskey::ClientBuilder, FabricError> {
        let vk = self.valkey_connection()?;
        let mut builder = ferriskey::ClientBuilder::new().host(&vk.host, vk.port);
        if vk.tls {
            builder = builder.tls();
        }
        if vk.cluster {
            builder = builder.cluster();
        }
        Ok(builder)
    }
}

/// Parse a `CAIRN_FABRIC_URL` value into a [`BackendConfig`].
///
/// Accepted schemes:
///
/// | Scheme     | Mapping                                                         |
/// |------------|-----------------------------------------------------------------|
/// | `valkey://host:port`            | `BackendConfig::valkey(host, port)`  |
/// | `rediss://host:port`            | Valkey + `ValkeyConnection.tls = true` |
/// | `valkey://host:port?tls=1`      | As above + `tls = true`              |
/// | `valkey://host:port?cluster=1`  | As above + `cluster = true`          |
/// | `rediss://host:port?tls=0`      | **Error** — scheme contradicts param |
/// | `valkey://[::1]:6379`           | IPv6, bracketed host preserved (`"[::1]"`) |
/// | anything else                   | `FabricError::Config("unknown fabric URL scheme: ...; expected one of: valkey, rediss")` |
///
/// Defaults: `valkey://host` (no port) → port `6379`. `redis://` is
/// **not** an alias; it was intentionally rejected during PR-A's
/// design review (no legacy users to migrate). `postgres://` URL
/// parsing lands with the Postgres runtime in PR-C.
fn parse_fabric_url(raw: &str) -> Result<BackendConfig, FabricError> {
    // Deliberately do NOT include the raw URL in the parse error —
    // operators can paste Valkey/Postgres URLs that embed credentials
    // (password, ACL user, query-string secrets) and the parse error
    // lands in boot logs. Point at the env var name; operators know
    // what they set.
    let url = url::Url::parse(raw)
        .map_err(|e| FabricError::Config(format!("CAIRN_FABRIC_URL is not a valid URL: {e}")))?;

    // Scheme-match FIRST so an unsupported scheme surfaces the
    // documented `unknown fabric URL scheme` error rather than a
    // query-param complaint (e.g. `http://host?x=1` should fail on
    // `http`, not `x`).
    match url.scheme() {
        "valkey" => {
            let (tls_param, cluster_param) = parse_valkey_query(&url)?;
            let (host, port) = extract_host_port(&url, 6379)?;
            let mut cfg = BackendConfig::valkey(host, port);
            if let BackendConnection::Valkey(ref mut vk) = cfg.connection {
                if let Some(tls) = tls_param {
                    vk.tls = tls;
                }
                if let Some(cluster) = cluster_param {
                    vk.cluster = cluster;
                }
            }
            Ok(cfg)
        }
        "rediss" => {
            let (tls_param, cluster_param) = parse_valkey_query(&url)?;
            // Scheme implies TLS; `?tls=0` contradicts and must fail
            // loud rather than silently honour one or the other.
            if let Some(false) = tls_param {
                return Err(FabricError::Config(
                    "rediss:// scheme contradicts tls=0 query param".into(),
                ));
            }
            let (host, port) = extract_host_port(&url, 6379)?;
            let mut cfg = BackendConfig::valkey(host, port);
            if let BackendConnection::Valkey(ref mut vk) = cfg.connection {
                vk.tls = true;
                if let Some(cluster) = cluster_param {
                    vk.cluster = cluster;
                }
            }
            Ok(cfg)
        }
        other => Err(FabricError::Config(format!(
            "unknown fabric URL scheme: {other}; expected one of: valkey, rediss"
        ))),
    }
}

/// Pull `host` and `port` out of a parsed URL. `url::Url::host_str()`
/// preserves IPv6 brackets (e.g. `"[::1]"`), which matches what FF's
/// `ValkeyConnection` stores and what ferriskey's TCP layer expects
/// for IPv6 endpoints — no re-bracketing required on the cairn side.
///
/// Errors embed a redacted `scheme://host[:port]` shape via
/// [`redact_url_for_error`], never the raw URL, so operators who
/// paste credentials into their Valkey/Postgres URL do not see them
/// echoed into boot logs.
fn extract_host_port(url: &url::Url, default_port: u16) -> Result<(String, u16), FabricError> {
    let host = url
        .host_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            FabricError::Config(format!(
                "fabric URL missing host: {}",
                redact_url_for_error(url)
            ))
        })?
        .to_owned();
    let port = url.port().unwrap_or(default_port);
    if port == 0 {
        return Err(FabricError::Config(format!(
            "fabric URL port must be > 0: {}",
            redact_url_for_error(url)
        )));
    }
    Ok((host, port))
}

/// Decode the `tls` / `cluster` query params, if present. Accepts
/// `1`, `0`, `true`, `false` (case-insensitive). Rejects unknown
/// query params so operator typos (`?tsl=1`) fail loud instead of
/// silently defaulting.
fn parse_valkey_query(url: &url::Url) -> Result<(Option<bool>, Option<bool>), FabricError> {
    let mut tls = None;
    let mut cluster = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "tls" => tls = Some(parse_bool_param("tls", &value)?),
            "cluster" => cluster = Some(parse_bool_param("cluster", &value)?),
            other => {
                return Err(FabricError::Config(format!(
                    "unknown fabric URL query param: {other:?}; expected one of: tls, cluster"
                )));
            }
        }
    }
    Ok((tls, cluster))
}

fn parse_bool_param(name: &str, value: &str) -> Result<bool, FabricError> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        other => Err(FabricError::Config(format!(
            "fabric URL query param {name}={other:?} must be one of: 0, 1, true, false"
        ))),
    }
}

/// Format only the backend *family* for error messages. Critical:
/// never `Debug`-print a `BackendConnection` into a user-facing
/// error, because `BackendConnection::Postgres` carries the
/// connection URL which may embed credentials (user, password,
/// sslpassword in the query string). This helper keeps the error
/// message informative while keeping secrets out of boot logs.
fn backend_kind(conn: &BackendConnection) -> &'static str {
    match conn {
        BackendConnection::Valkey(_) => "Valkey",
        BackendConnection::Postgres(_) => "Postgres",
        // `#[non_exhaustive]` upstream: future additive variants
        // cairn hasn't taught this helper about fall back to a
        // non-leaky placeholder rather than a Debug-print.
        _ => "unknown",
    }
}

/// Redact a parsed URL to `scheme://host[:port]` shape for error
/// messages. Strips userinfo, path, query, and fragment because
/// cairn's fabric URLs are Valkey endpoints whose `host` and `port`
/// are the only fields needed to diagnose a parse/validate failure,
/// and operators may (especially on Postgres URLs in PR-C) embed
/// passwords in those omitted components.
fn redact_url_for_error(url: &url::Url) -> String {
    let host = url.host_str().unwrap_or("<missing-host>");
    match url.port() {
        Some(port) => format!("{scheme}://{host}:{port}", scheme = url.scheme()),
        None => format!("{scheme}://{host}", scheme = url.scheme()),
    }
}

const INSTANCE_ID_FILE: &str = "/tmp/cairn-fabric-instance-id";

fn load_or_generate_instance_id() -> String {
    if let Ok(id) = std::fs::read_to_string(INSTANCE_ID_FILE) {
        let id = id.trim().to_owned();
        if !id.is_empty() {
            return id;
        }
    }
    let id = uuid::Uuid::new_v4().to_string();
    let _ = std::fs::write(INSTANCE_ID_FILE, &id);
    id
}

/// Shared env-var serialisation lock for cairn-fabric tests.
///
/// Both `config::tests` and `aggregate::tests` mutate the same
/// `CAIRN_FABRIC_*` environment variables. Rust's `#[test]` runner
/// multithreads across `#[test]`s in a crate by default, and
/// `std::env::{set_var, remove_var}` is process-global — so two
/// modules holding different private Mutexes will race with each
/// other and the non-guarded module may observe the other's
/// in-flight edits.
///
/// Historic flake: `fabric_config_from_env_defaults` (in
/// `aggregate::tests`) intermittently saw `CAIRN_FABRIC_URL` still
/// set to a non-Valkey scheme because a concurrent `config::tests`
/// case was in the middle of an `set_var` → `from_env` → `remove_var`
/// sequence. Sharing this lock at the crate level forces serial
/// access to the env across the whole cairn-fabric test binary.
#[cfg(test)]
pub(crate) static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::ENV_LOCK;
    use super::*;

    fn clear_fabric_env() {
        // `CAIRN_FABRIC_HOST/PORT/TLS/CLUSTER` were removed when cairn-rs
        // migrated to `CAIRN_FABRIC_URL`. Clear anyway so a stale value
        // in the test runner's env can't leak into the no-URL default
        // path (the implementation ignores them, but this keeps the
        // tests hermetic against developer `.env` files).
        for key in [
            "CAIRN_FABRIC_URL",
            "CAIRN_FABRIC_HOST",
            "CAIRN_FABRIC_PORT",
            "CAIRN_FABRIC_TLS",
            "CAIRN_FABRIC_CLUSTER",
            "CAIRN_FABRIC_LANE",
            "CAIRN_FABRIC_LEASE_TTL_MS",
            "CAIRN_FABRIC_MAX_TASKS",
            "CAIRN_FABRIC_GRANT_TTL_MS",
            "CAIRN_FABRIC_BACKEND",
        ] {
            std::env::remove_var(key);
        }
    }

    fn assert_valkey(cfg: &FabricConfig, host: &str, port: u16, tls: bool, cluster: bool) {
        let vk = cfg.valkey_connection().expect("valkey backend");
        assert_eq!(vk.host, host);
        assert_eq!(vk.port, port);
        assert_eq!(vk.tls, tls);
        assert_eq!(vk.cluster, cluster);
    }

    #[test]
    fn default_config_from_env_when_url_unset() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_fabric_env();

        let config = FabricConfig::from_env().unwrap();
        assert_valkey(&config, "localhost", 6379, false, false);
        assert_eq!(config.lane_id.as_str(), "cairn");
        assert_eq!(config.lease_ttl_ms, 180_000);
        assert_eq!(config.max_concurrent_tasks, 4);
    }

    #[test]
    fn cairn_fabric_url_valkey_scheme_populates_backend() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_fabric_env();
        std::env::set_var("CAIRN_FABRIC_URL", "valkey://some-host:7001");

        let config = FabricConfig::from_env().unwrap();
        assert_valkey(&config, "some-host", 7001, false, false);

        std::env::remove_var("CAIRN_FABRIC_URL");
    }

    #[test]
    fn cairn_fabric_url_empty_string_falls_back_to_default() {
        // Matches cairn's existing `.filter(|s| !s.is_empty())`
        // convention elsewhere in bootstrap: an operator who
        // `export CAIRN_FABRIC_URL=` shouldn't hit a parse error
        // downstream of a blank-string URL.
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_fabric_env();
        std::env::set_var("CAIRN_FABRIC_URL", "");

        let config = FabricConfig::from_env().unwrap();
        assert_valkey(&config, "localhost", 6379, false, false);

        std::env::remove_var("CAIRN_FABRIC_URL");
    }

    // ── CAIRN_FABRIC_BACKEND parser unit tests (PR-C3) ──────────────────

    #[test]
    fn backend_kind_defaults_to_valkey_when_env_unset() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_fabric_env();

        let config = FabricConfig::from_env().unwrap();
        assert_eq!(config.backend_kind, BackendKind::Valkey);
    }

    #[test]
    fn backend_kind_empty_string_falls_back_to_valkey() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_fabric_env();
        std::env::set_var("CAIRN_FABRIC_BACKEND", "");

        let config = FabricConfig::from_env().unwrap();
        assert_eq!(config.backend_kind, BackendKind::Valkey);

        std::env::remove_var("CAIRN_FABRIC_BACKEND");
    }

    #[test]
    fn backend_kind_accepts_valkey_explicit() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_fabric_env();
        std::env::set_var("CAIRN_FABRIC_BACKEND", "valkey");

        let config = FabricConfig::from_env().unwrap();
        assert_eq!(config.backend_kind, BackendKind::Valkey);

        std::env::remove_var("CAIRN_FABRIC_BACKEND");
    }

    // Note on the "postgres" case: when the `fabric-postgres` Cargo
    // feature is off (default-features-only test run) `validate()`
    // rejects `backend_kind = Postgres` with an actionable error.
    // Built via struct-literal because `parse_fabric_url` today only
    // accepts `valkey` / `rediss` schemes, so the env path cannot
    // produce this state (PR-C4 extends the URL parser). The
    // `base_config()` helper already wires a matching `Postgres`
    // BackendConnection via `BackendConfig::postgres(...)` so the
    // wire-mismatch guard passes and the feature-gate arm is reached.
    #[cfg(not(feature = "fabric-postgres"))]
    #[test]
    fn backend_kind_postgres_without_feature_fails_validate() {
        let mut cfg = base_config();
        cfg.backend = BackendConfig::postgres("postgres://u:p@h/db");
        cfg.backend_kind = BackendKind::Postgres;

        let err = cfg.validate().expect_err("validate must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("fabric-postgres") && msg.contains("CAIRN_FABRIC_BACKEND=postgres"),
            "expected feature-gate error pointing at fabric-postgres, got: {msg}"
        );
    }

    #[test]
    fn backend_kind_rejects_unknown_token() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_fabric_env();
        std::env::set_var("CAIRN_FABRIC_BACKEND", "sqlite");

        let err = FabricConfig::from_env().expect_err("unknown token must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("CAIRN_FABRIC_BACKEND") && msg.contains("sqlite"),
            "expected actionable error naming the env var + offending token, got: {msg}"
        );

        std::env::remove_var("CAIRN_FABRIC_BACKEND");
    }

    #[test]
    fn backend_kind_trims_whitespace_and_lowercases() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_fabric_env();
        std::env::set_var("CAIRN_FABRIC_BACKEND", "  VALKEY  ");

        let config = FabricConfig::from_env().unwrap();
        assert_eq!(config.backend_kind, BackendKind::Valkey);

        std::env::remove_var("CAIRN_FABRIC_BACKEND");
    }

    #[test]
    fn backend_kind_mismatch_with_wire_config_is_rejected() {
        // Covers the validate() cross-check: `backend_kind = Valkey`
        // paired with a Postgres-variant `BackendConnection` must fail
        // loud rather than dispatching to the Valkey runtime against
        // a PG connection.
        //
        // Built via struct-literal rather than `from_env` because
        // today's `parse_fabric_url` only accepts `valkey` / `rediss`
        // schemes — so an operator cannot actually produce
        // `BackendConnection::Postgres` through the env path. That
        // extension lands in PR-C4 when the URL parser grows a
        // `postgres://` arm; until then the struct-literal path (tests
        // + any caller that builds `FabricConfig` by hand) is where
        // this invariant matters.
        let mut cfg = base_config();
        cfg.backend = BackendConfig::postgres("postgres://u:p@h/db");
        // Leave backend_kind as Valkey (from base_config) — this is
        // the wire-mismatch we're asserting against.
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("CAIRN_FABRIC_BACKEND") && err.contains("CAIRN_FABRIC_URL"),
            "expected actionable error naming both env vars, got: {err}"
        );
    }

    // ── URL parser unit tests ────────────────────────────────────────────

    #[test]
    fn parse_valkey_url_basic() {
        let cfg = parse_fabric_url("valkey://example.com:7000").unwrap();
        match cfg.connection {
            BackendConnection::Valkey(vk) => {
                assert_eq!(vk.host, "example.com");
                assert_eq!(vk.port, 7000);
                assert!(!vk.tls);
                assert!(!vk.cluster);
            }
            other => panic!("expected Valkey, got {other:?}"),
        }
    }

    #[test]
    fn parse_valkey_url_default_port() {
        let cfg = parse_fabric_url("valkey://some-host").unwrap();
        match cfg.connection {
            BackendConnection::Valkey(vk) => {
                assert_eq!(vk.host, "some-host");
                assert_eq!(vk.port, 6379, "missing-port defaults to 6379");
            }
            other => panic!("expected Valkey, got {other:?}"),
        }
    }

    #[test]
    fn parse_rediss_url_implies_tls() {
        let cfg = parse_fabric_url("rediss://secure.host:6380").unwrap();
        match cfg.connection {
            BackendConnection::Valkey(vk) => {
                assert!(vk.tls, "rediss:// scheme must set tls=true");
                assert_eq!(vk.host, "secure.host");
                assert_eq!(vk.port, 6380);
            }
            other => panic!("expected Valkey, got {other:?}"),
        }
    }

    #[test]
    fn parse_url_with_tls_query_param() {
        let cfg = parse_fabric_url("valkey://h:6379?tls=1").unwrap();
        match cfg.connection {
            BackendConnection::Valkey(vk) => assert!(vk.tls),
            other => panic!("expected Valkey, got {other:?}"),
        }
    }

    #[test]
    fn parse_url_with_cluster_query_param() {
        let cfg = parse_fabric_url("valkey://h:6379?cluster=true").unwrap();
        match cfg.connection {
            BackendConnection::Valkey(vk) => {
                assert!(vk.cluster);
                assert!(!vk.tls);
            }
            other => panic!("expected Valkey, got {other:?}"),
        }
    }

    #[test]
    fn parse_url_with_tls_and_cluster_query_params() {
        let cfg = parse_fabric_url("valkey://h:6379?tls=1&cluster=1").unwrap();
        match cfg.connection {
            BackendConnection::Valkey(vk) => {
                assert!(vk.tls);
                assert!(vk.cluster);
            }
            other => panic!("expected Valkey, got {other:?}"),
        }
    }

    #[test]
    fn parse_rediss_with_tls_0_errors() {
        let err = parse_fabric_url("rediss://h:6379?tls=0")
            .expect_err("rediss + tls=0 must fail")
            .to_string();
        assert!(
            err.contains("rediss:// scheme contradicts tls=0"),
            "expected contradiction error, got: {err}"
        );
    }

    #[test]
    fn parse_rediss_with_explicit_tls_1_is_fine() {
        // Redundant but not contradictory — operators are allowed to be
        // explicit even when the scheme already implies TLS.
        let cfg = parse_fabric_url("rediss://h:6379?tls=1").unwrap();
        match cfg.connection {
            BackendConnection::Valkey(vk) => assert!(vk.tls),
            other => panic!("expected Valkey, got {other:?}"),
        }
    }

    // Guard for the decision locked on 2026-04-27: `redis://` is NOT
    // an alias. Operators must use `valkey://` or `rediss://`.
    #[test]
    fn redis_scheme_rejected_with_named_error() {
        let err = parse_fabric_url("redis://h:6379")
            .expect_err("redis:// must not be accepted")
            .to_string();
        assert!(
            err.contains("unknown fabric URL scheme: redis") && err.contains("valkey, rediss"),
            "expected named unknown-scheme error, got: {err}"
        );
    }

    #[test]
    fn postgres_scheme_rejected_on_pr_a() {
        // PR-C wires the postgres runtime; PR-A rejects at parse time.
        let err = parse_fabric_url("postgres://u:p@h:5432/db")
            .expect_err("postgres:// not accepted on PR-A")
            .to_string();
        assert!(
            err.contains("unknown fabric URL scheme: postgres"),
            "expected unknown-scheme error, got: {err}"
        );
    }

    #[test]
    fn other_schemes_rejected_with_named_error() {
        for bad in &[
            "http://example.com",
            "mysql://u:p@h/db",
            "ftp://archive.local",
        ] {
            let err = parse_fabric_url(bad)
                .expect_err("scheme must not be accepted")
                .to_string();
            assert!(
                err.contains("unknown fabric URL scheme"),
                "expected named error for {bad}, got: {err}"
            );
        }
    }

    #[test]
    fn unknown_scheme_with_query_params_errors_on_scheme_not_query() {
        // Regression guard: parse_fabric_url must match on scheme
        // BEFORE consulting query params, otherwise an unsupported
        // scheme with garbage query params surfaces the wrong error
        // (query-param complaint instead of scheme rejection).
        let err = parse_fabric_url("http://example.com?tsl=1")
            .expect_err("http:// must fail on scheme")
            .to_string();
        assert!(
            err.contains("unknown fabric URL scheme"),
            "expected scheme error before query-param error, got: {err}"
        );
        assert!(
            !err.contains("unknown fabric URL query param"),
            "scheme error must pre-empt query error, got: {err}"
        );
    }

    #[test]
    fn parse_error_does_not_echo_raw_url() {
        // Security guard: operators may paste a Valkey URL embedding
        // an ACL password (e.g. `valkey://user:pw@host:6379`). The
        // parse error must NOT splice the raw input into its message,
        // because the error lands in boot logs. The URL crate accepts
        // userinfo on valkey://, so we exercise the malformed branch.
        let secret = "super-secret-password-12345";
        let url = format!("::malformed//user:{secret}@host:6379");
        let err = parse_fabric_url(&url)
            .expect_err("malformed URL must not be accepted")
            .to_string();
        assert!(
            !err.contains(secret),
            "parse error must not echo secret into logs, got: {err}"
        );
    }

    #[test]
    fn extract_host_port_error_does_not_echo_userinfo() {
        // A valkey:// URL with userinfo and a missing host should
        // surface the redacted endpoint, not the raw URL (which
        // carries the password).
        let secret = "leak-me-into-logs-43211234";
        // No host between the `@` and the next slash — triggers the
        // missing-host arm in extract_host_port.
        let url = format!("valkey://user:{secret}@/path");
        let err = parse_fabric_url(&url)
            .expect_err("missing-host URL must fail")
            .to_string();
        assert!(
            !err.contains(secret),
            "extract_host_port error must not embed credentials, got: {err}"
        );
    }

    #[test]
    fn valkey_connection_error_does_not_echo_postgres_url() {
        // Postgres URLs embed credentials in userinfo. If an operator
        // configures a Postgres backend and calls `valkey_connection()`,
        // the "expected Valkey backend" error must NOT include the
        // full Postgres URL.
        let mut cfg = base_config();
        let secret = "postgres-password-42";
        let pg_url = format!("postgres://admin:{secret}@dbhost:5432/cairn");
        cfg.backend = BackendConfig::postgres(pg_url.clone());
        let err = cfg.valkey_connection().unwrap_err().to_string();
        assert!(
            !err.contains(secret),
            "valkey_connection error must not leak Postgres creds, got: {err}"
        );
        assert!(
            err.contains("Postgres"),
            "error should name the backend family, got: {err}"
        );
    }

    #[test]
    fn malformed_url_rejected() {
        let err = parse_fabric_url("::not a url::")
            .expect_err("malformed URL must not be accepted")
            .to_string();
        assert!(
            err.contains("not a valid URL"),
            "expected parse error, got: {err}"
        );
    }

    #[test]
    fn ipv6_url_preserves_brackets() {
        // `url::Url::host_str()` keeps the square brackets on IPv6
        // hosts (e.g. `"[::1]"`). FF's `ValkeyConnection.host` stores
        // this bracketed form verbatim; ferriskey's TCP layer expects
        // the bracketed shape for IPv6 endpoints — document by test.
        let cfg = parse_fabric_url("valkey://[::1]:6379").unwrap();
        match cfg.connection {
            BackendConnection::Valkey(vk) => {
                assert_eq!(vk.host, "[::1]", "url::Url preserves IPv6 host shape");
                assert_eq!(vk.port, 6379);
            }
            other => panic!("expected Valkey, got {other:?}"),
        }
    }

    #[test]
    fn ipv6_url_with_tls_query_param() {
        let cfg = parse_fabric_url("valkey://[2001:db8::1]:7001?tls=1").unwrap();
        match cfg.connection {
            BackendConnection::Valkey(vk) => {
                assert_eq!(vk.host, "[2001:db8::1]");
                assert_eq!(vk.port, 7001);
                assert!(vk.tls);
            }
            other => panic!("expected Valkey, got {other:?}"),
        }
    }

    #[test]
    fn rediss_ipv6_sets_tls() {
        let cfg = parse_fabric_url("rediss://[::1]:6380").unwrap();
        match cfg.connection {
            BackendConnection::Valkey(vk) => {
                assert!(vk.tls);
                assert_eq!(vk.host, "[::1]");
                assert_eq!(vk.port, 6380);
            }
            other => panic!("expected Valkey, got {other:?}"),
        }
    }

    #[test]
    fn unknown_query_param_rejected() {
        // Operators who typo `?tsl=1` should fail loud, not silently
        // get default TLS.
        let err = parse_fabric_url("valkey://h:6379?tsl=1")
            .expect_err("unknown query param must fail")
            .to_string();
        assert!(
            err.contains("unknown fabric URL query param"),
            "expected unknown-query-param error, got: {err}"
        );
    }

    #[test]
    fn query_param_invalid_bool_rejected() {
        let err = parse_fabric_url("valkey://h:6379?tls=yes")
            .expect_err("invalid bool must fail")
            .to_string();
        assert!(
            err.contains("must be one of"),
            "expected bool-format error, got: {err}"
        );
    }

    // ── client_builder (renamed from valkey_client_builder) ─────────────

    fn base_config() -> FabricConfig {
        FabricConfig {
            backend: BackendConfig::valkey("localhost", 6379),
            lane_id: LaneId::new("test"),
            worker_id: WorkerId::new("w"),
            worker_instance_id: WorkerInstanceId::new("i"),
            namespace: Namespace::new("ns"),
            lease_ttl_ms: 30_000,
            grant_ttl_ms: 5_000,
            max_concurrent_tasks: 1,
            signal_dedup_ttl_ms: 86_400_000,
            fcall_timeout_ms: 5_000,
            worker_capabilities: BTreeSet::new(),
            waitpoint_hmac_secret: None,
            waitpoint_hmac_kid: None,
            backend_kind: BackendKind::Valkey,
        }
    }

    #[cfg(feature = "fabric-valkey")]
    #[test]
    fn client_builder_without_tls() {
        // ferriskey's `ClientBuilder` does not expose public accessors on
        // its internal `ConnectionRequest`, so we can only assert that the
        // builder constructs without panicking and that `build_lazy()`
        // (the synchronous validation path) accepts the address list.
        // Full wire assertion requires an integration test against a real
        // Valkey instance; those live under `tests/` and in the downstream
        // `cairn-app` integration suite.
        let mut config = base_config();
        config.backend = BackendConfig::valkey("myhost", 6380);
        // build_lazy validates the address list synchronously without
        // establishing a TCP connection — any misconfiguration (empty
        // addresses, bad protocol/push_sender combo) surfaces here.
        assert!(config.client_builder().unwrap().build_lazy().is_ok());
    }

    #[cfg(feature = "fabric-valkey")]
    #[test]
    fn client_builder_with_tls() {
        // Same limitation as `client_builder_without_tls`: no public
        // accessors on `ClientBuilder`/`ConnectionRequest`. We assert
        // synchronous validation passes with TLS toggled on.
        let mut config = base_config();
        let mut cfg = BackendConfig::valkey("secure.host", 6379);
        if let BackendConnection::Valkey(ref mut vk) = cfg.connection {
            vk.tls = true;
        }
        config.backend = cfg;
        assert!(config.client_builder().unwrap().build_lazy().is_ok());
    }

    #[cfg(feature = "fabric-valkey")]
    #[test]
    fn client_builder_build_lazy_rejects_empty_addresses() {
        // Confirms the synchronous validation path we rely on actually
        // catches misconfiguration — otherwise the positive tests above
        // would pass even if `build_lazy()` silently accepted garbage.
        // `client_builder()` always pushes a host, so we build a bare
        // `ClientBuilder` directly to exercise the empty-address
        // rejection branch (see ferriskey ClientBuilder::build_lazy).
        // `LazyClient` does not implement `Debug`, so we can't use
        // `.expect_err(..)`. Match on the result directly.
        match ferriskey::ClientBuilder::new().build_lazy() {
            Ok(_) => panic!("empty-address builder must fail"),
            Err(e) => {
                assert!(
                    e.to_string().to_lowercase().contains("address"),
                    "expected empty-address error, got {e}"
                );
            }
        }
    }

    // ── validate() — backend-shape guard ────────────────────────────────

    fn test_config(
        port: u16,
        lease_ttl_ms: u64,
        max_tasks: usize,
    ) -> Result<FabricConfig, FabricError> {
        let config = FabricConfig {
            backend: BackendConfig::valkey("localhost", port),
            lane_id: LaneId::new("test"),
            worker_id: WorkerId::new("w"),
            worker_instance_id: WorkerInstanceId::new("i"),
            namespace: Namespace::new("ns"),
            lease_ttl_ms,
            grant_ttl_ms: 5_000,
            max_concurrent_tasks: max_tasks,
            signal_dedup_ttl_ms: 86_400_000,
            fcall_timeout_ms: 5_000,
            worker_capabilities: BTreeSet::new(),
            waitpoint_hmac_secret: None,
            waitpoint_hmac_kid: None,
            backend_kind: BackendKind::Valkey,
        };
        config.validate()?;
        Ok(config)
    }

    #[test]
    fn rejects_zero_port() {
        let result = test_config(0, 30_000, 4);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("CAIRN_FABRIC_URL") && err.contains("port=0"),
            "expected env-var-named port error, got: {err}"
        );
    }

    #[test]
    fn rejects_low_lease_ttl() {
        let result = test_config(6379, 500, 4);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("CAIRN_FABRIC_LEASE_TTL_MS"),
            "expected env-var-named lease-ttl error, got: {err}"
        );
    }

    #[test]
    fn rejects_zero_concurrent_tasks() {
        let result = test_config(6379, 30_000, 0);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("CAIRN_FABRIC_MAX_TASKS"),
            "expected env-var-named max-tasks error, got: {err}"
        );
    }

    // Gated on `fabric-postgres` because reaching the "postgres url
    // must not be empty" arm requires `backend_kind = Postgres` (to
    // satisfy the PR-C3 wire-mismatch cross-check) which itself
    // requires the feature (to satisfy the feature-gate cross-check
    // that runs even earlier). Without the feature flag, the test
    // terminates on the feature-gate arm before the empty-URL arm
    // ever fires — not useful signal.
    #[cfg(feature = "fabric-postgres")]
    #[test]
    fn rejects_empty_postgres_url_in_validate() {
        // Smoke-test the Postgres validation arm. Built via struct
        // literal; `parse_fabric_url` today only accepts valkey/rediss
        // schemes, so env-driven construction cannot produce this
        // state (PR-C4 extends the URL parser).
        let mut cfg = base_config();
        cfg.backend = BackendConfig::postgres("");
        cfg.backend_kind = BackendKind::Postgres;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("CAIRN_FABRIC_URL") && err.contains("Postgres backend"),
            "expected env-var-named empty-url error, got: {err}"
        );
    }

    #[cfg(feature = "fabric-valkey")]
    #[test]
    fn client_builder_rejects_non_valkey_backend() {
        // When the backend is not Valkey, `client_builder()` fails
        // loud instead of lying about a ferriskey connection.
        // `ClientBuilder` does not impl Debug, so the `Ok` arm is
        // unreachable by a direct `unwrap_err()` — match explicitly.
        let mut cfg = base_config();
        cfg.backend = BackendConfig::postgres("postgres://u:p@h:5432/db");
        match cfg.client_builder() {
            Ok(_) => panic!("non-Valkey backend must not return a client builder"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("expected Valkey backend"),
                    "expected non-valkey error, got: {msg}"
                );
            }
        }
    }

    // ── HMAC secret validation ────────────────────────────────────────────

    #[test]
    fn hmac_secret_none_validates() {
        let config = base_config();
        assert!(config.validate().is_ok());
        assert_eq!(config.resolved_waitpoint_hmac_kid(), None);
    }

    #[test]
    fn hmac_secret_valid_64_char_hex_validates() {
        let mut config = base_config();
        config.waitpoint_hmac_secret = Some("a".repeat(64));
        assert!(config.validate().is_ok());
        // No kid specified: defaults to "k1".
        assert_eq!(config.resolved_waitpoint_hmac_kid(), Some("k1"));
    }

    #[test]
    fn hmac_secret_explicit_kid_overrides_default() {
        let mut config = base_config();
        config.waitpoint_hmac_secret = Some("0".repeat(64));
        config.waitpoint_hmac_kid = Some("operator-kid-2026-04".into());
        assert!(config.validate().is_ok());
        assert_eq!(
            config.resolved_waitpoint_hmac_kid(),
            Some("operator-kid-2026-04"),
        );
    }

    #[test]
    fn hmac_secret_too_short_errors() {
        let mut config = base_config();
        config.waitpoint_hmac_secret = Some("a".repeat(63));
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("64 hex chars"),
            "expected length error, got {err}"
        );
    }

    #[test]
    fn hmac_secret_too_long_errors() {
        let mut config = base_config();
        config.waitpoint_hmac_secret = Some("a".repeat(65));
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("64 hex chars"),
            "expected length error, got {err}"
        );
    }

    #[test]
    fn hmac_secret_non_hex_errors() {
        let mut config = base_config();
        // Right length, but 'g' is not a hex digit.
        config.waitpoint_hmac_secret = Some("g".repeat(64));
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("hex-encoded"), "expected hex error, got {err}");
    }

    #[test]
    fn hmac_secret_mixed_case_hex_validates() {
        let mut config = base_config();
        // Operators sometimes paste upper-case hex from /dev/urandom tooling.
        config.waitpoint_hmac_secret = Some("AbCdEf0123456789".repeat(4));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn hmac_kid_empty_with_secret_errors() {
        let mut config = base_config();
        config.waitpoint_hmac_secret = Some("a".repeat(64));
        config.waitpoint_hmac_kid = Some(String::new());
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("CAIRN_FABRIC_WAITPOINT_HMAC_KID must not be empty")
                && err.contains("CAIRN_FABRIC_WAITPOINT_HMAC_SECRET is set"),
            "expected env-var-named empty-kid error, got {err}"
        );
    }

    #[test]
    fn hmac_kid_with_colon_errors() {
        // FF Lua builds `secret:<kid>` / `expires_at:<kid>` as hash-field
        // names; a colon in the kid would split the parser.
        let mut config = base_config();
        config.waitpoint_hmac_secret = Some("a".repeat(64));
        config.waitpoint_hmac_kid = Some("bad:kid".into());
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("':'"), "expected delimiter error, got {err}");
    }

    #[test]
    fn hmac_kid_without_secret_errors() {
        // Operator set a kid but forgot the secret: fail loud instead of
        // silently seeding nothing. Message must name the env var the
        // operator touched (issue #631) — the internal field name is
        // ungreppable from the operator's shell.
        let mut config = base_config();
        config.waitpoint_hmac_kid = Some("k1".into());
        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("CAIRN_FABRIC_WAITPOINT_HMAC_KID is set")
                && err.contains("CAIRN_FABRIC_WAITPOINT_HMAC_SECRET is missing"),
            "expected env-var-named missing-secret error, got {err}"
        );
        assert!(
            err.contains("openssl rand -hex 32"),
            "expected remediation hint naming the openssl command, got {err}"
        );
    }

    // ── Issue #631 regression tests: every validate() error names the
    //    env var the operator must set, never the internal field name. ───────

    /// Collect every error message `validate()` can produce for the HMAC
    /// pair, by exercising each failure arm. Used by the regression guards
    /// below to prove no message leaks a Rust field name back to operators.
    fn hmac_validate_error_messages() -> Vec<String> {
        let mut out = Vec::new();

        // kid-without-secret (the exact case from issue #631)
        let mut cfg = base_config();
        cfg.waitpoint_hmac_kid = Some("k1".into());
        out.push(cfg.validate().unwrap_err().to_string());

        // secret too short
        let mut cfg = base_config();
        cfg.waitpoint_hmac_secret = Some("a".repeat(63));
        out.push(cfg.validate().unwrap_err().to_string());

        // secret too long
        let mut cfg = base_config();
        cfg.waitpoint_hmac_secret = Some("a".repeat(65));
        out.push(cfg.validate().unwrap_err().to_string());

        // secret non-hex
        let mut cfg = base_config();
        cfg.waitpoint_hmac_secret = Some("g".repeat(64));
        out.push(cfg.validate().unwrap_err().to_string());

        // kid empty with secret set
        let mut cfg = base_config();
        cfg.waitpoint_hmac_secret = Some("a".repeat(64));
        cfg.waitpoint_hmac_kid = Some(String::new());
        out.push(cfg.validate().unwrap_err().to_string());

        // kid contains colon
        let mut cfg = base_config();
        cfg.waitpoint_hmac_secret = Some("a".repeat(64));
        cfg.waitpoint_hmac_kid = Some("bad:kid".into());
        out.push(cfg.validate().unwrap_err().to_string());

        out
    }

    #[test]
    fn hmac_error_messages_name_env_var_not_field_name() {
        // The original #631 defect: the fatal string spoke "waitpoint_hmac_secret"
        // (the private Rust field) instead of "CAIRN_FABRIC_WAITPOINT_HMAC_SECRET"
        // (the env var the operator controls). Lock both invariants:
        //
        // 1. Every HMAC validation error message must contain at least one of
        //    the real env var names. An operator reading the fatal should be
        //    able to grep their shell config for the symbol named in the log.
        // 2. No HMAC message may leak the lowercase Rust field names
        //    `waitpoint_hmac_secret` / `waitpoint_hmac_kid`. Case-sensitive,
        //    so the uppercased env-var form still passes this guard.
        for msg in hmac_validate_error_messages() {
            assert!(
                msg.contains("CAIRN_FABRIC_WAITPOINT_HMAC_SECRET")
                    || msg.contains("CAIRN_FABRIC_WAITPOINT_HMAC_KID"),
                "HMAC validate error must name the env var, got: {msg}"
            );
            assert!(
                !msg.contains("waitpoint_hmac_secret"),
                "HMAC validate error must not leak the Rust field name \
                 `waitpoint_hmac_secret`, got: {msg}"
            );
            assert!(
                !msg.contains("waitpoint_hmac_kid"),
                "HMAC validate error must not leak the Rust field name \
                 `waitpoint_hmac_kid`, got: {msg}"
            );
        }
    }

    #[test]
    fn hmac_kid_without_secret_has_remediation_hint() {
        // Production-quality fatals point at the fix. The exact case from
        // #631 (kid set, secret missing) must include the openssl command
        // the operator can run, plus the unset-alternative.
        let mut cfg = base_config();
        cfg.waitpoint_hmac_kid = Some("k1".into());
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("openssl rand -hex 32"),
            "expected openssl remediation hint, got: {err}"
        );
        assert!(
            err.contains("unset CAIRN_FABRIC_WAITPOINT_HMAC_KID"),
            "expected unset-alternative in remediation, got: {err}"
        );
    }

    #[test]
    fn hmac_secret_length_error_has_remediation_hint() {
        // A truncated / oversized secret is a common paste error; the fatal
        // must name the command that regenerates a correct one.
        let mut cfg = base_config();
        cfg.waitpoint_hmac_secret = Some("a".repeat(63));
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("openssl rand -hex 32"),
            "expected openssl remediation hint on length error, got: {err}"
        );
    }

    #[test]
    fn hmac_secret_non_hex_error_has_remediation_hint() {
        let mut cfg = base_config();
        cfg.waitpoint_hmac_secret = Some("g".repeat(64));
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("openssl rand -hex 32"),
            "expected openssl remediation hint on non-hex error, got: {err}"
        );
    }

    #[test]
    fn numeric_validate_error_messages_name_env_var() {
        // Sibling audit to the HMAC regression above: the lease / tasks /
        // grant / fcall / signal-dedup guards used the same field-name
        // pattern. Each must name its CAIRN_FABRIC_* env var so operators
        // can act on the fatal without reading the source.

        // lease_ttl_ms < 1000
        let mut cfg = base_config();
        cfg.lease_ttl_ms = 500;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("CAIRN_FABRIC_LEASE_TTL_MS"),
            "lease-ttl error must name CAIRN_FABRIC_LEASE_TTL_MS, got: {err}"
        );

        // max_concurrent_tasks = 0
        let mut cfg = base_config();
        cfg.max_concurrent_tasks = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("CAIRN_FABRIC_MAX_TASKS"),
            "max-tasks error must name CAIRN_FABRIC_MAX_TASKS, got: {err}"
        );

        // grant_ttl_ms = 0
        let mut cfg = base_config();
        cfg.grant_ttl_ms = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("CAIRN_FABRIC_GRANT_TTL_MS"),
            "grant-ttl error must name CAIRN_FABRIC_GRANT_TTL_MS, got: {err}"
        );

        // fcall_timeout_ms = 0
        let mut cfg = base_config();
        cfg.fcall_timeout_ms = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("CAIRN_FABRIC_FCALL_TIMEOUT_MS"),
            "fcall-timeout error must name CAIRN_FABRIC_FCALL_TIMEOUT_MS, got: {err}"
        );

        // signal_dedup_ttl_ms = 0
        let mut cfg = base_config();
        cfg.signal_dedup_ttl_ms = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("CAIRN_FABRIC_SIGNAL_DEDUP_TTL_MS"),
            "signal-dedup error must name CAIRN_FABRIC_SIGNAL_DEDUP_TTL_MS, got: {err}"
        );
    }
}
