//! HTTP endpoint contracts shared between cairn-api (consumer) and
//! downstream implementor crates such as cairn-memory.
//!
//! Holds *only* pure types: request/response DTOs, query shapes, and
//! endpoint traits. No IO, no store access, no runtime wiring — that
//! keeps this crate at or below the `cairn-domain`/`cairn-store` tier
//! and lets implementors depend on it without pulling the upper-layer
//! `cairn-api` crate (closes #440).
//!
//! # Layering rule
//!
//! Per `CLAUDE.md`:
//! `domain -> store -> runtime -> {memory, graph, evals, tools, agent,
//! signal, channels} -> api/plugin-proto -> app`
//!
//! cairn-api sits above cairn-memory. Before this crate existed,
//! cairn-memory had a production dependency on cairn-api because the
//! `MemoryEndpoints` / `FeedEndpoints` traits plus their DTOs
//! (`MemoryItem`, `ListQuery`, `ListResponse`, …) lived in cairn-api.
//! That inverted the layer ordering and left a latent cycle risk: any
//! non-dev `use cairn_memory::…` in cairn-api would have closed the
//! loop.
//!
//! This crate absorbs the minimal contract surface so both sides can
//! depend on it unidirectionally. cairn-api re-exports these symbols at
//! their original paths to preserve existing external callers without
//! breaking changes.

pub mod endpoints;
pub mod feed;
pub mod http;
pub mod memory_api;

pub use endpoints::ListQuery;
pub use feed::{FeedEndpoints, FeedItem, FeedQuery};
pub use http::{ApiError, HealthResponse, ListResponse, OkResponse};
pub use memory_api::{
    AddDocumentToCorpusRequest, AddSourceTagsRequest, CorpusEndpoints, CorpusRecord,
    CreateCorpusRequest, CreateMemoryRequest, MemoryEndpoints, MemoryItem, MemorySearchQuery,
    MemoryStatus, SourceTagsEndpoints, SourceTagsResponse,
};
