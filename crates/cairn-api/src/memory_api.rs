//! Memory endpoint boundaries per preserved route catalog.
//!
//! The trait + DTO contracts moved to `cairn-api-contracts` in #440 so
//! cairn-memory can implement them without inverting the layer
//! ordering. cairn-api re-exports them at the original module paths
//! so existing callers continue to resolve `cairn_api::memory_api::…`
//! unchanged.

pub use cairn_api_contracts::memory_api::{
    AddDocumentToCorpusRequest, AddSourceTagsRequest, CorpusEndpoints, CorpusRecord,
    CreateCorpusRequest, CreateMemoryRequest, MemoryEndpoints, MemoryItem, MemorySearchQuery,
    MemoryStatus, SourceTagsEndpoints, SourceTagsResponse,
};
