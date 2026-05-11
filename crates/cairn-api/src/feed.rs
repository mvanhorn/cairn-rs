//! Feed endpoint boundaries per preserved route catalog.
//!
//! The trait + DTO contracts moved to `cairn-api-contracts` in #440 so
//! cairn-memory can implement them without inverting the layer
//! ordering. cairn-api re-exports them at the original module paths
//! so existing callers continue to resolve `cairn_api::feed::…`
//! unchanged.

pub use cairn_api_contracts::feed::{FeedEndpoints, FeedItem, FeedQuery};
