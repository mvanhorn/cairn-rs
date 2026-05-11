//! SSE frame-shape sanity for `feed_update` events.
//!
//! Relocated from `cairn-api/tests/feed_wiring.rs` in #440. The other
//! two tests in that file (`list_feed_items`, `mark_read_and_read_all`)
//! exercised `cairn_memory::feed_impl::FeedStore` and moved to
//! `crates/cairn-memory/tests/feed_wiring.rs` so we could drop the
//! cairn-api -> cairn-memory dev-dep. This file keeps the single test
//! that only needs cairn-api's SSE helpers plus a FeedItem literal.

use cairn_api::feed::FeedItem;

#[tokio::test]
async fn feed_update_sse_from_feed_item() {
    let item = FeedItem {
        id: "101".to_owned(),
        source: "slack".to_owned(),
        kind: Some("message".to_owned()),
        title: Some("Item 101".to_owned()),
        body: Some("body".to_owned()),
        url: None,
        author: None,
        avatar_url: None,
        repo_full_name: None,
        is_read: false,
        is_archived: false,
        group_key: None,
        created_at: "2026-04-03T09:30:00Z".to_owned(),
    };

    let frame = cairn_api::sse_payloads::build_feed_update_frame(item, None).unwrap();
    assert_eq!(frame.event, cairn_api::sse::SseEventName::FeedUpdate);
    assert_eq!(frame.data["item"]["id"], "101");
    assert_eq!(frame.data["item"]["source"], "slack");
    assert_eq!(frame.data["item"]["isRead"], false);
    assert_eq!(frame.data["item"]["kind"], "message");
}
