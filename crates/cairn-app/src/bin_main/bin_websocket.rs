//! WebSocket handler for real-time event streaming.

#[allow(unused_imports)]
use crate::*;

use axum::extract::ws::{
    close_code, CloseFrame, Message as WsMessage, WebSocket, WebSocketUpgrade,
};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use cairn_api::auth::{Authenticator, ServiceTokenAuthenticator};
use serde::Deserialize;

/// #459: maximum allowed incoming WebSocket text frame in bytes.
///
/// Cairn's client→server protocol is two message shapes:
///   - `{"type":"subscribe","event_types":[...]}`
///   - `{"type":"ping"}`
///
/// Both fit comfortably under 64 KiB even with a few hundred event types
/// in the filter list. The Axum default (from tokio-tungstenite) allows
/// frames up to 16 MiB, which combined with the per-connection
/// `from_str::<Value>` path would let an authenticated caller hold open
/// a WebSocket and stream 16 MiB frames until the server OOMs. This cap
/// sits inside the handler AND is mirrored on the `WebSocketUpgrade`
/// config so tungstenite can short-circuit even earlier on the frame
/// boundary.
pub(crate) const MAX_WS_INCOMING_FRAME_BYTES: usize = 64 * 1024;

/// #459: decision function for an inbound WebSocket text frame.
///
/// Pure: no side effects, no awaits. Returns the action the caller should
/// take. Hoisted out of the `tokio::select!` arm so it can be unit tested
/// without constructing a real `WebSocket`.
#[derive(Debug, PartialEq)]
pub(crate) enum WsFrameDecision {
    /// Frame is within limits — proceed to JSON parse + dispatch.
    Accept,
    /// Frame exceeds [`MAX_WS_INCOMING_FRAME_BYTES`]; the caller should
    /// send a Close(1009 SIZE) and break the connection loop.
    RejectOversized { bytes: usize },
}

pub(crate) fn classify_inbound_text_frame(text: &str) -> WsFrameDecision {
    if text.len() > MAX_WS_INCOMING_FRAME_BYTES {
        WsFrameDecision::RejectOversized { bytes: text.len() }
    } else {
        WsFrameDecision::Accept
    }
}

// ── WebSocket handler ─────────────────────────────────────────────────────────

/// Query parameters accepted by `GET /v1/ws`.
#[derive(Deserialize)]
pub(crate) struct WsQueryParams {
    /// Bearer token — required because browsers can't set headers during WS upgrade.
    token: Option<String>,
}

/// `GET /v1/ws` — real-time event stream over WebSocket (RFC 002 companion).
///
/// ### Auth
/// Pass the admin token via `?token=<token>` query parameter.
/// Header-based auth cannot be used for WebSocket connections from browsers.
///
/// ### Client → Server messages (JSON)
/// ```json
/// { "type": "subscribe",  "event_types": ["run_created", "task_queued"] }
/// { "type": "ping" }
/// ```
///
/// ### Server → Client messages (JSON)
/// ```json
/// { "type": "connected",  "head_position": 42 }
/// { "type": "event",      "position": 43, "event_type": "run_created", "event_id": "...", "payload": {...} }
/// { "type": "pong" }
/// { "type": "warn",       "message": "lagged: missed N event(s)" }
/// ```
pub(crate) async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(params): Query<WsQueryParams>,
) -> impl IntoResponse {
    // Authenticate via query param — same token registry as bearer
    // auth. (Upstream `auth_middleware` also runs on `/v1/ws`; this is
    // defence-in-depth.) Carry the resolved principal into the upgraded
    // task so T6c-C2 tenant filtering can gate each event.
    let token = match params.token {
        Some(t) if !t.is_empty() => t,
        _ => return StatusCode::UNAUTHORIZED.into_response(),
    };
    let authenticator = ServiceTokenAuthenticator::new(state.tokens.clone());
    let Ok(principal) = authenticator.authenticate(&token) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };

    // #459: cap both the full message size and the individual frame size
    // at `MAX_WS_INCOMING_FRAME_BYTES`. The in-handler `if text.len() >`
    // check below is belt-and-suspenders for the case where the cap
    // changes; tungstenite will refuse oversize frames first.
    ws.max_message_size(MAX_WS_INCOMING_FRAME_BYTES)
        .max_frame_size(MAX_WS_INCOMING_FRAME_BYTES)
        .on_upgrade(move |socket| handle_ws_connection(socket, state, principal))
}

/// Drive a single WebSocket connection to completion.
pub(crate) async fn handle_ws_connection(
    mut socket: WebSocket,
    state: AppState,
    principal: cairn_api::auth::AuthPrincipal,
) {
    use tokio::sync::broadcast::error::RecvError;

    let mut receiver = state.runtime.store.subscribe();

    // Active event-type filter set by the client (None = all events).
    let mut filter: Option<Vec<String>> = None;

    // T6c-C2: per-connection tenant gate. Admin principals see every
    // event; non-admins see only events whose `ProjectKey.tenant_id`
    // matches their own tenant. Events that carry no project scope
    // (cross-tenant bootstrap or infra events) fall through to the
    // subscriber — matches the SSE handler's semantics.
    let subscriber_tenant: Option<String> =
        principal.tenant().map(|t| t.tenant_id.as_str().to_owned());
    let is_admin = cairn_app::extractors::is_admin_principal(&principal);

    // Send the "connected" handshake with the current log head position.
    let head_pos = state
        .runtime
        .store
        .head_position()
        .await
        .ok()
        .flatten()
        .map(|p| p.0)
        .unwrap_or(0);

    let connected = serde_json::json!({ "type": "connected", "head_position": head_pos });
    if socket
        .send(WsMessage::Text(connected.to_string()))
        .await
        .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            // ── Inbound from the browser ──────────────────────────────────
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(WsMessage::Text(text))) => {
                        // #459: refuse to parse oversized frames. The
                        // upgrade config already caps the frame size at
                        // the tungstenite layer, but if that cap is ever
                        // relaxed the handler still bounds the JSON
                        // parse cost here. Close with 1009 SIZE so the
                        // client sees the actual reason.
                        if let WsFrameDecision::RejectOversized { bytes } =
                            classify_inbound_text_frame(&text)
                        {
                            tracing::warn!(
                                frame_bytes = bytes,
                                max = MAX_WS_INCOMING_FRAME_BYTES,
                                "rejecting oversized WebSocket text frame",
                            );
                            let _ = socket
                                .send(WsMessage::Close(Some(CloseFrame {
                                    code: close_code::SIZE,
                                    reason: "frame exceeds 64 KiB limit".into(),
                                })))
                                .await;
                            break;
                        }
                        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                            match val.get("type").and_then(|t| t.as_str()) {
                                Some("subscribe") => {
                                    filter = val
                                        .get("event_types")
                                        .and_then(|e| e.as_array())
                                        .map(|arr| {
                                            arr.iter()
                                                .filter_map(|v| v.as_str().map(str::to_owned))
                                                .collect()
                                        });
                                }
                                Some("ping") => {
                                    let _ = socket
                                        .send(WsMessage::Text(r#"{"type":"pong"}"#.to_owned()))
                                        .await;
                                }
                                _ => {}
                            }
                        }
                    }
                    Some(Ok(WsMessage::Ping(data))) => {
                        let _ = socket.send(WsMessage::Pong(data)).await;
                    }
                    // Client closed or errored — end the task.
                    Some(Ok(WsMessage::Close(_))) | None | Some(Err(_)) => break,
                    _ => {}
                }
            }

            // ── Outbound broadcast events ─────────────────────────────────
            recv_result = receiver.recv() => {
                match recv_result {
                    Ok(event) => {
                        // T6c-C2 (+ R1): tenant scope gate — fail-closed.
                        // Drop events whose tenant doesn't match the
                        // authenticated subscriber. Rules:
                        //   - admin  → sees every event (match REST).
                        //   - event tenant-agnostic (System) → visible to all.
                        //   - event tenant-scoped + subscriber has no tenant
                        //     → REJECT (Gemini R1: prior `if let (Some, Some)`
                        //     leaked these to unscoped callers).
                        //   - both present → must be equal.
                        if !is_admin {
                            let event_tenant =
                                cairn_app::handlers::sse::ws_event_tenant_id(&event.envelope);
                            if let Some(et) = event_tenant {
                                if subscriber_tenant.as_deref() != Some(et) {
                                    continue;
                                }
                            }
                        }

                        let event_type = event_type_name(&event.envelope.payload);

                        // Apply the client's subscription filter when set.
                        if let Some(ref types) = filter {
                            if !types.is_empty()
                                && !types.iter().any(|t| t.as_str() == event_type)
                            {
                                continue;
                            }
                        }

                        let msg = serde_json::json!({
                            "type":       "event",
                            "position":   event.position.0,
                            "event_type": event_type,
                            "event_id":   event.envelope.event_id.as_str(),
                            "payload":    &event.envelope.payload,
                        });

                        if socket
                            .send(WsMessage::Text(msg.to_string()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    // Missed messages due to a slow consumer — notify and continue.
                    Err(RecvError::Lagged(n)) => {
                        let warn = serde_json::json!({
                            "type":    "warn",
                            "message": format!("lagged: missed {n} event(s)"),
                        });
                        let _ = socket
                            .send(WsMessage::Text(warn.to_string()))
                            .await;
                    }
                    // Broadcast channel dropped — server shutting down.
                    Err(RecvError::Closed) => break,
                }
            }
        }
    }
}

// Task, session, batch handlers → bin_handlers.rs
// Export/Import handlers → bin_export.rs
// Event replay + append handlers → bin_events.rs
// DB status, admin snapshot/restore, projections → bin_admin.rs

#[cfg(test)]
mod tests {
    use super::{classify_inbound_text_frame, WsFrameDecision, MAX_WS_INCOMING_FRAME_BYTES};

    #[test]
    fn small_frame_accepted() {
        assert_eq!(
            classify_inbound_text_frame(r#"{"type":"ping"}"#),
            WsFrameDecision::Accept,
        );
    }

    #[test]
    fn frame_at_limit_accepted() {
        let at_cap = "x".repeat(MAX_WS_INCOMING_FRAME_BYTES);
        assert_eq!(
            classify_inbound_text_frame(&at_cap),
            WsFrameDecision::Accept,
            "frame exactly at the cap must still be accepted",
        );
    }

    #[test]
    fn oversized_frame_rejected() {
        let oversize = "x".repeat(MAX_WS_INCOMING_FRAME_BYTES + 1);
        assert_eq!(
            classify_inbound_text_frame(&oversize),
            WsFrameDecision::RejectOversized {
                bytes: MAX_WS_INCOMING_FRAME_BYTES + 1
            },
        );
    }

    #[test]
    fn two_mb_frame_rejected() {
        // #459: the original audit repro — 2 MB attacker-crafted frame
        // must NOT reach `serde_json::from_str`.
        let big = "x".repeat(2 * 1024 * 1024);
        assert!(matches!(
            classify_inbound_text_frame(&big),
            WsFrameDecision::RejectOversized { .. },
        ));
    }
}
