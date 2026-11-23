//! Stand-ins for the unit tests: a websocket with no network under it, a
//! link over one, and a device whose hub never answers.

use std::sync::Arc;

use tokio::io::DuplexStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role as WsRole;
use tokio_util::task::TaskTracker;

use crate::conn::Conn;
use crate::device::{Device, Inner, Options};
use crate::link::{LiveLink, NewLink, Role};
use crate::store::MemoryStore;
use crate::wire::base64url;
use crate::wire::hub::Connections;
use crate::wire::ids::{new_connection_id, new_device_id};
use crate::wire::noise::{CipherState, Transport};
use crate::wire::relay::CandidateKind;

/// A socket as the device holds it, and its far end.
pub async fn conn(tasks: &TaskTracker) -> (Conn, WebSocketStream<DuplexStream>) {
    let (near, far) = tokio::io::duplex(1 << 16);
    let (near, far) = tokio::join!(
        WebSocketStream::from_raw_socket(near, WsRole::Server, None),
        WebSocketStream::from_raw_socket(far, WsRole::Client, None),
    );
    (Conn::accepted(near, tasks), far)
}

/// A device of the app, with a hub that never answers.
pub async fn device() -> Device {
    let options = Options::new("http://127.0.0.1:1", "inkwell", Connections::Single, Arc::new(MemoryStore::new()))
        .runtime(tokio::runtime::Handle::current());
    Device::new(options).await.unwrap()
}

/// A link this device listened for, from `device_id` under `peer_key`, over
/// a socket whose far end it also gives.
pub async fn listened(
    inner: &Inner,
    device_id: &str,
    connection_id: &str,
    peer_key: [u8; 32],
) -> (Arc<LiveLink>, WebSocketStream<DuplexStream>) {
    let (conn, far) = conn(&inner.tasks).await;
    let link = LiveLink::new(NewLink {
        conn: Arc::new(conn),
        // Never used: the tests read and write no messages on it.
        transport: Transport { send: CipherState::default(), receive: CipherState::default() },
        role: Role::Listener,
        device_id: device_id.to_owned(),
        peer_key,
        connection_id: connection_id.to_owned(),
        arrival: CandidateKind::Lan,
        greeting: Vec::new(),
    });
    (link, far)
}

/// A presence naming each device with its key.
pub fn presence(online: &[(&str, [u8; 32])]) -> String {
    let online: Vec<_> = online
        .iter()
        .map(|(device_id, key)| {
            serde_json::json!({ "deviceId": device_id, "publicKey": base64url::encode(key), "connections": "single" })
        })
        .collect();
    serde_json::json!({ "t": "presence", "online": online }).to_string()
}

pub fn ids() -> (String, String) {
    (new_device_id(), new_connection_id())
}
