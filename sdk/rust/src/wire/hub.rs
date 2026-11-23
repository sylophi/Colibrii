//! The hub socket's messages, limits and close codes (spec/hub-socket.md),
//! and the device record the service keeps (spec/service.md).

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::ids::{is_device_id, is_public_key};

pub const PING: &str = "ping";
pub const PONG: &str = "pong";

pub const MAX_MESSAGE_BYTES: usize = 65_536;
/// An app's device cap is at most this, so a presence stays within the cap
/// below.
pub const MAX_DEVICE_CAP: usize = 1_024;
/// A message from the service: a relay is a device's message with `from`
/// in place of `to`, and a presence names at most MAX_DEVICE_CAP devices.
pub const MAX_SERVICE_MESSAGE_BYTES: usize = 262_144;

pub const CLOSE_TICKET_REFUSED: u16 = 4101;
pub const CLOSE_DEVICE_REMOVED: u16 = 4102;
pub const CLOSE_SUPERSEDED: u16 = 4103;
pub const CLOSE_ENROLLED_AGAIN: u16 = 4104;
pub const CLOSE_TOO_MANY_MESSAGES: u16 = 4105;
pub const CLOSE_SILENT: u16 = 4106;
pub const CLOSE_TOO_BIG: u16 = 1009;

/// How a device enrolled: one running instance at a time, or several, as
/// the tabs of a browser profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Connections {
    Single,
    Multi,
}

pub const MAX_DEVICE_DATA_BYTES: usize = 1024;

/// 1 to 256 bytes of UTF-8.
pub fn is_device_name(name: &str) -> bool {
    (1..=256).contains(&name.len())
}

/// Measured as the service stores it, encoded again compactly.
pub fn is_device_data(data: &Map<String, Value>) -> bool {
    serde_json::to_vec(data).is_ok_and(|json| json.len() <= MAX_DEVICE_DATA_BYTES)
}

/// A device of the app and account, as the service keeps it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceRecord {
    pub device_id: String,
    pub name: String,
    /// The app's device data. Every device of the account can read it.
    pub data: Map<String, Value>,
    pub connections: Connections,
    /// Milliseconds since the Unix epoch.
    pub created_at: i64,
    /// When the device last connected or disconnected its hub socket.
    pub last_seen_at: Option<i64>,
    pub online: bool,
}

impl DeviceRecord {
    pub fn decode(value: &Value) -> Option<DeviceRecord> {
        let record: DeviceRecord = serde_json::from_value(value.clone()).ok()?;
        (is_device_id(&record.device_id) && is_device_name(&record.name) && is_device_data(&record.data))
            .then_some(record)
    }
}

/// Decodes each entry, and leaves out the ones it cannot read.
pub fn readable<T>(entries: &Value, decode: impl Fn(&Value) -> Option<T>) -> Option<Vec<T>> {
    Some(entries.as_array()?.iter().filter_map(decode).collect())
}

/// One device in a presence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OnlineDevice {
    pub device_id: String,
    pub public_key: String,
    pub connections: Connections,
}

impl OnlineDevice {
    pub fn decode(value: &Value) -> Option<OnlineDevice> {
        let device: OnlineDevice = serde_json::from_value(value.clone()).ok()?;
        (is_device_id(&device.device_id) && is_public_key(&device.public_key)).then_some(device)
    }
}

/// What the service sends. An entry of a presence a device cannot read
/// leaves that device out of its roster, and a message that does not parse
/// as one of these is dropped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "t", rename_all = "lowercase")]
pub enum ServiceMessage {
    Presence {
        online: Vec<OnlineDevice>,
    },
    Relay {
        from: String,
        frame: String,
    },
    /// `offline`. A reason the device does not know ends its asks all the
    /// same.
    Nack {
        to: String,
        reason: String,
    },
}

impl ServiceMessage {
    pub fn decode(text: &str) -> Option<ServiceMessage> {
        let value: Value = serde_json::from_str(text).ok()?;
        let string = |field: &str| value.get(field)?.as_str().map(str::to_owned);
        match value.get("t")?.as_str()? {
            "presence" => {
                Some(ServiceMessage::Presence { online: readable(value.get("online")?, OnlineDevice::decode)? })
            }
            "relay" => {
                let from = string("from")?;
                is_device_id(&from).then_some(ServiceMessage::Relay { from, frame: string("frame")? })
            }
            "nack" => {
                let to = string("to")?;
                is_device_id(&to).then_some(ServiceMessage::Nack { to, reason: string("reason")? })
            }
            _ => None,
        }
    }
}

/// What the device sends: a sealed frame for another device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayTo {
    pub t: String,
    pub to: String,
    pub frame: String,
}

pub fn encode_relay(to: &str, frame: &str) -> String {
    serde_json::to_string(&RelayTo { t: "relay".into(), to: to.into(), frame: frame.into() }).expect("a relay encodes")
}
