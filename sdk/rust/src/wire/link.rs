//! The peer link (spec/link.md): its connection tickets, its handshake and
//! transport messages, and the limits and timings the spec names.

use serde::Serialize;
use serde_json::Value;

use super::ids::{is_connection_id, is_device_id};
use super::noise::{HandshakeState, KeyPair, MAX_MESSAGE_BYTES, NoiseError, TAG_BYTES, Transport};
use super::{base64url, decode_json};

// ---- Tickets ----

pub const LINK_TICKET_PREFIX: &str = "colibrii_lt_";

/// The prefix and base64url of 16 random bytes. Only the listener that
/// made a ticket reads it.
pub fn make_link_ticket() -> String {
    format!("{LINK_TICKET_PREFIX}{}", base64url::random(16))
}

pub const TICKET_TTL_MS: u64 = 60_000;
pub const MAX_TICKET_SETS_PER_DEVICE: usize = 32;
pub const MAX_TICKETS: usize = 256;
pub const RETIRED_TICKETS_KEPT: usize = 1_024;

// ---- Admission ----

pub const MAX_SOCKETS: usize = 64;
pub const MAX_UNWELCOMED_SOCKETS: usize = 16;
pub const LOCKOUT_GUESSES: u32 = 5;
pub const LOCKOUT_MS: u64 = 30_000;
/// From the upgrade to `welcome`.
pub const WELCOME_WITHIN_MS: u64 = 10_000;

// ---- Liveness ----

pub const PING_EVERY_MS: u64 = 15_000;
/// The dialer's: nothing for this long after a ping, and then nothing
/// within SECOND_PING_WITHIN_MS of a second one.
pub const DEAD_AFTER_MS: u64 = 40_000;
pub const SECOND_PING_WITHIN_MS: u64 = 5_000;
pub const LISTENER_SILENT_MS: u64 = 120_000;

// ---- Dialing ----

pub const RACE_TIMEOUT_MS: u64 = 10_000;
/// The wait before each redial, staying at the last.
pub const REDIAL_DELAYS_MS: [u64; 5] = [1_000, 2_000, 4_000, 8_000, 16_000];
/// A link up this long starts the redial waits over.
pub const REDIAL_RESET_AFTER_MS: u64 = 30_000;

// ---- Close codes ----

pub const CLOSE_NORMAL: u16 = 1000;
pub const CLOSE_GOING_AWAY: u16 = 1001;
pub const CLOSE_PROTOCOL_ERROR: u16 = 1002;
pub const CLOSE_TOO_BIG: u16 = 1009;
pub const CLOSE_TRY_AGAIN: u16 = 1013;
pub const CLOSE_HELLO_REFUSED: u16 = 4002;
pub const CLOSE_LOCKED_OUT: u16 = 4003;
pub const CLOSE_HANDSHAKE_REFUSED: u16 = 4004;

// ---- The handshake ----

pub fn link_prologue(ticket: &str) -> Vec<u8> {
    format!("colibrii-link-v1:{ticket}").into_bytes()
}

/// The ticket's length in two bytes, the ticket, then Noise message 1.
pub fn encode_first_message(ticket: &str, message: &[u8]) -> Vec<u8> {
    let ticket = ticket.as_bytes();
    let mut first = Vec::with_capacity(2 + ticket.len() + message.len());
    first.extend_from_slice(&(ticket.len() as u16).to_be_bytes());
    first.extend_from_slice(ticket);
    first.extend_from_slice(message);
    first
}

/// The ticket and Noise message 1, or nothing for bytes too short to hold
/// them.
pub fn decode_first_message(bytes: &[u8]) -> Option<(String, &[u8])> {
    let length = usize::from(u16::from_be_bytes([*bytes.first()?, *bytes.get(1)?]));
    let ticket = bytes.get(2..2 + length)?;
    Some((String::from_utf8_lossy(ticket).into_owned(), &bytes[2 + length..]))
}

/// The dialer's first message, and the handshake that reads the answer.
/// Only the test vectors pass `e`.
pub fn start_handshake(
    ticket: &str,
    key: &KeyPair,
    listener_key: [u8; 32],
    e: Option<KeyPair>,
) -> Result<(Vec<u8>, HandshakeState), NoiseError> {
    let mut handshake = HandshakeState::initiator(&link_prologue(ticket), key.clone(), listener_key, e);
    let (message, _) = handshake.write_message(&[])?;
    Ok((encode_first_message(ticket, &message), handshake))
}

/// The listener's side, once the ticket checks out: the key the dialer
/// proved, which must be the roster's before the second message is sent.
pub struct Answered {
    pub dialer_key: [u8; 32],
    pub second: Vec<u8>,
    pub transport: Transport,
    pub handshake_hash: [u8; 32],
}

pub fn answer_handshake(ticket: &str, message: &[u8], key: &KeyPair, e: Option<KeyPair>) -> Option<Answered> {
    let mut handshake = HandshakeState::responder(&link_prologue(ticket), key.clone(), e);
    let (payload, _) = handshake.read_message(message).ok()?;
    if !payload.is_empty() {
        return None;
    }
    let (second, transport) = handshake.write_message(&[]).ok()?;
    Some(Answered {
        dialer_key: handshake.remote_static_key()?,
        second,
        transport: transport?,
        handshake_hash: handshake.handshake_hash(),
    })
}

pub fn finish_handshake(handshake: &mut HandshakeState, second: &[u8]) -> Option<Transport> {
    let (payload, transport) = handshake.read_message(second).ok()?;
    transport.filter(|_| payload.is_empty())
}

// ---- Transport messages ----

/// The app hello, welcome and refusal each, before encoding, so each of
/// their messages fits one transport message.
pub const MAX_APP_BYTES: usize = 16_384;

fn app_bytes(value: &Value) -> Option<Vec<u8>> {
    base64url::decode(value.as_str()?).filter(|bytes| bytes.len() <= MAX_APP_BYTES)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub device_id: String,
    pub connection_id: String,
    pub app_hello: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Welcome {
    pub app_welcome: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refuse {
    Colibrii { reason: String },
    App { reason: String, app_refusal: Option<Vec<u8>> },
}

pub const TYPE_DATA: u8 = 0x00;
pub const TYPE_DATA_MORE: u8 = 0x01;
pub const TYPE_HELLO: u8 = 0x10;
pub const TYPE_WELCOME: u8 = 0x11;
pub const TYPE_REFUSE: u8 = 0x12;
pub const TYPE_PING: u8 = 0x20;
pub const TYPE_PONG: u8 = 0x21;

/// `more` is a piece of an app message with more to come (0x01).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportMessage {
    Data { more: bool, body: Vec<u8> },
    Hello(Hello),
    Welcome(Welcome),
    Refuse(Refuse),
    Ping,
    Pong,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HelloOut<'a> {
    device_id: &'a str,
    connection_id: &'a str,
    app_hello: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WelcomeOut {
    app_welcome: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RefuseOut<'a> {
    by: &'static str,
    reason: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    app_refusal: Option<String>,
}

fn with_type(kind: u8, body: impl AsRef<[u8]>) -> Vec<u8> {
    let body = body.as_ref();
    let mut plaintext = Vec::with_capacity(1 + body.len());
    plaintext.push(kind);
    plaintext.extend_from_slice(body);
    plaintext
}

fn json(value: &impl Serialize) -> Vec<u8> {
    serde_json::to_vec(value).expect("a link message encodes")
}

/// A transport message's plaintext: its type, then its body.
pub fn encode_transport_message(message: &TransportMessage) -> Vec<u8> {
    match message {
        TransportMessage::Data { more, body } => with_type(if *more { TYPE_DATA_MORE } else { TYPE_DATA }, body),
        TransportMessage::Hello(hello) => with_type(
            TYPE_HELLO,
            json(&HelloOut {
                device_id: &hello.device_id,
                connection_id: &hello.connection_id,
                app_hello: base64url::encode(&hello.app_hello),
            }),
        ),
        TransportMessage::Welcome(welcome) => {
            with_type(TYPE_WELCOME, json(&WelcomeOut { app_welcome: base64url::encode(&welcome.app_welcome) }))
        }
        TransportMessage::Refuse(refuse) => with_type(
            TYPE_REFUSE,
            json(&match refuse {
                Refuse::Colibrii { reason } => RefuseOut { by: "colibrii", reason, app_refusal: None },
                Refuse::App { reason, app_refusal } => {
                    RefuseOut { by: "app", reason, app_refusal: app_refusal.as_deref().map(base64url::encode) }
                }
            }),
        ),
        TransportMessage::Ping => vec![TYPE_PING],
        TransportMessage::Pong => vec![TYPE_PONG],
    }
}

fn decode_hello(value: &Value) -> Option<Hello> {
    let device_id = value.get("deviceId")?.as_str()?;
    let connection_id = value.get("connectionId")?.as_str()?;
    (is_device_id(device_id) && is_connection_id(connection_id)).then_some(())?;
    Some(Hello {
        device_id: device_id.to_owned(),
        connection_id: connection_id.to_owned(),
        app_hello: app_bytes(value.get("appHello")?)?,
    })
}

fn decode_refuse(value: &Value) -> Option<Refuse> {
    let reason = value.get("reason")?.as_str()?.to_owned();
    match value.get("by")?.as_str()? {
        "colibrii" => Some(Refuse::Colibrii { reason }),
        "app" => Some(Refuse::App {
            reason,
            app_refusal: match value.get("appRefusal") {
                None => None,
                Some(bytes) => Some(app_bytes(bytes)?),
            },
        }),
        _ => None,
    }
}

/// Nothing for an unknown type or a body that does not decode, which
/// closes the link with 1002.
pub fn decode_transport_message(plaintext: &[u8]) -> Option<TransportMessage> {
    let (&kind, body) = plaintext.split_first()?;
    match kind {
        TYPE_DATA | TYPE_DATA_MORE => {
            Some(TransportMessage::Data { more: kind == TYPE_DATA_MORE, body: body.to_vec() })
        }
        TYPE_HELLO => decode_hello(&decode_json(body)?).map(TransportMessage::Hello),
        TYPE_WELCOME => {
            Some(TransportMessage::Welcome(Welcome { app_welcome: app_bytes(decode_json(body)?.get("appWelcome")?)? }))
        }
        TYPE_REFUSE => decode_refuse(&decode_json(body)?).map(TransportMessage::Refuse),
        TYPE_PING => body.is_empty().then_some(TransportMessage::Ping),
        TYPE_PONG => body.is_empty().then_some(TransportMessage::Pong),
        _ => None,
    }
}

/// Whether the message fits one transport message once sealed.
pub fn fits_transport_message(message: &TransportMessage) -> bool {
    encode_transport_message(message).len() + TAG_BYTES <= MAX_MESSAGE_BYTES
}

/// One websocket message: the plaintext under the sending cipher, with
/// empty associated data.
pub fn seal_transport_message(transport: &mut Transport, message: &TransportMessage) -> Result<Vec<u8>, NoiseError> {
    transport.send.encrypt_with_ad(&[], &encode_transport_message(message))
}

pub fn open_transport_message(transport: &mut Transport, ciphertext: &[u8]) -> Option<TransportMessage> {
    decode_transport_message(&transport.receive.decrypt_with_ad(&[], ciphertext).ok()?)
}

// ---- App messages ----

/// What fits one transport message beside its type byte and tag.
pub const MAX_PIECE_BYTES: usize = 65_518;
pub const MAX_APP_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

/// The pieces of an app message, each with whether more come after it.
pub fn split_app_message(message: &[u8]) -> Vec<TransportMessage> {
    if message.is_empty() {
        return vec![TransportMessage::Data { more: false, body: Vec::new() }];
    }
    let count = message.len().div_ceil(MAX_PIECE_BYTES);
    message
        .chunks(MAX_PIECE_BYTES)
        .enumerate()
        .map(|(index, body)| TransportMessage::Data { more: index + 1 < count, body: body.to_vec() })
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
pub enum Joined {
    Partial,
    Message(Vec<u8>),
    /// Closes the link with 1009.
    TooBig,
}

/// Joins the pieces of one app message at a time.
#[derive(Default)]
pub struct AppMessageJoiner {
    buffer: Vec<u8>,
}

impl AppMessageJoiner {
    pub fn add(&mut self, more: bool, body: Vec<u8>) -> Joined {
        if self.buffer.is_empty() && !more {
            return Joined::Message(body);
        }
        if self.buffer.len() + body.len() > MAX_APP_MESSAGE_BYTES {
            return Joined::TooBig;
        }
        self.buffer.extend_from_slice(&body);
        if more { Joined::Partial } else { Joined::Message(std::mem::take(&mut self.buffer)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::noise::KeyPair;

    #[test]
    fn a_handshake_message_that_carries_a_payload_is_refused() {
        let (ticket, dialer, listener) = (make_link_ticket(), KeyPair::generate(), KeyPair::generate());
        let mut dialing = HandshakeState::initiator(&link_prologue(&ticket), dialer.clone(), listener.public_key, None);
        let (first, _) = dialing.write_message(&[1]).unwrap();
        assert!(answer_handshake(&ticket, &first, &listener, None).is_none());

        let (first, mut dialing) = start_handshake(&ticket, &dialer, listener.public_key, None).unwrap();
        let (_, first) = decode_first_message(&first).unwrap();
        let mut answering = HandshakeState::responder(&link_prologue(&ticket), listener, None);
        answering.read_message(first).unwrap();
        let (second, _) = answering.write_message(&[1]).unwrap();
        assert!(finish_handshake(&mut dialing, &second).is_none());
    }
}
