//! The relay (spec/relay.md): the `ask` and `answer` frames, each ask
//! sealed as one Noise handshake, and the connectInfo ask.

use serde::Serialize;
use serde_json::Value;

use super::ids::is_connection_id;
use super::noise::{HandshakeState, KeyPair, NoiseError};
use super::{base64url, decode_json};

pub const ASK_TIMEOUT_MS: u64 = 10_000;
/// How long the asked device remembers an ask it opened.
pub const SEEN_ASK_MS: u64 = 10 * 60_000;
/// How long a device whose tunnel is starting waits for it before it
/// answers an asker that would get nothing else.
pub const TUNNEL_WAIT_MS: u64 = 6_000;
pub const MAX_LAN_CANDIDATES: usize = 6;

// ---- Frames ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    Ask,
    Answer,
}

/// `id` is compared as a string, never as a number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayFrame {
    pub kind: FrameKind,
    pub id: String,
    pub sealed: String,
}

pub fn encode_relay_frame(frame: &RelayFrame) -> String {
    let kind = match frame.kind {
        FrameKind::Ask => "ask",
        FrameKind::Answer => "answer",
    };
    format!("{kind}:{}:{}", frame.id, frame.sealed)
}

pub fn decode_relay_frame(text: &str) -> Option<RelayFrame> {
    let (kind, rest) = text.split_once(':')?;
    let (id, sealed) = rest.split_once(':')?;
    let kind = match kind {
        "ask" => FrameKind::Ask,
        "answer" => FrameKind::Answer,
        _ => return None,
    };
    let base64ish = |c: u8| c.is_ascii_alphanumeric() || c == b'-' || c == b'_';
    if !(1..=16).contains(&id.len()) || !id.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    if sealed.is_empty() || !sealed.bytes().all(base64ish) {
        return None;
    }
    Some(RelayFrame { kind, id: id.to_owned(), sealed: sealed.to_owned() })
}

// ---- Payloads ----

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CandidateKind {
    Lan,
    Tunnel,
}

impl CandidateKind {
    pub fn parse(text: &str) -> Option<CandidateKind> {
        match text {
            "lan" => Some(CandidateKind::Lan),
            "tunnel" => Some(CandidateKind::Tunnel),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            CandidateKind::Lan => "lan",
            CandidateKind::Tunnel => "tunnel",
        }
    }
}

/// The asked device answers for the kinds it knows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectInfoInput {
    pub dialable_kinds: Vec<CandidateKind>,
    pub connection_id: String,
}

impl ConnectInfoInput {
    /// Kinds this reader does not know are left out.
    pub fn decode(value: &Value) -> Option<ConnectInfoInput> {
        let dialable_kinds = value
            .get("dialableKinds")?
            .as_array()?
            .iter()
            .filter_map(|kind| CandidateKind::parse(kind.as_str()?))
            .collect();
        let connection_id = value.get("connectionId")?.as_str()?;
        is_connection_id(connection_id)
            .then(|| ConnectInfoInput { dialable_kinds, connection_id: connection_id.to_owned() })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct Candidate {
    pub kind: CandidateKind,
    pub url: String,
    pub ticket: String,
}

debug_without_secrets!(Candidate { kind, url });

fn is_octet(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= 3
        && text.bytes().all(|c| c.is_ascii_digit())
        && (text == "0" || !text.starts_with('0'))
        && text.parse::<u16>().is_ok_and(|n| n <= 255)
}

fn is_label(text: &str) -> bool {
    !text.is_empty()
        && text.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-')
        && !text.starts_with('-')
        && !text.ends_with('-')
}

/// A `lan` URL is `ws://<IP address>:<port>`, IPv6 in brackets, and a
/// `tunnel` URL `wss://<hostname>`.
pub fn is_candidate_url(kind: CandidateKind, url: &str) -> bool {
    match kind {
        CandidateKind::Tunnel => {
            url.strip_prefix("wss://").is_some_and(|host| host.split('.').count() > 1 && host.split('.').all(is_label))
        }
        CandidateKind::Lan => {
            let Some(rest) = url.strip_prefix("ws://") else { return false };
            let Some((host, port)) = rest.rsplit_once(':') else { return false };
            let port_ok = !port.is_empty()
                && port.len() <= 5
                && !port.starts_with('0')
                && port.bytes().all(|c| c.is_ascii_digit())
                && port.parse::<u32>().is_ok_and(|n| n <= 65_535);
            let host_ok = match host.strip_prefix('[').and_then(|inner| inner.strip_suffix(']')) {
                Some(inner) => {
                    inner.bytes().all(|c| c.is_ascii_hexdigit() || c == b':' || c == b'.')
                        && inner.parse::<std::net::Ipv6Addr>().is_ok()
                }
                None => host.split('.').count() == 4 && host.split('.').all(is_octet),
            };
            port_ok && host_ok
        }
    }
}

impl Candidate {
    fn decode(value: &Value, kind: CandidateKind) -> Option<Candidate> {
        let url = value.get("url")?.as_str()?;
        let ticket = value.get("ticket")?.as_str()?;
        is_candidate_url(kind, url).then(|| Candidate { kind, url: url.to_owned(), ticket: ticket.to_owned() })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectInfoResult {
    Unavailable,
    Available(Vec<Candidate>),
}

impl ConnectInfoResult {
    /// The asker skips a candidate of a kind it does not know, and one of a
    /// kind it knows must decode, or the whole answer is unreadable.
    pub fn decode(value: &Value) -> Option<ConnectInfoResult> {
        match value.get("available")?.as_bool()? {
            false => Some(ConnectInfoResult::Unavailable),
            true => {
                let mut candidates = Vec::new();
                for entry in value.get("candidates")?.as_array()? {
                    let Some(kind) = entry.get("kind").and_then(Value::as_str).and_then(CandidateKind::parse) else {
                        continue;
                    };
                    candidates.push(Candidate::decode(entry, kind)?);
                }
                let count = |kind| candidates.iter().filter(|candidate| candidate.kind == kind).count();
                (count(CandidateKind::Lan) <= MAX_LAN_CANDIDATES && count(CandidateKind::Tunnel) <= 1)
                    .then_some(ConnectInfoResult::Available(candidates))
            }
        }
    }
}

/// An ask as the asked device opens it. An ask it does not know is
/// answered `unknown-ask`, and an input that does not decode `bad-input`.
#[derive(Debug, Clone, PartialEq)]
pub struct Ask {
    pub ask: String,
    pub input: Option<Value>,
}

impl Ask {
    pub fn decode(value: &Value) -> Option<Ask> {
        Some(Ask { ask: value.get("ask")?.as_str()?.to_owned(), input: value.get("input").cloned() })
    }
}

/// A refusal's code is `no-listener`, `unknown-ask` or `bad-input`. The
/// asker reads a code it does not know as a refusal all the same.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Ok(ConnectInfoResult),
    Refused(String),
}

impl Answer {
    pub fn decode(value: &Value) -> Option<Answer> {
        match value.get("ok")?.as_bool()? {
            true => Some(Answer::Ok(ConnectInfoResult::decode(value.get("result")?)?)),
            false => Some(Answer::Refused(value.get("code")?.as_str()?.to_owned())),
        }
    }
}

#[derive(Serialize)]
struct AskOut<'a> {
    ask: &'static str,
    input: &'a ConnectInfoInput,
}

#[derive(Serialize)]
#[serde(untagged)]
enum ResultOut<'a> {
    Unavailable { available: bool },
    Available { available: bool, candidates: &'a [Candidate] },
}

#[derive(Serialize)]
#[serde(untagged)]
enum AnswerOut<'a> {
    Ok { ok: bool, result: ResultOut<'a> },
    Refused { ok: bool, code: &'a str },
}

/// The exact bytes a connectInfo ask is sealed as.
pub fn encode_ask(input: &ConnectInfoInput) -> Vec<u8> {
    serde_json::to_vec(&AskOut { ask: "connectInfo", input }).expect("an ask encodes")
}

/// The exact bytes an answer is sealed as.
pub fn encode_answer(answer: &Answer) -> Vec<u8> {
    let out = match answer {
        Answer::Ok(ConnectInfoResult::Unavailable) => {
            AnswerOut::Ok { ok: true, result: ResultOut::Unavailable { available: false } }
        }
        Answer::Ok(ConnectInfoResult::Available(candidates)) => {
            AnswerOut::Ok { ok: true, result: ResultOut::Available { available: true, candidates } }
        }
        Answer::Refused(code) => AnswerOut::Refused { ok: false, code },
    };
    serde_json::to_vec(&out).expect("an answer encodes")
}

// ---- Sealing ----

pub fn relay_prologue(asker: &str, asked: &str) -> Vec<u8> {
    format!("colibrii-relay-v1:{asker}>{asked}").into_bytes()
}

/// The asker's side: the sealed ask, and the handshake that opens the
/// answer. Only the test vectors pass `e`.
pub fn seal_ask(
    asker: &str,
    asked: &str,
    key: &KeyPair,
    asked_key: [u8; 32],
    payload: &[u8],
    e: Option<KeyPair>,
) -> Result<(String, HandshakeState), NoiseError> {
    let mut handshake = HandshakeState::initiator(&relay_prologue(asker, asked), key.clone(), asked_key, e);
    let (message, _) = handshake.write_message(payload)?;
    Ok((base64url::encode(&message), handshake))
}

/// An ask the asked device opened. The caller checks `asker_key` against
/// the roster and `handshake_hash` against the asks it has seen, and
/// answers through `handshake`.
pub struct OpenedAsk {
    pub ask: Ask,
    pub asker_key: [u8; 32],
    pub handshake_hash: [u8; 32],
    pub handshake: HandshakeState,
}

/// The asked device's side. Nothing for an ask that does not open.
pub fn open_ask(asker: &str, asked: &str, key: &KeyPair, sealed: &str, e: Option<KeyPair>) -> Option<OpenedAsk> {
    let message = base64url::decode(sealed)?;
    let mut handshake = HandshakeState::responder(&relay_prologue(asker, asked), key.clone(), e);
    let (payload, _) = handshake.read_message(&message).ok()?;
    let asker_key = handshake.remote_static_key()?;
    let ask = Ask::decode(&decode_json(&payload)?)?;
    Some(OpenedAsk { ask, asker_key, handshake_hash: handshake.handshake_hash(), handshake })
}

pub fn seal_answer(handshake: &mut HandshakeState, payload: &[u8]) -> Result<String, NoiseError> {
    Ok(base64url::encode(&handshake.write_message(payload)?.0))
}

pub fn open_answer(handshake: &mut HandshakeState, sealed: &str) -> Option<Answer> {
    let message = base64url::decode(sealed)?;
    let (payload, _) = handshake.read_message(&message).ok()?;
    Answer::decode(&decode_json(&payload)?)
}
