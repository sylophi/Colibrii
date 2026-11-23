//! Every case of every file in spec/vectors/, read from the repository.

use std::path::PathBuf;

use colibrii::wire::api::{
    DeviceList, DevicePatch, EnrollRequest, EnrollResponse, ServiceError, SignInTokenRequest, SignInTokenResponse,
    TicketRequest, TicketResponse, TunnelRequest, TunnelResponse,
};
use colibrii::wire::hub::{RelayTo, ServiceMessage};
use colibrii::wire::ids::hex;
use colibrii::wire::link::{
    self, AppMessageJoiner, Joined, TYPE_HELLO, TYPE_PING, TYPE_PONG, TYPE_REFUSE, TYPE_WELCOME, TransportMessage,
};
use colibrii::wire::noise::{HandshakeState, KeyPair};
use colibrii::wire::relay::{self, ConnectInfoInput, FrameKind, RelayFrame};
use colibrii::wire::{base64url, decode_public_key};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn read(file: &str) -> Vec<Value> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spec/vectors").join(file);
    let cases: Vec<Value> = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert!(!cases.is_empty(), "{file} has cases");
    cases
}

fn text(value: &Value) -> &str {
    value.as_str().unwrap()
}

fn bytes(value: &Value) -> Vec<u8> {
    base64url::decode(text(value)).unwrap()
}

fn b64(bytes: &[u8]) -> String {
    base64url::encode(bytes)
}

fn key_pair(value: &Value) -> KeyPair {
    KeyPair::from_private_key(bytes(value).try_into().unwrap())
}

fn from_hex(text: &str) -> Vec<u8> {
    (0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap()).collect()
}

#[test]
fn base64url() {
    for case in read("base64url.json") {
        let (name, inputs, expected) = (&case["name"], &case["inputs"], &case["expected"]);
        if let Some(input) = inputs["hex"].as_str() {
            assert_eq!(base64url::encode(&from_hex(input)), text(&expected["base64url"]), "{name}");
        } else {
            let decoded = base64url::decode(text(&inputs["base64url"])).map(|bytes| hex(&bytes));
            let wanted =
                if expected["invalid"] == json!(true) { None } else { Some(text(&expected["hex"]).to_owned()) };
            assert_eq!(decoded, wanted, "{name}");
        }
    }
}

fn sha256_hex(text: &str) -> String {
    hex(&Sha256::digest(text.as_bytes()))
}

#[test]
fn tunnel_label() {
    for case in read("tunnel-label.json") {
        let inputs = &case["inputs"];
        let label =
            sha256_hex(&format!("{}:{}:{}", text(&inputs["app"]), text(&inputs["account"]), text(&inputs["device"])));
        assert_eq!(&label[..32], text(&case["expected"]["label"]), "{}", case["name"]);
    }
}

#[test]
fn credential_hash() {
    for case in read("credential-hash.json") {
        assert_eq!(
            sha256_hex(text(&case["inputs"]["credential"])),
            text(&case["expected"]["sha256"]),
            "{}",
            case["name"]
        );
    }
}

#[test]
fn noise() {
    for case in read("noise.json") {
        let (name, inputs, expected) = (&case["name"], &case["inputs"], &case["expected"]);
        let prologue = bytes(&inputs["prologue"]);
        let initiator_static = key_pair(&inputs["initiatorStaticPrivateKey"]);
        let responder_static = key_pair(&inputs["responderStaticPrivateKey"]);
        let mut initiator = HandshakeState::initiator(
            &prologue,
            initiator_static.clone(),
            responder_static.public_key,
            Some(key_pair(&inputs["initiatorEphemeralPrivateKey"])),
        );
        let mut responder = HandshakeState::responder(
            &prologue,
            responder_static.clone(),
            Some(key_pair(&inputs["responderEphemeralPrivateKey"])),
        );
        let messages = inputs["messages"].as_array().unwrap();
        let payloads: Vec<Vec<u8>> = messages.iter().map(|message| bytes(&message["payload"])).collect();
        let (one, _) = initiator.write_message(&payloads[0]).unwrap();
        let (read1, _) = responder.read_message(&one).unwrap();
        let (two, responder_transport) = responder.write_message(&payloads[1]).unwrap();
        let (read2, initiator_transport) = initiator.read_message(&two).unwrap();
        assert_eq!(read1, payloads[0], "{name}");
        assert_eq!(read2, payloads[1], "{name}");
        let (mut at_initiator, mut at_responder) = (initiator_transport.unwrap(), responder_transport.unwrap());
        let mut sent = vec![b64(&one), b64(&two)];
        for (message, payload) in messages.iter().zip(&payloads).skip(2) {
            let (writer, reader) = if message["from"] == "initiator" {
                (&mut at_initiator, &mut at_responder)
            } else {
                (&mut at_responder, &mut at_initiator)
            };
            let ciphertext = writer.send.encrypt_with_ad(&[], payload).unwrap();
            assert_eq!(&reader.receive.decrypt_with_ad(&[], &ciphertext).unwrap(), payload, "{name}");
            sent.push(b64(&ciphertext));
        }
        assert_eq!(b64(&initiator_static.public_key), text(&expected["initiatorStaticPublicKey"]), "{name}");
        assert_eq!(b64(&responder_static.public_key), text(&expected["responderStaticPublicKey"]), "{name}");
        assert_eq!(json!(sent), expected["messages"], "{name}");
        assert_eq!(b64(&initiator.handshake_hash()), text(&expected["handshakeHash"]), "{name}");
        assert_eq!(b64(&responder.handshake_hash()), text(&expected["handshakeHash"]), "{name}");
    }
}

#[test]
fn relay() {
    for case in read("relay.json") {
        let (name, inputs, expected) = (&case["name"], &case["inputs"], &case["expected"]);
        let (asker, asked) = (text(&inputs["asker"]), text(&inputs["asked"]));
        let asker_key = key_pair(&inputs["askerStaticPrivateKey"]);
        let asked_key = key_pair(&inputs["askedStaticPrivateKey"]);
        let id = text(&inputs["id"]);
        assert_eq!(b64(&asker_key.public_key), text(&expected["askerStaticPublicKey"]), "{name}");
        assert_eq!(b64(&asked_key.public_key), text(&expected["askedStaticPublicKey"]), "{name}");
        assert_eq!(
            String::from_utf8(relay::relay_prologue(asker, asked)).unwrap(),
            text(&expected["prologue"]),
            "{name}"
        );

        let (sealed, mut asking) = relay::seal_ask(
            asker,
            asked,
            &asker_key,
            asked_key.public_key,
            &bytes(&inputs["askPayload"]),
            Some(key_pair(&inputs["askerEphemeralPrivateKey"])),
        )
        .unwrap();
        let ask_frame = relay::encode_relay_frame(&RelayFrame { kind: FrameKind::Ask, id: id.into(), sealed });
        assert_eq!(ask_frame, text(&expected["askFrame"]), "{name}");

        let read_ask = relay::decode_relay_frame(text(&expected["askFrame"])).unwrap();
        assert_eq!((read_ask.kind, read_ask.id.as_str()), (FrameKind::Ask, id), "{name}");
        let open = |from: &str, to: &str| {
            relay::open_ask(from, to, &asked_key, &read_ask.sealed, Some(key_pair(&inputs["askedEphemeralPrivateKey"])))
        };
        assert!(open(asked, asker).is_none(), "{name}: opens only under its own prologue");
        let mut opened = open(asker, asked).unwrap();
        assert_eq!(b64(&opened.asker_key), text(&expected["askerStaticPublicKey"]), "{name}");
        assert_eq!(b64(&opened.handshake_hash), text(&expected["handshakeHash"]), "{name}");
        assert_eq!(opened.ask.ask, "connectInfo", "{name}");
        let input = ConnectInfoInput::decode(opened.ask.input.as_ref().unwrap()).unwrap();
        let ask: Value = serde_json::from_slice(&relay::encode_ask(&input)).unwrap();
        assert_eq!(ask, expected["ask"], "{name}");

        let sealed_answer = relay::seal_answer(&mut opened.handshake, &bytes(&inputs["answerPayload"])).unwrap();
        let answer_frame = relay::encode_relay_frame(&RelayFrame {
            kind: FrameKind::Answer,
            id: id.into(),
            sealed: sealed_answer.clone(),
        });
        assert_eq!(answer_frame, text(&expected["answerFrame"]), "{name}");
        let read_answer = relay::decode_relay_frame(text(&expected["answerFrame"])).unwrap();
        assert_eq!(read_answer, RelayFrame { kind: FrameKind::Answer, id: id.into(), sealed: sealed_answer }, "{name}");
        let answer = relay::open_answer(&mut asking, &read_answer.sealed).unwrap();
        let answer: Value = serde_json::from_slice(&relay::encode_answer(&answer)).unwrap();
        assert_eq!(answer, expected["answer"], "{name}");
    }
}

struct LinkHandshake {
    first: Vec<u8>,
    dialer_key: KeyPair,
    listener_key: KeyPair,
    answered: link::Answered,
    dialer: colibrii::wire::noise::Transport,
}

fn link_handshake(inputs: &Value) -> LinkHandshake {
    let ticket = text(&inputs["ticket"]);
    let listener_key = key_pair(&inputs["listenerStaticPrivateKey"]);
    let dialer_key = key_pair(&inputs["dialerStaticPrivateKey"]);
    let (first, mut handshake) = link::start_handshake(
        ticket,
        &dialer_key,
        listener_key.public_key,
        Some(key_pair(&inputs["dialerEphemeralPrivateKey"])),
    )
    .unwrap();
    let (read_ticket, message) = link::decode_first_message(&first).unwrap();
    assert_eq!(read_ticket, ticket);
    let answer = |as_ticket: &str| {
        link::answer_handshake(
            as_ticket,
            message,
            &listener_key,
            Some(key_pair(&inputs["listenerEphemeralPrivateKey"])),
        )
    };
    assert!(answer(&format!("{ticket}x")).is_none(), "a ticket is bound into the handshake");
    let answered = answer(&read_ticket).unwrap();
    let dialer = link::finish_handshake(&mut handshake, &answered.second).unwrap();
    LinkHandshake { first, dialer_key, listener_key, answered, dialer }
}

#[test]
fn link_handshake_vectors() {
    for case in read("link-handshake.json") {
        let (name, inputs, expected) = (&case["name"], &case["inputs"], &case["expected"]);
        let shaken = link_handshake(inputs);
        assert_eq!(
            json!({
                "dialerStaticPublicKey": b64(&shaken.dialer_key.public_key),
                "listenerStaticPublicKey": b64(&shaken.listener_key.public_key),
                "prologue": String::from_utf8(link::link_prologue(text(&inputs["ticket"]))).unwrap(),
                "firstMessage": b64(&shaken.first),
                "secondMessage": b64(&shaken.answered.second),
                "handshakeHash": b64(&shaken.answered.handshake_hash),
            }),
            *expected,
            "{name}"
        );
        assert_eq!(b64(&shaken.answered.dialer_key), text(&expected["dialerStaticPublicKey"]), "{name}");
    }
}

fn type_of(name: &str) -> u8 {
    match name {
        "hello" => TYPE_HELLO,
        "welcome" => TYPE_WELCOME,
        "refuse" => TYPE_REFUSE,
        "ping" => TYPE_PING,
        "pong" => TYPE_PONG,
        other => panic!("no type {other}"),
    }
}

#[test]
fn link_transport() {
    for case in read("link-transport.json") {
        let (name, inputs, expected) = (&case["name"], &case["inputs"], &case["expected"]);
        if let Some(sent) = inputs.get("json") {
            // What a later version may send, read as this one reads it.
            let mut plaintext = vec![type_of(text(&inputs["type"]))];
            plaintext.extend(serde_json::to_vec(sent).unwrap());
            let message = link::decode_transport_message(&plaintext).unwrap();
            let read: Value = serde_json::from_slice(&link::encode_transport_message(&message)[1..]).unwrap();
            assert_eq!(read, expected["json"], "{name}");
            continue;
        }
        let shaken = link_handshake(inputs);
        let (mut dialer, mut listener) = (shaken.dialer, shaken.answered.transport);
        let (mut at_dialer, mut at_listener) = (AppMessageJoiner::default(), AppMessageJoiner::default());
        let mut apps = Vec::new();
        let mut frames = Vec::new();
        for send in inputs["messages"].as_array().unwrap() {
            let kind = text(&send["type"]);
            let content = send.get("body").map(bytes).unwrap_or_default();
            let plaintexts = if kind == "app" {
                link::split_app_message(&content).iter().map(link::encode_transport_message).collect()
            } else {
                let mut plaintext = vec![type_of(kind)];
                plaintext.extend(&content);
                vec![plaintext]
            };
            let (writer, reader, joiner) = if send["from"] == "dialer" {
                (&mut dialer, &mut listener, &mut at_listener)
            } else {
                (&mut listener, &mut dialer, &mut at_dialer)
            };
            let mut sent = Vec::new();
            for plaintext in plaintexts {
                let message = link::decode_transport_message(&plaintext).unwrap();
                assert_eq!(link::encode_transport_message(&message), plaintext, "{name}");
                let ciphertext = link::seal_transport_message(writer, &message).unwrap();
                match link::open_transport_message(reader, &ciphertext).unwrap() {
                    TransportMessage::Data { more, body } => {
                        if let Joined::Message(whole) = joiner.add(more, body) {
                            apps.push(b64(&whole));
                        }
                    }
                    received => assert_eq!(received, message, "{name}"),
                }
                sent.push(json!({ "plaintext": b64(&plaintext), "ciphertext": b64(&ciphertext) }));
            }
            frames.push(json!({ "frames": sent }));
        }
        assert_eq!(json!(frames), expected["messages"], "{name}");
        let sent_apps: Vec<Value> = inputs["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|send| send["type"] == "app")
            .map(|send| send["body"].clone())
            .collect();
        assert_eq!(json!(apps), json!(sent_apps), "{name}");
    }
}

fn round<T: serde::de::DeserializeOwned + serde::Serialize>(value: &Value) -> Value {
    json!(serde_json::from_value::<T>(value.clone()).unwrap())
}

/// Reads a hub.json sample as the device does and writes it back.
fn read_back(name: &str, value: &Value) -> Value {
    if let Some(tag) = name.strip_prefix("error: ") {
        let error = ServiceError::decode(value).unwrap();
        assert_eq!(error.tag(), tag);
        return json!(error);
    }
    match name {
        n if n.starts_with("socket: relay from the device") => round::<RelayTo>(value),
        n if n.starts_with("socket: ") => json!(ServiceMessage::decode(&value.to_string()).unwrap()),
        n if n.starts_with("POST /devices/enroll request") => round::<EnrollRequest>(value),
        n if n.starts_with("POST /devices/enroll answer") => json!(EnrollResponse::decode(value).unwrap()),
        n if n.starts_with("POST /sign-in-token request") => round::<SignInTokenRequest>(value),
        n if n.starts_with("POST /sign-in-token answer") => round::<SignInTokenResponse>(value),
        n if n.starts_with("GET /devices answer") => json!(DeviceList::decode(value).unwrap()),
        n if n.starts_with("PATCH /devices/:deviceId request") => round::<DevicePatch>(value),
        n if n.starts_with("POST /tickets request") => round::<TicketRequest>(value),
        n if n.starts_with("POST /tickets answer") => round::<TicketResponse>(value),
        n if n.starts_with("POST /tunnel request") => round::<TunnelRequest>(value),
        n if n.starts_with("POST /tunnel answer") => round::<TunnelResponse>(value),
        other => panic!("no reader for {other}"),
    }
}

#[test]
fn hub() {
    let mut samples = 0;
    for case in read("hub.json") {
        let (name, inputs, expected) = (text(&case["name"]), &case["inputs"], &case["expected"]);
        if let Some(wanted) = expected.get("text") {
            assert!(name == "socket: ping" || name == "socket: pong", "{name}");
            assert_eq!(text(wanted), if name.ends_with("ping") { "ping" } else { "pong" });
            continue;
        }
        // A sample a later version may send is read as `expected` says.
        for json in [inputs.get("json"), expected.get("json")].into_iter().flatten() {
            assert_eq!(read_back(name, json), expected["json"], "{name}");
            samples += 1;
        }
        if let Some(status) = expected.get("status") {
            assert_eq!(
                u64::from(ServiceError::decode(&expected["json"]).unwrap().status()),
                status.as_u64().unwrap(),
                "{name}"
            );
        }
    }
    assert!(samples > 0);
}

#[test]
fn roster_keys_decode() {
    let presence = ServiceMessage::decode(&read("hub.json")[2]["expected"]["json"].to_string()).unwrap();
    let ServiceMessage::Presence { online } = presence else { panic!("a presence") };
    assert!(online.iter().all(|device| decode_public_key(&device.public_key).is_some()));
}

/// The Noise core against cacophony's vector for this protocol, which the
/// TypeScript SDK's tests keep, so the spec's own vectors are not only
/// consistent with themselves.
#[test]
fn cacophony() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../ts/test/cacophony.json");
    let file: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let vectors = file["vectors"].as_array().unwrap();
    assert_eq!(vectors.len(), 1);
    for vector in vectors {
        assert_eq!(vector["protocol_name"], colibrii::wire::noise::PROTOCOL_NAME);
        let key = |field: &str| KeyPair::from_private_key(from_hex(text(&vector[field])).try_into().unwrap());
        let mut initiator = HandshakeState::initiator(
            &from_hex(text(&vector["init_prologue"])),
            key("init_static"),
            from_hex(text(&vector["init_remote_static"])).try_into().unwrap(),
            Some(key("init_ephemeral")),
        );
        let mut responder = HandshakeState::responder(
            &from_hex(text(&vector["resp_prologue"])),
            key("resp_static"),
            Some(key("resp_ephemeral")),
        );
        let messages = vector["messages"].as_array().unwrap();
        let payload = |i: usize| from_hex(text(&messages[i]["payload"]));
        let (one, _) = initiator.write_message(&payload(0)).unwrap();
        assert_eq!(hex(&one), text(&messages[0]["ciphertext"]));
        assert_eq!(responder.read_message(&one).unwrap().0, payload(0));
        let (two, at_responder) = responder.write_message(&payload(1)).unwrap();
        assert_eq!(hex(&two), text(&messages[1]["ciphertext"]));
        let (read, at_initiator) = initiator.read_message(&two).unwrap();
        assert_eq!(read, payload(1));
        let mut sides = [at_initiator.unwrap(), at_responder.unwrap()];
        // The two sides take turns, the initiator first.
        for (i, message) in messages.iter().enumerate().skip(2) {
            let writer = (i - 2) % 2;
            let sent = sides[writer].send.encrypt_with_ad(&[], &payload(i)).unwrap();
            assert_eq!(hex(&sent), text(&message["ciphertext"]), "transport message {i}");
            assert_eq!(sides[1 - writer].receive.decrypt_with_ad(&[], &sent).unwrap(), payload(i));
        }
        assert_eq!(hex(&initiator.handshake_hash()), text(&vector["handshake_hash"]));
        assert_eq!(hex(&responder.handshake_hash()), text(&vector["handshake_hash"]));
    }
}
