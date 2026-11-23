mod common;

use std::sync::Arc;
use std::time::Duration;

use colibrii::testing::Msg;
use colibrii::wire::base64url;
use colibrii::wire::hub::encode_relay;
use colibrii::wire::link::make_link_ticket;
use colibrii::wire::noise::KeyPair;
use colibrii::wire::relay::{FrameKind, RelayFrame, decode_relay_frame, encode_relay_frame, open_ask, seal_answer};
use colibrii::{
    AccountState, CandidateKind, Device, DialError, IncomingHello, Link, LinkCloseCause, LinkClosed, RefusedBy, Role,
};
use common::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

/// Past one piece (65,518 bytes), so it goes in several.
fn big() -> Vec<u8> {
    (0..200_000).map(|index| (index % 251) as u8).collect()
}

struct Pair {
    account: String,
    desk: Device,
    phone: Device,
    desk_id: String,
    phone_id: String,
    links: mpsc::UnboundedReceiver<Link>,
}

/// A listening desk and a phone that dials it, both enrolled and each in
/// the other's roster, with the desk deciding each hello by `decide`.
async fn pair_with<F, Fut>(desk_kit: &Kit, phone_kit: &Kit, decide: F) -> Pair
where
    F: Fn(IncomingHello) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Option<Link>> + Send + 'static,
{
    let account = new_account_id();
    let desk = Device::new(listening(desk_kit)).await.unwrap();
    let links = serve(&desk, decide);
    let phone = phone_kit.device().await;
    let desk_id = device_id_of(&desk_kit.enroll(&desk, &account, "Desk").await);
    let phone_id = device_id_of(&phone_kit.enroll(&phone, &account, "Phone").await);
    sees(&phone, &desk_id).await;
    sees(&desk, &phone_id).await;
    Pair { account, desk, phone, desk_id, phone_id, links }
}

async fn pair(kit: &Kit) -> Pair {
    let account = new_account_id();
    let desk = Device::new(listening(kit)).await.unwrap();
    let links = welcome_all(&desk);
    let phone = kit.device().await;
    let desk_id = device_id_of(&kit.enroll(&desk, &account, "Desk").await);
    let phone_id = device_id_of(&kit.enroll(&phone, &account, "Phone").await);
    sees(&phone, &desk_id).await;
    sees(&desk, &phone_id).await;
    Pair { account, desk, phone, desk_id, phone_id, links }
}

type Deciding = std::pin::Pin<Box<dyn std::future::Future<Output = Option<Link>> + Send>>;

/// A desk that holds each hello until `decided`, after saying in `deciding`
/// that one came.
fn held() -> (Arc<Notify>, Arc<Notify>, impl Fn(IncomingHello) -> Deciding + Send + Sync + 'static) {
    let (deciding, decided) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (told, waited) = (deciding.clone(), decided.clone());
    let decide = move |hello: IncomingHello| {
        let (told, waited) = (told.clone(), waited.clone());
        Box::pin(async move {
            told.notify_one();
            waited.notified().await;
            hello.welcome(Vec::new()).await
        }) as Deciding
    };
    (deciding, decided, decide)
}

/// The candidate socket the phone won with: the one it sent a hello on.
fn winner(kit: &Kit) -> colibrii::testing::Opened {
    let opened = kit.net.opened.lock().unwrap();
    opened.iter().rev().find(|entry| !entry.url.contains("/connect?") && entry.sent().len() > 1).unwrap().clone()
}

/// The device's hub socket closes, and the device never hears of it.
fn close_unheard(kit: &Kit) {
    let hub = kit.net.hub_socket();
    hub.set_deaf(true);
    hub.close(1000, "down");
}

async fn closed_within(link: &Link, within: Duration) -> Option<LinkClosed> {
    timeout(within, link.closed()).await.ok()
}

#[tokio::test]
async fn carries_app_messages_both_ways_one_of_them_in_several_pieces() {
    let kit = Kit::new();
    let mut pair = pair(&kit).await;
    let dialed = pair.phone.dial(&pair.desk_id, b"hi").await.unwrap();
    let admitted = next_link(&mut pair.links).await;
    assert_eq!(
        (dialed.device_id(), dialed.role(), dialed.arrival()),
        (pair.desk_id.as_str(), Role::Dialer, CandidateKind::Lan)
    );
    assert_eq!(
        (admitted.device_id(), admitted.role(), admitted.arrival(), admitted.connection_id()),
        (pair.phone_id.as_str(), Role::Listener, CandidateKind::Lan, dialed.connection_id())
    );
    assert_eq!(dialed.greeting(), b"welcome hi");
    assert_eq!(admitted.greeting(), b"hi");

    dialed.send(b"one").unwrap();
    dialed.send(&big()).unwrap();
    admitted.send(&big()).unwrap();
    admitted.send(b"two").unwrap();
    assert_eq!(admitted.recv().await.unwrap(), b"one");
    assert_eq!(admitted.recv().await.unwrap(), big());
    assert_eq!(dialed.recv().await.unwrap(), big());
    assert_eq!(dialed.recv().await.unwrap(), b"two");

    dialed.close();
    assert_eq!(dialed.closed().await, LinkClosed { code: 1000, cause: LinkCloseCause::Closed });
    assert_eq!(admitted.closed().await, LinkClosed { code: 1000, cause: LinkCloseCause::Peer });
}

#[tokio::test]
async fn keeps_both_links_up_when_two_listening_single_devices_dial_each_other() {
    let kit = Kit::new();
    let account = new_account_id();
    let desk = Device::new(listening(&kit)).await.unwrap();
    let laptop = Device::new(listening(&kit)).await.unwrap();
    let mut at_desk = welcome_all(&desk);
    let mut at_laptop = welcome_all(&laptop);
    let desk_id = device_id_of(&kit.enroll(&desk, &account, "Desk").await);
    let laptop_id = device_id_of(&kit.enroll(&laptop, &account, "Laptop").await);
    sees(&desk, &laptop_id).await;
    sees(&laptop, &desk_id).await;
    let to_laptop = desk.dial(&laptop_id, b"desk").await.unwrap();
    let _kept_at_laptop = next_link(&mut at_laptop).await;
    let to_desk = laptop.dial(&desk_id, b"laptop").await.unwrap();
    let _kept_at_desk = next_link(&mut at_desk).await;
    assert_eq!(closed_within(&to_laptop, Duration::from_secs(1)).await, None);
    assert_eq!(closed_within(&to_desk, Duration::from_secs(1)).await, None);
}

#[tokio::test]
async fn is_dialed_at_the_port_in_front_of_the_listener_through_what_is_there() {
    let kit = Kit::new();
    let account = new_account_id();
    // A TCP front that passes each connection on to the desk's listener,
    // wherever it bound.
    let front = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let front_port = front.local_addr().unwrap().port();
    let carried = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (net, counting) = (kit.net.clone(), carried.clone());
    let forwarding = tokio::spawn(async move {
        while let Ok((mut socket, _)) = front.accept().await {
            counting.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let bound = *net.ports.lock().unwrap().last().unwrap();
            tokio::spawn(async move {
                if let Ok(mut upstream) = tokio::net::TcpStream::connect(("127.0.0.1", bound)).await {
                    let _ = tokio::io::copy_bidirectional(&mut socket, &mut upstream).await;
                }
            });
        }
    });
    let desk = Device::new(kit.options().listen(colibrii::Listen::new().advertised_port(front_port))).await.unwrap();
    let mut links = welcome_all(&desk);
    let phone = kit.device().await;
    let desk_id = device_id_of(&kit.enroll(&desk, &account, "Desk").await);
    kit.enroll(&phone, &account, "Phone").await;
    sees(&phone, &desk_id).await;
    let _dialed = phone.dial(&desk_id, b"hi").await.unwrap();
    next_link(&mut links).await;
    assert!(winner(&kit).url.ends_with(&format!(":{front_port}")));
    assert!(carried.load(std::sync::atomic::Ordering::SeqCst) > 0);
    forwarding.abort();
}

#[tokio::test]
async fn ends_its_messages_as_it_closes_however_it_closed() {
    let kit = Kit::new();
    let mut pair = pair(&kit).await;
    let dialed = pair.phone.dial(&pair.desk_id, b"hi").await.unwrap();
    let admitted = next_link(&mut pair.links).await;
    dialed.close();
    assert_eq!(timeout(Duration::from_secs(2), dialed.recv()).await, Ok(None));
    assert_eq!(timeout(Duration::from_secs(2), admitted.recv()).await, Ok(None));
}

#[tokio::test]
async fn closes_a_link_once_the_app_lets_go_of_it() {
    let kit = Kit::new();
    let mut pair = pair(&kit).await;
    let dialed = pair.phone.dial(&pair.desk_id, b"hi").await.unwrap();
    let admitted = next_link(&mut pair.links).await;
    drop(dialed);
    assert_eq!(admitted.closed().await, LinkClosed { code: 1000, cause: LinkCloseCause::Peer });
}

#[tokio::test]
async fn lets_a_dialed_link_go_once_it_closed() {
    let kit = Kit::new();
    let pair = pair(&kit).await;
    let dialed = pair.phone.dial(&pair.desk_id, b"hi").await.unwrap();
    let socket = winner(&kit);
    dialed.close();
    socket.closed().await;
    let sent = socket.sent().len();
    // A link still kept would ping at once.
    pair.phone.wake();
    let _again = pair.phone.dial(&pair.desk_id, b"again").await.unwrap();
    assert_eq!(socket.sent().len(), sent);
}

#[tokio::test]
async fn pings_at_once_on_a_wake() {
    let kit = Kit::new();
    let pair = pair(&kit).await;
    let _dialed = pair.phone.dial(&pair.desk_id, b"hi").await.unwrap();
    let socket = winner(&kit);
    let sent = socket.sent().len();
    pair.phone.wake();
    eventually(|| async { socket.sent().len() > sent }).await;
}

#[tokio::test]
async fn gives_the_dialer_the_apps_refusal_and_its_bytes() {
    let kit = Kit::new();
    let pair = pair_with(&kit, &kit, |hello| async move {
        hello.refuse("busy", Some(b"later".to_vec()));
        None
    })
    .await;
    let refused = pair.phone.dial(&pair.desk_id, b"hi").await.unwrap_err();
    assert_eq!(
        refused,
        DialError::Refused { by: RefusedBy::App, reason: "busy".into(), app_refusal: Some(b"later".to_vec()) }
    );
}

/// A socket of the test's own to the listener, which says how it ended:
/// refused at the upgrade with a status, or closed with a code.
async fn raw_socket(url: &str, origin: Option<&str>, first: Option<Vec<u8>>) -> (Option<u16>, Option<u16>) {
    let mut request = url.into_client_request().unwrap();
    if let Some(origin) = origin {
        request.headers_mut().insert("origin", origin.parse().unwrap());
    }
    let mut ws = match tokio_tungstenite::connect_async(request).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => return (Some(response.status().as_u16()), None),
        Err(error) => panic!("{error}"),
        Ok((ws, _)) => ws,
    };
    if let Some(first) = first {
        ws.send(Message::binary(first)).await.unwrap();
    }
    while let Some(Ok(message)) = ws.next().await {
        if let Message::Close(frame) = message {
            return (None, frame.map(|frame| u16::from(frame.code)));
        }
    }
    (None, None)
}

#[tokio::test]
async fn refuses_a_socket_from_an_origin_it_does_not_allow_and_a_ticket_used_before() {
    let kit = Kit::new();
    let pair = pair(&kit).await;
    let _dialed = pair.phone.dial(&pair.desk_id, b"hi").await.unwrap();
    let won = winner(&kit);
    assert_eq!(raw_socket(&won.url, Some("https://elsewhere.example"), None).await, (Some(401), None));
    let Msg::Binary(first) = won.sent()[0].clone() else { panic!("a binary first message") };
    assert_eq!(raw_socket(&won.url, None, Some(first)).await, (None, Some(4004)));
}

#[tokio::test]
async fn closes_when_the_peer_is_removed() {
    let kit = Kit::new();
    let mut pair = pair(&kit).await;
    let dialed = pair.phone.dial(&pair.desk_id, b"hi").await.unwrap();
    let admitted = next_link(&mut pair.links).await;
    pair.desk.devices().remove(&pair.phone_id).await.unwrap();
    // The phone's own stop closes both ends: the desk, still settling,
    // leaves the link to it.
    assert_eq!(dialed.closed().await, LinkClosed { code: 1001, cause: LinkCloseCause::Stopped });
    assert_eq!(admitted.closed().await, LinkClosed { code: 1001, cause: LinkCloseCause::Peer });
    wait_for(&pair.phone.account(), |state| matches!(state, AccountState::SignedOut { .. })).await;
}

#[tokio::test]
async fn drops_a_hello_whose_socket_closed_while_the_app_decided() {
    let kit = Kit::new();
    let (deciding, decided) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (told, waited) = (deciding.clone(), decided.clone());
    let mut pair = pair_with(&kit, &kit, move |hello| {
        let (told, waited) = (told.clone(), waited.clone());
        async move {
            if hello.app_hello == b"gone" {
                told.notify_one();
                waited.notified().await;
            }
            hello.welcome(Vec::new()).await
        }
    })
    .await;
    let kept = pair.phone.dial(&pair.desk_id, b"kept").await.unwrap();
    let admitted = next_link(&mut pair.links).await;
    let phone = Arc::new(pair.phone);
    let dialing = phone.clone();
    let desk_id = pair.desk_id.clone();
    let gone = tokio::spawn(async move { dialing.dial(&desk_id, b"gone").await.map(drop) });
    deciding.notified().await;
    // Closes its sockets.
    gone.abort();
    let _ = gone.await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    decided.notify_one();
    // Welcomed, it would have replaced the phone's link.
    kept.send(b"still").unwrap();
    assert_eq!(admitted.recv().await.unwrap(), b"still");
}

async fn takes_no_link_once_the_roster_gives_another_key_while_the_app_decides(withdrawn: Role) {
    let (desk_kit, phone_kit) = (Kit::new(), Kit::new());
    let (deciding, decided, decide) = held();
    let mut pair = pair_with(&desk_kit, &phone_kit, decide).await;
    let phone = Arc::new(pair.phone);
    let (dialing, desk_id) = (phone.clone(), pair.desk_id.clone());
    let dial = tokio::spawn(async move { dialing.dial(&desk_id, b"hi").await.map(drop) });
    deciding.notified().await;

    // The withdrawn end enrolls again as another copy. The old one, its hub
    // socket deaf, never hears of it and keeps its key and its roster, as a
    // copy left behind would.
    let (stale_kit, stale_id, other): (&Kit, &str, &Device) = match withdrawn {
        Role::Listener => (&desk_kit, &pair.desk_id, &phone),
        Role::Dialer => (&phone_kit, &pair.phone_id, &pair.desk),
    };
    for entry in stale_kit.net.opened.lock().unwrap().iter() {
        entry.set_deaf(entry.url.contains("/connect?"));
    }
    let old_key = other.roster().get().iter().find(|peer| peer.device_id == stale_id).unwrap().public_key;
    let copy = Device::new(Kit::new().options().device_id(stale_id)).await.unwrap();
    desk_kit.enroll(&copy, &pair.account, "Copy").await;
    wait_for(&other.roster(), |peers| {
        peers.iter().any(|peer| peer.device_id == stale_id && peer.public_key != old_key)
    })
    .await;
    decided.notify_one();
    let failed = dial.await.unwrap().unwrap_err();
    match withdrawn {
        Role::Listener => assert_eq!(failed, DialError::Offline),
        Role::Dialer => {
            assert!(matches!(failed, DialError::Refused { by: RefusedBy::Colibrii, .. }), "{failed:?}");
            assert!(pair.links.try_recv().is_err());
        }
    }
}

#[tokio::test]
async fn takes_no_link_once_the_roster_gives_the_listener_another_key_while_the_app_decides() {
    takes_no_link_once_the_roster_gives_another_key_while_the_app_decides(Role::Listener).await;
}

#[tokio::test]
async fn takes_no_link_once_the_roster_gives_the_dialer_another_key_while_the_app_decides() {
    takes_no_link_once_the_roster_gives_another_key_while_the_app_decides(Role::Dialer).await;
}

async fn takes_no_link_while_a_hub_socket_is_down(down: Role) {
    let (desk_kit, phone_kit) = (Kit::new(), Kit::new());
    let (deciding, decided, decide) = held();
    let mut pair = pair_with(&desk_kit, &phone_kit, decide).await;
    let phone = Arc::new(pair.phone);
    let (dialing, desk_id) = (phone.clone(), pair.desk_id.clone());
    let dial = tokio::spawn(async move { dialing.dial(&desk_id, b"hi").await.map(drop) });
    deciding.notified().await;

    // The phone leaving the service would have the desk refuse it, so the
    // desk keeps its roster: its hub socket closes unheard.
    if down == Role::Dialer {
        close_unheard(&desk_kit);
    }
    let (down_kit, down_device): (&Kit, &Device) = match down {
        Role::Listener => (&desk_kit, &pair.desk),
        Role::Dialer => (&phone_kit, &phone),
    };
    *down_kit.net.refusing.lock().unwrap() = true;
    down_kit.net.hub_socket().close(1000, "down");
    wait_for(&down_device.account(), |state| matches!(state, AccountState::Enrolled { connected: false, .. })).await;
    decided.notify_one();
    let failed = dial.await.unwrap().unwrap_err();
    match down {
        Role::Listener => {
            let DialError::Unreachable { failures, .. } = &failed else { panic!("{failed:?}") };
            assert!(failures.iter().any(|failure| failure.close_code == Some(1013)), "{failures:?}");
            assert!(pair.links.try_recv().is_err());
        }
        Role::Dialer => assert_eq!(failed, DialError::NotConnected),
    }
}

#[tokio::test]
async fn takes_no_link_while_the_listeners_hub_socket_is_down() {
    takes_no_link_while_a_hub_socket_is_down(Role::Listener).await;
}

#[tokio::test]
async fn takes_no_link_while_the_dialers_hub_socket_is_down() {
    takes_no_link_while_a_hub_socket_is_down(Role::Dialer).await;
}

#[tokio::test]
async fn takes_a_link_from_a_peer_missing_from_a_roster_not_yet_settled_until_it_settles() {
    let (desk_kit, phone_kit) = (Kit::new(), Kit::new());
    let (deciding, decided, decide) = held();
    let pair = pair_with(&desk_kit, &phone_kit, decide).await;
    let phone = Arc::new(pair.phone);
    let (dialing, desk_id) = (phone.clone(), pair.desk_id.clone());
    let dial = tokio::spawn(async move { dialing.dial(&desk_id, b"hi").await });
    deciding.notified().await;

    // The desk drops off the service while its app decides, as after a
    // restart before it reconnected, and keeps its roster.
    close_unheard(&desk_kit);
    wait_for(&phone.roster(), |peers| !peers.iter().any(|peer| peer.device_id == pair.desk_id)).await;
    assert!(matches!(phone.account().get(), AccountState::Enrolled { connected: true, settled: false, .. }));
    decided.notify_one();
    let link = dial.await.unwrap().unwrap();
    let mut account = phone.account();
    timeout(
        Duration::from_secs(12),
        account.wait_for(|state| matches!(state, AccountState::Enrolled { settled: true, .. })),
    )
    .await
    .unwrap();
    assert_eq!(
        closed_within(&link, Duration::from_secs(1)).await,
        Some(LinkClosed { code: 1001, cause: LinkCloseCause::LeftRoster })
    );
}

#[tokio::test]
async fn ends_a_dial_still_under_way_when_the_device_signs_out() {
    let kit = Kit::new();
    let deciding = Arc::new(Notify::new());
    let told = deciding.clone();
    let pair = pair_with(&kit, &kit, move |hello| {
        let told = told.clone();
        async move {
            told.notify_one();
            std::future::pending::<()>().await;
            drop(hello);
            None
        }
    })
    .await;
    let phone = Arc::new(pair.phone);
    let (dialing, desk_id) = (phone.clone(), pair.desk_id.clone());
    let dial = tokio::spawn(async move { dialing.dial(&desk_id, b"hi").await.map(drop) });
    deciding.notified().await;
    phone.sign_out().await.unwrap();
    assert_eq!(timeout(Duration::from_secs(1), dial).await.unwrap().unwrap(), Err(DialError::NotConnected));
}

#[tokio::test]
async fn keeps_a_link_to_a_peer_missing_from_the_roster_until_the_roster_settled() {
    let (desk_kit, phone_kit) = (Kit::new(), Kit::new());
    let mut pair = pair_with(&desk_kit, &phone_kit, |hello| hello.welcome(Vec::new())).await;
    let _dialed = pair.phone.dial(&pair.desk_id, b"hi").await.unwrap();
    let admitted = next_link(&mut pair.links).await;

    // The phone's hub socket goes down and stays down, as a device still
    // reconnecting after the service restarted.
    *phone_kit.net.refusing.lock().unwrap() = true;
    phone_kit.net.hub_socket().close(1000, "down");
    wait_for(&pair.desk.roster(), |peers| peers.is_empty()).await;
    assert!(matches!(pair.desk.account().get(), AccountState::Enrolled { connected: true, settled: false, .. }));
    assert_eq!(closed_within(&admitted, Duration::from_secs(1)).await, None);
    let mut account = pair.desk.account();
    timeout(
        Duration::from_secs(12),
        account.wait_for(|state| matches!(state, AccountState::Enrolled { settled: true, .. })),
    )
    .await
    .unwrap();
    assert_eq!(
        closed_within(&admitted, Duration::from_secs(1)).await,
        Some(LinkClosed { code: 1001, cause: LinkCloseCause::LeftRoster })
    );
}

#[tokio::test]
async fn closes_when_the_roster_gives_the_peer_another_key() {
    let (desk_kit, phone_kit) = (Kit::new(), Kit::new());
    let account = new_account_id();
    let desk = Device::new(listening(&desk_kit)).await.unwrap();
    let _links = welcome_all(&desk);
    let phone = phone_kit.device().await;
    let desk_id = device_id_of(&desk_kit.enroll(&desk, &account, "Desk").await);
    phone_kit.enroll(&phone, &account, "Phone").await;
    sees(&phone, &desk_id).await;
    wait_for(&desk.roster(), |peers| !peers.is_empty()).await;
    let dialed = phone.dial(&desk_id, b"hi").await.unwrap();

    // The phone's hub socket goes down and stays down while the desk
    // enrolls again, and the desk's close of the link never reaches it, as
    // over a network that dropped.
    for entry in phone_kit.net.opened.lock().unwrap().iter() {
        entry.set_deaf(!entry.url.contains("/connect?"));
    }
    *phone_kit.net.refusing.lock().unwrap() = true;
    phone_kit.net.hub_socket().close(1000, "down");
    wait_for(&phone.account(), |state| matches!(state, AccountState::Enrolled { connected: false, .. })).await;
    desk_kit.enroll(&desk, &account, "Desk").await;
    *phone_kit.net.refusing.lock().unwrap() = false;
    phone.wake();
    assert_eq!(closed_within(&dialed, WAIT).await, Some(LinkClosed { code: 1001, cause: LinkCloseCause::LeftRoster }));
}

// ---- Dialing ----

/// A listener of the test's own, enrolled through the service's API, which
/// answers each ask with whatever candidates it is told to.
struct Liar {
    device_id: String,
    key: KeyPair,
    ws: tokio::sync::Mutex<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    >,
}

async fn post(path: &str, bearer: &str, body: Value) -> Value {
    let response = reqwest::Client::new()
        .post(format!("{}{path}", hub()))
        .header("colibrii-version", "1")
        .header("authorization", format!("Bearer {bearer}"))
        .header("cf-connecting-ip", new_ip())
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "{path} answered {}", response.status());
    serde_json::from_slice(&response.bytes().await.unwrap()).unwrap()
}

/// `public_key` enrolls it under a key that is not its own.
async fn lying_listener(account: &str, public_key: Option<[u8; 32]>) -> Liar {
    let key = KeyPair::generate();
    let device_id = new_device_id();
    let enrolled = post(
        "/devices/enroll",
        &session_token(account),
        json!({
            "app": "inkwell",
            "deviceId": device_id,
            "name": "Liar",
            "publicKey": base64url::encode(&public_key.unwrap_or(key.public_key)),
            "connections": "single",
        }),
    )
    .await;
    let credential = enrolled["credential"].as_str().unwrap();
    let ticket =
        post("/tickets", credential, json!({ "connectionId": colibrii::wire::ids::new_connection_id() })).await;
    let url = format!("{}/connect?ticket={}", hub().replacen("http", "ws", 1), ticket["ticket"].as_str().unwrap());
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert("cf-connecting-ip", new_ip().parse().unwrap());
    let (ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    Liar { device_id, key, ws: tokio::sync::Mutex::new(ws) }
}

impl Liar {
    async fn answer_next(&self, candidates: Value) {
        let mut ws = self.ws.lock().await;
        loop {
            let Some(Ok(Message::Text(text))) = ws.next().await else { continue };
            let message: Value = serde_json::from_str(&text).unwrap();
            if message["t"] != "relay" {
                continue;
            }
            let from = message["from"].as_str().unwrap();
            let frame = decode_relay_frame(message["frame"].as_str().unwrap()).unwrap();
            let mut opened = open_ask(from, &self.device_id, &self.key, &frame.sealed, None).unwrap();
            let answer = json!({ "ok": true, "result": { "available": true, "candidates": candidates } });
            let sealed = seal_answer(&mut opened.handshake, answer.to_string().as_bytes()).unwrap();
            let frame = encode_relay_frame(&RelayFrame { kind: FrameKind::Answer, id: frame.id, sealed });
            ws.send(Message::text(encode_relay(from, &frame))).await.unwrap();
            return;
        }
    }
}

fn lan(url: &str) -> Value {
    json!({ "kind": "lan", "url": url, "ticket": make_link_ticket() })
}

fn tunnel(url: &str) -> Value {
    json!({ "kind": "tunnel", "url": url, "ticket": make_link_ticket() })
}

/// What the dial failed with, and every candidate URL it tried.
async fn answer(kit: &Kit, dialer: &Device, liar: &Liar, candidates: Value) -> (DialError, Vec<String>) {
    let before = kit.net.tried.lock().unwrap().len();
    let (failed, ()) = tokio::join!(dialer.dial(&liar.device_id, b"hi"), liar.answer_next(candidates));
    let tried =
        kit.net.tried.lock().unwrap()[before..].iter().filter(|url| !url.contains("/connect?")).cloned().collect();
    (failed.unwrap_err(), tried)
}

struct Liars {
    liar: Liar,
    phone_kit: Kit,
    phone: Device,
    page_kit: Kit,
    page: Device,
}

async fn liars() -> Liars {
    let account = new_account_id();
    let liar = lying_listener(&account, None).await;
    let phone_kit = Kit::new();
    let phone = phone_kit.device().await;
    let page_kit = Kit::new();
    *page_kit.net.dialable.lock().unwrap() = vec![CandidateKind::Tunnel];
    let page = page_kit.device().await;
    phone_kit.enroll(&phone, &account, "Phone").await;
    page_kit.enroll(&page, &account, "Page").await;
    sees(&phone, &liar.device_id).await;
    sees(&page, &liar.device_id).await;
    Liars { liar, phone_kit, phone, page_kit, page }
}

#[tokio::test]
async fn fails_on_a_candidate_the_spec_does_not_allow_and_opens_nothing() {
    let Liars { liar, phone_kit, phone, page_kit, page } = liars().await;
    let seven: Vec<Value> = (0..7).map(|index| lan(&format!("ws://10.0.0.{index}:1"))).collect();
    for candidates in [
        json!([lan("ws://desk.example:41734")]),
        json!([lan("ws://192.168.1.20")]),
        json!([lan("ws://192.168.1.20:41734/")]),
        json!([lan("ws://me@192.168.1.20:41734")]),
        json!([lan("ws://192.168.1.256:41734")]),
        json!([lan("ws://[2001:db8::20:41734")]),
        json!([lan("wss://192.168.1.20:41734")]),
        json!([tunnel("wss://desk.example.link/path")]),
        json!([tunnel("wss://desk.example.link?x=1")]),
        json!([tunnel("wss://desk.example.link#x")]),
        json!([tunnel("wss://desk.example.link:8443")]),
        json!([tunnel("ws://desk.example.link")]),
        json!([lan("wss://desk.example.link")]),
        json!(seven),
        json!([tunnel("wss://a.example.link"), tunnel("wss://b.example.link")]),
    ] {
        let (failed, tried) = answer(&phone_kit, &phone, &liar, candidates.clone()).await;
        assert!(matches!(failed, DialError::Unreachable { .. }), "{candidates}: {failed:?}");
        assert_eq!(tried, Vec::<String>::new());
    }
    let (failed, tried) = answer(&page_kit, &page, &liar, json!([lan("ws://192.168.1.20:41734")])).await;
    assert_eq!((failed, tried), (DialError::Unavailable, Vec::new()));
}

#[tokio::test]
async fn skips_a_candidate_of_a_kind_it_does_not_know_or_did_not_ask_for_and_dials_the_rest() {
    let Liars { liar, phone_kit, phone, page_kit, page } = liars().await;
    let later = json!({ "kind": "carrier-pigeon", "url": "coo://192.168.1.20", "ticket": "a ticket of another form" });
    let (_, tried) = answer(&phone_kit, &phone, &liar, json!([later, lan("ws://127.0.0.1:1"), "a later form"])).await;
    assert_eq!(tried, vec!["ws://127.0.0.1:1".to_owned()]);
    let (_, tried) =
        answer(&page_kit, &page, &liar, json!([lan("ws://127.0.0.1:1"), tunnel("wss://desk.invalid"), later])).await;
    assert_eq!(tried, vec!["wss://desk.invalid".to_owned()]);
}

#[tokio::test]
async fn fails_a_dial_to_a_device_whose_roster_key_no_handshake_can_use() {
    let kit = Kit::new();
    let account = new_account_id();
    // All zeros is a point every X25519 exchange refuses.
    let liar = lying_listener(&account, Some([0; 32])).await;
    let phone = kit.device().await;
    kit.enroll(&phone, &account, "Phone").await;
    sees(&phone, &liar.device_id).await;
    let failed = phone.dial(&liar.device_id, b"hi").await.unwrap_err();
    assert!(matches!(failed, DialError::Unreachable { .. }), "{failed:?}");
}

#[tokio::test]
async fn answers_no_listener_from_a_device_that_does_not_listen() {
    let kit = Kit::new();
    let account = new_account_id();
    let desk = kit.device().await;
    let phone = kit.device().await;
    let desk_id = device_id_of(&kit.enroll(&desk, &account, "Desk").await);
    kit.enroll(&phone, &account, "Phone").await;
    wait_for(&phone.roster(), |peers| !peers.is_empty()).await;
    assert_eq!(phone.dial(&desk_id, b"hi").await.unwrap_err(), DialError::NoListener);
}

#[tokio::test]
async fn refuses_an_app_hello_over_16_kib() {
    let kit = Kit::new();
    let phone = kit.device().await;
    let refused = phone.dial(&new_device_id(), &vec![0; 16_385]).await.unwrap_err();
    assert_eq!(refused, DialError::InvalidInput(colibrii::InvalidInput::AppHello));
}

#[tokio::test]
async fn the_listener_closes_a_socket_that_sends_a_message_past_the_cap_before_any_handshake() {
    let kit = Kit::new();
    let desk = Device::new(listening(&kit)).await.unwrap();
    kit.enroll(&desk, &new_account_id(), "Desk").await;
    let port = *kit.net.ports.lock().unwrap().last().unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}")).await.unwrap();
    ws.send(Message::binary(vec![0u8; 70_000])).await.unwrap();
    let mut code = None;
    while let Some(Ok(message)) = ws.next().await {
        if let Message::Close(frame) = message {
            code = frame.map(|frame| u16::from(frame.code));
            break;
        }
    }
    assert_eq!(code, Some(1009));
}
