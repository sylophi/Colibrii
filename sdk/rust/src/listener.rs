//! The listener (spec/link.md, "Admission"), and this device's answer to a
//! connectInfo ask (spec/relay.md, "connectInfo").

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use crate::conn::{Conn, Msg};
use crate::device::{Inner, TunnelState};
use crate::hub_socket::{Identity, Join, Shared};
use crate::link::{LiveLink, NewLink, Role};
use crate::tunnel::RunningTunnel;
use crate::wire::link::{
    CLOSE_GOING_AWAY, CLOSE_HANDSHAKE_REFUSED, CLOSE_HELLO_REFUSED, CLOSE_LOCKED_OUT, CLOSE_PROTOCOL_ERROR,
    CLOSE_TRY_AGAIN, LOCKOUT_GUESSES, LOCKOUT_MS, MAX_APP_BYTES, MAX_SOCKETS, MAX_UNWELCOMED_SOCKETS, Refuse,
    TransportMessage, WELCOME_WITHIN_MS, Welcome, answer_handshake, decode_first_message, fits_transport_message,
    open_transport_message, seal_transport_message,
};
use crate::wire::noise::{MAX_MESSAGE_BYTES, Transport};
use crate::wire::relay::{
    Candidate, CandidateKind, ConnectInfoInput, ConnectInfoResult, MAX_LAN_CANDIDATES, TUNNEL_WAIT_MS,
};
use crate::wire::same_key;
use crate::{Link, tickets::Check, tickets::LinkTickets};

/// How the device listens for links from the person's other devices.
#[derive(Debug, Clone, Default)]
pub struct Listen {
    pub(crate) origins: Vec<String>,
    pub(crate) advertised_port: Option<u16>,
}

impl Listen {
    pub fn new() -> Listen {
        Listen::default()
    }

    /// The browser origins that may dial, such as the app's web client's. A
    /// dialer that is not a browser sends none, and is let in.
    pub fn origins(mut self, origins: impl IntoIterator<Item = impl Into<String>>) -> Listen {
        self.origins = origins.into_iter().map(Into::into).collect();
        self
    }

    /// The port peers reach the listener at, when something on this
    /// machine (a proxy, a port forward) takes connections there and passes
    /// them on to the port the listener bound: the LAN candidates name it,
    /// and the tunnel routes to it.
    pub fn advertised_port(mut self, port: u16) -> Listen {
        self.advertised_port = Some(port);
        self
    }
}

enum Decision {
    Welcome(Vec<u8>, oneshot::Sender<Link>),
    Refuse { reason: String, app_refusal: Option<Vec<u8>> },
}

/// A peer's hello, for the app to welcome or refuse. Dropping it refuses
/// it. The dialer has 10 seconds from its upgrade to the welcome.
pub struct IncomingHello {
    pub device_id: String,
    pub connection_id: String,
    pub arrival: CandidateKind,
    pub app_hello: Vec<u8>,
    decide: oneshot::Sender<Decision>,
}

impl IncomingHello {
    /// Welcomes the link with the app welcome, of at most 16 KiB. The link,
    /// unless it closed or its peer left the roster while the app decided.
    pub async fn welcome(self, app_welcome: Vec<u8>) -> Option<Link> {
        let (linked, link) = oneshot::channel();
        self.decide.send(Decision::Welcome(app_welcome, linked)).ok()?;
        link.await.ok()
    }

    /// Refuses the link, with the app's own bytes for the dialer, of at most
    /// 16 KiB.
    pub fn refuse(self, reason: impl Into<String>, app_refusal: Option<Vec<u8>>) {
        let _ = self.decide.send(Decision::Refuse { reason: reason.into(), app_refusal });
    }
}

impl std::fmt::Debug for IncomingHello {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingHello")
            .field("device_id", &self.device_id)
            .field("connection_id", &self.connection_id)
            .field("arrival", &self.arrival)
            .finish()
    }
}

fn wall_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |since| since.as_millis() as u64)
}

fn is_link_local(address: &IpAddr) -> bool {
    match address {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => v6.segments()[0] & 0xffc0 == 0xfe80,
    }
}

/// The LAN candidates' addresses: not loopback or link-local, IPv4 first,
/// at most six.
pub(crate) fn lan_addresses(found: &[IpAddr]) -> Vec<String> {
    let usable = found.iter().filter(|address| !address.is_loopback() && !is_link_local(address));
    let v4 = usable.clone().filter(|address| address.is_ipv4()).map(IpAddr::to_string);
    let v6 = usable.filter(|address| address.is_ipv6()).map(|address| format!("[{address}]"));
    v4.chain(v6).take(MAX_LAN_CANDIDATES).collect()
}

fn interface_addresses() -> Vec<IpAddr> {
    if_addrs::get_if_addrs().map(|found| found.into_iter().map(|entry| entry.ip()).collect()).unwrap_or_default()
}

#[derive(Default)]
struct State {
    tickets: LinkTickets,
    /// Guesses in a row by address, and when the last one was.
    guesses: HashMap<String, (u32, Instant)>,
    /// The sockets not yet welcomed, with the device their ticket names once
    /// it checked out.
    unwelcomed: HashMap<u64, (Arc<Conn>, Option<String>)>,
    /// By device, the asks in a row offered the tunnel since a ticket of its
    /// last reached the listener.
    unreached: HashMap<String, u32>,
    open: usize,
    next_socket: u64,
}

impl State {
    fn locked_out(&mut self, address: &str, now: Instant) -> bool {
        let Some((count, at)) = self.guesses.get(address).copied() else { return false };
        if now - at >= Duration::from_millis(LOCKOUT_MS) {
            self.guesses.remove(address);
            return false;
        }
        count >= LOCKOUT_GUESSES
    }

    fn guessed(&mut self, address: &str) {
        let now = Instant::now();
        self.guesses.retain(|_, (_, at)| now - *at < Duration::from_millis(LOCKOUT_MS));
        let count = self.guesses.get(address).map_or(0, |(count, _)| *count);
        self.guesses.insert(address.to_owned(), (count + 1, now));
    }
}

// A peer that asks again after this many asks that offered the tunnel and
// never reached the listener has the tunnel checked (spec/tunnels.md,
// "Checking again").
const UNREACHED_ASKS: u32 = 2;

pub(crate) struct Listener {
    pub port: u16,
    /// The port peers reach it at, which the tunnel routes to.
    pub reached_at: u16,
    pub tunnel: std::sync::OnceLock<Arc<RunningTunnel>>,
    inner: Arc<Inner>,
    identity: Arc<Identity>,
    shared: Arc<Shared>,
    origins: Vec<String>,
    state: Mutex<State>,
}

/// The listener on every interface, at `port` or, when that is taken or
/// 0, at any.
async fn bind(port: u16) -> std::io::Result<Vec<TcpListener>> {
    let v6 = TcpListener::bind(SocketAddr::from((Ipv6Addr::UNSPECIFIED, port))).await;
    let v6 = match v6 {
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => return Err(error),
        other => other.ok(),
    };
    let Some(v6) = v6 else {
        return Ok(vec![TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port))).await?]);
    };
    // Elsewhere an IPv6 socket takes IPv4 as well.
    if cfg!(windows) {
        let bound = v6.local_addr()?.port();
        let v4 = TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, bound))).await?;
        return Ok(vec![v6, v4]);
    }
    Ok(vec![v6])
}

/// A socket arrived from the loopback interface carrying a
/// `CF-Connecting-IP` header came through the tunnel, and its address is
/// that header's.
struct Arrival {
    address: String,
    kind: CandidateKind,
}

impl Listener {
    pub async fn start(
        inner: Arc<Inner>,
        identity: Arc<Identity>,
        shared: Arc<Shared>,
        listen: &crate::listener::Listen,
        remembered: u16,
    ) -> std::io::Result<Arc<Listener>> {
        let sockets = match bind(remembered).await {
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse && remembered != 0 => bind(0).await?,
            other => other?,
        };
        let port = sockets[0].local_addr()?.port();
        inner.options.hooks.bound(port);
        let listener = Arc::new(Listener {
            port,
            reached_at: listen.advertised_port.unwrap_or(port),
            tunnel: std::sync::OnceLock::new(),
            inner,
            identity,
            shared: shared.clone(),
            origins: listen.origins.clone(),
            state: Mutex::new(State::default()),
        });
        for socket in sockets {
            let serving = listener.clone();
            shared.spawn(async move {
                while let Ok((stream, remote)) = socket.accept().await {
                    let serving_one = serving.clone();
                    serving.shared.spawn(async move { serving_one.serve(stream, remote.ip()).await });
                }
            });
        }
        // Once the roster settled, a peer that left it spends no ticket, and
        // its sockets not yet welcomed close. Its links close in the
        // session's own sweep.
        let mut roster = shared.roster.subscribe();
        let sweeping = listener.clone();
        shared.spawn(async move {
            loop {
                let online: Option<HashSet<String>> = roster
                    .borrow_and_update()
                    .as_ref()
                    .filter(|roster| roster.settled)
                    .map(|roster| roster.peers.iter().map(|peer| peer.device_id.clone()).collect());
                if let Some(online) = online {
                    let mut state = sweeping.state.lock().unwrap();
                    state.tickets.keep_devices(&online);
                    state.unreached.retain(|device_id, _| online.contains(device_id));
                    for (conn, device_id) in state.unwelcomed.values() {
                        if device_id.as_ref().is_some_and(|device_id| !online.contains(device_id)) {
                            conn.close(CLOSE_GOING_AWAY, "left the roster");
                        }
                    }
                }
                if roster.changed().await.is_err() {
                    return;
                }
            }
        });
        Ok(listener)
    }

    // The refusal's type is tungstenite's.
    #[allow(clippy::result_large_err)]
    async fn upgrade(&self, stream: TcpStream) -> Option<(Conn, Option<String>)> {
        let mut connecting_ip = None;
        let origins = &self.origins;
        let check = |request: &Request, response: Response| {
            let header = |name: &str| request.headers().get(name).and_then(|value| value.to_str().ok());
            connecting_ip = header("cf-connecting-ip").map(str::to_owned);
            match header("origin") {
                Some(origin) if !origins.iter().any(|allowed| allowed == origin) => {
                    let mut refused = ErrorResponse::new(None);
                    *refused.status_mut() = StatusCode::UNAUTHORIZED;
                    Err(refused)
                }
                _ => Ok(response),
            }
        };
        // A peer's message past Noise's cap closes the socket with 1009.
        let config = WebSocketConfig::default()
            .max_message_size(Some(MAX_MESSAGE_BYTES + 1))
            .max_frame_size(Some(MAX_MESSAGE_BYTES + 1));
        let ws = tokio_tungstenite::accept_hdr_async_with_config(stream, check, Some(config)).await.ok()?;
        Some((Conn::accepted(ws, &self.inner.tasks), connecting_ip))
    }

    async fn serve(self: Arc<Self>, stream: TcpStream, remote: IpAddr) {
        let welcome_within = Duration::from_millis(WELCOME_WITHIN_MS);
        let Ok(Some((conn, connecting_ip))) = timeout(welcome_within, self.upgrade(stream)).await else { return };
        let conn = Arc::new(conn);
        let remote = remote.to_canonical();
        let arrival = match connecting_ip {
            Some(address) if remote.is_loopback() => Arrival { address, kind: CandidateKind::Tunnel },
            _ => Arrival { address: remote.to_string(), kind: CandidateKind::Lan },
        };
        let id = {
            let mut state = self.state.lock().unwrap();
            if state.open >= MAX_SOCKETS || state.unwelcomed.len() >= MAX_UNWELCOMED_SOCKETS {
                return conn.close(CLOSE_TRY_AGAIN, "over capacity");
            }
            if state.locked_out(&arrival.address, Instant::now()) {
                return conn.close(CLOSE_LOCKED_OUT, "locked out");
            }
            state.open += 1;
            state.next_socket += 1;
            let id = state.next_socket;
            state.unwelcomed.insert(id, (conn.clone(), None));
            id
        };
        struct Counted<'a>(&'a Listener, u64);
        impl Drop for Counted<'_> {
            fn drop(&mut self) {
                let mut state = self.0.state.lock().unwrap();
                state.open -= 1;
                state.unwelcomed.remove(&self.1);
            }
        }
        let _counted = Counted(&self, id);
        let Ok(admitted) = timeout(welcome_within, self.admit(&conn, id, &arrival)).await else {
            return conn.close(CLOSE_HELLO_REFUSED, "too late");
        };
        if let Some(link) = admitted {
            self.state.lock().unwrap().unwelcomed.remove(&id);
            link.ended().await;
        }
    }

    fn refuse_handshake(&self, conn: &Conn, address: Option<&str>) {
        if let Some(address) = address {
            self.state.lock().unwrap().guessed(address);
        }
        conn.close(CLOSE_HANDSHAKE_REFUSED, "refused");
    }

    /// From the upgrade to the welcome: the link, or nothing once the
    /// socket was closed.
    async fn admit(&self, conn: &Arc<Conn>, id: u64, arrival: &Arrival) -> Option<Arc<LiveLink>> {
        let refuse_with = |transport: &mut Transport, refuse: Refuse| {
            if let Ok(sealed) = seal_transport_message(transport, &TransportMessage::Refuse(refuse)) {
                conn.send(Msg::Binary(sealed));
            }
            conn.close(CLOSE_HELLO_REFUSED, "refused");
        };
        let colibrii = |reason: &str| Refuse::Colibrii { reason: reason.to_owned() };
        let address = Some(arrival.address.as_str());

        let first = conn.receive().await.ok()?;
        let Some((ticket, message)) = (match &first {
            Msg::Binary(bytes) => decode_first_message(bytes),
            Msg::Text(_) => None,
        }) else {
            self.refuse_handshake(conn, address);
            return None;
        };
        if self.shared.roster().is_none() {
            conn.close(CLOSE_TRY_AGAIN, "roster unavailable");
            return None;
        }
        let check = self.state.lock().unwrap().tickets.check(&ticket, arrival.kind, wall_ms());
        let Check::Pending { device_id, connection_id } = check else {
            self.refuse_handshake(conn, (check == Check::Unknown).then_some(arrival.address.as_str()));
            return None;
        };
        {
            let mut state = self.state.lock().unwrap();
            if let Some(entry) = state.unwelcomed.get_mut(&id) {
                entry.1 = Some(device_id.clone());
            }
            state.unreached.remove(&device_id);
        }
        let peer = self.shared.peer(&device_id);
        let opened = answer_handshake(&ticket, message, &self.identity.key, None);
        let (Some(peer), Some(opened)) = (peer, opened) else {
            self.refuse_handshake(conn, address);
            return None;
        };
        if !same_key(&opened.dialer_key, &peer.public_key) {
            self.refuse_handshake(conn, address);
            return None;
        }
        let (dialer_key, mut transport) = (opened.dialer_key, opened.transport);
        conn.send(Msg::Binary(opened.second));

        let said = match conn.receive().await.ok()? {
            Msg::Binary(bytes) => open_transport_message(&mut transport, &bytes),
            Msg::Text(_) => None,
        };
        let Some(TransportMessage::Hello(hello)) = said else {
            conn.close(CLOSE_PROTOCOL_ERROR, "expected hello");
            return None;
        };
        if hello.device_id != device_id || hello.connection_id != connection_id {
            refuse_with(&mut transport, colibrii("the hello does not match its ticket"));
            return None;
        }
        let Some(roster) = self.shared.roster() else {
            conn.close(CLOSE_TRY_AGAIN, "roster unavailable");
            return None;
        };
        let Some(current) = roster.get(&hello.device_id).filter(|current| same_key(&current.public_key, &dialer_key))
        else {
            refuse_with(&mut transport, colibrii("the device is not in the roster"));
            return None;
        };
        let connections = current.connections;
        if !self.state.lock().unwrap().tickets.spend(&ticket, arrival.kind, wall_ms()) {
            refuse_with(&mut transport, colibrii("the ticket expired"));
            return None;
        }
        let (decide, decided) = oneshot::channel();
        let _ = self.inner.incoming.0.send(IncomingHello {
            device_id: hello.device_id.clone(),
            connection_id: hello.connection_id.clone(),
            arrival: arrival.kind,
            app_hello: hello.app_hello.clone(),
            decide,
        });
        let decision = decided.await.unwrap_or(Decision::Refuse { reason: "refused".into(), app_refusal: None });
        // The peer went, or left the roster, while the app decided.
        if !conn.is_open() {
            return None;
        }
        let (app_welcome, linked) = match decision {
            Decision::Welcome(app_welcome, _) if app_welcome.len() > MAX_APP_BYTES => {
                refuse_with(&mut transport, colibrii("the app's welcome is over 16 KiB"));
                return None;
            }
            Decision::Refuse { app_refusal: Some(bytes), .. } if bytes.len() > MAX_APP_BYTES => {
                refuse_with(&mut transport, colibrii("the app's refusal is over 16 KiB"));
                return None;
            }
            Decision::Refuse { reason, app_refusal } => {
                let refuse = Refuse::App { reason, app_refusal };
                let fits = fits_transport_message(&TransportMessage::Refuse(refuse.clone()));
                refuse_with(&mut transport, if fits { refuse } else { colibrii("the app's refusal is too long") });
                return None;
            }
            Decision::Welcome(app_welcome, linked) => (app_welcome, linked),
        };
        let link = LiveLink::new(NewLink {
            conn: conn.clone(),
            transport,
            role: Role::Listener,
            device_id: hello.device_id.clone(),
            peer_key: dialer_key,
            connection_id: hello.connection_id.clone(),
            arrival: arrival.kind,
            greeting: hello.app_hello,
        });
        // The roster may have moved on, or gone, while the app decided.
        match self.shared.join(&link) {
            Join::Unavailable => {
                self.state.lock().unwrap().tickets.unspend(&ticket);
                conn.close(CLOSE_TRY_AGAIN, "roster unavailable");
                return None;
            }
            Join::Refused => {
                link.send_message(&TransportMessage::Refuse(colibrii("the device is not in the roster")));
                conn.close(CLOSE_HELLO_REFUSED, "refused");
                return None;
            }
            Join::Joined => {}
        }
        self.shared.replace(&link, connections);
        link.send_message(&TransportMessage::Welcome(Welcome { app_welcome }));
        link.run(&self.inner);
        self.state.lock().unwrap().guesses.remove(&arrival.address);
        let _ = linked.send(Link::of(link.clone()));
        Some(link)
    }

    /// This device's answer to a peer's connectInfo ask. An asker that can
    /// dial the tunnel and would get nothing waits a little for a tunnel
    /// that is starting.
    pub async fn connect_info(&self, from: &str, input: &ConnectInfoInput) -> ConnectInfoResult {
        let Some(tunnel) = self.tunnel.get() else { return self.offer(from, input) };
        if self.state.lock().unwrap().unreached.get(from) == Some(&UNREACHED_ASKS) {
            tunnel.check();
        }
        let now = self.offer(from, input);
        if matches!(now, ConnectInfoResult::Available(_)) || !input.dialable_kinds.contains(&CandidateKind::Tunnel) {
            return now;
        }
        let mut state = tunnel.state.subscribe();
        let settled = timeout(
            Duration::from_millis(TUNNEL_WAIT_MS),
            state.wait_for(|state| !matches!(state, TunnelState::Starting)),
        )
        .await;
        match settled {
            Ok(Ok(state)) if matches!(*state, TunnelState::Up { .. }) => {
                drop(state);
                self.offer(from, input)
            }
            _ => now,
        }
    }

    fn offer(&self, from: &str, input: &ConnectInfoInput) -> ConnectInfoResult {
        let mut candidates: Vec<(CandidateKind, String)> = Vec::new();
        if input.dialable_kinds.contains(&CandidateKind::Lan) {
            let found = self.inner.options.hooks.addresses(interface_addresses());
            for address in lan_addresses(&found) {
                candidates.push((CandidateKind::Lan, format!("ws://{address}:{}", self.reached_at)));
            }
        }
        if input.dialable_kinds.contains(&CandidateKind::Tunnel)
            && let Some(tunnel) = self.tunnel.get()
            && let TunnelState::Up { hostname } = &*tunnel.state.borrow()
        {
            candidates.push((CandidateKind::Tunnel, format!("wss://{hostname}")));
        }
        if candidates.is_empty() {
            return ConnectInfoResult::Unavailable;
        }
        let kinds: Vec<CandidateKind> = candidates.iter().map(|(kind, _)| *kind).collect();
        let mut state = self.state.lock().unwrap();
        let Some(minted) = state.tickets.mint(from, &input.connection_id, &kinds, wall_ms()) else {
            return ConnectInfoResult::Unavailable;
        };
        if kinds.contains(&CandidateKind::Tunnel) {
            *state.unreached.entry(from.to_owned()).or_default() += 1;
        }
        ConnectInfoResult::Available(
            candidates.into_iter().zip(minted).map(|((kind, url), ticket)| Candidate { kind, url, ticket }).collect(),
        )
    }
}
