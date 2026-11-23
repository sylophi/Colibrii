//! The listener (spec/link.md, "Admission"), and this device's answer to a
//! connectInfo ask (spec/relay.md, "connectInfo").

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, oneshot};
use tokio::time::{Instant, timeout};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};

use crate::conn::{CLOSE_GRACE, Conn, Msg};
use crate::device::{Inner, TunnelState};
use crate::hub_socket::{Identity, Join, Shared};
use crate::link::{LiveLink, NewLink, Role, send_sealed};
use crate::tunnel::RunningTunnel;
use crate::wire::link::{
    CLOSE_GOING_AWAY, CLOSE_HANDSHAKE_REFUSED, CLOSE_HELLO_REFUSED, CLOSE_LOCKED_OUT, CLOSE_PROTOCOL_ERROR,
    CLOSE_TRY_AGAIN, LOCKOUT_GUESSES, LOCKOUT_MS, MAX_APP_BYTES, MAX_SOCKETS, MAX_UNWELCOMED_SOCKETS, Refuse,
    TransportMessage, WELCOME_WITHIN_MS, Welcome, answer_handshake, decode_first_message, fits_transport_message,
    open_transport_message,
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

/// The hellos waiting for the app, each only while its dialer still waits
/// for the answer, so at most one per socket not yet welcomed.
#[derive(Default)]
pub(crate) struct Hellos {
    waiting: Mutex<VecDeque<IncomingHello>>,
    added: Notify,
}

impl Hellos {
    fn add(&self, hello: IncomingHello) {
        let mut waiting = self.waiting.lock().unwrap();
        waiting.retain(|waiting| !waiting.decide.is_closed());
        waiting.push_back(hello);
        drop(waiting);
        self.added.notify_waiters();
    }

    /// The oldest hello whose dialer still waits.
    pub async fn next(&self) -> IncomingHello {
        loop {
            let added = self.added.notified();
            {
                let mut waiting = self.waiting.lock().unwrap();
                while let Some(hello) = waiting.pop_front() {
                    if !hello.decide.is_closed() {
                        return hello;
                    }
                }
            }
            added.await;
        }
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
    /// Connections not yet past their upgrade, which hold no slot yet.
    upgrading: usize,
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
                    // As many upgrade at once as may wait for a welcome.
                    {
                        let mut state = serving.state.lock().unwrap();
                        if state.upgrading >= MAX_UNWELCOMED_SOCKETS {
                            continue;
                        }
                        state.upgrading += 1;
                    }
                    let serving_one = serving.clone();
                    serving.shared.spawn(async move { serving_one.serve(stream, remote.ip()).await });
                }
            });
        }
        // Once the roster settled, a peer that left it spends no ticket, and
        // its sockets not yet welcomed close.
        let sweeping = Arc::downgrade(&listener);
        shared.sweep_settled(Box::new(move |online| {
            let Some(listener) = sweeping.upgrade() else { return };
            let mut state = listener.state.lock().unwrap();
            state.tickets.keep_devices(online);
            state.unreached.retain(|device_id, _| online.contains(device_id));
            for (conn, device_id) in state.unwelcomed.values() {
                if device_id.as_ref().is_some_and(|device_id| !online.contains(device_id)) {
                    conn.close(CLOSE_GOING_AWAY, "left the roster");
                }
            }
        }));
        Ok(listener)
    }

    // The refusal's type is tungstenite's.
    #[allow(clippy::result_large_err)]
    async fn upgrade(&self, stream: TcpStream) -> Option<(WebSocketStream<TcpStream>, Option<String>)> {
        let mut connecting_ip = None;
        let origins = &self.origins;
        let check = |request: &Request, response: Response| {
            let header = |name: &str| request.headers().get(name).map(|value| value.to_str().ok());
            connecting_ip = header("cf-connecting-ip").flatten().map(str::to_owned);
            match header("origin") {
                Some(origin) if !origin.is_some_and(|origin| origins.iter().any(|allowed| allowed == origin)) => {
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
        Some((ws, connecting_ip))
    }

    async fn serve(self: Arc<Self>, stream: TcpStream, remote: IpAddr) {
        let welcome_within = Duration::from_millis(WELCOME_WITHIN_MS);
        let upgraded = timeout(welcome_within, self.upgrade(stream)).await;
        self.state.lock().unwrap().upgrading -= 1;
        let Ok(Some((ws, connecting_ip))) = upgraded else { return };
        let remote = remote.to_canonical();
        let arrival = match connecting_ip {
            Some(address) if remote.is_loopback() => Arrival { address, kind: CandidateKind::Tunnel },
            _ => Arrival { address: remote.to_string(), kind: CandidateKind::Lan },
        };
        // The slot comes before the socket's driver, which a refused one
        // never gets.
        let slot = {
            let mut state = self.state.lock().unwrap();
            if state.open >= MAX_SOCKETS || state.unwelcomed.len() >= MAX_UNWELCOMED_SOCKETS {
                Err((ws, CLOSE_TRY_AGAIN, "over capacity"))
            } else if state.locked_out(&arrival.address, Instant::now()) {
                Err((ws, CLOSE_LOCKED_OUT, "locked out"))
            } else {
                state.open += 1;
                state.next_socket += 1;
                let (id, conn) = (state.next_socket, Arc::new(Conn::accepted(ws, &self.inner.tasks)));
                state.unwelcomed.insert(id, (conn.clone(), None));
                Ok((id, conn))
            }
        };
        let (id, conn) = match slot {
            Ok(slot) => slot,
            Err((mut ws, code, reason)) => {
                let frame = CloseFrame { code: CloseCode::from(code), reason: reason.into() };
                let _ = timeout(CLOSE_GRACE, ws.close(Some(frame))).await;
                return;
            }
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
            send_sealed(conn, transport, &TransportMessage::Refuse(refuse));
            conn.close(CLOSE_HELLO_REFUSED, "refused");
        };
        let colibrii = |reason: &str| Refuse::Colibrii { reason: reason.to_owned() };
        let address = Some(arrival.address.as_str());

        let first = conn.receive().await.ok()?;
        let Some((ticket, message)) = first.as_binary().and_then(decode_first_message) else {
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

        let said =
            conn.receive().await.ok()?.as_binary().and_then(|bytes| open_transport_message(&mut transport, bytes));
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
        self.inner.incoming.add(IncomingHello {
            device_id: hello.device_id.clone(),
            connection_id: hello.connection_id.clone(),
            arrival: arrival.kind,
            app_hello: hello.app_hello.clone(),
            decide,
        });
        // The peer went, or left the roster, while the app decided.
        let decision = tokio::select! {
            decision = decided => decision.unwrap_or(Decision::Refuse { reason: "refused".into(), app_refusal: None }),
            _ = conn.closed() => return None,
        };
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
        match self.shared.join(&link, Some(connections)) {
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
            for address in lan_addresses(&interface_addresses()) {
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

#[cfg(test)]
mod tests {
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::DuplexStream;
    use tokio::task::JoinHandle;
    use tokio_tungstenite::WebSocketStream;
    use tokio_tungstenite::tungstenite::Message;

    use super::*;
    use crate::Device;
    use crate::fakes;
    use crate::hub_socket::FakeAttempt;
    use crate::wire::link::{Hello, finish_handshake, seal_transport_message, start_handshake};
    use crate::wire::noise::KeyPair;

    /// A listener whose settled roster names one peer.
    struct Listening {
        _device: Device,
        listener: Arc<Listener>,
        shared: Arc<Shared>,
        attempt: FakeAttempt,
        peer: String,
        peer_key: KeyPair,
    }

    impl Drop for Listening {
        fn drop(&mut self) {
            self.shared.ended.cancel();
        }
    }

    async fn listening() -> Listening {
        let device = fakes::device().await;
        let inner = device.inner().clone();
        let identity =
            Arc::new(Identity { device_id: fakes::ids().0, credential: "credential".into(), key: KeyPair::generate() });
        let shared = Shared::new();
        let listener = Listener::start(inner.clone(), identity, shared.clone(), &Listen::new(), 0).await.unwrap();
        let attempt = FakeAttempt::new(&inner, shared.clone()).await;
        let (peer, peer_key) = (fakes::ids().0, KeyPair::generate());
        attempt.hear(&fakes::presence(&[(&peer, peer_key.public_key)]));
        attempt.settle();
        Listening { _device: device, listener, shared, attempt, peer, peer_key }
    }

    impl Listening {
        fn ticket(&self, connection_id: &str) -> String {
            let mut state = self.listener.state.lock().unwrap();
            state.tickets.mint(&self.peer, connection_id, &[CandidateKind::Lan], wall_ms()).unwrap().remove(0)
        }

        /// Dials as the peer over a socket of its own, up to its hello: the
        /// socket's far end, the dialer's transport, and the admission.
        async fn hello(
            &self,
            connection_id: &str,
        ) -> (WebSocketStream<DuplexStream>, Transport, JoinHandle<Option<Arc<LiveLink>>>) {
            let (conn, mut far) = fakes::conn(&self.listener.inner.tasks).await;
            let (listener, arrival) =
                (self.listener.clone(), Arrival { address: "192.0.2.1".into(), kind: CandidateKind::Lan });
            let admitting = tokio::spawn(async move { listener.admit(&Arc::new(conn), 0, &arrival).await });
            let ticket = self.ticket(connection_id);
            let listener_key = self.listener.identity.key.public_key;
            let (first, mut handshake) = start_handshake(&ticket, &self.peer_key, listener_key, None).unwrap();
            far.send(Message::binary(first)).await.unwrap();
            let second = far.next().await.unwrap().unwrap().into_data();
            let mut transport = finish_handshake(&mut handshake, &second).unwrap();
            let hello = TransportMessage::Hello(Hello {
                device_id: self.peer.clone(),
                connection_id: connection_id.to_owned(),
                app_hello: Vec::new(),
            });
            far.send(Message::binary(seal_transport_message(&mut transport, &hello).unwrap())).await.unwrap();
            (far, transport, admitting)
        }
    }

    #[tokio::test]
    async fn a_peer_that_leaves_a_settled_roster_and_comes_back_at_once_loses_its_tickets() {
        let listening = listening().await;
        let ticket = listening.ticket(&fakes::ids().1);
        let (with, without) =
            (fakes::presence(&[(&listening.peer, listening.peer_key.public_key)]), fakes::presence(&[]));
        listening.attempt.hear(&without);
        listening.attempt.hear(&with);
        tokio::task::yield_now().await;
        let check = listening.listener.state.lock().unwrap().tickets.check(&ticket, CandidateKind::Lan, wall_ms());
        assert!(!matches!(check, Check::Pending { .. }));
    }

    #[tokio::test]
    async fn a_hello_whose_dialer_gave_up_never_reaches_the_app() {
        let listening = listening().await;
        let incoming = &listening.listener.inner.incoming;
        for _ in 0..20 {
            let (_far, _, admitting) = listening.hello(&fakes::ids().1).await;
            while incoming.waiting.lock().unwrap().iter().all(|hello| hello.decide.is_closed()) {
                tokio::task::yield_now().await;
            }
            // As the welcome's deadline does.
            admitting.abort();
            let _ = admitting.await;
        }
        let current = fakes::ids().1;
        let (_far, _, _admitting) = listening.hello(&current).await;
        let hello = incoming.next().await;
        assert_eq!(hello.connection_id, current);
        assert!(incoming.waiting.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn connections_past_those_that_may_upgrade_at_once_are_let_go_at_once() {
        use tokio::io::AsyncReadExt;
        let listening = listening().await;
        let port = listening.listener.port;
        let mut idle = Vec::new();
        for _ in 0..MAX_UNWELCOMED_SOCKETS {
            idle.push(TcpStream::connect(("127.0.0.1", port)).await.unwrap());
        }
        let mut past = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let read = timeout(Duration::from_secs(2), past.read(&mut [0; 1])).await;
        assert!(matches!(read, Ok(Ok(0) | Err(_))), "{read:?}");
    }

    #[tokio::test]
    async fn an_origin_that_does_not_decode_is_refused() {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        use tokio_tungstenite::tungstenite::http::HeaderValue;
        let listening = listening().await;
        let url = format!("ws://127.0.0.1:{}", listening.listener.port);
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert("origin", HeaderValue::from_bytes(b"https://\xffapp.example").unwrap());
        let stream = TcpStream::connect(("127.0.0.1", listening.listener.port)).await.unwrap();
        let upgraded = tokio_tungstenite::client_async(request, stream).await;
        let status = match upgraded {
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => Some(response.status()),
            _ => None,
        };
        assert_eq!(status, Some(StatusCode::UNAUTHORIZED));
    }
}
