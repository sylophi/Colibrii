//! A peer link once welcomed (spec/link.md, "Transport messages" and
//! "Liveness"): app messages split and joined, pings answered, and the
//! link's end, said once.

use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::conn::{Conn, Msg};
use crate::device::Inner;
use crate::errors::SendError;
use crate::liveness::{self, Heard};
use crate::runtime::spawn_until;
use crate::wire::link::{
    AppMessageJoiner, CLOSE_GOING_AWAY, CLOSE_NORMAL, CLOSE_PROTOCOL_ERROR, CLOSE_TOO_BIG, Joined,
    MAX_APP_MESSAGE_BYTES, TransportMessage, open_transport_message, seal_transport_message, split_app_message,
};
use crate::wire::noise::Transport;
use crate::wire::relay::CandidateKind;

/// Which end of the link this device is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Dialer,
    Listener,
}

/// Why a link closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkCloseCause {
    /// This app closed it, or let go of it.
    Closed,
    /// The other side closed it, or the socket dropped.
    Peer,
    /// The peer left the roster, or came back to it with another key.
    LeftRoster,
    /// Nothing was heard from the peer for too long.
    Silent,
    /// A newer link from the same peer replaced it.
    Replaced,
    /// This device signed out, was removed or stopped, or was closed.
    Stopped,
    /// The other side broke the protocol, or this side's cipher ran out.
    ProtocolError,
    /// The other side sent an app message over the cap.
    TooBig,
}

impl LinkCloseCause {
    fn as_str(self) -> &'static str {
        match self {
            LinkCloseCause::Closed => "closed",
            LinkCloseCause::Peer => "peer",
            LinkCloseCause::LeftRoster => "left-roster",
            LinkCloseCause::Silent => "silent",
            LinkCloseCause::Replaced => "replaced",
            LinkCloseCause::Stopped => "stopped",
            LinkCloseCause::ProtocolError => "protocol-error",
            LinkCloseCause::TooBig => "too-big",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkClosed {
    pub code: u16,
    pub cause: LinkCloseCause,
}

/// Seals a transport message and sends it at once, which keeps the sending
/// cipher's counter and the socket in one order. One that cannot be sealed,
/// as once the cipher ran out of nonces, closes the socket with 1002, and
/// this says it did not go.
pub(crate) fn send_sealed(conn: &Conn, transport: &mut Transport, message: &TransportMessage) -> bool {
    match seal_transport_message(transport, message) {
        Ok(sealed) => {
            conn.send(Msg::Binary(sealed));
            true
        }
        Err(_) => {
            conn.cut(CLOSE_PROTOCOL_ERROR, "cannot seal");
            false
        }
    }
}

/// What the device keeps of a link: the key its peer proved, and its end
/// with a cause of the device's own.
pub(crate) struct LiveLink {
    pub device_id: String,
    pub peer_key: [u8; 32],
    pub connection_id: String,
    pub role: Role,
    pub arrival: CandidateKind,
    pub greeting: Vec<u8>,
    conn: Arc<Conn>,
    transport: Mutex<Transport>,
    received: Mutex<Option<mpsc::UnboundedSender<Vec<u8>>>>,
    messages: tokio::sync::Mutex<mpsc::UnboundedReceiver<Vec<u8>>>,
    closed: watch::Sender<Option<LinkClosed>>,
    stop: CancellationToken,
}

pub(crate) struct NewLink {
    pub conn: Arc<Conn>,
    pub transport: Transport,
    pub role: Role,
    pub device_id: String,
    pub peer_key: [u8; 32],
    pub connection_id: String,
    pub arrival: CandidateKind,
    pub greeting: Vec<u8>,
}

impl LiveLink {
    /// The link, which runs once it joined its session.
    pub fn new(new: NewLink) -> Arc<LiveLink> {
        let (sender, messages) = mpsc::unbounded_channel();
        Arc::new(LiveLink {
            device_id: new.device_id,
            peer_key: new.peer_key,
            connection_id: new.connection_id,
            role: new.role,
            arrival: new.arrival,
            greeting: new.greeting,
            conn: new.conn,
            transport: Mutex::new(new.transport),
            received: Mutex::new(Some(sender)),
            messages: tokio::sync::Mutex::new(messages),
            closed: watch::channel(None).0,
            stop: CancellationToken::new(),
        })
    }

    /// Its reader and its liveness, until it ends.
    pub fn run(self: &Arc<Self>, inner: &Arc<Inner>) {
        let heard = Arc::new(Heard::new());
        let (reading, heard_by_reader) = (self.clone(), heard.clone());
        spawn_until(&inner.tasks, &self.stop, async move { reading.read(&heard_by_reader).await });
        let (watching, wakes) = (self.clone(), inner.wakes.subscribe());
        spawn_until(&inner.tasks, &self.stop, async move {
            match watching.role {
                Role::Dialer => liveness::watch(|| watching.send_message(&TransportMessage::Ping), &heard, wakes).await,
                Role::Listener => liveness::silence(&heard).await,
            }
            watching.end(CLOSE_GOING_AWAY, LinkCloseCause::Silent);
        });
    }

    pub fn send_message(&self, message: &TransportMessage) {
        if !send_sealed(&self.conn, &mut self.transport.lock().unwrap(), message) {
            self.end(CLOSE_PROTOCOL_ERROR, LinkCloseCause::ProtocolError);
        }
    }

    async fn read(&self, heard: &Heard) {
        let mut joiner = AppMessageJoiner::default();
        loop {
            let data = match self.conn.receive().await {
                Ok(data) => data,
                Err(close) => return self.end(close.code, LinkCloseCause::Peer),
            };
            heard.hear();
            let message =
                data.as_binary().and_then(|bytes| open_transport_message(&mut self.transport.lock().unwrap(), bytes));
            match message {
                Some(TransportMessage::Data { more, body }) => match joiner.add(more, body) {
                    Joined::TooBig => return self.end(CLOSE_TOO_BIG, LinkCloseCause::TooBig),
                    Joined::Message(message) => {
                        if let Some(sender) = self.received.lock().unwrap().as_ref() {
                            let _ = sender.send(message);
                        }
                    }
                    Joined::Partial => {}
                },
                Some(TransportMessage::Ping) => self.send_message(&TransportMessage::Pong),
                Some(TransportMessage::Pong) => {}
                _ => return self.end(CLOSE_PROTOCOL_ERROR, LinkCloseCause::ProtocolError),
            }
        }
    }

    /// Closes the socket unless the peer did, once, and ends the messages.
    pub fn end(&self, code: u16, cause: LinkCloseCause) {
        let mut first = false;
        self.closed.send_if_modified(|closed| {
            first = closed.is_none();
            if first {
                *closed = Some(LinkClosed { code, cause });
            }
            first
        });
        if !first {
            return;
        }
        match cause {
            LinkCloseCause::Peer => {}
            // What the app sent before its close still goes out.
            LinkCloseCause::Closed => self.conn.close(code, cause.as_str()),
            _ => self.conn.cut(code, cause.as_str()),
        }
        self.received.lock().unwrap().take();
        self.stop.cancel();
    }

    pub fn is_ended(&self) -> bool {
        self.closed.borrow().is_some()
    }

    /// Until the link ends.
    pub async fn ended(&self) {
        let mut closed = self.closed.subscribe();
        let _ = closed.wait_for(Option::is_some).await;
    }
}

/// An open peer link, as the app holds it at either end (spec/link.md,
/// "What the app gets"). Clones are the same link, which closes once the
/// last of them is dropped.
#[derive(Clone)]
pub struct Link(Arc<Held>);

struct Held(Arc<LiveLink>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.end(CLOSE_NORMAL, LinkCloseCause::Closed);
    }
}

impl Link {
    pub(crate) fn of(live: Arc<LiveLink>) -> Link {
        Link(Arc::new(Held(live)))
    }

    fn live(&self) -> &LiveLink {
        &self.0.0
    }

    /// The other device.
    pub fn device_id(&self) -> &str {
        &self.live().device_id
    }

    /// The connection id the dialer named the link with, the same at both
    /// ends.
    pub fn connection_id(&self) -> &str {
        &self.live().connection_id
    }

    pub fn role(&self) -> Role {
        self.live().role
    }

    /// How the link arrived: over the LAN or through the listener's
    /// tunnel.
    pub fn arrival(&self) -> CandidateKind {
        self.live().arrival
    }

    /// What the other side's app sent as the link opened: its app hello at
    /// the listener, its app welcome at the dialer.
    pub fn greeting(&self) -> &[u8] {
        &self.live().greeting
    }

    /// Sends one app message, of at most 32 MiB, whole and in order.
    pub fn send(&self, message: &[u8]) -> Result<(), SendError> {
        if message.len() > MAX_APP_MESSAGE_BYTES {
            return Err(SendError::TooBig);
        }
        let live = self.live();
        if live.is_ended() || !live.conn.is_open() {
            return Err(SendError::Closed);
        }
        // Every piece under one lock, so no other message comes between
        // them.
        let mut transport = live.transport.lock().unwrap();
        for piece in split_app_message(message) {
            if !send_sealed(&live.conn, &mut transport, &piece) {
                drop(transport);
                live.end(CLOSE_PROTOCOL_ERROR, LinkCloseCause::ProtocolError);
                return Err(SendError::Closed);
            }
        }
        Ok(())
    }

    /// The next app message received, in order. Nothing once the link
    /// closed and every message before its close was taken.
    pub async fn recv(&self) -> Option<Vec<u8>> {
        self.live().messages.lock().await.recv().await
    }

    pub fn close(&self) {
        self.live().end(CLOSE_NORMAL, LinkCloseCause::Closed);
    }

    /// Waits for the link to close, and says why.
    pub async fn closed(&self) -> LinkClosed {
        let mut closed = self.live().closed.subscribe();
        let closed = closed.wait_for(Option::is_some).await.expect("the link keeps its close");
        closed.expect("a close")
    }
}

impl std::fmt::Debug for Link {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Link")
            .field("device_id", &self.device_id())
            .field("connection_id", &self.connection_id())
            .field("role", &self.role())
            .field("arrival", &self.arrival())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes;
    use crate::wire::noise::CipherState;

    #[tokio::test]
    async fn a_message_its_cipher_cannot_seal_ends_the_link() {
        let device = fakes::device().await;
        let (conn, _far) = fakes::conn(&device.inner().tasks).await;
        let (device_id, connection_id) = fakes::ids();
        let live = LiveLink::new(NewLink {
            conn: Arc::new(conn),
            transport: Transport { send: CipherState::spent(), receive: CipherState::default() },
            role: Role::Listener,
            device_id,
            peer_key: [9; 32],
            connection_id,
            arrival: CandidateKind::Lan,
            greeting: Vec::new(),
        });
        let link = Link::of(live.clone());
        assert!(matches!(link.send(b"hi"), Err(SendError::Closed)));
        let closed = *live.closed.borrow();
        assert_eq!(closed, Some(LinkClosed { code: CLOSE_PROTOCOL_ERROR, cause: LinkCloseCause::ProtocolError }));
    }
}
