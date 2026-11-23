//! The hub socket (spec/hub-socket.md), kept up while the device is
//! enrolled, and the relay's asks over it (spec/relay.md): this device's
//! asks of its peers, and its answers to theirs.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use reqwest::Method;
use tokio::sync::{oneshot, watch};
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::conn::{self, Conn, Msg};
use crate::device::{Inner, Peer, Stop, StopReason};
use crate::errors::{DialError, Error};
use crate::hub_api::{self, stop_of_error};
use crate::link::{LinkCloseCause, LiveLink, Role};
use crate::liveness::{self, Heard};
use crate::runtime::spawn_until;
use crate::wire::api::{TicketRequest, TicketResponse};
use crate::wire::hub::{CLOSE_DEVICE_REMOVED, CLOSE_SUPERSEDED, Connections, PING, PONG, ServiceMessage, encode_relay};
use crate::wire::link::CLOSE_GOING_AWAY;
use crate::wire::link::{REDIAL_DELAYS_MS, REDIAL_RESET_AFTER_MS};
use crate::wire::noise::{HandshakeState, KeyPair};
use crate::wire::relay::{
    ASK_TIMEOUT_MS, Answer, Ask, ConnectInfoInput, ConnectInfoResult, FrameKind, RelayFrame, SEEN_ASK_MS,
    decode_relay_frame, encode_answer, encode_ask, encode_relay_frame, open_answer, open_ask, seal_answer, seal_ask,
};
use crate::wire::{decode_public_key, same_key};

// The first presence comes within this long of the socket opening, or the
// attempt failed. The ticket's mint and the socket's opening get as long
// each.
const ADMISSION: Duration = Duration::from_secs(10);

// After the socket is admitted, a peer missing from the roster may be one
// still reconnecting, as every device does after the service restarts
// (spec/link.md, "When a peer goes").
const SETTLE: Duration = Duration::from_secs(10);

/// The device's peers as the latest presence named them, the device itself
/// left out, and whether the settle period since the socket was admitted
/// ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Roster {
    pub peers: Vec<Peer>,
    pub settled: bool,
}

impl Roster {
    pub fn get(&self, device_id: &str) -> Option<&Peer> {
        self.peers.iter().find(|peer| peer.device_id == device_id)
    }
}

/// What the session's parts share: the roster, and the links that joined
/// it.
pub(crate) struct Shared {
    /// Set by the hub socket: none while it is down.
    roster: watch::Sender<Option<Roster>>,
    /// The listener's sweep of the peers a settled roster names.
    settled_sweep: OnceLock<SettledSweep>,
    links: Mutex<Links>,
    pub tasks: TaskTracker,
    pub ended: CancellationToken,
}

pub(crate) type SettledSweep = Box<dyn Fn(&HashSet<String>) + Send + Sync>;

#[derive(Default)]
struct Links {
    ended: bool,
    /// Each until it ends, held as long as anything else holds it.
    all: Vec<Weak<LiveLink>>,
}

impl Links {
    /// The links that have not ended, the rest let go.
    fn live(&mut self) -> Vec<Arc<LiveLink>> {
        let mut live = Vec::new();
        self.all.retain(|link| match link.upgrade().filter(|link| !link.is_ended()) {
            Some(link) => {
                live.push(link);
                true
            }
            None => false,
        });
        live
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Join {
    Joined,
    /// The session ended, or its roster is unknown.
    Unavailable,
    /// The roster does not name the link's peer with the key it proved.
    Refused,
}

impl Shared {
    pub fn new() -> Arc<Shared> {
        Arc::new(Shared {
            roster: watch::channel(None).0,
            settled_sweep: OnceLock::new(),
            links: Mutex::new(Links::default()),
            tasks: TaskTracker::new(),
            ended: CancellationToken::new(),
        })
    }

    /// Runs `work` until the session ends.
    pub fn spawn(&self, work: impl Future<Output = ()> + Send + 'static) {
        spawn_until(&self.tasks, &self.ended, work);
    }

    pub fn roster(&self) -> Option<Roster> {
        self.roster.borrow().clone()
    }

    pub fn follow_roster(&self) -> watch::Receiver<Option<Roster>> {
        self.roster.subscribe()
    }

    pub fn sweep_settled(&self, sweep: SettledSweep) {
        let _ = self.settled_sweep.set(sweep);
    }

    /// Changes the roster, then sweeps what the new one no longer names,
    /// before the next change: a watcher of the roster may see only the
    /// latest of several. The hub socket publishes under its `live` lock,
    /// which keeps the changes in turn.
    fn publish(&self, change: impl FnOnce(&mut Option<Roster>)) {
        self.roster.send_modify(change);
        let Some(roster) = self.roster() else { return };
        self.sweep(&roster);
        if let Some(sweep) = self.settled_sweep.get().filter(|_| roster.settled) {
            sweep(&roster.peers.iter().map(|peer| peer.device_id.clone()).collect());
        }
    }

    /// The peer as the latest roster names it, while the hub socket is up.
    pub fn peer(&self, device_id: &str) -> Option<Peer> {
        self.roster.borrow().as_ref().and_then(|roster| roster.get(device_id).cloned())
    }

    /// A link joins only while the session runs and the roster names its
    /// peer with the key the link proved, checked in the same step, so the
    /// sweep and the session's end see every link that joined. A dialer
    /// chose its peer from the roster moments ago, so it leaves one missing
    /// from a roster not yet settled to the sweep, as it does an older
    /// link.
    ///
    /// A link this device listened for joins `replacing` the ones it
    /// replaces, by its peer's `connections`: every other one this device
    /// listened for from that peer when `single`, or the one with the same
    /// connection id when `multi`. Two that join at once from a `single`
    /// peer leave the later one.
    pub fn join(&self, link: &Arc<LiveLink>, replacing: Option<Connections>) -> Join {
        let replaced: Vec<_> = {
            let mut links = self.links.lock().unwrap();
            let roster = self.roster.borrow();
            let Some(roster) = roster.as_ref().filter(|_| !links.ended) else { return Join::Unavailable };
            let named = match roster.get(&link.device_id) {
                None => link.role == Role::Dialer && !roster.settled,
                Some(peer) => same_key(&peer.public_key, &link.peer_key),
            };
            if !named {
                return Join::Refused;
            }
            let others = links.live();
            let replaced = match replacing {
                None => Vec::new(),
                Some(connections) => others
                    .into_iter()
                    .filter(|other| {
                        other.role == Role::Listener
                            && other.device_id == link.device_id
                            && (connections == Connections::Single || other.connection_id == link.connection_id)
                    })
                    .collect(),
            };
            links.all.push(Arc::downgrade(link));
            replaced
        };
        for other in replaced {
            other.end(CLOSE_GOING_AWAY, LinkCloseCause::Replaced);
        }
        Join::Joined
    }

    /// A live roster ends the links of a peer that left it, once settled,
    /// or that it gives another key.
    fn sweep(&self, roster: &Roster) {
        let links = self.links.lock().unwrap().live();
        for link in links {
            let gone = match roster.get(&link.device_id) {
                None => roster.settled,
                Some(peer) => !same_key(&peer.public_key, &link.peer_key),
            };
            if gone {
                link.end(CLOSE_GOING_AWAY, LinkCloseCause::LeftRoster);
            }
        }
    }

    /// The session ends: no link joins from now on, and every link closes.
    pub fn end_links(&self) {
        let links = {
            let mut links = self.links.lock().unwrap();
            links.ended = true;
            let live = links.live();
            links.all.clear();
            live
        };
        for link in links {
            link.end(CLOSE_GOING_AWAY, LinkCloseCause::Stopped);
        }
    }
}

pub(crate) struct Identity {
    pub device_id: String,
    pub credential: String,
    pub key: KeyPair,
}

pub(crate) type AnswerFn = Arc<dyn Fn(String, Ask) -> Pin<Box<dyn Future<Output = Answer> + Send>> + Send + Sync>;

struct Pending {
    device_id: String,
    handshake: HandshakeState,
    answered: oneshot::Sender<Result<Answer, DialError>>,
}

/// One admitted socket: what an ask goes out on, and the asks waiting for
/// an answer on it.
struct Live {
    conn: Arc<Conn>,
    pending: Mutex<HashMap<String, Pending>>,
    next_id: AtomicU64,
    /// Set once the attempt ended, under the hub socket's `live` lock, which
    /// every publication of its roster takes too, and before it fails the
    /// asks pending.
    ended: AtomicBool,
}

impl Live {
    fn new(conn: Arc<Conn>) -> Live {
        Live {
            conn,
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(u64::from_le_bytes(crate::wire::random_bytes(8).try_into().unwrap()) >> 24),
            ended: AtomicBool::new(false),
        }
    }

    fn fail(&self, which: impl Fn(&Pending) -> bool, error: DialError) {
        for (_, entry) in self.pending.lock().unwrap().extract_if(|_, entry| which(entry)) {
            let _ = entry.answered.send(Err(error.clone()));
        }
    }
}

pub(crate) struct HubSocket {
    inner: Arc<Inner>,
    shared: Arc<Shared>,
    identity: Arc<Identity>,
    live: Mutex<Option<Arc<Live>>>,
    answer: AnswerFn,
    /// The asks this device opened, by handshake hash, each until its
    /// memory ends. Kept across reconnects.
    seen_asks: Mutex<HashMap<[u8; 32], Instant>>,
}

/// How one attempt went: how long it stayed up, and the least wait the far
/// end asked for before the next.
struct Run {
    uptime: Duration,
    wait_at_least: Duration,
}

/// The reconnect ladder: 1, 2, 4, 8, then 16 seconds, staying at 16, and
/// starting over after a run that stayed up 30 seconds.
#[derive(Default)]
struct Ladder {
    rung: usize,
}

impl Ladder {
    /// Waits before the attempt after `run`. A wake cuts a wait on the
    /// ladder short, never one the far end asked for.
    async fn wait(&mut self, run: &Run, mut wakes: watch::Receiver<u64>) {
        if run.uptime >= Duration::from_millis(REDIAL_RESET_AFTER_MS) {
            self.rung = 0;
        }
        let delay = Duration::from_millis(REDIAL_DELAYS_MS[self.rung.min(REDIAL_DELAYS_MS.len() - 1)]);
        self.rung += 1;
        if run.wait_at_least > Duration::ZERO {
            sleep(delay.max(run.wait_at_least)).await;
        } else {
            tokio::select! {
                _ = sleep(delay) => {}
                Ok(()) = wakes.changed() => {}
            }
        }
    }
}

fn failed(wait_at_least: Duration) -> Result<Run, Stop> {
    Ok(Run { uptime: Duration::ZERO, wait_at_least })
}

/// A retry-after past what a Duration holds waits as long as one can.
fn wait_of(seconds: f64) -> Duration {
    Duration::try_from_secs_f64(seconds).unwrap_or(Duration::MAX)
}

fn stop_of(code: u16) -> Option<Stop> {
    match code {
        CLOSE_DEVICE_REMOVED => Some(Stop::Removed),
        CLOSE_SUPERSEDED => Some(Stop::Stopped(StopReason::Superseded)),
        _ => None,
    }
}

fn connect_url(hub: &str, ticket: &str) -> String {
    let hub = hub.strip_prefix("http").map_or_else(|| hub.to_owned(), |rest| format!("ws{rest}"));
    format!("{hub}/connect?ticket={}", percent_encode(ticket))
}

/// As JavaScript's `encodeURIComponent`.
pub(crate) fn percent_encode(text: &str) -> String {
    text.bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn roster_of(online: &[crate::wire::hub::OnlineDevice], me: &str) -> Vec<Peer> {
    online
        .iter()
        .filter(|device| device.device_id != me)
        .filter_map(|device| {
            Some(Peer {
                device_id: device.device_id.clone(),
                public_key: decode_public_key(&device.public_key)?,
                connections: device.connections,
            })
        })
        .collect()
}

/// What an attempt leaves behind once it ends, however it ends.
struct AttemptEnd {
    hub: Arc<HubSocket>,
    live: Arc<Live>,
    ended: CancellationToken,
}

impl Drop for AttemptEnd {
    fn drop(&mut self) {
        let mut live = self.hub.live.lock().unwrap();
        self.live.ended.store(true, Ordering::SeqCst);
        if live.as_ref().is_some_and(|now| Arc::ptr_eq(now, &self.live)) {
            *live = None;
        }
        self.hub.shared.publish(|roster| *roster = None);
        drop(live);
        self.ended.cancel();
        self.live.fail(|_| true, DialError::NotConnected);
    }
}

impl HubSocket {
    pub fn new(inner: Arc<Inner>, shared: Arc<Shared>, identity: Arc<Identity>, answer: AnswerFn) -> Arc<HubSocket> {
        Arc::new(HubSocket {
            inner,
            shared,
            identity,
            live: Mutex::new(None),
            answer,
            seen_asks: Mutex::new(HashMap::new()),
        })
    }

    /// Keeps the socket up, on the reconnect ladder. Returns why the device
    /// stops.
    pub async fn run(self: Arc<Self>) -> Stop {
        let mut ladder = Ladder::default();
        loop {
            let run = match self.clone().attempt().await {
                Ok(run) => run,
                Err(stop) => return stop,
            };
            ladder.wait(&run, self.inner.wakes.subscribe()).await;
        }
    }

    /// One socket's life, from the ticket's mint to its close: how long it
    /// was up, or why the device stops.
    async fn attempt(self: Arc<Self>) -> Result<Run, Stop> {
        let request = TicketRequest { connection_id: self.inner.connection_id.clone() };
        let minted = timeout(
            ADMISSION,
            self.inner.api.call(Method::POST, "/tickets", &self.identity.credential, Some(&request), hub_api::DEVICE),
        )
        .await;
        let ticket = match minted {
            Err(_) => return failed(Duration::ZERO),
            Ok(Err(error)) => {
                if let Some(stop) = stop_of_error(&error) {
                    return Err(stop);
                }
                return match error {
                    Error::RateLimited { retry_after_seconds } => failed(wait_of(retry_after_seconds)),
                    _ => failed(Duration::ZERO),
                };
            }
            Ok(Ok(answer)) => match serde_json::from_value::<TicketResponse>(answer) {
                Ok(answer) => answer.ticket,
                Err(_) => return failed(Duration::ZERO),
            },
        };
        let url = connect_url(self.inner.api.hub(), &ticket);
        let conn = match timeout(
            ADMISSION,
            conn::open(&url, self.inner.options.hooks.headers(), &self.inner.options.hooks, &self.inner.tasks),
        )
        .await
        {
            Err(_) => return failed(Duration::ZERO),
            Ok(Err(error)) => return failed(error.retry_after_seconds.map_or(Duration::ZERO, wait_of)),
            Ok(Ok(conn)) => Arc::new(conn),
        };
        let current = Arc::new(Live::new(conn.clone()));
        let ended = CancellationToken::new();
        let _end = AttemptEnd { hub: self.clone(), live: current.clone(), ended: ended.clone() };
        let heard = Arc::new(Heard::new());
        let (admitted, mut admission) = watch::channel(false);

        let hub = self.clone();
        let (receiving, heard_by_receiver, ended_by_receiver) = (current.clone(), heard.clone(), ended.clone());
        spawn_until(&self.shared.tasks, &ended, async move {
            while let Ok(message) = receiving.conn.receive().await {
                heard_by_receiver.hear();
                if let Msg::Text(text) = message {
                    hub.handle(&receiving, &ended_by_receiver, &admitted, &text);
                }
            }
        });

        let first = tokio::select! {
            close = conn.closed() => Some(close.code),
            admitted = timeout(ADMISSION, admission.wait_for(|admitted| *admitted)) => match admitted {
                Ok(Ok(_)) => None,
                _ => Some(0),
            },
        };
        if let Some(code) = first {
            return stop_of(code).map_or(failed(Duration::ZERO), Err);
        }
        let (hub, settling) = (self.clone(), current.clone());
        spawn_until(&self.shared.tasks, &ended, async move {
            sleep(SETTLE).await;
            hub.publish(&settling, |now| {
                if let Some(now) = now {
                    now.settled = true;
                }
            });
        });

        let up_since = Instant::now();
        let pinging = conn.clone();
        let code = tokio::select! {
            close = conn.closed() => Some(close.code),
            _ = liveness::watch(|| pinging.send(Msg::Text(PING.into())), &heard, self.inner.wakes.subscribe()) => None,
        };
        let uptime = up_since.elapsed();
        match code.and_then(stop_of) {
            Some(stop) => Err(stop),
            None => Ok(Run { uptime, wait_at_least: Duration::ZERO }),
        }
    }

    fn handle(
        self: &Arc<Self>,
        current: &Arc<Live>,
        ended: &CancellationToken,
        admitted: &watch::Sender<bool>,
        text: &str,
    ) {
        if text == PONG {
            return;
        }
        let Some(message) = ServiceMessage::decode(text) else { return };
        match message {
            ServiceMessage::Presence { online } => {
                let peers = roster_of(&online, &self.identity.device_id);
                current.fail(|entry| !peers.iter().any(|peer| peer.device_id == entry.device_id), DialError::Offline);
                // Asks go out on it from the first presence, which admits it.
                let published = self.publish(current, |before| {
                    let settled = before.as_ref().is_some_and(|then| then.settled);
                    *before = Some(Roster { peers, settled });
                });
                if published {
                    admitted.send_replace(true);
                }
            }
            ServiceMessage::Nack { to, reason } => {
                let error = if reason == "offline" {
                    DialError::Offline
                } else {
                    DialError::unreachable(format!("the service could not relay ({reason})"))
                };
                current.fail(|entry| entry.device_id == to, error);
            }
            ServiceMessage::Relay { from, frame } => {
                let Some(frame) = decode_relay_frame(&frame) else { return };
                match frame.kind {
                    FrameKind::Answer => read_answer(current, &from, &frame),
                    // Answered apart, since an answer may wait on the tunnel.
                    FrameKind::Ask => {
                        let (hub, conn) = (self.clone(), current.conn.clone());
                        spawn_until(&self.shared.tasks, ended, async move { hub.answer_ask(&conn, from, frame).await });
                    }
                }
            }
        }
    }

    /// Changes the roster as `current` says, and makes it the socket asks
    /// go out on, unless its attempt ended: false then.
    fn publish(&self, current: &Arc<Live>, change: impl FnOnce(&mut Option<Roster>)) -> bool {
        let mut live = self.live.lock().unwrap();
        if current.ended.load(Ordering::SeqCst) {
            return false;
        }
        *live = Some(current.clone());
        self.shared.publish(change);
        true
    }

    async fn answer_ask(&self, conn: &Conn, from: String, frame: RelayFrame) {
        let Some(peer_key) = self.shared.peer(&from).map(|peer| peer.public_key) else { return };
        let identity = &self.identity;
        let Some(mut opened) = open_ask(&from, &identity.device_id, &identity.key, &frame.sealed, None) else {
            return;
        };
        if !same_key(&opened.asker_key, &peer_key) {
            return;
        }
        // The service could replay an ask under any id: one already opened
        // is dropped by its handshake, which no other ask shares.
        {
            let now = Instant::now();
            let mut seen = self.seen_asks.lock().unwrap();
            seen.retain(|_, until| *until > now);
            if seen.contains_key(&opened.handshake_hash) {
                return;
            }
            seen.insert(opened.handshake_hash, now + Duration::from_millis(SEEN_ASK_MS));
        }
        let answer = (self.answer)(from.clone(), opened.ask.clone()).await;
        let Ok(sealed) = seal_answer(&mut opened.handshake, &encode_answer(&answer)) else { return };
        let frame = encode_relay_frame(&RelayFrame { kind: FrameKind::Answer, id: frame.id, sealed });
        conn.send(Msg::Text(encode_relay(&from, &frame)));
    }

    /// The asker's side: the peer's connectInfo result, or why there is
    /// none.
    pub async fn ask(&self, device_id: &str, input: ConnectInfoInput) -> Result<ConnectInfoResult, DialError> {
        let current = self.live.lock().unwrap().clone().ok_or(DialError::NotConnected)?;
        let peer = self.shared.peer(device_id).ok_or(DialError::Offline)?;
        let id = current.next_id.fetch_add(1, Ordering::SeqCst).to_string();
        let identity = &self.identity;
        let (sealed, handshake) =
            seal_ask(&identity.device_id, device_id, &identity.key, peer.public_key, &encode_ask(&input), None)
                .map_err(|_| DialError::unreachable("the roster's key for the device is unusable"))?;
        let (answered, answer) = oneshot::channel();
        {
            let mut pending = current.pending.lock().unwrap();
            // Its end failed every ask it held, and would not fail this one.
            if current.ended.load(Ordering::SeqCst) {
                return Err(DialError::NotConnected);
            }
            pending.insert(id.clone(), Pending { device_id: device_id.to_owned(), handshake, answered });
        }
        let _asked = Asked { live: current.clone(), id: id.clone() };
        let frame = encode_relay_frame(&RelayFrame { kind: FrameKind::Ask, id, sealed });
        current.conn.send(Msg::Text(encode_relay(device_id, &frame)));
        let answer = timeout(Duration::from_millis(ASK_TIMEOUT_MS), answer).await;
        match answer {
            Err(_) => Err(DialError::TimedOut),
            Ok(Err(_)) => Err(DialError::NotConnected),
            Ok(Ok(Err(error))) => Err(error),
            // A candidate of a kind this device did not ask for is skipped.
            Ok(Ok(Ok(Answer::Ok(ConnectInfoResult::Available(candidates))))) => Ok(ConnectInfoResult::Available(
                candidates.into_iter().filter(|candidate| input.dialable_kinds.contains(&candidate.kind)).collect(),
            )),
            Ok(Ok(Ok(Answer::Ok(ConnectInfoResult::Unavailable)))) => Ok(ConnectInfoResult::Unavailable),
            Ok(Ok(Ok(Answer::Refused(code)))) if code == "no-listener" => Err(DialError::NoListener),
            Ok(Ok(Ok(Answer::Refused(code)))) => Err(DialError::unreachable(code)),
        }
    }
}

/// An ask's place among those waiting, until the asker stops waiting.
struct Asked {
    live: Arc<Live>,
    id: String,
}

impl Drop for Asked {
    fn drop(&mut self) {
        self.live.pending.lock().unwrap().remove(&self.id);
    }
}

fn read_answer(current: &Live, from: &str, frame: &RelayFrame) {
    let mut pending = current.pending.lock().unwrap();
    if !pending.get(&frame.id).is_some_and(|entry| entry.device_id == from) {
        return;
    }
    let Some(mut entry) = pending.remove(&frame.id) else { return };
    let answer = open_answer(&mut entry.handshake, &frame.sealed)
        .ok_or_else(|| DialError::unreachable("the answer could not be read"));
    let _ = entry.answered.send(answer);
}

/// An attempt whose socket is up, read as its reader would.
#[cfg(test)]
pub(crate) struct FakeAttempt {
    hub: Arc<HubSocket>,
    live: Arc<Live>,
    ended: CancellationToken,
    admitted: watch::Sender<bool>,
}

#[cfg(test)]
impl FakeAttempt {
    /// One of a hub socket that publishes to `shared` and answers no ask.
    pub async fn new(inner: &Arc<Inner>, shared: Arc<Shared>) -> FakeAttempt {
        let identity = Arc::new(Identity {
            device_id: crate::fakes::ids().0,
            credential: "credential".into(),
            key: KeyPair::generate(),
        });
        let answer: AnswerFn = Arc::new(|_, _| Box::pin(async { Answer::Refused("unknown-ask".into()) }));
        let (conn, _) = crate::fakes::conn(&inner.tasks).await;
        FakeAttempt {
            hub: HubSocket::new(inner.clone(), shared, identity, answer),
            live: Arc::new(Live::new(Arc::new(conn))),
            ended: CancellationToken::new(),
            admitted: watch::channel(false).0,
        }
    }

    pub fn hear(&self, text: &str) {
        self.hub.handle(&self.live, &self.ended, &self.admitted, text);
    }

    pub fn settle(&self) {
        self.hub.publish(&self.live, |now| now.as_mut().unwrap().settled = true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes;

    #[tokio::test]
    async fn a_presence_read_after_its_socket_closed_publishes_nothing() {
        let device = fakes::device().await;
        let attempt = FakeAttempt::new(device.inner(), Shared::new()).await;
        let (hub, presence) = (&attempt.hub, fakes::presence(&[(&fakes::ids().0, KeyPair::generate().public_key)]));
        attempt.hear(&presence);
        assert!(hub.shared.roster().is_some());
        drop(AttemptEnd { hub: hub.clone(), live: attempt.live.clone(), ended: attempt.ended.clone() });
        // One the reader had taken before the end.
        attempt.hear(&presence);
        assert!(hub.shared.roster().is_none());
        assert!(hub.live.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn a_rate_limit_that_asks_to_wait_past_all_time_waits_as_long_as_it_can() {
        let hub = fakes::hub_answering("429 Too Many Requests\r\nretry-after: 1e300").await;
        let store = Arc::new(crate::store::MemoryStore::new());
        let options = crate::device::Options::new(hub, "inkwell", Connections::Single, store)
            .runtime(tokio::runtime::Handle::current());
        let device = crate::device::Device::new(options).await.unwrap();
        let attempt = FakeAttempt::new(device.inner(), Shared::new()).await;
        let run = attempt.hub.clone().attempt().await.ok().unwrap();
        assert_eq!(run.wait_at_least, Duration::MAX);
        let mut ladder = Ladder::default();
        let waiting = ladder.wait(&run, device.inner().wakes.subscribe());
        assert!(timeout(Duration::from_millis(20), waiting).await.is_err());
    }

    #[tokio::test]
    async fn two_links_that_join_at_once_from_a_single_peer_leave_one() {
        let device = fakes::device().await;
        let (inner, shared) = (device.inner().clone(), Shared::new());
        let attempt = FakeAttempt::new(&inner, shared.clone()).await;
        let (peer, key) = (fakes::ids().0, KeyPair::generate().public_key);
        attempt.hear(&fakes::presence(&[(&peer, key)]));
        for _ in 0..10_000 {
            let (first, _far) = fakes::listened(&inner, &peer, &fakes::ids().1, key).await;
            let (second, _far) = fakes::listened(&inner, &peer, &fakes::ids().1, key).await;
            let together = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                for link in [&first, &second] {
                    let (shared, together) = (&shared, &together);
                    scope.spawn(move || {
                        together.wait();
                        assert_eq!(shared.join(link, Some(Connections::Single)), Join::Joined);
                    });
                }
            });
            assert!(!first.is_ended() || !second.is_ended());
            first.end(CLOSE_GOING_AWAY, LinkCloseCause::Stopped);
            second.end(CLOSE_GOING_AWAY, LinkCloseCause::Stopped);
        }
    }

    #[tokio::test]
    async fn an_ask_given_up_on_leaves_nothing_waiting() {
        let device = fakes::device().await;
        let attempt = FakeAttempt::new(device.inner(), Shared::new()).await;
        let peer = fakes::ids().0;
        attempt.hear(&fakes::presence(&[(&peer, KeyPair::generate().public_key)]));
        let input = ConnectInfoInput { dialable_kinds: Vec::new(), connection_id: fakes::ids().1 };
        assert!(timeout(Duration::from_millis(20), attempt.hub.ask(&peer, input)).await.is_err());
        assert!(attempt.live.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn an_ask_made_as_its_attempt_ends_fails_at_once() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let _entered = runtime.enter();
        let device = runtime.block_on(fakes::device());
        for _ in 0..2_000 {
            let attempt = runtime.block_on(FakeAttempt::new(device.inner(), Shared::new()));
            let peer = fakes::ids().0;
            attempt.hear(&fakes::presence(&[(&peer, KeyPair::generate().public_key)]));
            let together = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    together.wait();
                    let (hub, live, ended) = (attempt.hub.clone(), attempt.live.clone(), attempt.ended.clone());
                    drop(AttemptEnd { hub, live, ended });
                });
                let input = ConnectInfoInput { dialable_kinds: Vec::new(), connection_id: fakes::ids().1 };
                together.wait();
                let asked = runtime.block_on(timeout(Duration::from_secs(1), attempt.hub.ask(&peer, input)));
                assert!(matches!(asked, Ok(Err(DialError::NotConnected))), "{asked:?}");
            });
        }
    }

    #[tokio::test]
    async fn a_link_that_ended_is_let_go() {
        let device = fakes::device().await;
        let (inner, shared) = (device.inner().clone(), Shared::new());
        let attempt = FakeAttempt::new(&inner, shared.clone()).await;
        let (peer, key) = (fakes::ids().0, KeyPair::generate().public_key);
        attempt.hear(&fakes::presence(&[(&peer, key)]));
        let (link, _far) = fakes::listened(&inner, &peer, &fakes::ids().1, key).await;
        assert_eq!(shared.join(&link, None), Join::Joined);
        let held = Arc::downgrade(&link);
        link.end(CLOSE_GOING_AWAY, LinkCloseCause::Closed);
        drop(link);
        assert!(held.upgrade().is_none());
    }

    fn after(uptime: u64, wait_at_least: u64) -> Run {
        Run { uptime: Duration::from_secs(uptime), wait_at_least: Duration::from_secs(wait_at_least) }
    }

    /// How long `ladder` waits after `run`, in whole seconds.
    async fn waited(ladder: &mut Ladder, run: Run, wakes: &watch::Sender<u64>) -> u64 {
        let started = Instant::now();
        ladder.wait(&run, wakes.subscribe()).await;
        started.elapsed().as_secs()
    }

    #[tokio::test(start_paused = true)]
    async fn climbs_1_2_4_8_16_and_starts_over_after_a_run_up_30_seconds() {
        let wakes = watch::channel(0).0;
        let mut ladder = Ladder::default();
        let mut waits = Vec::new();
        for _ in 0..6 {
            waits.push(waited(&mut ladder, after(0, 0), &wakes).await);
        }
        waits.push(waited(&mut ladder, after(29, 0), &wakes).await);
        waits.push(waited(&mut ladder, after(30, 0), &wakes).await);
        assert_eq!(waits, [1, 2, 4, 8, 16, 16, 16, 1]);
    }

    #[tokio::test(start_paused = true)]
    async fn waits_at_least_what_the_far_end_asked_for() {
        let wakes = watch::channel(0).0;
        let mut ladder = Ladder::default();
        assert_eq!(waited(&mut ladder, after(0, 7), &wakes).await, 7);
        assert_eq!(waited(&mut ladder, after(0, 1), &wakes).await, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_wake_cuts_a_wait_on_the_ladder_short_and_never_one_the_far_end_asked_for() {
        let wakes = watch::channel(0).0;
        let mut ladder = Ladder { rung: 4 };
        let waking = async {
            sleep(Duration::from_secs(3)).await;
            wakes.send_modify(|count| *count += 1);
        };
        let (cut, ()) = tokio::join!(waited(&mut ladder, after(0, 0), &wakes), waking);
        assert_eq!(cut, 3);
        let waking = async {
            sleep(Duration::from_secs(3)).await;
            wakes.send_modify(|count| *count += 1);
        };
        let (asked, ()) = tokio::join!(waited(&mut ladder, after(0, 20), &wakes), waking);
        assert_eq!(asked, 20);
    }
}
