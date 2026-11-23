//! The hub socket (spec/hub-socket.md), kept up while the device is
//! enrolled, and the relay's asks over it (spec/relay.md): this device's
//! asks of its peers, and its answers to theirs.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use reqwest::Method;
use tokio::sync::{oneshot, watch};
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::conn::{self, Conn, Msg};
use crate::device::{Inner, Peer, Stop};
use crate::errors::{DialError, Error};
use crate::hub_api::{self, stop_of_error};
use crate::link::{LinkCloseCause, LiveLink, Role};
use crate::liveness::{self, Heard};
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
    pub roster: watch::Sender<Option<Roster>>,
    links: Mutex<Links>,
    pub tasks: TaskTracker,
    pub ended: CancellationToken,
}

#[derive(Default)]
struct Links {
    ended: bool,
    all: Vec<Arc<LiveLink>>,
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
    pub fn join(&self, link: &Arc<LiveLink>) -> Join {
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
        links.all.retain(|other| !other.is_ended());
        links.all.push(link.clone());
        Join::Joined
    }

    /// Ends the links a new one from the same peer replaces: every other
    /// one this device listened for when the peer is `single`, or the one
    /// with the same connection id when it is `multi`.
    pub fn replace(&self, link: &Arc<LiveLink>, connections: Connections) {
        let others: Vec<_> = self.links.lock().unwrap().all.clone();
        for other in others {
            if !Arc::ptr_eq(&other, link)
                && other.role == Role::Listener
                && other.device_id == link.device_id
                && (connections == Connections::Single || other.connection_id == link.connection_id)
            {
                other.end(CLOSE_GOING_AWAY, LinkCloseCause::Replaced);
            }
        }
    }

    /// A live roster ends the links of a peer that left it, once settled,
    /// or that it gives another key.
    pub fn sweep(&self, roster: &Roster) {
        let links: Vec<_> = self.links.lock().unwrap().all.clone();
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
            std::mem::take(&mut links.all)
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
}

impl Live {
    fn fail(&self, which: impl Fn(&Pending) -> bool, error: DialError) {
        let mut pending = self.pending.lock().unwrap();
        let ids: Vec<String> = pending.iter().filter(|(_, entry)| which(entry)).map(|(id, _)| id.clone()).collect();
        for id in ids {
            if let Some(entry) = pending.remove(&id) {
                let _ = entry.answered.send(Err(error.clone()));
            }
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

fn stop_of(code: u16) -> Option<Stop> {
    match code {
        CLOSE_DEVICE_REMOVED => Some(Stop::Removed),
        CLOSE_SUPERSEDED => Some(Stop::Superseded),
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

/// Runs `work` until `token` is cancelled.
pub(crate) fn spawn_until(
    tasks: &TaskTracker,
    token: &CancellationToken,
    work: impl Future<Output = ()> + Send + 'static,
) {
    let token = token.clone();
    tasks.spawn(async move {
        tokio::select! {
            _ = token.cancelled() => {}
            _ = work => {}
        }
    });
}

/// What an attempt leaves behind once it ends, however it ends.
struct AttemptEnd {
    hub: Arc<HubSocket>,
    live: Arc<Live>,
    ended: CancellationToken,
}

impl Drop for AttemptEnd {
    fn drop(&mut self) {
        self.ended.cancel();
        let mut live = self.hub.live.lock().unwrap();
        if live.as_ref().is_some_and(|now| Arc::ptr_eq(now, &self.live)) {
            *live = None;
        }
        drop(live);
        self.live.fail(|_| true, DialError::NotConnected);
        self.hub.shared.roster.send_replace(None);
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

    /// Keeps the socket up, on the reconnect ladder: 1, 2, 4, 8, then 16
    /// seconds, staying at 16, and starting over after a run that stayed up
    /// 30 seconds. A wake cuts a wait on the ladder short, never one the far
    /// end asked for. Returns why the device stops.
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
                    Error::RateLimited { retry_after_seconds } => failed(Duration::from_secs_f64(retry_after_seconds)),
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
            Ok(Err(error)) => {
                return match (error.status, error.retry_after_seconds) {
                    (Some(429), Some(seconds)) => failed(Duration::from_secs_f64(seconds)),
                    _ => failed(Duration::ZERO),
                };
            }
            Ok(Ok(conn)) => Arc::new(conn),
        };
        let current = Arc::new(Live {
            conn: conn.clone(),
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(u64::from_le_bytes(crate::wire::random_bytes(8).try_into().unwrap()) >> 24),
        });
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
        let shared = self.shared.clone();
        spawn_until(&self.shared.tasks, &ended, async move {
            sleep(SETTLE).await;
            shared.roster.send_modify(|now| {
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
                *self.live.lock().unwrap() = Some(current.clone());
                self.shared.roster.send_modify(|before| {
                    let settled = before.as_ref().is_some_and(|then| then.settled);
                    *before = Some(Roster { peers, settled });
                });
                admitted.send_replace(true);
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
        current
            .pending
            .lock()
            .unwrap()
            .insert(id.clone(), Pending { device_id: device_id.to_owned(), handshake, answered });
        let frame = encode_relay_frame(&RelayFrame { kind: FrameKind::Ask, id: id.clone(), sealed });
        current.conn.send(Msg::Text(encode_relay(device_id, &frame)));
        let answer = timeout(Duration::from_millis(ASK_TIMEOUT_MS), answer).await;
        current.pending.lock().unwrap().remove(&id);
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

#[cfg(test)]
mod tests {
    use super::*;

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
