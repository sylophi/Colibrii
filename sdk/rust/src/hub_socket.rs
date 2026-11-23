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
use crate::liveness::{self, Heard};
use crate::wire::api::{TicketRequest, TicketResponse};
use crate::wire::hub::{CLOSE_DEVICE_REMOVED, CLOSE_SUPERSEDED, PING, PONG, ServiceMessage, encode_relay};
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
    tasks: TaskTracker,
    identity: Arc<Identity>,
    /// Set by the socket: none while it is down.
    pub roster: watch::Sender<Option<Roster>>,
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
    let hub = hub.trim_end_matches('/');
    let hub = hub.strip_prefix("http").map_or_else(|| hub.to_owned(), |rest| format!("ws{rest}"));
    let ticket: String = ticket
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect();
    format!("{hub}/connect?ticket={ticket}")
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
        self.hub.roster.send_replace(None);
    }
}

impl HubSocket {
    pub fn new(inner: Arc<Inner>, tasks: TaskTracker, identity: Arc<Identity>, answer: AnswerFn) -> Arc<HubSocket> {
        Arc::new(HubSocket {
            inner,
            tasks,
            identity,
            roster: watch::channel(None).0,
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
        let mut rung = 0;
        loop {
            let run = match self.clone().attempt().await {
                Ok(run) => run,
                Err(stop) => return stop,
            };
            if run.uptime >= Duration::from_millis(REDIAL_RESET_AFTER_MS) {
                rung = 0;
            }
            let delay = Duration::from_millis(REDIAL_DELAYS_MS[rung.min(REDIAL_DELAYS_MS.len() - 1)]);
            rung += 1;
            if run.wait_at_least > Duration::ZERO {
                sleep(delay.max(run.wait_at_least)).await;
            } else {
                let mut wakes = self.inner.wakes.subscribe();
                tokio::select! {
                    _ = sleep(delay) => {}
                    Ok(()) = wakes.changed() => {}
                }
            }
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
        let conn = match timeout(ADMISSION, conn::open(&url, &self.inner.options.hooks, &self.inner.tasks)).await {
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
        spawn_until(&self.tasks, &ended, async move {
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
        let roster = self.roster.clone();
        spawn_until(&self.tasks, &ended, async move {
            sleep(SETTLE).await;
            roster.send_modify(|now| {
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
                self.roster.send_modify(|before| {
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
                        spawn_until(&self.tasks, ended, async move { hub.answer_ask(&conn, from, frame).await });
                    }
                }
            }
        }
    }

    async fn answer_ask(&self, conn: &Conn, from: String, frame: RelayFrame) {
        let Some(peer_key) = self.peer(&from).map(|peer| peer.public_key) else { return };
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

    /// The peer as the latest roster names it, while the socket is up.
    pub fn peer(&self, device_id: &str) -> Option<Peer> {
        self.roster.borrow().as_ref().and_then(|roster| roster.get(device_id).cloned())
    }

    /// The asker's side: the peer's connectInfo result, or why there is
    /// none.
    pub async fn ask(&self, device_id: &str, input: ConnectInfoInput) -> Result<ConnectInfoResult, DialError> {
        let current = self.live.lock().unwrap().clone().ok_or(DialError::NotConnected)?;
        let peer = self.peer(device_id).ok_or(DialError::Offline)?;
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
