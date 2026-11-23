//! Dialing a peer (spec/link.md, "Dialing"): ask how to reach it, open
//! every candidate at once, send `hello` on one at a time in the order
//! their handshakes finished, and keep the first welcome.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::Link;
use crate::conn::{self, Conn, Msg};
use crate::device::Inner;
use crate::errors::{CandidateFailure, DialError, RefusedBy};
use crate::hub_socket::{HubSocket, Identity, Join, Shared};
use crate::link::{LinkCloseCause, LiveLink, NewLink, Role, send_sealed};
use crate::wire::ids::new_connection_id;
use crate::wire::link::{
    CLOSE_GOING_AWAY, CLOSE_PROTOCOL_ERROR, Hello, RACE_TIMEOUT_MS, Refuse, TransportMessage, finish_handshake,
    open_transport_message, start_handshake,
};
use crate::wire::noise::Transport;
use crate::wire::relay::{Candidate, ConnectInfoInput, ConnectInfoResult};

const RACE: Duration = Duration::from_millis(RACE_TIMEOUT_MS);

fn failure(candidate: &Candidate, close_code: Option<u16>, reason: impl Into<String>) -> CandidateFailure {
    CandidateFailure { kind: candidate.kind, url: candidate.url.clone(), close_code, reason: reason.into() }
}

/// A socket to the candidate, past the handshake.
async fn shake(
    inner: Arc<Inner>,
    candidate: Candidate,
    identity: Arc<Identity>,
    listener_key: [u8; 32],
) -> Result<(Conn, Transport), CandidateFailure> {
    let conn = conn::open(&candidate.url, Vec::new(), &inner.options.hooks, &inner.tasks)
        .await
        .map_err(|error| failure(&candidate, None, error.cause))?;
    let (first, mut handshake) = start_handshake(&candidate.ticket, &identity.key, listener_key, None)
        .map_err(|_| failure(&candidate, None, "the handshake failed"))?;
    conn.send(Msg::Binary(first));
    let second = conn.receive().await.map_err(|close| failure(&candidate, Some(close.code), close.reason))?;
    let transport = second.as_binary().and_then(|second| finish_handshake(&mut handshake, second));
    let transport = transport.ok_or_else(|| failure(&candidate, None, "the handshake failed"))?;
    Ok((conn, transport))
}

struct Won {
    index: usize,
    conn: Conn,
    transport: Transport,
    app_welcome: Vec<u8>,
}

pub(crate) async fn dial(
    inner: &Arc<Inner>,
    hub: &HubSocket,
    shared: &Arc<Shared>,
    identity: &Arc<Identity>,
    device_id: &str,
    app_hello: Vec<u8>,
) -> Result<Link, DialError> {
    let connection_id = new_connection_id();
    let input =
        ConnectInfoInput { dialable_kinds: inner.options.hooks.dialable_kinds(), connection_id: connection_id.clone() };
    let candidates = match hub.ask(device_id, input).await? {
        ConnectInfoResult::Available(candidates) if !candidates.is_empty() => candidates,
        _ => return Err(DialError::Unavailable),
    };
    let peer = shared.peer(device_id).ok_or(DialError::Offline)?;

    let hello = TransportMessage::Hello(Hello {
        device_id: identity.device_id.clone(),
        connection_id: connection_id.clone(),
        app_hello,
    });
    let mut refusal: Option<DialError> = None;
    let mut failures: Vec<CandidateFailure> = Vec::new();
    let race = async {
        // Each candidate's socket closes with 1000 unless it wins, as its
        // task ends or is dropped with the race.
        let mut shaken = JoinSet::new();
        for (index, candidate) in candidates.iter().enumerate() {
            let shaking = shake(inner.clone(), candidate.clone(), identity.clone(), peer.public_key);
            shaken.spawn(async move { (index, shaking.await) });
        }
        while let Some(done) = shaken.join_next().await {
            let Ok((index, shook)) = done else { continue };
            let candidate = &candidates[index];
            let (conn, mut transport) = match shook {
                Ok(shook) => shook,
                Err(failed) => {
                    failures.push(failed);
                    continue;
                }
            };
            send_sealed(&conn, &mut transport, &hello);
            let answer = match conn.receive().await {
                Ok(message) => message.as_binary().and_then(|bytes| open_transport_message(&mut transport, bytes)),
                Err(close) => {
                    failures.push(failure(candidate, Some(close.code), close.reason));
                    continue;
                }
            };
            match answer {
                Some(TransportMessage::Welcome(welcome)) => {
                    return Ok(Won { index, conn, transport, app_welcome: welcome.app_welcome });
                }
                Some(TransportMessage::Refuse(refuse)) => {
                    refusal.get_or_insert(match refuse {
                        Refuse::Colibrii { reason } => {
                            DialError::Refused { by: RefusedBy::Colibrii, reason, app_refusal: None }
                        }
                        Refuse::App { reason, app_refusal } => {
                            DialError::Refused { by: RefusedBy::App, reason, app_refusal }
                        }
                    });
                }
                _ if conn.is_open() => {
                    failures.push(failure(candidate, None, "expected welcome or refuse"));
                    conn.close(CLOSE_PROTOCOL_ERROR, "expected welcome or refuse");
                }
                _ => {}
            }
        }
        Err(())
    };
    let won = match timeout(RACE, race).await {
        Ok(Ok(won)) => won,
        Ok(Err(())) => {
            return Err(refusal.unwrap_or(DialError::Unreachable { cause: "every candidate failed".into(), failures }));
        }
        Err(_) => return Err(refusal.unwrap_or(DialError::TimedOut)),
    };
    let link = LiveLink::new(NewLink {
        conn: Arc::new(won.conn),
        transport: won.transport,
        role: Role::Dialer,
        device_id: device_id.to_owned(),
        peer_key: peer.public_key,
        connection_id,
        arrival: candidates[won.index].kind,
        greeting: won.app_welcome,
    });
    // The roster may have moved on, or gone, while the race ran.
    match shared.join(&link, None) {
        Join::Joined => {
            link.run(inner);
            Ok(Link::of(link))
        }
        joined => {
            link.end(CLOSE_GOING_AWAY, LinkCloseCause::LeftRoster);
            Err(if joined == Join::Refused { DialError::Offline } else { DialError::NotConnected })
        }
    }
}
