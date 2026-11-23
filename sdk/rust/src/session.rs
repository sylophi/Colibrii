//! What runs while the device is enrolled: the listener, the hub socket,
//! and the peers, links and account state that follow its roster.

use std::sync::Arc;

use crate::Link;
use crate::device::{AccountState, Inner, TunnelState};
use crate::errors::{DialError, Error};
use crate::hub_api;
use crate::hub_socket::{AnswerFn, HubSocket, Identity, Shared};
use crate::listener::Listener;
use crate::store::{Enrollment, Stored};
use crate::tunnel::{self, Provision};
use crate::wire::api::{TunnelRequest, TunnelResponse};
use crate::wire::noise::KeyPair;
use crate::wire::relay::{Answer, ConnectInfoInput};
use reqwest::Method;

pub(crate) struct Session {
    identity: Arc<Identity>,
    hub: Arc<HubSocket>,
    shared: Arc<Shared>,
    inner: Arc<Inner>,
}

fn answer_for(listener: Option<Arc<Listener>>) -> AnswerFn {
    Arc::new(move |from, ask| {
        let listener = listener.clone();
        Box::pin(async move {
            if ask.ask != "connectInfo" {
                return Answer::Refused("unknown-ask".into());
            }
            let Some(input) = ask.input.as_ref().and_then(ConnectInfoInput::decode) else {
                return Answer::Refused("bad-input".into());
            };
            match listener {
                None => Answer::Refused("no-listener".into()),
                Some(listener) => Answer::Ok(listener.connect_info(&from, &input).await),
            }
        })
    })
}

/// `POST /tunnel` for the port. An error that stops the device settles it
/// apart from the tunnel's run, which settling ends.
fn provision(inner: &Arc<Inner>, credential: &str, port: u16) -> Provision {
    let (inner, credential) = (inner.clone(), credential.to_owned());
    Arc::new(move || {
        let (inner, credential) = (inner.clone(), credential.clone());
        Box::pin(async move {
            let request = TunnelRequest { port };
            match inner.api.call(Method::POST, "/tunnel", &credential, Some(&request), hub_api::DEVICE).await {
                Ok(answer) => serde_json::from_value::<TunnelResponse>(answer).ok(),
                Err(error) => {
                    inner.settle_on(&credential, &error);
                    None
                }
            }
        })
    })
}

impl Session {
    pub async fn start(inner: Arc<Inner>, stored: Enrollment) -> Result<Arc<Session>, Error> {
        let identity = Arc::new(Identity {
            device_id: stored.device_id.clone(),
            credential: stored.credential.clone(),
            key: KeyPair::from_private_key(stored.private_key),
        });
        let shared = Shared::new();
        let listener = match &inner.options.listen {
            None => None,
            Some(listen) => {
                let started =
                    Listener::start(inner.clone(), identity.clone(), shared.clone(), listen, stored.port.unwrap_or(0))
                        .await;
                let listener = match started {
                    Ok(listener) => listener,
                    Err(error) => {
                        shared.ended.cancel();
                        return Err(Error::Listen(error.to_string()));
                    }
                };
                if Some(listener.port) != stored.port {
                    let current = inner.state.lock().unwrap().enrollment.clone();
                    if let Some(mut current) = current.filter(|current| current.credential == stored.credential) {
                        current.port = Some(listener.port);
                        inner.state.lock().unwrap().enrollment = Some(current.clone());
                        inner.write_stored(Stored::Enrollment(current)).await?;
                    }
                }
                if let Some(options) = inner.options.tunnel.clone() {
                    let (running, runner) = tunnel::run(
                        options,
                        inner.options.hooks.clone(),
                        provision(&inner, &stored.credential, listener.reached_at),
                        shared.ended.clone(),
                        inner.tunnel.clone(),
                    );
                    // Not cut short as the session ends: it stops its
                    // cloudflared first.
                    shared.tasks.spawn(runner);
                    let (checking, mut wakes) = (running.clone(), inner.wakes.subscribe());
                    shared.spawn(async move {
                        while wakes.changed().await.is_ok() {
                            checking.check();
                        }
                    });
                    let _ = listener.tunnel.set(running);
                }
                Some(listener)
            }
        };
        let hub = HubSocket::new(inner.clone(), shared.clone(), identity.clone(), answer_for(listener));
        let session = Arc::new(Session { identity, hub: hub.clone(), shared: shared.clone(), inner: inner.clone() });

        let (mut roster, following) = (shared.follow_roster(), inner.clone());
        let device_id = stored.device_id.clone();
        shared.spawn(async move {
            loop {
                let now = roster.borrow_and_update().clone();
                following.peers.send_replace(now.as_ref().map(|now| now.peers.clone()).unwrap_or_default());
                following.account.send_modify(|state| {
                    if let AccountState::Enrolled { device_id: enrolled, connected, settled, .. } = state
                        && *enrolled == device_id
                    {
                        *connected = now.is_some();
                        *settled = now.as_ref().is_some_and(|now| now.settled);
                    }
                });
                if roster.changed().await.is_err() {
                    return;
                }
            }
        });

        let (credential, settling) = (stored.credential, inner.clone());
        shared.spawn(async move { settling.settle_apart(credential, hub.run().await) });
        Ok(session)
    }

    pub async fn dial(&self, device_id: &str, app_hello: Vec<u8>) -> Result<Link, DialError> {
        let dialing = crate::dial::dial(&self.inner, &self.hub, &self.shared, &self.identity, device_id, app_hello);
        tokio::select! {
            dialed = dialing => dialed,
            _ = self.shared.ended.cancelled() => Err(DialError::NotConnected),
        }
    }

    pub async fn end(&self) {
        self.shared.end_links();
        self.shared.ended.cancel();
        self.shared.tasks.close();
        self.shared.tasks.wait().await;
        self.inner.peers.send_replace(Vec::new());
        self.inner.tunnel.send_replace(TunnelState::Off);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes;
    use crate::hub_socket::{FakeAttempt, Join};

    #[tokio::test]
    async fn a_peer_that_leaves_a_settled_roster_and_comes_back_at_once_loses_its_links() {
        let device = fakes::device().await;
        let inner = device.inner().clone();
        let enrollment = Enrollment {
            device_id: fakes::ids().0,
            account_id: "account".into(),
            credential: "credential".into(),
            private_key: KeyPair::generate().private_key,
            port: None,
        };
        let session = Session::start(inner.clone(), enrollment).await.unwrap();
        let attempt = FakeAttempt::new(&inner, session.shared.clone()).await;
        let (peer, connection_id) = fakes::ids();
        let (link, key, _far) = fakes::listened(&inner, &peer, &connection_id).await;
        let (with, without) = (fakes::presence(&[(&peer, key)]), fakes::presence(&[]));
        attempt.hear(&with);
        attempt.settle();
        assert_eq!(session.shared.join(&link), Join::Joined);
        attempt.hear(&without);
        attempt.hear(&with);
        tokio::task::yield_now().await;
        assert!(link.is_ended());
        session.end().await;
    }
}
