//! What runs while the device is enrolled: the hub socket, and the peers
//! and account state that follow its roster.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::device::{AccountState, Inner};
use crate::errors::Error;
use crate::hub_socket::{AnswerFn, HubSocket, Identity, spawn_until};
use crate::store::Enrollment;
use crate::wire::noise::KeyPair;
use crate::wire::relay::{Answer, ConnectInfoInput};

pub(crate) struct Session {
    pub identity: Arc<Identity>,
    pub hub: Arc<HubSocket>,
    inner: Arc<Inner>,
    ended: CancellationToken,
    tasks: TaskTracker,
}

fn answer_for() -> AnswerFn {
    Arc::new(|_from, ask| {
        Box::pin(async move {
            if ask.ask != "connectInfo" {
                return Answer::Refused("unknown-ask".into());
            }
            if ask.input.as_ref().and_then(ConnectInfoInput::decode).is_none() {
                return Answer::Refused("bad-input".into());
            }
            Answer::Refused("no-listener".into())
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
        let tasks = TaskTracker::new();
        let ended = CancellationToken::new();
        let hub = HubSocket::new(inner.clone(), tasks.clone(), identity.clone(), answer_for());
        let session = Arc::new(Session { identity, hub: hub.clone(), inner: inner.clone(), ended, tasks });

        let mut roster = hub.roster.subscribe();
        let following = inner.clone();
        let device_id = stored.device_id.clone();
        spawn_until(&session.tasks, &session.ended, async move {
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

        let credential = stored.credential;
        let settling = inner.clone();
        spawn_until(&session.tasks, &session.ended, async move {
            let stop = hub.run().await;
            // Apart from this session, which settling ends.
            let device = settling.clone();
            settling.tasks.spawn(async move { device.settle(&credential, stop).await });
        });
        Ok(session)
    }

    pub async fn end(&self) {
        self.ended.cancel();
        self.tasks.close();
        self.tasks.wait().await;
        self.inner.peers.send_replace(Vec::new());
    }
}
