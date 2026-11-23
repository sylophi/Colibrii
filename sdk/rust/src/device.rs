//! A device: one install of one app. It enrolls, keeps its hub socket up
//! while enrolled, follows its peers, dials them, and, when it listens, is
//! dialed by them.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use reqwest::Method;
use serde_json::{Map, Value};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::Link;
use crate::errors::{DialError, Error, InvalidInput, StoreOperation};
use crate::hub_api::{self, HubApi};
use crate::listener::{Hellos, IncomingHello, Listen};
use crate::runtime::{Runtime, Task, spawn_until};
use crate::session::Session;
use crate::store::{CredentialStore, Enrollment, Owed, Stored};
use crate::testing::{self, Hooks};
use crate::tunnel::TunnelOptions;
use crate::watch::Watch;
use crate::wire::api::{DeviceList, DevicePatch, EnrollRequest, EnrollResponse, ServiceError};
use crate::wire::base64url;
use crate::wire::hub::{Connections, DeviceRecord, is_device_data, is_device_name};
use crate::wire::ids::{is_device_id, new_connection_id, new_device_id};
use crate::wire::link::MAX_APP_BYTES;
use crate::wire::noise::KeyPair;

/// Why a device stopped, until the person acts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// Another copy of this `single` device connected: a second copy of
    /// the app runs on the same device id.
    Superseded,
    /// Its credential was replaced, so it enrolls again.
    CredentialRejected,
    /// The service wants a newer SDK.
    UpdateRequired,
}

/// Why the hub socket stops the device (spec/hub-socket.md, "Close codes"
/// and "Reconnecting").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stop {
    Removed,
    Stopped(StopReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountState {
    /// Not enrolled. `removed` says another device or the account's cap
    /// removed this one, until it enrolls again or the app signs it out.
    SignedOut {
        removed: bool,
    },
    Enrolled {
        device_id: String,
        account_id: String,
        /// Whether the hub socket is up.
        connected: bool,
        /// Whether the 10 seconds after the hub socket connected are over.
        /// Until then a peer missing from the roster may be one still
        /// reconnecting, so its links stay up.
        settled: bool,
    },
    Stopped {
        device_id: String,
        account_id: String,
        reason: StopReason,
    },
}

const SIGNED_OUT: AccountState = AccountState::SignedOut { removed: false };
const REMOVED: AccountState = AccountState::SignedOut { removed: true };

/// A peer online now, with the key the service admitted its socket under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub device_id: String,
    pub public_key: [u8; 32],
    pub connections: Connections,
}

/// The device's tunnel (spec/tunnels.md).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TunnelState {
    /// The device is not enrolled, does not listen, or runs no tunnel.
    Off,
    /// No `cloudflared` runs here, until the device is next made.
    NoBinary,
    /// Finding `cloudflared`, provisioning, or waiting until the hostname
    /// routes.
    Starting,
    /// Routing: only now does the device offer its tunnel to a peer.
    Up { hostname: String },
    /// It failed, and will be retried.
    Error,
}

/// What a device is made with.
#[derive(Clone)]
pub struct Options {
    pub(crate) hub: String,
    pub(crate) app: String,
    pub(crate) connections: Connections,
    pub(crate) store: Arc<dyn CredentialStore>,
    pub(crate) device_id: Option<String>,
    pub(crate) listen: Option<Listen>,
    pub(crate) tunnel: Option<TunnelOptions>,
    pub(crate) runtime: Option<tokio::runtime::Handle>,
    pub(crate) hooks: Arc<dyn Hooks>,
}

impl Options {
    /// A device of `app` at the instance whose hub address is `hub`, such
    /// as `https://hub.example.com`, keeping what it must in `store`.
    pub fn new(
        hub: impl Into<String>,
        app: impl Into<String>,
        connections: Connections,
        store: Arc<dyn CredentialStore>,
    ) -> Options {
        Options {
            hub: hub.into(),
            app: app.into(),
            connections,
            store,
            device_id: None,
            listen: None,
            tunnel: None,
            runtime: None,
            hooks: testing::none(),
        }
    }

    /// The device id to enroll under, a UUID in lowercase canonical form,
    /// which then outlives sign-outs. Without one, the device makes one up
    /// when it first enrolls, and forgets it once the service heard it
    /// signed out.
    pub fn device_id(mut self, device_id: impl Into<String>) -> Options {
        self.device_id = Some(device_id.into());
        self
    }

    /// Listens for links from the person's other devices, which arrive
    /// through [`Device::incoming`]. A device that does not listen tells a
    /// peer asking so.
    pub fn listen(mut self, listen: Listen) -> Options {
        self.listen = Some(listen);
        self
    }

    /// Runs a tunnel to the listener with `cloudflared`, so peers outside
    /// the LAN can dial it. Without one, a listening device offers its LAN
    /// addresses alone.
    pub fn tunnel(mut self, tunnel: TunnelOptions) -> Options {
        self.tunnel = Some(tunnel);
        self
    }

    /// Runs the device on this tokio runtime instead of a thread of its own.
    pub fn runtime(mut self, handle: tokio::runtime::Handle) -> Options {
        self.runtime = Some(handle);
        self
    }

    #[doc(hidden)]
    pub fn hooks(mut self, hooks: Arc<dyn Hooks>) -> Options {
        self.hooks = hooks;
        self
    }
}

/// What the store holds, and the session it runs.
#[derive(Default)]
pub(crate) struct State {
    pub enrollment: Option<Enrollment>,
    pub session: Option<Arc<Session>>,
}

pub(crate) struct Inner {
    pub options: Options,
    pub api: HubApi,
    pub account: watch::Sender<AccountState>,
    pub peers: watch::Sender<Vec<Peer>>,
    pub tunnel: watch::Sender<TunnelState>,
    pub wakes: watch::Sender<u64>,
    // Enrolling, signing out and stopping, one at a time.
    lifecycle: tokio::sync::Mutex<()>,
    pub state: Mutex<State>,
    /// The hub socket's name, kept for the life of the device.
    pub connection_id: String,
    /// Done once the device closes.
    pub closing: CancellationToken,
    /// Every task of the device's, which closing waits for.
    pub tasks: TaskTracker,
    /// The hellos for the app to decide on.
    pub incoming: Hellos,
}

/// One install of one app: enrolled under the person's account, it keeps
/// its hub socket up, follows which of the person's other devices are
/// online, and opens end-to-end encrypted links to them. Dropping it closes
/// it, as [`Device::close`] does without waiting.
///
/// Enrolling, signing out and closing, once begun, run to their end even if
/// what they returned is dropped, so the device and its store never part
/// ways halfway. Every other call stops when what it returned is dropped.
pub struct Device {
    inner: Arc<Inner>,
    runtime: Arc<Runtime>,
    closed: bool,
}

fn valid(ok: bool, field: InvalidInput) -> Result<(), Error> {
    if ok { Ok(()) } else { Err(Error::InvalidInput(field)) }
}

impl Device {
    /// Makes the device, which starts its hub socket at once when the store
    /// holds an enrollment.
    pub async fn new(options: Options) -> Result<Device, Error> {
        if let Some(device_id) = &options.device_id {
            valid(is_device_id(device_id), InvalidInput::DeviceId)?;
        }
        let runtime =
            Arc::new(Runtime::new(options.runtime.clone()).map_err(|error| Error::Runtime(error.to_string()))?);
        let inner = Arc::new(Inner {
            api: HubApi::new(&options.hub, options.hooks.clone()),
            options,
            account: watch::channel(SIGNED_OUT).0,
            peers: watch::channel(Vec::new()).0,
            tunnel: watch::channel(TunnelState::Off).0,
            wakes: watch::channel(0).0,
            lifecycle: tokio::sync::Mutex::new(()),
            state: Mutex::new(State::default()),
            connection_id: new_connection_id(),
            closing: CancellationToken::new(),
            tasks: TaskTracker::new(),
            incoming: Hellos::default(),
        });
        let started = runtime.run(inner.clone().start_up()).await;
        let device = Device { inner, runtime, closed: false };
        started?;
        Ok(device)
    }

    /// The account state now, and every change.
    pub fn account(&self) -> Watch<AccountState> {
        Watch(self.inner.account.subscribe())
    }

    /// The peers online now, with their keys, and every change. Empty while
    /// the hub socket is down.
    pub fn roster(&self) -> Watch<Vec<Peer>> {
        Watch(self.inner.peers.subscribe())
    }

    /// The tunnel's state now, and every change.
    pub fn tunnel(&self) -> Watch<TunnelState> {
        Watch(self.inner.tunnel.subscribe())
    }

    /// Enrolls under the account the login token (a Clerk session token or
    /// OAuth access token) names, with a new key pair, named `name` and
    /// with the app's device data. Enrolling again keeps the device id.
    pub async fn enroll(&self, login_token: &str, name: &str, data: Option<Map<String, Value>>) -> Result<(), Error> {
        valid(is_device_name(name), InvalidInput::Name)?;
        if let Some(data) = &data {
            valid(is_device_data(data), InvalidInput::Data)?;
        }
        let (login_token, name) = (login_token.to_owned(), name.to_owned());
        self.run_through(|inner| async move { inner.enroll(login_token, name, data).await }).await
    }

    /// Forgets the credential and reads as signed out at once, then removes
    /// this device from the account. A removal the service could not be
    /// told of is kept in the store, and delivered when a device is next
    /// made, on a wake, before an enrollment, and on the next sign-out.
    /// True once the service let the device go and nothing is owed.
    pub async fn sign_out(&self) -> Result<bool, Error> {
        self.run_through(|inner| async move { inner.sign_out().await }).await
    }

    /// Dials a peer: the link, once its app welcomed the app hello, of at
    /// most 16 KiB.
    pub async fn dial(&self, device_id: &str, app_hello: &[u8]) -> Result<Link, DialError> {
        if app_hello.len() > MAX_APP_BYTES {
            return Err(DialError::InvalidInput(InvalidInput::AppHello));
        }
        let session = self.inner.state.lock().unwrap().session.clone().ok_or(DialError::NotConnected)?;
        let (device_id, app_hello) = (device_id.to_owned(), app_hello.to_vec());
        self.runtime.run(async move { session.dial(&device_id, app_hello).await }).await
    }

    /// The hellos of peers dialing this device, for the app to welcome or
    /// refuse, while it listens.
    pub fn incoming(&self) -> Incoming {
        Incoming { inner: self.inner.clone() }
    }

    /// The account's device list.
    pub fn devices(&self) -> Devices<'_> {
        Devices { device: self }
    }

    /// The machine woke or the network went or came back: the hub socket
    /// and the dialed links check in at once, and a reconnect waits no
    /// longer. The device notices a sleep on its own, and a network change
    /// only through this.
    pub fn wake(&self) {
        self.inner.wake();
    }

    /// Counts every wake the device hears, its own and [`Device::wake`]. An
    /// app that keeps links to its peers cuts its redial waits short on
    /// each change.
    pub fn wakes(&self) -> Watch<u64> {
        Watch(self.inner.wakes.subscribe())
    }

    /// Runs `work` on the device's runtime, to its end.
    fn run_through<F>(&self, work: impl FnOnce(Arc<Inner>) -> F) -> Task<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.run_through(&self.inner.tasks, work(self.inner.clone()))
    }

    /// Runs `work` on the device's runtime, until what this returns is
    /// dropped.
    fn run<F>(&self, work: impl FnOnce(Arc<Inner>) -> F) -> Task<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.run(work(self.inner.clone()))
    }

    #[cfg(test)]
    pub(crate) fn inner(&self) -> &Arc<Inner> {
        &self.inner
    }

    /// Closes the hub socket, the listener, every link and the tunnel, and
    /// waits until they are.
    pub async fn close(mut self) {
        self.closed = true;
        let _ = self.shut_down().await;
        self.runtime.exited().await;
    }

    /// Shuts the device down on its runtime, whether or not anything waits.
    fn shut_down(&self) -> JoinHandle<()> {
        let (inner, runtime) = (self.inner.clone(), self.runtime.clone());
        self.runtime.spawn(async move {
            inner.shut_down().await;
            runtime.stop();
        })
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        if !self.closed {
            drop(self.shut_down());
        }
    }
}

/// The hellos of peers dialing the device. It ends once the device closes.
pub struct Incoming {
    inner: Arc<Inner>,
}

impl Incoming {
    pub async fn next(&self) -> Option<IncomingHello> {
        tokio::select! {
            hello = self.inner.incoming.next() => Some(hello),
            _ = self.inner.closing.cancelled() => None,
        }
    }
}

/// The account's devices, through this device's credential.
pub struct Devices<'a> {
    device: &'a Device,
}

impl Devices<'_> {
    /// Every device of the app and account, oldest first.
    pub async fn list(&self) -> Result<Vec<DeviceRecord>, Error> {
        self.device
            .run(|inner| async move {
                let answer =
                    inner.with_credential(Method::GET, "/devices".into(), None::<&()>, hub_api::DEVICE).await?;
                DeviceList::decode(&answer).map(|list| list.devices).ok_or_else(Error::unreadable)
            })
            .await
    }

    pub async fn rename(&self, device_id: &str, name: &str) -> Result<(), Error> {
        valid(is_device_name(name), InvalidInput::Name)?;
        self.update(device_id, DevicePatch { name: Some(name.to_owned()), data: None }).await
    }

    /// Replaces the device's data whole.
    pub async fn set_data(&self, device_id: &str, data: Map<String, Value>) -> Result<(), Error> {
        valid(is_device_data(&data), InvalidInput::Data)?;
        self.update(device_id, DevicePatch { name: None, data: Some(data) }).await
    }

    async fn update(&self, device_id: &str, patch: DevicePatch) -> Result<(), Error> {
        valid(is_device_id(device_id), InvalidInput::DeviceId)?;
        let path = format!("/devices/{device_id}");
        self.device
            .run(|inner| async move {
                inner.with_credential(Method::PATCH, path, Some(&patch), hub_api::DEVICE_BY_ID).await.map(|_| ())
            })
            .await
    }

    /// Removes a device. Removing this one reads as removed.
    pub async fn remove(&self, device_id: &str) -> Result<(), Error> {
        valid(is_device_id(device_id), InvalidInput::DeviceId)?;
        let device_id = device_id.to_owned();
        self.device
            .run(|inner| async move {
                let this = inner.state.lock().unwrap().enrollment.clone();
                let path = format!("/devices/{device_id}");
                inner.with_credential(Method::DELETE, path, None::<&()>, hub_api::DEVICE_BY_ID).await?;
                // Without its hub socket to say so, as while stopped.
                if let Some(this) = this.filter(|this| this.device_id == device_id) {
                    let _ = inner.settle_apart(this.credential, Stop::Removed).await;
                }
                Ok(())
            })
            .await
    }
}

impl Inner {
    async fn start_up(self: Arc<Self>) -> Result<(), Error> {
        let first = self.read_stored().await?;
        if matches!(first, Some(Stored::Removed { .. })) {
            self.account.send_replace(REMOVED);
        }
        if let Some(loaded) = first.as_ref().and_then(Stored::enrollment).cloned() {
            self.start(loaded).await?;
        }
        // A removal still owed goes now, and on every wake.
        let (inner, mut wakes) = (self.clone(), self.wakes.subscribe());
        spawn_until(&self.tasks, &self.closing, async move {
            loop {
                inner.deliver_owed().await;
                if wakes.changed().await.is_err() {
                    return;
                }
            }
        });
        spawn_until(&self.tasks, &self.closing, crate::wakes::watch_clocks(self.clone()));
        Ok(())
    }

    pub fn wake(&self) {
        self.wakes.send_modify(|count| *count += 1);
    }

    async fn shut_down(self: Arc<Self>) {
        if self.closing.is_cancelled() {
            return;
        }
        self.closing.cancel();
        let held = self.lifecycle.lock().await;
        self.end().await;
        drop(held);
        // Until every socket closed as it should.
        self.tasks.close();
        self.tasks.wait().await;
    }

    // ---- The store ----

    async fn blocking<T: Send + 'static>(
        &self,
        operation: StoreOperation,
        run: impl FnOnce(&dyn CredentialStore) -> std::io::Result<T> + Send + 'static,
    ) -> Result<T, Error> {
        let store = self.options.store.clone();
        tokio::task::spawn_blocking(move || run(store.as_ref()))
            .await
            .unwrap_or_else(|error| Err(std::io::Error::other(error.to_string())))
            .map_err(|error| Error::store(operation, error))
    }

    /// What the store holds. A document that does not decode reads as none:
    /// the device enrolls again. So does one of another device than the
    /// app's own device id.
    pub async fn read_stored(&self) -> Result<Option<Stored>, Error> {
        let text = self.blocking(StoreOperation::Read, |store| store.read()).await?;
        Ok(text
            .and_then(|text| Stored::decode(&text))
            .filter(|stored| self.options.device_id.as_ref().is_none_or(|device_id| stored.device_id() == device_id)))
    }

    pub async fn write_stored(&self, stored: Stored) -> Result<(), Error> {
        let text = stored.encode();
        self.blocking(StoreOperation::Write, move |store| store.write(&text)).await
    }

    async fn remove_stored(&self) -> Result<(), Error> {
        self.blocking(StoreOperation::Remove, |store| store.remove()).await
    }

    // ---- Sessions ----

    async fn end(&self) {
        let session = self.state.lock().unwrap().session.take();
        if let Some(session) = session {
            session.end().await;
        }
    }

    /// Runs `stored`, which the device now holds, unless it is closing.
    /// Under the lifecycle lock, or before the device is made. Boxed, as a
    /// session that stops settles, which may start another.
    fn start(self: &Arc<Self>, stored: Enrollment) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
        Box::pin(async move {
            // Closing ended the session already, and would not end this one.
            if self.closing.is_cancelled() {
                return Ok(());
            }
            self.state.lock().unwrap().enrollment = Some(stored.clone());
            self.end().await;
            self.account.send_replace(AccountState::Enrolled {
                device_id: stored.device_id.clone(),
                account_id: stored.account_id.clone(),
                connected: false,
                settled: false,
            });
            let session = Session::start(self.clone(), stored).await?;
            self.state.lock().unwrap().session = Some(session);
            Ok(())
        })
    }

    /// The device stops, if `credential` is still the one it holds.
    async fn settle(self: &Arc<Self>, credential: &str, stop: Stop) {
        let _held = self.lifecycle.lock().await;
        if self.closing.is_cancelled() {
            return;
        }
        let Some(current) = self.state.lock().unwrap().enrollment.clone() else { return };
        if current.credential != credential {
            return;
        }
        self.end().await;
        // Another run sharing the store may have acted since. One that
        // enrolled again is run.
        let stored = self.read_stored().await;
        let now = stored.as_ref().ok().and_then(Option::as_ref).and_then(Stored::enrollment).cloned();
        if let Some(now) = now.as_ref().filter(|now| now.credential != credential) {
            let _ = self.start(now.clone()).await;
            return;
        }
        let reason = match stop {
            Stop::Removed => {
                self.state.lock().unwrap().enrollment = None;
                if now.is_some() {
                    let _ = self.write_stored(Stored::Removed { device_id: current.device_id }).await;
                }
                // One that signed out left nothing to mark.
                let signed_out = matches!(stored, Ok(None | Some(Stored::Owed(_))));
                self.account.send_replace(if signed_out { SIGNED_OUT } else { REMOVED });
                return;
            }
            Stop::Stopped(reason) => reason,
        };
        self.account.send_replace(AccountState::Stopped {
            device_id: current.device_id,
            account_id: current.account_id,
            reason,
        });
    }

    /// Settles apart from the caller, to its end whether or not anything
    /// waits, such as a session that settling ends or a call dropped.
    pub fn settle_apart(self: &Arc<Self>, credential: String, stop: Stop) -> JoinHandle<()> {
        let inner = self.clone();
        self.tasks.spawn(async move { inner.settle(&credential, stop).await })
    }

    /// Settles a call's error that stops the device, apart from the call.
    pub fn settle_on(self: &Arc<Self>, credential: &str, error: &Error) {
        if let Some(stop) = hub_api::stop_of_error(error) {
            drop(self.settle_apart(credential.to_owned(), stop));
        }
    }

    /// A call under the device credential. One the service answers with the
    /// device removed, its credential replaced, or the SDK too old stops the
    /// device as the hub socket would.
    pub async fn with_credential(
        self: &Arc<Self>,
        method: Method,
        path: String,
        body: Option<&(impl serde::Serialize + Sync)>,
        route: hub_api::Route,
    ) -> Result<Value, Error> {
        let Some(current) = self.state.lock().unwrap().enrollment.clone() else { return Err(Error::NotEnrolled) };
        let answer = self.api.call(method, &path, &current.credential, body, route).await;
        if let Err(error) = &answer
            && let Some(stop) = hub_api::stop_of_error(error)
        {
            let _ = self.settle_apart(current.credential, stop).await;
        }
        answer
    }

    // ---- Enrolling and signing out ----

    /// Tells the service of a sign-out it did not hear of: true once it
    /// answered, and the store owes it no more. Under the lifecycle lock.
    async fn deliver(&self, owed: &Owed) -> Result<bool, Error> {
        let path = format!("/devices/{}", owed.device_id);
        let answer = self.api.call(Method::DELETE, &path, &owed.credential, None::<&()>, hub_api::DEVICE_BY_ID).await;
        let done = match answer {
            Ok(_) => true,
            Err(Error::Service(
                ServiceError::CredentialRejected | ServiceError::DeviceRevoked | ServiceError::UnknownDevice { .. },
            )) => true,
            Err(_) => false,
        };
        if done {
            self.remove_stored().await?;
        }
        Ok(done)
    }

    async fn deliver_owed(&self) {
        if !matches!(self.read_stored().await, Ok(Some(Stored::Owed(_)))) {
            return;
        }
        let _held = self.lifecycle.lock().await;
        // Another run sharing the store may have delivered it since.
        if let Ok(Some(Stored::Owed(owed))) = self.read_stored().await {
            let _ = self.deliver(&owed).await;
        }
    }

    async fn enroll(
        self: &Arc<Self>,
        login_token: String,
        name: String,
        data: Option<Map<String, Value>>,
    ) -> Result<(), Error> {
        let seen = self.state.lock().unwrap().enrollment.as_ref().map(|enrollment| enrollment.credential.clone());
        let _held = self.lifecycle.lock().await;
        let stored = self.read_stored().await?;
        let previous = stored.as_ref().and_then(Stored::enrollment).cloned();
        // Another run sharing the store enrolled while this one waited: this
        // one runs that enrollment.
        if let Some(previous) = previous.as_ref().filter(|previous| Some(&previous.credential) != seen.as_ref()) {
            let running =
                self.state.lock().unwrap().enrollment.as_ref().map(|enrollment| enrollment.credential.clone());
            if running.as_ref() != Some(&previous.credential) {
                self.start(previous.clone()).await?;
            }
            return Ok(());
        }
        let owed = stored.as_ref().and_then(Stored::owed).cloned();
        let owing = match &owed {
            Some(owed) => !self.deliver(owed).await?,
            None => false,
        };
        // A removal still owed keeps its device id, so enrolling replaces the
        // credential it was for, or meets DeviceEnrolledElsewhere and
        // delivers it. Once the service heard of the removal, a made-up id is
        // made up again. A device the service removed keeps its id.
        let fresh = self.options.device_id.clone().unwrap_or_else(new_device_id);
        let key = KeyPair::generate();
        let first_id = match &stored {
            Some(Stored::Owed(owed)) if owing => owed.device_id.clone(),
            Some(Stored::Enrollment(Enrollment { device_id, .. }) | Stored::Removed { device_id }) => device_id.clone(),
            _ => fresh.clone(),
        };
        let mut enrolled = self.enroll_as(&login_token, &first_id, &name, &data, &key).await;
        if let (Err(Error::Service(ServiceError::DeviceEnrolledElsewhere)), Some(owed), true) =
            (&enrolled, &owed, owing)
            && self.deliver(owed).await?
        {
            enrolled = self.enroll_as(&login_token, &fresh, &name, &data, &key).await;
        }
        let enrolled = enrolled?;
        let next = Enrollment {
            device_id: enrolled.device.device_id,
            account_id: enrolled.account_id,
            credential: enrolled.credential,
            private_key: key.private_key,
            port: previous.and_then(|previous| previous.port),
        };
        self.write_stored(Stored::Enrollment(next.clone())).await?;
        self.start(next).await
    }

    async fn enroll_as(
        &self,
        login_token: &str,
        device_id: &str,
        name: &str,
        data: &Option<Map<String, Value>>,
        key: &KeyPair,
    ) -> Result<EnrollResponse, Error> {
        let request = EnrollRequest {
            app: self.options.app.clone(),
            device_id: device_id.to_owned(),
            name: name.to_owned(),
            public_key: base64url::encode(&key.public_key),
            connections: self.options.connections,
            data: data.clone(),
        };
        let answer =
            self.api.call(Method::POST, "/devices/enroll", login_token, Some(&request), hub_api::ENROLL).await?;
        EnrollResponse::decode(&answer).ok_or_else(Error::unreadable)
    }

    async fn sign_out(self: &Arc<Self>) -> Result<bool, Error> {
        let _held = self.lifecycle.lock().await;
        // Another run sharing the store may have enrolled again, or signed
        // out, since this one last heard.
        let enrollment = self.state.lock().unwrap().enrollment.clone();
        let current = self.read_stored().await?.or(enrollment.map(Stored::Enrollment));
        let current = match current {
            None => {
                if *self.account.borrow() == REMOVED {
                    self.account.send_replace(SIGNED_OUT);
                }
                return Ok(true);
            }
            // The service let the device go: nothing to revoke.
            Some(Stored::Removed { .. }) => {
                self.remove_stored().await?;
                self.forget().await;
                return Ok(true);
            }
            // Signed out by another run sharing the store.
            Some(Stored::Owed(owed)) => {
                self.forget().await;
                return self.deliver(&owed).await;
            }
            Some(Stored::Enrollment(current)) => current,
        };
        let owed = Owed { device_id: current.device_id, credential: current.credential };
        // The store first, so a write that fails leaves the device running
        // as it was.
        self.write_stored(Stored::Owed(owed.clone())).await?;
        self.forget().await;
        self.deliver(&owed).await
    }

    /// Ends what runs, and reads as signed out.
    async fn forget(&self) {
        self.end().await;
        self.state.lock().unwrap().enrollment = None;
        self.account.send_replace(SIGNED_OUT);
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::mpsc as std_mpsc;
    use std::time::Duration;

    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::store::MemoryStore;

    /// A store whose next read or write waits for the test once it is
    /// handed a gate.
    #[derive(Default)]
    struct Gated {
        held: MemoryStore,
        gate: Mutex<Option<std_mpsc::Receiver<()>>>,
        read_gate: Mutex<Option<std_mpsc::Receiver<()>>>,
    }

    impl CredentialStore for Gated {
        fn read(&self) -> io::Result<Option<String>> {
            let gate = self.read_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                let _ = gate.recv();
            }
            self.held.read()
        }

        fn write(&self, text: &str) -> io::Result<()> {
            let gate = self.gate.lock().unwrap().take();
            if let Some(gate) = gate {
                let _ = gate.recv();
            }
            self.held.write(text)
        }

        fn remove(&self) -> io::Result<()> {
            self.held.remove()
        }
    }

    /// A device whose store holds an enrollment, with a hub that never
    /// answers.
    async fn enrolled(store: Arc<dyn CredentialStore>) -> (Device, Enrollment) {
        let enrollment = Enrollment {
            device_id: new_device_id(),
            account_id: "account".into(),
            credential: "credential".into(),
            private_key: KeyPair::generate().private_key,
            port: None,
        };
        store.write(&Stored::Enrollment(enrollment.clone()).encode()).unwrap();
        let options = Options::new("http://127.0.0.1:1", "inkwell", Connections::Single, store)
            .runtime(tokio::runtime::Handle::current());
        (Device::new(options).await.unwrap(), enrollment)
    }

    fn running(device: &Device) -> bool {
        device.inner.state.lock().unwrap().session.is_some()
    }

    async fn until(mut done: impl FnMut() -> bool) -> bool {
        timeout(Duration::from_secs(5), async {
            while !done() {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    }

    /// A store that holds an enrollment and fails every write.
    struct Unwritable(MemoryStore);

    impl CredentialStore for Unwritable {
        fn read(&self) -> io::Result<Option<String>> {
            self.0.read()
        }

        fn write(&self, _text: &str) -> io::Result<()> {
            Err(io::Error::other("full"))
        }

        fn remove(&self) -> io::Result<()> {
            self.0.remove()
        }
    }

    #[derive(Default)]
    struct Bound(Mutex<Option<u16>>);

    impl Hooks for Bound {
        fn bound(&self, port: u16) {
            *self.0.lock().unwrap() = Some(port);
        }
    }

    #[tokio::test]
    async fn a_session_whose_port_was_not_saved_stops_listening() {
        let (held, bound) = (MemoryStore::new(), Arc::new(Bound::default()));
        let enrollment = Enrollment {
            device_id: new_device_id(),
            account_id: "account".into(),
            credential: "credential".into(),
            private_key: KeyPair::generate().private_key,
            port: None,
        };
        held.write(&Stored::Enrollment(enrollment).encode()).unwrap();
        let options = Options::new("http://127.0.0.1:1", "inkwell", Connections::Single, Arc::new(Unwritable(held)))
            .listen(Listen::new())
            .hooks(bound.clone())
            .runtime(tokio::runtime::Handle::current());
        assert!(matches!(Device::new(options).await, Err(Error::Store { .. })));
        let port = bound.0.lock().unwrap().unwrap();
        let refused = async {
            while tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                sleep(Duration::from_millis(5)).await;
            }
        };
        assert!(timeout(Duration::from_secs(5), refused).await.is_ok());
    }

    #[tokio::test]
    async fn a_close_dropped_while_it_waits_still_closes() {
        let (device, _) = enrolled(Arc::new(MemoryStore::new())).await;
        let inner = device.inner.clone();
        let held = inner.lifecycle.lock().await;
        assert!(timeout(Duration::from_millis(20), device.close()).await.is_err());
        drop(held);
        assert!(until(|| inner.state.lock().unwrap().session.is_none()).await);
    }

    #[tokio::test]
    async fn an_enrollment_that_waited_on_a_closing_device_runs_nothing() {
        use futures_util::FutureExt;
        let store = Arc::new(MemoryStore::new());
        let options = Options::new("http://127.0.0.1:1", "inkwell", Connections::Single, store.clone())
            .runtime(tokio::runtime::Handle::current());
        let device = Device::new(options).await.unwrap();
        // Another run sharing the store enrolled, which this enrollment runs.
        let enrollment = Enrollment {
            device_id: new_device_id(),
            account_id: "account".into(),
            credential: "credential".into(),
            private_key: KeyPair::generate().private_key,
            port: None,
        };
        store.write(&Stored::Enrollment(enrollment).encode()).unwrap();
        let held = device.inner.lifecycle.lock().await;
        let closing = tokio::spawn(device.inner.clone().shut_down());
        assert!(until(|| device.inner.closing.is_cancelled()).await);
        assert!(device.enroll("login", "Laptop", None).now_or_never().is_none());
        tokio::task::yield_now().await;
        drop(held);
        closing.await.unwrap();
        assert!(!running(&device));
    }

    #[tokio::test]
    async fn a_list_dropped_while_it_waits_lets_its_request_go() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub = format!("http://{}", listener.local_addr().unwrap());
        let (heard, mut listing) = tokio::sync::mpsc::unbounded_channel();
        let (let_go, mut gone) = tokio::sync::mpsc::unbounded_channel();
        // Takes every request, answers none, and says when the list's was
        // let go.
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let (heard, let_go) = (heard.clone(), let_go.clone());
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buffer = [0; 1024];
                    while !request.windows(4).any(|end| end == b"\r\n\r\n") {
                        match stream.read(&mut buffer).await {
                            Ok(0) | Err(_) => return,
                            Ok(read) => request.extend_from_slice(&buffer[..read]),
                        }
                    }
                    if !request.starts_with(b"GET /devices ") {
                        return std::future::pending().await;
                    }
                    let _ = heard.send(());
                    while let Ok(1..) = stream.read(&mut buffer).await {}
                    let _ = let_go.send(());
                });
            }
        });
        let store = Arc::new(MemoryStore::new());
        let enrollment = Enrollment {
            device_id: new_device_id(),
            account_id: "account".into(),
            credential: "credential".into(),
            private_key: KeyPair::generate().private_key,
            port: None,
        };
        store.write(&Stored::Enrollment(enrollment).encode()).unwrap();
        let options =
            Options::new(hub, "inkwell", Connections::Single, store).runtime(tokio::runtime::Handle::current());
        let device = Device::new(options).await.unwrap();
        let devices = device.devices();
        tokio::select! {
            _ = devices.list() => panic!("the hub answers nothing"),
            _ = listing.recv() => {}
        }
        assert!(timeout(Duration::from_secs(5), gone.recv()).await.is_ok());
    }

    #[tokio::test]
    async fn a_sign_out_dropped_while_its_store_write_runs_still_signs_out() {
        let store = Arc::new(Gated::default());
        let (device, _) = enrolled(store.clone()).await;
        let (open, gate) = std_mpsc::channel();
        *store.gate.lock().unwrap() = Some(gate);
        let writing = async {
            while store.gate.lock().unwrap().is_some() {
                sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::select! {
            _ = device.sign_out() => panic!("the write is held"),
            () = writing => {}
        }
        open.send(()).unwrap();
        let mut account = device.account();
        let signed_out = timeout(Duration::from_secs(5), account.wait_for(|state| *state == SIGNED_OUT));
        assert!(signed_out.await.is_ok());
        assert!(!running(&device));
    }

    #[tokio::test]
    async fn a_sign_out_after_another_run_signed_out_ends_this_one() {
        let store = Arc::new(MemoryStore::new());
        let (device, enrollment) = enrolled(store.clone()).await;
        let owed = Owed { device_id: enrollment.device_id, credential: enrollment.credential };
        store.write(&Stored::Owed(owed).encode()).unwrap();
        device.sign_out().await.unwrap();
        assert_eq!(device.account().get(), SIGNED_OUT);
        assert!(!running(&device));
    }

    #[test]
    fn a_close_leaves_its_caller_free_while_the_device_thread_ends() {
        let (done, finished) = std_mpsc::channel();
        std::thread::spawn(move || {
            let caller = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            caller.block_on(async {
                let store = Arc::new(Gated::default());
                let options = Options::new("http://127.0.0.1:1", "inkwell", Connections::Single, store.clone());
                let device = Device::new(options).await.unwrap();
                // A store read still running once the device's tasks ended.
                let (open, gate) = std_mpsc::channel();
                *store.read_gate.lock().unwrap() = Some(gate);
                device.wake();
                assert!(until(|| store.read_gate.lock().unwrap().is_none()).await);
                let opening = async {
                    sleep(Duration::from_millis(50)).await;
                    open.send(()).unwrap();
                };
                tokio::join!(device.close(), opening);
            });
            let _ = done.send(());
        });
        assert!(finished.recv_timeout(Duration::from_secs(5)).is_ok());
    }

    #[tokio::test]
    async fn a_removal_answered_with_anything_but_its_204_stays_owed() {
        let store = Arc::new(MemoryStore::new());
        let owed = Owed { device_id: new_device_id(), credential: "credential".into() };
        store.write(&Stored::Owed(owed.clone()).encode()).unwrap();
        let hub = crate::fakes::hub_answering("202 Accepted").await;
        let options =
            Options::new(hub, "inkwell", Connections::Single, store.clone()).runtime(tokio::runtime::Handle::current());
        let device = Device::new(options).await.unwrap();
        assert!(!device.sign_out().await.unwrap());
        assert_eq!(store.read().unwrap().as_deref().and_then(Stored::decode), Some(Stored::Owed(owed)));
    }
}
