//! Devices in this process against the real service in workerd, which
//! `harness/src/serve.ts` starts once for each test binary and stops once
//! the binary exits and its stdin ends.
#![allow(dead_code)]

use std::future::Future;
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use colibrii::testing::{Hooks, Opened};
use colibrii::wire::relay::CandidateKind;
use colibrii::{AccountState, Connections, CredentialStore, Device, MemoryStore, Options, Watch};
use serde_json::{Value, json};

/// What a test waits for, at most.
pub const WAIT: Duration = Duration::from_secs(8);

pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}

struct Service {
    url: String,
    io: Mutex<(ChildStdin, BufReader<ChildStdout>)>,
    _child: Child,
}

fn service() -> &'static Service {
    static SERVICE: OnceLock<Service> = OnceLock::new();
    SERVICE.get_or_init(|| {
        let mut child = Command::new("node")
            .arg("harness/src/serve.ts")
            .current_dir(repo_root())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("node runs the harness");
        let stdin = child.stdin.take().unwrap();
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        stdout.read_line(&mut line).unwrap();
        let first: Value = serde_json::from_str(&line).expect("the harness prints its address");
        let url = first["url"].as_str().unwrap().trim_end_matches('/').to_owned();
        Service { url, io: Mutex::new((stdin, stdout)), _child: child }
    })
}

pub fn hub() -> String {
    service().url.clone()
}

/// A login token as `harness/src/serve.ts` reads `ask`.
pub fn token(ask: Value) -> String {
    let mut io = service().io.lock().unwrap();
    writeln!(io.0, "{ask}").unwrap();
    let mut line = String::new();
    io.1.read_line(&mut line).unwrap();
    let answer: Value = serde_json::from_str(&line).unwrap();
    answer["token"].as_str().unwrap().to_owned()
}

/// A Clerk session token for `account`, from inkwell's origin.
pub fn session_token(account: &str) -> String {
    token(json!({ "account": account }))
}

/// One signed with the key of another app's Clerk instance.
pub fn foreign_session_token(account: &str) -> String {
    token(json!({ "account": account, "otherKey": true }))
}

pub fn oauth_token(account: &str, client: &str) -> String {
    token(json!({ "account": account, "kind": "oauth", "from": client }))
}

pub fn new_account_id() -> String {
    format!("user_{}", &new_device_id()[..18])
}

pub fn new_device_id() -> String {
    colibrii::wire::ids::new_device_id()
}

pub fn new_ip() -> String {
    let bytes = colibrii::wire::random_bytes(4);
    bytes.iter().map(u8::to_string).collect::<Vec<_>>().join(".")
}

/// Where a request goes instead of the hub: `Refused` fails as if the hub
/// could not be reached, and `Silent` meets a port that takes the
/// connection and never answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Diversion {
    Refused,
    Silent,
}

fn silent_port() -> u16 {
    static PORT: OnceLock<u16> = OnceLock::new();
    *PORT.get_or_init(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let mut held: Vec<TcpStream> = Vec::new();
            for stream in listener.incoming().flatten() {
                held.push(stream);
            }
        });
        port
    })
}

type Divert = Box<dyn Fn(&str, &str) -> Option<Diversion> + Send>;

/// The network as one test's devices see it. Each request and socket comes
/// from a client address of its own, so only a test that means to reach a
/// rate limit meets one.
pub struct Net {
    divert: Mutex<Option<Divert>>,
    pub refusing: Mutex<bool>,
    pub tried: Mutex<Vec<String>>,
    pub opened: Mutex<Vec<Opened>>,
    pub dialable: Mutex<Vec<CandidateKind>>,
    pub addresses: Mutex<Option<Vec<IpAddr>>>,
    pub ports: Mutex<Vec<u16>>,
}

impl Net {
    pub fn set_divert(&self, divert: impl Fn(&str, &str) -> Option<Diversion> + Send + 'static) {
        *self.divert.lock().unwrap() = Some(Box::new(divert));
    }

    pub fn clear_divert(&self) {
        *self.divert.lock().unwrap() = None;
    }

    /// The last hub socket a device of this test opened.
    pub fn hub_socket(&self) -> Opened {
        self.opened.lock().unwrap().iter().rev().find(|opened| opened.url.contains("/connect?")).unwrap().clone()
    }
}

impl Hooks for Net {
    fn divert(&self, method: &str, url: &str) -> Option<String> {
        let divert = self.divert.lock().unwrap();
        let diversion = divert.as_ref()?(method, url)?;
        let port = match diversion {
            Diversion::Refused => 1,
            Diversion::Silent => silent_port(),
        };
        Some(format!("http://127.0.0.1:{port}/"))
    }

    fn headers(&self) -> Vec<(String, String)> {
        vec![("cf-connecting-ip".into(), new_ip())]
    }

    fn open(&self, url: &str) -> bool {
        self.tried.lock().unwrap().push(url.to_owned());
        !*self.refusing.lock().unwrap()
    }

    fn opened(&self, socket: Opened) {
        self.opened.lock().unwrap().push(socket);
    }

    fn dialable_kinds(&self) -> Vec<CandidateKind> {
        self.dialable.lock().unwrap().clone()
    }

    fn addresses(&self, found: Vec<IpAddr>) -> Vec<IpAddr> {
        self.addresses.lock().unwrap().clone().unwrap_or(found)
    }

    fn bound(&self, port: u16) {
        self.ports.lock().unwrap().push(port);
    }
}

pub struct Kit {
    pub hub: String,
    pub net: Arc<Net>,
}

impl Kit {
    pub fn new() -> Kit {
        Kit {
            hub: hub(),
            net: Arc::new(Net {
                divert: Mutex::new(None),
                refusing: Mutex::new(false),
                tried: Mutex::new(Vec::new()),
                opened: Mutex::new(Vec::new()),
                dialable: Mutex::new(vec![CandidateKind::Lan, CandidateKind::Tunnel]),
                addresses: Mutex::new(None),
                ports: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Options for an inkwell device with a store of its own.
    pub fn options(&self) -> Options {
        self.options_with(Arc::new(MemoryStore::new()), Connections::Single)
    }

    pub fn options_with(&self, store: Arc<dyn CredentialStore>, connections: Connections) -> Options {
        Options::new(&self.hub, "inkwell", connections, store).hooks(self.net.clone())
    }

    pub async fn device(&self) -> Device {
        Device::new(self.options()).await.unwrap()
    }

    pub async fn device_in(&self, store: &Arc<MemoryStore>) -> Device {
        Device::new(self.options_with(store.clone(), Connections::Single)).await.unwrap()
    }

    /// Enrolls `device` under `account`, and waits until it is connected.
    pub async fn enroll(&self, device: &Device, account: &str, name: &str) -> AccountState {
        let token = session_token(account);
        device.enroll(&token, name, None).await.unwrap();
        connected(device).await
    }

    /// Runs `run` with every request to the hub diverted.
    pub async fn diverted<T>(&self, diversion: Diversion, run: impl Future<Output = T>) -> T {
        self.net.set_divert(move |_, _| Some(diversion));
        let value = run.await;
        self.net.clear_divert();
        value
    }
}

pub async fn wait_for<T: Clone>(watch: &Watch<T>, test: impl FnMut(&T) -> bool) -> T {
    let mut watch = watch.clone();
    tokio::time::timeout(WAIT, watch.wait_for(test)).await.expect("in time").expect("the device runs")
}

pub async fn connected(device: &Device) -> AccountState {
    wait_for(&device.account(), |state| matches!(state, AccountState::Enrolled { connected: true, .. })).await
}

/// Runs `check` until it passes.
pub async fn eventually<F: Future<Output = bool>>(mut check: impl FnMut() -> F) {
    tokio::time::timeout(WAIT, async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("in time")
}

pub fn device_id_of(state: &AccountState) -> String {
    match state {
        AccountState::SignedOut { .. } => String::new(),
        AccountState::Enrolled { device_id, .. } | AccountState::Stopped { device_id, .. } => device_id.clone(),
    }
}

pub fn account_id_of(state: &AccountState) -> String {
    match state {
        AccountState::SignedOut { .. } => String::new(),
        AccountState::Enrolled { account_id, .. } | AccountState::Stopped { account_id, .. } => account_id.clone(),
    }
}

/// The ids of the devices the account lists.
pub async fn listed(device: &Device) -> Vec<String> {
    device.devices().list().await.unwrap().into_iter().map(|entry| entry.device_id).collect()
}

/// The store's document, read as JSON.
pub fn document(store: &MemoryStore) -> Option<Value> {
    store.read().unwrap().map(|text| serde_json::from_str(&text).unwrap())
}

// ---- Links ----

pub fn listening(kit: &Kit) -> Options {
    kit.options().listen(colibrii::Listen::new())
}

/// Each hello the device hears, decided by `decide` in a task of its own,
/// and the links it welcomed.
pub fn serve<F, Fut>(device: &Device, decide: F) -> tokio::sync::mpsc::UnboundedReceiver<colibrii::Link>
where
    F: Fn(colibrii::IncomingHello) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Option<colibrii::Link>> + Send + 'static,
{
    let (links, received) = tokio::sync::mpsc::unbounded_channel();
    let incoming = device.incoming();
    let decide = Arc::new(decide);
    tokio::spawn(async move {
        while let Some(hello) = incoming.next().await {
            let (decide, links) = (decide.clone(), links.clone());
            tokio::spawn(async move {
                if let Some(link) = decide(hello).await {
                    let _ = links.send(link);
                }
            });
        }
    });
    received
}

/// Welcomes every hello with "welcome " and its app hello.
pub fn welcome_all(device: &Device) -> tokio::sync::mpsc::UnboundedReceiver<colibrii::Link> {
    serve(device, |hello| async move {
        let mut welcome = b"welcome ".to_vec();
        welcome.extend_from_slice(&hello.app_hello);
        hello.welcome(welcome).await
    })
}

pub async fn next_link(links: &mut tokio::sync::mpsc::UnboundedReceiver<colibrii::Link>) -> colibrii::Link {
    tokio::time::timeout(WAIT, links.recv()).await.expect("in time").expect("a link")
}

/// Until `device`'s roster names `peer`.
pub async fn sees(device: &Device, peer: &str) {
    wait_for(&device.roster(), |peers| peers.iter().any(|entry| entry.device_id == peer)).await;
}

// ---- A stand-in for a service the SDK calls ----

pub struct Request {
    pub method: String,
    pub path: String,
    headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(key, _)| key.eq_ignore_ascii_case(name)).map(|(_, value)| value.as_str())
    }
}

/// A local HTTP server, at the origin this returns, answering each request
/// with the status and body `answer` gives.
pub async fn http_stub(answer: impl Fn(&Request) -> (u16, String) + Send + Sync + 'static) -> String {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let answer = Arc::new(answer);
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let answer = answer.clone();
            tokio::spawn(async move {
                let mut socket = tokio::io::BufReader::new(socket);
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap_or(0);
                let mut words = line.split_whitespace();
                let (method, path) =
                    (words.next().unwrap_or_default().to_owned(), words.next().unwrap_or_default().to_owned());
                let mut headers = Vec::new();
                loop {
                    line.clear();
                    if socket.read_line(&mut line).await.unwrap_or(0) <= 2 {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        headers.push((name.trim().to_owned(), value.trim().to_owned()));
                    }
                }
                let mut request = Request { method, path, headers, body: Vec::new() };
                let length = request.header("content-length").map_or(0, |length| length.parse().unwrap());
                request.body = vec![0; length];
                if socket.read_exact(&mut request.body).await.is_err() {
                    return;
                }
                let (status, body) = answer(&request);
                let reply =
                    format!("HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                let _ = socket.get_mut().write_all(reply.as_bytes()).await;
            });
        }
    });
    origin
}
