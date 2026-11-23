//! The device's tunnel (spec/tunnels.md, "What the device does"): finds a
//! `cloudflared`, provisions the tunnel through the service, runs it, waits
//! until the hostname routes, restarts it on a ladder, and ends one a
//! crashed run left behind. The connector token reaches `cloudflared` in
//! its environment alone.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

use crate::cloudflared::pinned_path;
use crate::device::TunnelState;
use crate::testing::Hooks;
use crate::wire::api::TunnelResponse;

/// Where and with what the device runs its tunnel.
#[derive(Debug, Clone)]
pub struct TunnelOptions {
    pub(crate) directory: PathBuf,
    pub(crate) cloudflared: Option<PathBuf>,
    pub(crate) timings: Timings,
}

impl TunnelOptions {
    /// A directory of the device's own. The runner keeps the pid of its
    /// `cloudflared` there, and finds the copy [`crate::fetch_cloudflared`]
    /// put there.
    pub fn new(directory: impl Into<PathBuf>) -> TunnelOptions {
        TunnelOptions { directory: directory.into(), cloudflared: None, timings: Timings::default() }
    }

    /// A `cloudflared` the app gives, tried first.
    pub fn cloudflared(mut self, path: impl Into<PathBuf>) -> TunnelOptions {
        self.cloudflared = Some(path.into());
        self
    }

    #[doc(hidden)]
    pub fn timings(mut self, timings: Timings) -> TunnelOptions {
        self.timings = timings;
        self
    }
}

/// The spec's timings, which the SDK's tests shorten.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct Timings {
    /// One fetch of the hostname.
    pub try_within: Duration,
    /// The waits before each try, staying on the last, for a hostname the
    /// provision just created and for one that existed.
    pub fresh_tries: Vec<Duration>,
    pub existing_tries: Vec<Duration>,
    /// From the start, or from a check that found it not routing.
    pub route_within: Duration,
    pub restarts: Vec<Duration>,
    /// A run up this long starts the restarts over.
    pub healthy_for: Duration,
    /// From asking `cloudflared` to end to killing it.
    pub stop_grace: Duration,
}

impl Default for Timings {
    fn default() -> Timings {
        let ms = Duration::from_millis;
        Timings {
            try_within: ms(5_000),
            fresh_tries: vec![ms(5_000), ms(8_000)],
            existing_tries: vec![ms(1_000), ms(2_000), ms(4_000), ms(8_000)],
            route_within: ms(60_000),
            restarts: vec![ms(1_000), ms(2_000), ms(4_000), ms(8_000), ms(16_000), ms(60_000)],
            healthy_for: ms(30_000),
            stop_grace: ms(3_000),
        }
    }
}

const ARGS: [&str; 5] = ["tunnel", "--no-autoupdate", "--ha-connections", "1", "run"];
const PID_FILE: &str = "cloudflared.pid";
const LOG_FILE: &str = "cloudflared.log";
/// The hostname a fresh provision last routed with. Its record is out,
/// however new, so the next run tries it as `cloudflared` registers.
const REACHED_FILE: &str = "cloudflared.reached";
/// How often the log is read until it says `cloudflared` registered.
const LOG_READ_EVERY: Duration = Duration::from_millis(100);
/// How often a leftover asked to end is checked for until it has.
const LEFTOVER_CHECK_EVERY: Duration = Duration::from_millis(50);
const VERSION_WITHIN: Duration = Duration::from_secs(10);
const IDENTIFY_WITHIN: Duration = Duration::from_secs(5);

/// The ladder's wait for the given rung, staying on the last.
fn rung(delays: &[Duration], rung: usize) -> Duration {
    delays.get(rung.min(delays.len().saturating_sub(1))).copied().unwrap_or_default()
}

/// What a program wrote on stdout, or nothing when it failed or took too
/// long.
async fn output(program: &Path, args: &[&str], within: Duration, env: &[(&str, &str)]) -> Option<String> {
    let mut command = Command::new(program);
    command.args(args).stdin(Stdio::null()).stderr(Stdio::null()).kill_on_drop(true);
    for (name, value) in env {
        command.env(name, value);
    }
    hide_window(&mut command);
    let ran = timeout(within, command.output()).await.ok()?.ok()?;
    ran.status.success().then(|| String::from_utf8_lossy(&ran.stdout).into_owned())
}

#[cfg(windows)]
fn hide_window(command: &mut Command) {
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    command.creation_flags(CREATE_NO_WINDOW);
}

#[cfg(not(windows))]
fn hide_window(_command: &mut Command) {}

/// The first candidate that answers `--version`: the app's, the pinned
/// copy, then one on the `PATH`.
async fn find_binary(options: &TunnelOptions, hooks: &dyn Hooks) -> Option<PathBuf> {
    let candidates = options
        .cloudflared
        .iter()
        .cloned()
        .chain([pinned_path(&options.directory), PathBuf::from(hooks.cloudflared_on_path())]);
    for candidate in candidates {
        if output(&candidate, &["--version"], VERSION_WITHIN, &[]).await.is_some() {
            return Some(candidate);
        }
    }
    None
}

/// When the process `pid` started, as the system's process table says, or
/// nothing when it can't tell. With the pid, it names one process: a pid
/// given to another program since names another start.
async fn start_of(pid: u32) -> String {
    let started = if cfg!(windows) {
        let query = format!("(Get-Process -Id {pid}).StartTime.ToFileTimeUtc()");
        output(
            Path::new("powershell.exe"),
            &["-NoProfile", "-NonInteractive", "-Command", &query],
            IDENTIFY_WITHIN,
            &[],
        )
        .await
    } else {
        let pid = pid.to_string();
        output(Path::new("ps"), &["-o", "lstart=", "-p", &pid], IDENTIFY_WITHIN, &[("LC_ALL", "C"), ("TZ", "UTC")])
            .await
    };
    started.unwrap_or_default().trim().to_owned()
}

/// The pid file: the pid, and when that process started.
#[doc(hidden)]
pub async fn pid_record(pid: u32) -> String {
    format!("{pid}\n{}\n", start_of(pid).await)
}

#[cfg(unix)]
fn signal(pid: u32, kill: bool) {
    let signal = if kill { libc::SIGKILL } else { libc::SIGTERM };
    // SAFETY: kill(2) only sends a signal.
    unsafe {
        libc::kill(pid as libc::pid_t, signal);
    }
}

#[cfg(unix)]
fn is_alive(pid: u32) -> bool {
    // SAFETY: signal 0 only checks that the process exists.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[cfg(windows)]
fn signal(pid: u32, _kill: bool) {
    let _ = std::process::Command::new("taskkill").args(["/F", "/PID", &pid.to_string()]).output();
}

#[cfg(windows)]
fn is_alive(pid: u32) -> bool {
    std::process::Command::new("tasklist")
        .args(["/NH", "/FI", &format!("PID eq {pid}")])
        .output()
        .is_ok_and(|listed| String::from_utf8_lossy(&listed.stdout).contains(&pid.to_string()))
}

/// Ends the `cloudflared` a run that crashed left running in a tunnel's
/// directory, and only that process: its pid may have gone to another
/// program since. Asked to end, it leaves the edge's routing at once and
/// closes the links it carries as their listener goes, which may be a
/// moment after the run that crashed. Killed outright, it would leave both
/// to the edge, which notices seconds later. A device does this as its
/// tunnel starts, and an app may at its own start.
pub async fn end_leftover_cloudflared(directory: impl AsRef<Path>) {
    crate::runtime::on_own_runtime(end_leftover(directory.as_ref().to_owned(), Timings::default().stop_grace)).await
}

pub(crate) async fn end_leftover(directory: PathBuf, grace: Duration) {
    let pid_file = directory.join(PID_FILE);
    let Ok(text) = tokio::fs::read_to_string(&pid_file).await else { return };
    let mut lines = text.split('\n');
    let pid = lines.next().unwrap_or_default().parse::<u32>().ok().filter(|pid| *pid > 1);
    let started = lines.next().unwrap_or_default().to_owned();
    if let Some(pid) = pid
        && !started.is_empty()
        && start_of(pid).await == started
    {
        signal(pid, false);
        let ended = timeout(grace, async {
            while is_alive(pid) {
                sleep(LEFTOVER_CHECK_EVERY).await;
            }
        })
        .await;
        // Its pid may have gone to another program while it ended.
        if ended.is_err() && start_of(pid).await == started {
            signal(pid, true);
        }
    }
    let _ = tokio::fs::remove_file(&pid_file).await;
}

/// Asks it to end, and kills it if it has not within the grace. On Windows
/// the first already ends it outright.
async fn stop(child: &mut Child, grace: Duration) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        signal(pid, false);
        if timeout(grace, child.wait()).await.is_ok() {
            return;
        }
    }
    let _ = child.start_kill();
    let _ = child.wait().await;
}

/// `POST /tunnel` for the listener's port, or nothing when it failed.
pub(crate) type Provision = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Option<TunnelResponse>> + Send>> + Send + Sync>;

/// A tunnel the runner keeps up.
pub(crate) struct RunningTunnel {
    pub state: watch::Sender<TunnelState>,
    checks: mpsc::Sender<()>,
}

impl RunningTunnel {
    /// Asks a tunnel that is up whether it still routes, and one that found
    /// no `cloudflared` to look again.
    pub fn check(&self) {
        if matches!(*self.state.borrow(), TunnelState::Up { .. } | TunnelState::NoBinary) {
            let _ = self.checks.try_send(());
        }
    }
}

struct Runner {
    options: TunnelOptions,
    hooks: Arc<dyn Hooks>,
    provision: Provision,
    state: watch::Sender<TunnelState>,
    checks: tokio::sync::Mutex<mpsc::Receiver<()>>,
    http: reqwest::Client,
    /// The provision the last run routed with, which the next run reuses.
    routed: Mutex<Option<TunnelResponse>>,
    ended: CancellationToken,
}

/// Runs the tunnel until `ended`, then stops its `cloudflared`. The
/// returned future is the runner.
pub(crate) fn run(
    options: TunnelOptions,
    hooks: Arc<dyn Hooks>,
    provision: Provision,
    ended: CancellationToken,
) -> (Arc<RunningTunnel>, impl Future<Output = ()> + Send) {
    let state = watch::channel(TunnelState::Starting).0;
    let (checks, checked) = mpsc::channel(1);
    let running = Arc::new(RunningTunnel { state: state.clone(), checks });
    let runner = Arc::new(Runner {
        options,
        hooks,
        provision,
        state,
        checks: tokio::sync::Mutex::new(checked),
        http: reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().expect("a client"),
        routed: Mutex::new(None),
        ended,
    });
    (running, async move { runner.run().await })
}

impl Runner {
    fn set(&self, state: TunnelState) {
        self.state.send_replace(state);
    }

    async fn run(&self) {
        let ended = self.ended.clone();
        let found = async {
            let (_, binary) = tokio::join!(
                end_leftover(self.options.directory.clone(), self.options.timings.stop_grace),
                find_binary(&self.options, self.hooks.as_ref())
            );
            if binary.is_none() {
                self.set(TunnelState::NoBinary);
            }
            let mut binary = binary;
            while binary.is_none() {
                self.checks.lock().await.recv().await;
                binary = find_binary(&self.options, self.hooks.as_ref()).await;
            }
            binary
        };
        let binary = tokio::select! {
            binary = found => binary.expect("a binary"),
            _ = ended.cancelled() => return,
        };
        let timings = &self.options.timings;
        let mut step = 0;
        loop {
            let Some(up) = self.attempt(&binary).await else { return };
            if up >= timings.healthy_for {
                step = 0;
            }
            let wait = rung(&timings.restarts, step);
            step += 1;
            tokio::select! {
                _ = sleep(wait) => {}
                _ = ended.cancelled() => return,
            }
        }
    }

    /// One fetch of the hostname: anything below 500 came from the
    /// listener.
    async fn routes(&self, hostname: &str) -> bool {
        let probe = async {
            let response = self.http.get(self.hooks.probe_url(hostname)).send().await.ok()?;
            Some(response.status().as_u16() < 500)
        };
        timeout(self.options.timings.try_within, probe).await.ok().flatten().unwrap_or(false)
    }

    /// Whether the hostname routes within the deadline, trying after each
    /// wait in turn.
    async fn routes_soon(&self, hostname: &str, waits: &[Duration]) -> bool {
        let trying = async {
            sleep(waits.first().copied().unwrap_or_default()).await;
            let rest = &waits[waits.len().min(1)..];
            let mut step = 0;
            while !self.routes(hostname).await {
                sleep(rung(rest, step)).await;
                step += 1;
            }
        };
        timeout(self.options.timings.route_within, trying).await.is_ok()
    }

    /// Up until a check finds it not routing and it does not route again in
    /// time.
    async fn while_up(&self, hostname: &str) {
        loop {
            self.checks.lock().await.recv().await;
            if self.routes(hostname).await {
                continue;
            }
            self.set(TunnelState::Starting);
            if !self.routes_soon(hostname, &self.options.timings.existing_tries).await {
                return;
            }
            self.set(TunnelState::Up { hostname: hostname.to_owned() });
        }
    }

    /// One run from provisioning to failing: how long it was up, or nothing
    /// once the tunnel ended.
    async fn attempt(&self, binary: &Path) -> Option<Duration> {
        self.set(TunnelState::Starting);
        let reused = self.routed.lock().unwrap().take();
        let current = match reused.clone() {
            Some(reused) => Some(reused),
            None => tokio::select! {
                provisioned = (self.provision)() => provisioned,
                _ = self.ended.cancelled() => return None,
            },
        };
        let mut up_at = None;
        if let Some(current) = current {
            let directory = &self.options.directory;
            let hostname = current.hostname.clone();
            let reached_before =
                tokio::fs::read_to_string(directory.join(REACHED_FILE)).await.is_ok_and(|reached| reached == hostname);
            let fresh = reused.is_none() && current.fresh && !reached_before;
            let timings = &self.options.timings;
            let waits = if fresh { &timings.fresh_tries } else { &timings.existing_tries };
            if let Some(mut child) = self.spawn(binary, &current.connector_token).await {
                let routing = async {
                    // For a hostname that existed, a try as `cloudflared`
                    // registers, which the ladder would likely miss by
                    // seconds, and the ladder for when it says nothing or
                    // the hostname does not route yet. A fresh one waits out
                    // its first wait: a lookup before its record is out is a
                    // miss resolvers keep for the zone's negative TTL.
                    let reached = if fresh {
                        self.routes_soon(&hostname, waits).await
                    } else {
                        let at_once: Vec<Duration> =
                            [Duration::ZERO].into_iter().chain(waits.iter().skip(1).copied()).collect();
                        tokio::select! {
                            reached = self.routes_soon(&hostname, waits) => reached,
                            reached = async {
                                self.registered().await;
                                self.routes_soon(&hostname, &at_once).await
                            } => reached,
                        }
                    };
                    if !reached {
                        return;
                    }
                    *self.routed.lock().unwrap() = Some(current.clone());
                    if fresh {
                        let _ = tokio::fs::write(directory.join(REACHED_FILE), &hostname).await;
                    }
                    up_at = Some(Instant::now());
                    while self.checks.lock().await.try_recv().is_ok() {}
                    self.set(TunnelState::Up { hostname: hostname.clone() });
                    self.while_up(&hostname).await;
                };
                let ended = tokio::select! {
                    _ = child.wait() => false,
                    _ = routing => false,
                    _ = self.ended.cancelled() => true,
                };
                // Before it is stopped, so it never reads as up meanwhile.
                self.set(TunnelState::Error);
                stop(&mut child, timings.stop_grace).await;
                let _ = tokio::fs::remove_file(directory.join(PID_FILE)).await;
                if ended {
                    return None;
                }
            }
        }
        self.set(TunnelState::Error);
        Some(up_at.map_or(Duration::ZERO, |at| at.elapsed()))
    }

    /// `cloudflared`, with its pid and start in the directory. Its log goes
    /// to a file there, never a pipe: it ends at a write to a pipe no one
    /// reads, and one the device's process left behind still closes the
    /// links it carries as their listener goes.
    async fn spawn(&self, binary: &Path, connector_token: &str) -> Option<Child> {
        let directory = &self.options.directory;
        tokio::fs::create_dir_all(directory).await.ok()?;
        let log = std::fs::File::create(directory.join(LOG_FILE)).ok()?;
        let mut command = Command::new(binary);
        command
            .args(ARGS)
            .env("TUNNEL_TOKEN", connector_token)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .kill_on_drop(true);
        hide_window(&mut command);
        let child = command.spawn().ok()?;
        if let Some(pid) = child.id() {
            let _ = tokio::fs::write(directory.join(PID_FILE), pid_record(pid).await).await;
        }
        Some(child)
    }

    /// Once the log says it registered a connection at the edge.
    async fn registered(&self) {
        let log = self.options.directory.join(LOG_FILE);
        loop {
            if tokio::fs::read_to_string(&log).await.is_ok_and(|log| log.contains("Registered tunnel connection")) {
                return;
            }
            sleep(LOG_READ_EVERY).await;
        }
    }
}

/// The runner against a fake `cloudflared` (a shell script that notes each
/// run and stays up until ended), with a local HTTP stub for the hostname.
#[cfg(all(test, unix))]
mod tests {
    use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    const FAKE: &str = r#"#!/bin/sh
if [ "$1" = "--version" ]; then echo "cloudflared version fake"; exit 0; fi
dir=$(dirname "$0")
if [ -e "$dir/registers" ]; then echo "INF Registered tunnel connection connIndex=0 protocol=quic" >&2; fi
if [ -f /dev/stderr ]; then file=true; else file=false; fi
printf '{"pid":%s,"args":"%s","token":"%s","logsToFile":%s}\n' "$$" "$*" "$TUNNEL_TOKEN" "$file" >> "$dir/runs.log"
while :; do sleep 1; done
"#;
    const TOKEN: &str = "connector-token-for-tests";
    const HOSTNAME: &str = "a1b2c3.tunnel.test";
    const WAIT: Duration = Duration::from_secs(8);

    fn fast() -> Timings {
        let ms = Duration::from_millis;
        Timings {
            try_within: ms(500),
            fresh_tries: vec![ms(150), ms(100)],
            existing_tries: vec![ms(50), ms(100)],
            route_within: ms(1_500),
            restarts: vec![ms(100), ms(200)],
            healthy_for: ms(60_000),
            stop_grace: ms(500),
        }
    }

    /// The hostname's edge, answering every request with `status`.
    struct Edge {
        origin: String,
        status: Arc<AtomicU16>,
    }

    async fn edge(status: u16) -> Edge {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let status = Arc::new(AtomicU16::new(status));
        let answering = status.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let status = answering.load(Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut request = [0u8; 4096];
                    let _ = socket.read(&mut request).await;
                    let answer = format!("HTTP/1.1 {status} X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
                    let _ = socket.write_all(answer.as_bytes()).await;
                });
            }
        });
        Edge { origin, status }
    }

    struct Probe {
        origin: String,
    }

    impl Hooks for Probe {
        fn cloudflared_on_path(&self) -> String {
            "/nonexistent/cloudflared".into()
        }

        fn probe_url(&self, hostname: &str) -> String {
            format!("{}/{hostname}", self.origin)
        }
    }

    /// A directory of its own, with the fake as its pinned copy.
    fn directory() -> PathBuf {
        let path = std::env::temp_dir().join(format!("colibrii-tunnel-{}", crate::wire::ids::new_connection_id()));
        std::fs::create_dir_all(&path).unwrap();
        install(&path);
        path
    }

    fn install(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(pinned_path(path), FAKE).unwrap();
        std::fs::set_permissions(pinned_path(path), std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Run {
        pid: u32,
        args: String,
        token: String,
        logs_to_file: bool,
    }

    fn runs_in(path: &Path) -> Vec<Run> {
        std::fs::read_to_string(path.join("runs.log"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    async fn until(mut test: impl FnMut() -> bool) {
        timeout(WAIT, async {
            while !test() {
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("in time");
    }

    fn provision_for(fresh: bool) -> (Arc<AtomicUsize>, Provision) {
        let counted = Arc::new(AtomicUsize::new(0));
        let counting = counted.clone();
        let provision: Provision = Arc::new(move || {
            counting.fetch_add(1, Ordering::SeqCst);
            Box::pin(
                async move { Some(TunnelResponse { hostname: HOSTNAME.into(), connector_token: TOKEN.into(), fresh }) },
            )
        });
        (counted, provision)
    }

    /// A tunnel running until the returned token is cancelled and its
    /// runner awaited.
    struct Running {
        tunnel: Arc<RunningTunnel>,
        ended: CancellationToken,
        runner: tokio::task::JoinHandle<()>,
    }

    impl Running {
        async fn end(self) {
            self.ended.cancel();
            self.runner.await.unwrap();
        }

        async fn status(&self, wanted: fn(&TunnelState) -> bool) -> TunnelState {
            let mut state = self.tunnel.state.subscribe();
            timeout(WAIT, state.wait_for(wanted)).await.expect("in time").unwrap().clone()
        }
    }

    fn start(options: TunnelOptions, edge: &Edge, provision: Provision) -> Running {
        let ended = CancellationToken::new();
        let hooks: Arc<dyn Hooks> = Arc::new(Probe { origin: edge.origin.clone() });
        let (tunnel, runner) = run(options, hooks, provision, ended.clone());
        Running { tunnel, ended, runner: tokio::spawn(runner) }
    }

    fn is_up(state: &TunnelState) -> bool {
        matches!(state, TunnelState::Up { .. })
    }

    #[tokio::test]
    async fn finds_no_cloudflared_then_prefers_the_apps_over_the_pinned_one() {
        let edge = edge(404).await;
        let path = directory();
        let (_, provision) = provision_for(false);
        let nowhere = TunnelOptions::new(path.join("empty")).cloudflared(path.join("none")).timings(fast());
        let unfound = start(nowhere, &edge, provision.clone());
        assert_eq!(unfound.status(|state| *state == TunnelState::NoBinary).await, TunnelState::NoBinary);
        unfound.end().await;

        let app_copy = directory();
        let preferred =
            start(TunnelOptions::new(&path).cloudflared(pinned_path(&app_copy)).timings(fast()), &edge, provision);
        until(|| !runs_in(&app_copy).is_empty()).await;
        preferred.end().await;
        assert!(runs_in(&path).is_empty());
        std::fs::remove_dir_all(path).unwrap();
        std::fs::remove_dir_all(app_copy).unwrap();
    }

    #[tokio::test]
    async fn looks_for_cloudflared_again_on_a_check_while_it_has_none() {
        let edge = edge(404).await;
        let path = std::env::temp_dir().join(format!("colibrii-tunnel-{}", crate::wire::ids::new_connection_id()));
        std::fs::create_dir_all(&path).unwrap();
        let (_, provision) = provision_for(false);
        let tunnel = start(TunnelOptions::new(&path).timings(fast()), &edge, provision);
        tunnel.status(|state| *state == TunnelState::NoBinary).await;
        install(&path);
        tunnel.tunnel.check();
        until(|| !runs_in(&path).is_empty()).await;
        tunnel.end().await;
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn comes_up_once_the_hostname_routes_its_token_in_the_environment_alone_and_its_log_in_a_file() {
        let edge = edge(502).await;
        let path = directory();
        let (_, provision) = provision_for(true);
        let timings = Timings { route_within: Duration::from_secs(5), ..fast() };
        let tunnel = start(TunnelOptions::new(&path).timings(timings), &edge, provision);
        until(|| runs_in(&path).len() == 1).await;
        assert_eq!(*tunnel.tunnel.state.borrow(), TunnelState::Starting);
        let first = &runs_in(&path)[0];
        assert_eq!(first.args, "tunnel --no-autoupdate --ha-connections 1 run");
        assert_eq!(first.token, TOKEN);
        assert!(first.logs_to_file);
        assert_eq!(std::fs::read_to_string(path.join(PID_FILE)).unwrap(), pid_record(first.pid).await);
        edge.status.store(426, Ordering::SeqCst);
        assert_eq!(tunnel.status(is_up).await, TunnelState::Up { hostname: HOSTNAME.into() });
        tunnel.end().await;
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn tries_the_hostname_as_soon_as_cloudflared_registers() {
        let edge = edge(426).await;
        let path = directory();
        std::fs::write(path.join("registers"), "").unwrap();
        let (_, provision) = provision_for(false);
        let timings =
            Timings { existing_tries: vec![Duration::from_secs(60)], route_within: Duration::from_secs(120), ..fast() };
        let tunnel = start(TunnelOptions::new(&path).timings(timings), &edge, provision);
        tunnel.status(is_up).await;
        tunnel.end().await;
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn tries_a_fresh_hostname_it_routed_in_an_earlier_run_as_soon_as_cloudflared_registers() {
        let edge = edge(426).await;
        let path = directory();
        std::fs::write(path.join("registers"), "").unwrap();
        let (_, provision) = provision_for(true);
        let earlier = start(TunnelOptions::new(&path).timings(fast()), &edge, provision.clone());
        earlier.status(is_up).await;
        earlier.end().await;
        let timings = Timings {
            fresh_tries: vec![Duration::from_secs(60)],
            existing_tries: vec![Duration::from_secs(60)],
            route_within: Duration::from_secs(120),
            ..fast()
        };
        let tunnel = start(TunnelOptions::new(&path).timings(timings), &edge, provision);
        tunnel.status(is_up).await;
        tunnel.end().await;
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn restarts_after_cloudflared_exits_with_the_provision_that_routed() {
        let edge = edge(404).await;
        let path = directory();
        let (counted, provision) = provision_for(false);
        let tunnel = start(TunnelOptions::new(&path).timings(fast()), &edge, provision);
        tunnel.status(is_up).await;
        until(|| runs_in(&path).len() == 1).await;
        signal(runs_in(&path)[0].pid, true);
        tunnel.status(|state| *state == TunnelState::Error).await;
        until(|| runs_in(&path).len() == 2).await;
        tunnel.status(is_up).await;
        assert_eq!(counted.load(Ordering::SeqCst), 1);
        tunnel.end().await;
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn errors_when_the_hostname_never_routes_and_provisions_again() {
        let edge = edge(530).await;
        let path = directory();
        let (counted, provision) = provision_for(true);
        let tunnel = start(TunnelOptions::new(&path).timings(fast()), &edge, provision);
        until(|| runs_in(&path).len() == 1).await;
        tunnel.status(|state| *state == TunnelState::Error).await;
        let first = runs_in(&path)[0].pid;
        until(|| !is_alive(first)).await;
        until(|| counted.load(Ordering::SeqCst) == 2).await;
        tunnel.end().await;
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn goes_back_to_starting_when_a_check_finds_it_not_routing() {
        let edge = edge(404).await;
        let path = directory();
        let (_, provision) = provision_for(false);
        let timings = Timings { route_within: Duration::from_secs(5), ..fast() };
        let tunnel = start(TunnelOptions::new(&path).timings(timings), &edge, provision);
        tunnel.status(is_up).await;
        edge.status.store(502, Ordering::SeqCst);
        tunnel.tunnel.check();
        tunnel.status(|state| *state == TunnelState::Starting).await;
        edge.status.store(404, Ordering::SeqCst);
        tunnel.status(is_up).await;
        assert_eq!(runs_in(&path).len(), 1);
        tunnel.end().await;
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn ends_cloudflared_when_its_run_ends() {
        let edge = edge(404).await;
        let path = directory();
        let (_, provision) = provision_for(false);
        let tunnel = start(TunnelOptions::new(&path).timings(fast()), &edge, provision);
        tunnel.status(is_up).await;
        until(|| runs_in(&path).len() == 1).await;
        let first = runs_in(&path)[0].pid;
        tunnel.end().await;
        assert!(!is_alive(first));
        assert!(!path.join(PID_FILE).exists());
        std::fs::remove_dir_all(path).unwrap();
    }

    struct Killed(std::process::Child);

    impl Drop for Killed {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn sleeper() -> Killed {
        Killed(std::process::Command::new("/bin/sleep").arg("30").spawn().unwrap())
    }

    #[tokio::test]
    async fn ends_the_cloudflared_a_crashed_run_left_and_nothing_else_under_its_pid() {
        let edge = edge(404).await;
        let path = directory();
        let other = sleeper();
        let stale = sleeper();
        let leftover = |record: String| {
            let (path, edge) = (path.clone(), &edge);
            async move {
                std::fs::write(path.join(PID_FILE), record).unwrap();
                let (_, provision) = provision_for(false);
                let tunnel = start(TunnelOptions::new(&path).timings(fast()), edge, provision);
                until(|| !runs_in(&path).is_empty()).await;
                tunnel.end().await;
                std::fs::remove_file(path.join("runs.log")).unwrap();
            }
        };
        // A pid given to another process since: it started at another time
        // than the one recorded.
        leftover(format!("{}\nThu Jan  1 00:00:00 1970\n", other.0.id())).await;
        leftover(format!("{}\n", other.0.id())).await;
        assert!(is_alive(other.0.id()));
        leftover(pid_record(stale.0.id()).await).await;
        let mut stale = stale;
        timeout(WAIT, async {
            while stale.0.try_wait().unwrap().is_none() {
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("ended in time");
        drop(other);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn ends_a_leftover_cloudflared_without_a_run_for_an_app_at_startup() {
        let path = directory();
        let mut stale = sleeper();
        std::fs::write(path.join(PID_FILE), pid_record(stale.0.id()).await).unwrap();
        end_leftover_cloudflared(&path).await;
        timeout(WAIT, async {
            while stale.0.try_wait().unwrap().is_none() {
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("ended in time");
        assert!(!path.join(PID_FILE).exists());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn asks_a_leftover_cloudflared_to_end_and_kills_one_that_does_not() {
        let path = directory();
        let leftover = |name: &str, on_term: &str| {
            let ready = path.join(format!("{name}-ready"));
            let script = format!("trap '{on_term}' TERM; touch {}; while :; do sleep 0.05; done", ready.display());
            let child = Killed(std::process::Command::new("/bin/sh").args(["-c", &script]).spawn().unwrap());
            (child, ready)
        };
        let asked = path.join("asked");
        let (mut polite, ready) = leftover("polite", &format!("touch {}; exit 0", asked.display()));
        until(|| ready.exists()).await;
        std::fs::write(path.join(PID_FILE), pid_record(polite.0.id()).await).unwrap();
        end_leftover(path.clone(), Duration::from_secs(5)).await;
        assert!(asked.exists());
        until(|| polite.0.try_wait().unwrap().is_some()).await;
        let (mut stubborn, ready) = leftover("stubborn", "");
        until(|| ready.exists()).await;
        std::fs::write(path.join(PID_FILE), pid_record(stubborn.0.id()).await).unwrap();
        end_leftover(path.clone(), Duration::from_millis(200)).await;
        until(|| stubborn.0.try_wait().unwrap().is_some()).await;
        std::fs::remove_dir_all(path).unwrap();
    }
}
