//! A listening device's tunnel, run by a fake `cloudflared` (a shell script
//! that stays up until ended), with a local HTTP stub for the hostname and
//! for `POST /tunnel`, whose real route needs Cloudflare.
#![cfg(unix)]

mod common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use colibrii::testing::{Hooks, Opened, Timings};
use colibrii::wire::relay::CandidateKind;
use colibrii::{Device, DialError, Listen, TunnelOptions, TunnelState};
use common::*;

const FAKE: &str = "#!/bin/sh\n[ \"$1\" = \"--version\" ] && exit 0\nwhile :; do sleep 1; done\n";
const HOSTNAME: &str = "a1b2c3.tunnel.test";

struct Provisioned {
    port: u16,
    bearer: String,
}

/// The hostname's edge, answering with `routing`, and `POST /tunnel`.
struct Stub {
    origin: String,
    routing: Arc<AtomicU16>,
    provisioned: Arc<Mutex<Vec<Provisioned>>>,
}

async fn stub() -> Stub {
    let routing = Arc::new(AtomicU16::new(502));
    let provisioned = Arc::new(Mutex::new(Vec::new()));
    let (status, noted) = (routing.clone(), provisioned.clone());
    let origin = http_stub(move |request| {
        if request.method != "POST" || request.path != "/tunnel" {
            return (status.load(Ordering::SeqCst), String::new());
        }
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        noted.lock().unwrap().push(Provisioned {
            port: body["port"].as_u64().unwrap() as u16,
            bearer: request.header("authorization").unwrap_or_default().to_owned(),
        });
        let answer = serde_json::json!({
            "hostname": HOSTNAME,
            "connectorToken": "connector-token-for-tests",
            "fresh": false,
        });
        (200, answer.to_string())
    })
    .await;
    Stub { origin, routing, provisioned }
}

/// The test's network, with `POST /tunnel` and the hostname at the stub.
struct Desk {
    net: Arc<Net>,
    origin: String,
}

impl Hooks for Desk {
    fn divert(&self, method: &str, url: &str) -> Option<String> {
        if url.ends_with("/tunnel") {
            return Some(format!("{}/tunnel", self.origin));
        }
        self.net.divert(method, url)
    }

    fn headers(&self) -> Vec<(String, String)> {
        self.net.headers()
    }

    fn spies(&self) -> bool {
        self.net.spies()
    }

    fn opened(&self, socket: Opened) {
        self.net.opened(socket)
    }

    fn cloudflared_on_path(&self) -> String {
        "/nonexistent/cloudflared".into()
    }

    fn probe_url(&self, hostname: &str) -> String {
        format!("{}/{hostname}", self.origin)
    }
}

/// A page's sockets: the tunnel alone, which never opens here.
struct Phone {
    net: Arc<Net>,
    tunnel_dials: Mutex<Vec<String>>,
}

impl Hooks for Phone {
    fn divert(&self, method: &str, url: &str) -> Option<String> {
        self.net.divert(method, url)
    }

    fn headers(&self) -> Vec<(String, String)> {
        self.net.headers()
    }

    fn open(&self, url: &str) -> bool {
        if !url.starts_with("wss://") {
            return true;
        }
        self.tunnel_dials.lock().unwrap().push(url.to_owned());
        false
    }

    fn dialable_kinds(&self) -> Vec<CandidateKind> {
        vec![CandidateKind::Tunnel]
    }
}

fn directory() -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = std::env::temp_dir().join(format!("colibrii-tunnel-{}", new_device_id()));
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("cloudflared"), FAKE).unwrap();
    std::fs::set_permissions(path.join("cloudflared"), std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn fast() -> Timings {
    let ms = Duration::from_millis;
    Timings {
        try_within: ms(500),
        fresh_tries: vec![ms(150), ms(100)],
        existing_tries: vec![ms(50), ms(100)],
        route_within: ms(20_000),
        restarts: vec![ms(100), ms(200)],
        healthy_for: ms(60_000),
        stop_grace: ms(500),
    }
}

#[tokio::test]
async fn offers_its_tunnel_only_while_up_and_checks_it_when_a_peer_never_arrives() {
    let stub = stub().await;
    let path = directory();
    let kit = Kit::new();
    let account = new_account_id();
    let desk = Device::new(
        kit.options()
            .listen(Listen::new())
            .tunnel(TunnelOptions::new(&path).timings(fast()))
            .hooks(Arc::new(Desk { net: kit.net.clone(), origin: stub.origin.clone() })),
    )
    .await
    .unwrap();
    let phone_hooks = Arc::new(Phone { net: kit.net.clone(), tunnel_dials: Mutex::new(Vec::new()) });
    let phone = Device::new(kit.options().hooks(phone_hooks.clone())).await.unwrap();
    assert_eq!(desk.tunnel().get(), TunnelState::Off);
    let desk_id = device_id_of(&kit.enroll(&desk, &account, "Desk").await);
    kit.enroll(&phone, &account, "Phone").await;
    sees(&phone, &desk_id).await;
    wait_for(&desk.tunnel(), |state| *state == TunnelState::Starting).await;
    eventually(|| async { !stub.provisioned.lock().unwrap().is_empty() }).await;
    {
        let provisioned = stub.provisioned.lock().unwrap();
        assert!(provisioned[0].port > 0);
        assert!(provisioned[0].bearer.starts_with("Bearer colibrii_dc_"));
    }

    // Starting: nothing to offer after the wait.
    assert_eq!(phone.dial(&desk_id, &[]).await.unwrap_err(), DialError::Unavailable);
    assert!(phone_hooks.tunnel_dials.lock().unwrap().is_empty());

    stub.routing.store(404, Ordering::SeqCst);
    wait_for(&desk.tunnel(), |state| matches!(state, TunnelState::Up { .. })).await;
    let unreachable = || async { matches!(phone.dial(&desk_id, &[]).await, Err(DialError::Unreachable { .. })) };
    assert!(unreachable().await);
    assert_eq!(*phone_hooks.tunnel_dials.lock().unwrap(), vec![format!("wss://{HOSTNAME}")]);

    // Two asks that never arrived: the third has the tunnel checked.
    assert!(unreachable().await);
    stub.routing.store(502, Ordering::SeqCst);
    assert!(unreachable().await);
    wait_for(&desk.tunnel(), |state| *state == TunnelState::Starting).await;

    desk.close().await;
    phone.close().await;
    assert!(!path.join("cloudflared.pid").exists());
    std::fs::remove_dir_all(path).unwrap();
}
