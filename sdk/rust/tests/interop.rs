//! A Rust device and a TypeScript one (`sdk/ts/scripts/peer.ts`) on one
//! account, linked both ways. The TypeScript device sends each app message
//! back on the link it came on.

mod common;

use colibrii::{Device, Link, Role};
use common::*;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

struct TsPeer {
    child: Child,
    stdin: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
}

impl TsPeer {
    fn start() -> TsPeer {
        let mut child = Command::new("node")
            .arg("sdk/ts/scripts/peer.ts")
            .arg(hub())
            .current_dir(repo_root())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("node runs the peer");
        let stdin = child.stdin.take().unwrap();
        let lines = BufReader::new(child.stdout.take().unwrap()).lines();
        TsPeer { child, stdin, lines }
    }

    async fn tell(&mut self, command: Value) {
        self.stdin.write_all(format!("{command}\n").as_bytes()).await.unwrap();
    }

    async fn heard(&mut self) -> Value {
        let line = timeout(WAIT, self.lines.next_line()).await.expect("in time").unwrap().expect("a line");
        serde_json::from_str(&line).unwrap()
    }

    async fn enroll(&mut self, account: &str) -> String {
        self.tell(json!({ "op": "enroll", "loginToken": session_token(account) })).await;
        let enrolled = self.heard().await;
        enrolled["deviceId"].as_str().unwrap().to_owned()
    }

    async fn sees(&mut self, device_id: &str) {
        self.tell(json!({ "op": "sees", "deviceId": device_id })).await;
        assert_eq!(self.heard().await["event"], "sees");
    }

    /// Ends the peer, which exits once its stdin ends.
    async fn end(mut self) {
        drop(self.stdin);
        assert!(timeout(WAIT, self.child.wait()).await.unwrap().unwrap().success());
    }
}

/// Messages of several sizes: empty, small, one piece's worth (65,518
/// bytes) and one byte past it, and a few that go in many pieces.
fn messages() -> Vec<Vec<u8>> {
    [0, 1, 1_000, 65_518, 65_519, 200_000, 3_000_000]
        .into_iter()
        .map(|size| (0..size).map(|index| (index * 7 % 251) as u8).collect())
        .collect()
}

/// Every message goes, and comes back whole and in order.
async fn echoed(link: &Link) {
    for message in messages() {
        link.send(&message).unwrap();
    }
    for message in messages() {
        let back = timeout(WAIT, link.recv()).await.expect("in time").expect("a message");
        assert_eq!(back.len(), message.len());
        assert!(back == message);
    }
}

#[tokio::test]
async fn a_rust_device_dials_a_typescript_one() {
    let kit = Kit::new();
    let account = new_account_id();
    let mut ts = TsPeer::start();
    let ts_id = ts.enroll(&account).await;
    let rust = kit.device().await;
    let rust_id = device_id_of(&kit.enroll(&rust, &account, "Rust").await);
    sees(&rust, &ts_id).await;
    ts.sees(&rust_id).await;

    let link = rust.dial(&ts_id, b"from rust").await.unwrap();
    assert_eq!(link.greeting(), b"ts welcome from rust");
    let linked = ts.heard().await;
    assert_eq!(
        (&linked["event"], &linked["role"], &linked["deviceId"], &linked["greeting"]),
        (
            &json!("linked"),
            &json!("listener"),
            &json!(rust_id),
            &json!(colibrii::wire::base64url::encode(b"from rust"))
        )
    );
    echoed(&link).await;
    ts.end().await;
}

#[tokio::test]
async fn a_typescript_device_dials_a_rust_one() {
    let kit = Kit::new();
    let account = new_account_id();
    let rust = Device::new(listening(&kit)).await.unwrap();
    let mut links = welcome_all(&rust);
    let rust_id = device_id_of(&kit.enroll(&rust, &account, "Rust").await);
    let mut ts = TsPeer::start();
    let ts_id = ts.enroll(&account).await;
    sees(&rust, &ts_id).await;
    ts.sees(&rust_id).await;

    let hello = colibrii::wire::base64url::encode(b"from ts");
    ts.tell(json!({ "op": "dial", "deviceId": rust_id, "hello": hello })).await;
    let linked = ts.heard().await;
    assert_eq!(
        (&linked["event"], &linked["role"], &linked["greeting"]),
        (&json!("linked"), &json!("dialer"), &json!(colibrii::wire::base64url::encode(b"welcome from ts")))
    );
    let link = next_link(&mut links).await;
    assert_eq!((link.device_id(), link.role(), link.greeting()), (ts_id.as_str(), Role::Listener, &b"from ts"[..]));
    echoed(&link).await;
    ts.end().await;
}
