//! Devices driven from outside any tokio runtime, from smol as a GPUI app
//! drives them and from a plain thread, and on a runtime the app hands in.

mod common;

use std::future::Future;
use std::time::Duration;

use colibrii::{AccountState, Device, Listen};
use common::*;

/// Two devices that enroll, see each other, and carry a message each way
/// on a link, with every wait on the caller's side.
async fn link_two(kit: &Kit, runtime: Option<tokio::runtime::Handle>) {
    let on = |options: colibrii::Options| match &runtime {
        Some(handle) => options.runtime(handle.clone()),
        None => options,
    };
    let account = new_account_id();
    let desk = Device::new(on(kit.options().listen(Listen::new()))).await.unwrap();
    let phone = Device::new(on(kit.options())).await.unwrap();
    for (device, name) in [(&desk, "Desk"), (&phone, "Phone")] {
        device.enroll(&session_token(&account), name, None).await.unwrap();
    }
    let desk_id = match desk.account().get() {
        AccountState::Enrolled { device_id, .. } => device_id,
        state => panic!("{state:?}"),
    };
    phone.roster().wait_for(|peers| peers.iter().any(|peer| peer.device_id == desk_id)).await.unwrap();

    let incoming = desk.incoming();
    let welcoming = async {
        let hello = incoming.next().await.unwrap();
        hello.welcome(b"welcome".to_vec()).await.unwrap()
    };
    let (dialed, welcomed) = futures_util::future::join(phone.dial(&desk_id, b"hi"), welcoming).await;
    let dialed = dialed.unwrap();
    assert_eq!(dialed.greeting(), b"welcome");
    dialed.send(b"from the phone").unwrap();
    assert_eq!(welcomed.recv().await.unwrap(), b"from the phone");
    welcomed.send(b"from the desk").unwrap();
    assert_eq!(dialed.recv().await.unwrap(), b"from the desk");
    dialed.close();
    welcomed.closed().await;
    desk.close().await;
    phone.close().await;
}

/// `run`, or a panic once it took a minute.
fn within_a_minute(run: impl Future<Output = ()>) -> impl Future<Output = ()> {
    let limit = async {
        smol::Timer::after(Duration::from_secs(60)).await;
        panic!("not done in a minute");
    };
    smol::future::or(run, limit)
}

#[test]
fn runs_from_smol() {
    smol::block_on(within_a_minute(link_two(&Kit::new(), None)));
}

#[test]
fn runs_from_a_plain_thread() {
    colibrii::block_on(within_a_minute(link_two(&Kit::new(), None)));
}

#[tokio::test]
async fn runs_on_the_apps_tokio_runtime() {
    let runtime = tokio::runtime::Handle::current();
    within_a_minute(link_two(&Kit::new(), Some(runtime))).await;
}
