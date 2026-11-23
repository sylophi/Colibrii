mod common;

use std::sync::Arc;

use colibrii::testing::Msg;
use colibrii::{AccountState, Connections, Device, Error, InvalidInput, MemoryStore, ServiceError, StopReason};
use common::*;
use serde_json::json;

const SIGNED_OUT: AccountState = AccountState::SignedOut { removed: false };

#[tokio::test]
async fn enrolls_and_enrolls_again_under_the_same_device_id() {
    let kit = Kit::new();
    let account = new_account_id();
    let laptop = kit.device().await;
    assert_eq!(laptop.account().get(), SIGNED_OUT);
    let first = kit.enroll(&laptop, &account, "Laptop").await;
    let again = kit.enroll(&laptop, &account, "Laptop again").await;
    assert_eq!(device_id_of(&again), device_id_of(&first));
    let devices = laptop.devices().list().await.unwrap();
    let named: Vec<_> = devices.into_iter().map(|entry| (entry.device_id, entry.name)).collect();
    assert_eq!(named, vec![(device_id_of(&first), "Laptop again".to_owned())]);
}

#[tokio::test]
async fn starts_from_the_store_and_signs_out() {
    let kit = Kit::new();
    let store = Arc::new(MemoryStore::new());
    let account = new_account_id();
    let first = kit.device_in(&store).await;
    let enrolled = kit.enroll(&first, &account, "Laptop").await;
    first.close().await;
    let later = kit.device_in(&store).await;
    assert_eq!(device_id_of(&connected(&later).await), device_id_of(&enrolled));
    assert!(later.sign_out().await.unwrap());
    assert_eq!(later.account().get(), SIGNED_OUT);
    assert_eq!(document(&store), None);
}

#[tokio::test]
async fn refuses_a_login_token_for_another_apps_clerk_instance() {
    let kit = Kit::new();
    let desk = kit.device().await;
    let token = foreign_session_token(&new_account_id());
    let refused = desk.enroll(&token, "Desk", None).await.unwrap_err();
    assert_eq!(refused, Error::Service(ServiceError::LoginRejected));
}

// ---- Signing out while the hub cannot be reached ----

#[tokio::test]
async fn reads_as_signed_out_at_once_and_removes_the_device_once_made_again() {
    let kit = Kit::new();
    let store = Arc::new(MemoryStore::new());
    let account = new_account_id();
    let desk = kit.device().await;
    kit.enroll(&desk, &account, "Desk").await;
    let laptop = kit.device_in(&store).await;
    let laptop_id = device_id_of(&kit.enroll(&laptop, &account, "Laptop").await);
    assert!(!kit.diverted(Diversion::Refused, laptop.sign_out()).await.unwrap());
    assert_eq!(laptop.account().get(), SIGNED_OUT);
    laptop.close().await;
    let owed = document(&store).unwrap();
    assert_eq!((&owed["deviceId"], &owed["signedOut"]), (&json!(laptop_id), &json!(true)));
    assert!(listed(&desk).await.contains(&laptop_id));
    let later = kit.device_in(&store).await;
    assert_eq!(later.account().get(), SIGNED_OUT);
    eventually(|| async { document(&store).is_none() }).await;
    assert!(!listed(&desk).await.contains(&laptop_id));
}

#[tokio::test]
async fn gives_up_on_a_hub_that_never_answers_and_owes_the_removal() {
    let kit = Kit::new();
    let store = Arc::new(MemoryStore::new());
    let laptop = kit.device_in(&store).await;
    kit.enroll(&laptop, &new_account_id(), "Laptop").await;
    assert!(!kit.diverted(Diversion::Silent, laptop.sign_out()).await.unwrap());
    assert_eq!(document(&store).unwrap()["signedOut"], json!(true));
}

#[tokio::test]
async fn removes_the_device_on_a_wake() {
    let kit = Kit::new();
    let store = Arc::new(MemoryStore::new());
    let account = new_account_id();
    let desk = kit.device().await;
    kit.enroll(&desk, &account, "Desk").await;
    let laptop = kit.device_in(&store).await;
    let laptop_id = device_id_of(&kit.enroll(&laptop, &account, "Laptop").await);
    kit.diverted(Diversion::Refused, laptop.sign_out()).await.unwrap();
    assert!(listed(&desk).await.contains(&laptop_id));
    laptop.wake();
    eventually(|| async { document(&store).is_none() }).await;
    assert!(!listed(&desk).await.contains(&laptop_id));
}

#[tokio::test]
async fn removes_the_device_on_the_next_sign_out_and_says_when_it_did() {
    let kit = Kit::new();
    let store = Arc::new(MemoryStore::new());
    let account = new_account_id();
    let desk = kit.device().await;
    kit.enroll(&desk, &account, "Desk").await;
    let laptop = kit.device_in(&store).await;
    let laptop_id = device_id_of(&kit.enroll(&laptop, &account, "Laptop").await);
    kit.diverted(Diversion::Refused, laptop.sign_out()).await.unwrap();
    assert!(!kit.diverted(Diversion::Refused, laptop.sign_out()).await.unwrap());
    assert!(document(&store).is_some());
    assert!(laptop.sign_out().await.unwrap());
    assert_eq!(document(&store), None);
    assert!(!listed(&desk).await.contains(&laptop_id));
    assert!(laptop.sign_out().await.unwrap());
}

#[tokio::test]
async fn removes_the_device_before_it_enrolls_again() {
    let kit = Kit::new();
    let account = new_account_id();
    let desk = kit.device().await;
    let desk_id = device_id_of(&kit.enroll(&desk, &account, "Desk").await);
    let laptop = kit.device().await;
    kit.enroll(&laptop, &account, "Laptop").await;
    kit.diverted(Diversion::Refused, laptop.sign_out()).await.unwrap();
    let again = device_id_of(&kit.enroll(&laptop, &account, "Laptop").await);
    assert_eq!(listed(&desk).await, vec![desk_id, again]);
}

#[tokio::test]
async fn enrolls_again_under_the_device_id_whose_removal_it_still_owes() {
    let kit = Kit::new();
    let account = new_account_id();
    let laptop = kit.device().await;
    let laptop_id = device_id_of(&kit.enroll(&laptop, &account, "Laptop").await);
    kit.diverted(Diversion::Refused, laptop.sign_out()).await.unwrap();
    // Every removal fails, the one tried before the enrollment too.
    kit.net.set_divert(|method, _| (method == "DELETE").then_some(Diversion::Refused));
    let again = kit.enroll(&laptop, &account, "Laptop").await;
    kit.net.clear_divert();
    assert_eq!(device_id_of(&again), laptop_id);
    assert_eq!(listed(&laptop).await, vec![laptop_id]);
}

async fn enrolls_under_another_account_once_the_old_one_heard(chosen: bool) {
    let kit = Kit::new();
    let mut options = kit.options();
    if chosen {
        options = options.device_id(new_device_id());
    }
    let laptop = Device::new(options).await.unwrap();
    let device_id = device_id_of(&kit.enroll(&laptop, &new_account_id(), "Laptop").await);
    kit.diverted(Diversion::Refused, laptop.sign_out()).await.unwrap();
    // The removal tried before the enrollment fails, and the one after its
    // DeviceEnrolledElsewhere goes through.
    let deletes = Arc::new(std::sync::Mutex::new(0));
    let counted = deletes.clone();
    kit.net.set_divert(move |method, _| {
        let mut deletes = counted.lock().unwrap();
        if method != "DELETE" {
            return None;
        }
        *deletes += 1;
        (*deletes == 1).then_some(Diversion::Refused)
    });
    let other = new_account_id();
    let enrolled = kit.enroll(&laptop, &other, "Laptop").await;
    kit.net.clear_divert();
    assert_eq!(account_id_of(&enrolled), other);
    // A made-up id goes with the removal the service heard.
    assert_eq!(device_id_of(&enrolled) == device_id, chosen);
    assert_eq!(*deletes.lock().unwrap(), 2);
}

#[tokio::test]
async fn enrolls_a_chosen_device_id_under_another_account_once_the_old_one_heard() {
    enrolls_under_another_account_once_the_old_one_heard(true).await;
}

#[tokio::test]
async fn enrolls_a_made_up_device_id_under_another_account_once_the_old_one_heard() {
    enrolls_under_another_account_once_the_old_one_heard(false).await;
}

// ---- A device id the app chose ----

#[tokio::test]
async fn enrolls_under_a_chosen_device_id_and_keeps_it_across_a_sign_out() {
    let kit = Kit::new();
    let device_id = new_device_id();
    let laptop = Device::new(kit.options().device_id(&device_id)).await.unwrap();
    assert_eq!(device_id_of(&kit.enroll(&laptop, &new_account_id(), "Laptop").await), device_id);
    laptop.sign_out().await.unwrap();
    assert_eq!(device_id_of(&kit.enroll(&laptop, &new_account_id(), "Laptop").await), device_id);
}

#[tokio::test]
async fn reads_a_store_holding_another_device_as_signed_out() {
    let kit = Kit::new();
    let store = Arc::new(MemoryStore::new());
    let first = kit.device_in(&store).await;
    kit.enroll(&first, &new_account_id(), "Laptop").await;
    first.close().await;
    let device_id = new_device_id();
    let laptop = Device::new(kit.options_with(store.clone(), Connections::Single).device_id(&device_id)).await.unwrap();
    assert_eq!(laptop.account().get(), SIGNED_OUT);
    assert_eq!(device_id_of(&kit.enroll(&laptop, &new_account_id(), "Laptop").await), device_id);
}

#[tokio::test]
async fn refuses_a_chosen_device_id_that_is_not_a_lowercase_uuid() {
    let kit = Kit::new();
    let refused = Device::new(kit.options().device_id(new_device_id().to_uppercase())).await.err();
    assert_eq!(refused, Some(Error::InvalidInput(InvalidInput::DeviceId)));
}

// ---- The device list ----

#[tokio::test]
async fn lists_renames_and_replaces_device_data() {
    let kit = Kit::new();
    let account = new_account_id();
    let desk = kit.device().await;
    let phone = kit.device().await;
    let desk_id = device_id_of(&kit.enroll(&desk, &account, "Desk").await);
    let phone_id = device_id_of(&kit.enroll(&phone, &account, "Phone").await);
    desk.devices().rename(&phone_id, "Pocket").await.unwrap();
    let data = json!({ "icon": "desktop" }).as_object().unwrap().clone();
    desk.devices().set_data(&desk_id, data).await.unwrap();
    let devices = phone.devices().list().await.unwrap();
    let seen: Vec<_> =
        devices.into_iter().map(|entry| (entry.device_id, entry.name, json!(entry.data), entry.online)).collect();
    assert_eq!(
        seen,
        vec![
            (desk_id, "Desk".to_owned(), json!({ "icon": "desktop" }), true),
            (phone_id.clone(), "Pocket".to_owned(), json!({}), true),
        ]
    );
    let refused = desk.devices().rename(&phone_id, "").await.unwrap_err();
    assert_eq!(refused, Error::InvalidInput(InvalidInput::Name));
}

#[tokio::test]
async fn a_device_list_call_needs_an_enrollment() {
    let kit = Kit::new();
    let laptop = kit.device().await;
    assert_eq!(laptop.devices().list().await.unwrap_err(), Error::NotEnrolled);
}

#[tokio::test]
async fn reads_as_signed_out_once_another_device_removes_it() {
    let kit = Kit::new();
    let account = new_account_id();
    let desk = kit.device().await;
    let store = Arc::new(MemoryStore::new());
    let phone = kit.device_in(&store).await;
    kit.enroll(&desk, &account, "Desk").await;
    let phone_id = device_id_of(&kit.enroll(&phone, &account, "Phone").await);
    desk.devices().remove(&phone_id).await.unwrap();
    let removed = wait_for(&phone.account(), |state| matches!(state, AccountState::SignedOut { .. })).await;
    assert_eq!(removed, REMOVED);
    assert_eq!(document(&store), Some(json!({ "deviceId": phone_id, "removed": true })));
    assert_eq!(phone.devices().list().await.unwrap_err(), Error::NotEnrolled);
    assert!(phone.sign_out().await.unwrap());
    assert_eq!(phone.account().get(), SIGNED_OUT);
    assert_eq!(document(&store), None);
}

#[tokio::test]
async fn stops_a_single_device_when_a_second_copy_of_it_connects() {
    let kit = Kit::new();
    let store = Arc::new(MemoryStore::new());
    let first = kit.device_in(&store).await;
    let enrolled = kit.enroll(&first, &new_account_id(), "Laptop").await;
    let second = kit.device_in(&store).await;
    connected(&second).await;
    let stopped = wait_for(&first.account(), |state| matches!(state, AccountState::Stopped { .. })).await;
    assert_eq!(
        stopped,
        AccountState::Stopped {
            device_id: device_id_of(&enrolled),
            account_id: account_id_of(&enrolled),
            reason: StopReason::Superseded,
        }
    );
}

// ---- A removal ----

const REMOVED: AccountState = AccountState::SignedOut { removed: true };

/// A device in `store` that another device of the account removed while it
/// ran.
async fn removed_in(kit: &Kit, store: &Arc<MemoryStore>) -> String {
    let account = new_account_id();
    let desk = kit.device().await;
    kit.enroll(&desk, &account, "Desk").await;
    let phone = kit.device_in(store).await;
    desk.devices().remove(&device_id_of(&kit.enroll(&phone, &account, "Phone").await)).await.unwrap();
    wait_for(&phone.account(), |state| matches!(state, AccountState::SignedOut { .. })).await;
    phone.close().await;
    desk.close().await;
    account
}

#[tokio::test]
async fn a_removal_lasts_into_the_next_run_on_the_same_store() {
    let kit = Kit::new();
    let store = Arc::new(MemoryStore::new());
    removed_in(&kit, &store).await;
    let later = kit.device_in(&store).await;
    assert_eq!(later.account().get(), REMOVED);
}

#[tokio::test]
async fn a_removal_is_kept_by_a_device_that_learns_of_it_as_it_starts() {
    let kit = Kit::new();
    let store = Arc::new(MemoryStore::new());
    let account = new_account_id();
    let desk = kit.device().await;
    kit.enroll(&desk, &account, "Desk").await;
    let first = kit.device_in(&store).await;
    let phone_id = device_id_of(&kit.enroll(&first, &account, "Phone").await);
    first.close().await;
    desk.devices().remove(&phone_id).await.unwrap();
    let phone = kit.device_in(&store).await;
    wait_for(&phone.account(), |state| matches!(state, AccountState::SignedOut { .. })).await;
    phone.close().await;
    let later = kit.device_in(&store).await;
    assert_eq!(later.account().get(), REMOVED);
}

#[tokio::test]
async fn a_removal_is_cleared_by_enrolling() {
    let kit = Kit::new();
    let store = Arc::new(MemoryStore::new());
    let account = removed_in(&kit, &store).await;
    let removed = document(&store).unwrap();
    let phone = kit.device_in(&store).await;
    let enrolled = kit.enroll(&phone, &account, "Phone").await;
    assert_eq!(json!(device_id_of(&enrolled)), removed["deviceId"]);
    phone.close().await;
    let later = kit.device_in(&store).await;
    assert_eq!(device_id_of(&connected(&later).await), device_id_of(&enrolled));
}

#[tokio::test]
async fn a_removal_is_cleared_by_signing_out() {
    let kit = Kit::new();
    let store = Arc::new(MemoryStore::new());
    removed_in(&kit, &store).await;
    let phone = kit.device_in(&store).await;
    assert!(phone.sign_out().await.unwrap());
    phone.close().await;
    let later = kit.device_in(&store).await;
    assert_eq!(later.account().get(), SIGNED_OUT);
}

/// A `multi` device on `store`, one of several runs sharing it.
async fn multi_in(kit: &Kit, store: &Arc<MemoryStore>) -> Device {
    Device::new(kit.options_with(store.clone(), Connections::Multi)).await.unwrap()
}

#[tokio::test]
async fn runs_the_enrollment_another_run_made_since() {
    let kit = Kit::new();
    let store = Arc::new(MemoryStore::new());
    let account = new_account_id();
    let desk = kit.device().await;
    kit.enroll(&desk, &account, "Desk").await;
    let slow = multi_in(&kit, &store).await;
    let phone_id = device_id_of(&kit.enroll(&slow, &account, "Phone").await);
    kit.net.hub_socket().set_deaf(true);
    let quick = multi_in(&kit, &store).await;
    connected(&quick).await;
    desk.devices().remove(&phone_id).await.unwrap();
    wait_for(&quick.account(), |state| matches!(state, AccountState::SignedOut { .. })).await;
    let enrolled = kit.enroll(&quick, &account, "Phone").await;
    // The old credential, which the service no longer takes.
    slow.devices().list().await.unwrap_err();
    assert_eq!(device_id_of(&connected(&slow).await), device_id_of(&enrolled));
    assert_eq!(document(&store).unwrap()["deviceId"], json!(device_id_of(&enrolled)));
}

#[tokio::test]
async fn reads_as_signed_out_where_another_run_signed_out_since() {
    let kit = Kit::new();
    let store = Arc::new(MemoryStore::new());
    let slow = multi_in(&kit, &store).await;
    kit.enroll(&slow, &new_account_id(), "Phone").await;
    kit.net.hub_socket().set_deaf(true);
    let quick = multi_in(&kit, &store).await;
    connected(&quick).await;
    assert!(quick.sign_out().await.unwrap());
    assert_eq!(slow.devices().list().await.unwrap_err(), Error::Service(ServiceError::DeviceRevoked));
    assert_eq!(wait_for(&slow.account(), |state| !matches!(state, AccountState::Enrolled { .. })).await, SIGNED_OUT);
    assert_eq!(document(&store), None);
}

// ---- The hub socket ----

#[tokio::test]
async fn follows_the_roster_as_devices_come_and_go() {
    let kit = Kit::new();
    let account = new_account_id();
    let desk = kit.device().await;
    kit.enroll(&desk, &account, "Desk").await;
    let phone = kit.device().await;
    let phone_id = device_id_of(&kit.enroll(&phone, &account, "Phone").await);
    let roster = wait_for(&desk.roster(), |peers| !peers.is_empty()).await;
    assert_eq!(roster.iter().map(|peer| peer.device_id.clone()).collect::<Vec<_>>(), vec![phone_id]);
    phone.close().await;
    wait_for(&desk.roster(), |peers| peers.is_empty()).await;
}

#[tokio::test]
async fn connects_again_after_the_service_drops_it() {
    let kit = Kit::new();
    let desk = kit.device().await;
    kit.enroll(&desk, &new_account_id(), "Desk").await;
    let hub_socket = kit.net.hub_socket();
    // A message past the cap, which the service closes the socket for.
    hub_socket.send(Msg::Text("x".repeat(70_000)));
    assert_eq!(hub_socket.closed().await.code, 1009);
    wait_for(&desk.account(), |state| matches!(state, AccountState::Enrolled { connected: false, .. })).await;
    connected(&desk).await;
}

#[tokio::test]
async fn connects_again_at_once_on_a_wake_while_its_hub_socket_is_down() {
    let kit = Kit::new();
    let desk = kit.device().await;
    kit.enroll(&desk, &new_account_id(), "Desk").await;
    let tickets = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = tickets.clone();
    kit.net.set_divert(move |method, url| {
        let ticket = method == "POST" && url.ends_with("/tickets");
        (ticket && counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 3).then_some(Diversion::Refused)
    });
    kit.net.hub_socket().send(Msg::Text("x".repeat(70_000)));
    // Three tries refused, after waits of 1, 2 and 4 seconds: the next is 8
    // seconds away.
    eventually(|| async { tickets.load(std::sync::atomic::Ordering::SeqCst) == 3 }).await;
    desk.wake();
    let mut account = desk.account();
    let again = account.wait_for(|state| matches!(state, AccountState::Enrolled { connected: true, .. }));
    assert!(tokio::time::timeout(std::time::Duration::from_secs(3), again).await.is_ok());
}

#[tokio::test]
async fn settles_ten_seconds_after_it_connects() {
    let kit = Kit::new();
    let desk = kit.device().await;
    kit.enroll(&desk, &new_account_id(), "Desk").await;
    assert!(matches!(desk.account().get(), AccountState::Enrolled { settled: false, .. }));
    let mut account = desk.account();
    let settled = tokio::time::timeout(
        std::time::Duration::from_secs(12),
        account.wait_for(|state| matches!(state, AccountState::Enrolled { settled: true, .. })),
    );
    assert!(settled.await.is_ok());
}
