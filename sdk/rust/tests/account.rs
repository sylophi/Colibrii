mod common;

use std::sync::Arc;

use colibrii::{AccountState, Connections, Device, Error, InvalidInput, MemoryStore, ServiceError};
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
    let seen: Vec<_> = devices.into_iter().map(|entry| (entry.device_id, entry.name, json!(entry.data))).collect();
    assert_eq!(
        seen,
        vec![
            (desk_id, "Desk".to_owned(), json!({ "icon": "desktop" })),
            (phone_id.clone(), "Pocket".to_owned(), json!({})),
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
