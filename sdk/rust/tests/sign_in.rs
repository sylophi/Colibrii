//! Signing in with a code against a stand-in for Clerk, and trading its
//! access token for a sign-in token against the service.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use colibrii::testing::Hooks;
use colibrii::{CodeSignIn, CodeSignInError, Error, ServiceError, SignInCode, sign_in_token};
use common::*;
use serde_json::{Value, json};

/// Clerk's requests go to `origin`.
struct Clerk {
    origin: String,
}

impl Hooks for Clerk {
    fn divert(&self, _method: &str, url: &str) -> Option<String> {
        Some(url.replace("https://clerk.test", &self.origin))
    }
}

/// A token ask, as the stand-in heard it.
struct Asked {
    at: Instant,
    form: String,
}

/// Clerk with the device grant on, handing out a code that lives 10
/// seconds and is asked about every `interval` seconds, answering the
/// `n`th token ask with `token(n, device_code)`.
async fn clerk(
    interval: f64,
    token: impl Fn(usize, &str) -> (u16, Value) + Send + Sync + 'static,
) -> (CodeSignIn, Arc<Mutex<Vec<Asked>>>) {
    let asks = Arc::new(Mutex::new(Vec::new()));
    let noted = asks.clone();
    let origin = http_stub(move |request| {
        let (status, body) = match request.path.as_str() {
            "/.well-known/openid-configuration" => (
                200,
                json!({
                    "token_endpoint": "https://clerk.test/token",
                    "device_authorization_endpoint": "https://clerk.test/device",
                }),
            ),
            "/device" => (
                200,
                json!({
                    "device_code": "device-code",
                    "user_code": "ABCD-EFGH",
                    "verification_uri": "https://clerk.test/verify",
                    "expires_in": 10,
                    "interval": interval,
                }),
            ),
            _ => {
                let form = String::from_utf8(request.body.clone()).unwrap();
                let code = form.split('&').find_map(|field| field.strip_prefix("device_code=")).unwrap_or_default();
                let mut asks = noted.lock().unwrap();
                let answer = token(asks.len(), code);
                asks.push(Asked { at: Instant::now(), form });
                answer
            }
        };
        (status, if body.is_null() { "<html>".into() } else { body.to_string() })
    })
    .await;
    (sign_in_with(&origin), asks)
}

fn sign_in_with(origin: &str) -> CodeSignIn {
    CodeSignIn::new(format!("pk_test_{}", standard_base64(b"clerk.test$")), "inkwell-oauth")
        .hooks(Arc::new(Clerk { origin: origin.to_owned() }))
}

/// Base64 as `btoa` writes it, with `+`, `/` and padding.
fn standard_base64(bytes: &[u8]) -> String {
    let url = colibrii::wire::base64url::encode(bytes).replace('-', "+").replace('_', "/");
    let padding = "=".repeat((4 - url.len() % 4) % 4);
    url + &padding
}

fn pending() -> (u16, Value) {
    (400, json!({ "error": "authorization_pending" }))
}

/// A code issued just now, asked about every 100 milliseconds.
fn code(device_code: &str) -> SignInCode {
    SignInCode {
        user_code: "ABCD-EFGH".into(),
        verification_uri: "https://clerk.test/verify".into(),
        verification_uri_complete: None,
        device_code: device_code.into(),
        interval: Duration::from_millis(100),
        expires_at: SystemTime::now() + Duration::from_secs(10),
    }
}

#[tokio::test]
async fn answers_the_access_token_once_the_person_approves() {
    let (clerk, asks) =
        clerk(0.2, |n, _| if n < 2 { pending() } else { (200, json!({ "access_token": "access-token" })) }).await;
    let code = clerk.start().await.unwrap();
    assert_eq!(code.user_code, "ABCD-EFGH");
    assert_eq!(code.verification_uri, "https://clerk.test/verify");
    assert_eq!(code.verification_uri_complete, None);
    assert_eq!(code.interval, Duration::from_millis(200));
    let life = code.expires_at.duration_since(SystemTime::now()).unwrap();
    assert!(life > Duration::from_secs(9) && life <= Duration::from_secs(10));
    assert_eq!(clerk.finish(&code).await.unwrap(), "access-token");
    let asks = asks.lock().unwrap();
    assert_eq!(asks.len(), 3);
    assert_eq!(
        asks[0].form,
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code&device_code=device-code&client_id=inkwell-oauth"
    );
    assert!(asks[2].at - asks[1].at >= Duration::from_millis(200));
}

#[tokio::test]
async fn asks_for_no_token_past_the_codes_life_counted_from_its_issue() {
    let (clerk, asks) = clerk(5.0, |_, _| pending()).await;
    let mut code = clerk.start().await.unwrap();
    // Issued 8 seconds ago, so it lives 2 more, less than one interval.
    code.expires_at -= Duration::from_secs(8);
    let started = Instant::now();
    assert_eq!(clerk.finish(&code).await, Err(CodeSignInError::Expired));
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(asks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn waits_5_seconds_longer_from_a_slow_down_on() {
    let (clerk, asks) = clerk(0.1, |n, _| match n {
        0 => (400, json!({ "error": "slow_down" })),
        1 => pending(),
        _ => (200, json!({ "access_token": "access-token" })),
    })
    .await;
    let mut code = code("device-code");
    code.expires_at += Duration::from_secs(10);
    assert_eq!(clerk.finish(&code).await.unwrap(), "access-token");
    let asks = asks.lock().unwrap();
    for pair in asks.windows(2) {
        assert!(pair[1].at - pair[0].at >= Duration::from_millis(5_100));
    }
}

#[tokio::test]
async fn asks_again_after_a_turn_that_fails_to_reach_clerk() {
    let (clerk, asks) =
        clerk(0.1, |n, _| if n == 0 { (502, Value::Null) } else { (200, json!({ "access_token": "access-token" })) })
            .await;
    assert_eq!(clerk.finish(&code("device-code")).await.unwrap(), "access-token");
    assert_eq!(asks.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn ends_on_a_denial_an_expiry_or_any_other_refusal() {
    let (clerk, _) = clerk(0.1, |_, code| match code {
        "denied" => (400, json!({ "error": "access_denied" })),
        "expired" => (400, json!({ "error": "expired_token" })),
        "described" => (400, json!({ "error": "invalid_client", "error_description": "no such client" })),
        _ => (400, json!({ "error": "invalid_grant" })),
    })
    .await;
    assert_eq!(clerk.finish(&code("denied")).await, Err(CodeSignInError::Denied));
    assert_eq!(clerk.finish(&code("expired")).await, Err(CodeSignInError::Expired));
    assert_eq!(
        clerk.finish(&code("described")).await,
        Err(CodeSignInError::Refused { words: Some("no such client".into()) })
    );
    assert_eq!(
        clerk.finish(&code("other")).await,
        Err(CodeSignInError::Refused { words: Some("invalid_grant".into()) })
    );
}

#[tokio::test]
async fn is_off_at_an_instance_that_names_no_device_endpoint() {
    let origin = http_stub(|_| (200, json!({ "token_endpoint": "https://clerk.test/token" }).to_string())).await;
    assert_eq!(sign_in_with(&origin).start().await, Err(CodeSignInError::Off));
}

#[tokio::test]
async fn says_why_clerk_refused_a_code() {
    let origin = http_stub(|request| match request.path.as_str() {
        "/device" => (400, json!({ "error": "invalid_client" }).to_string()),
        _ => (
            200,
            json!({
                "token_endpoint": "https://clerk.test/token",
                "device_authorization_endpoint": "https://clerk.test/device",
            })
            .to_string(),
        ),
    })
    .await;
    assert_eq!(
        sign_in_with(&origin).start().await,
        Err(CodeSignInError::Refused { words: Some("invalid_client".into()) })
    );
}

#[tokio::test]
async fn refuses_a_bad_publishable_key_and_fails_where_clerk_cannot_be_reached() {
    let bad = CodeSignIn::new("pk_test_%%%", "inkwell-oauth");
    assert_eq!(bad.start().await, Err(CodeSignInError::Refused { words: Some("bad publishable key".into()) }));
    let nowhere = sign_in_with("http://127.0.0.1:1");
    assert!(matches!(nowhere.start().await, Err(CodeSignInError::Unreachable(_))));
}

#[tokio::test]
async fn trades_an_access_token_for_a_sign_in_token_only_where_the_app_mints_them() {
    let account = new_account_id();
    let off = sign_in_token(&hub(), "inkwell", &oauth_token(&account, "inkwell-oauth")).await;
    assert_eq!(off, Err(Error::Service(ServiceError::SignInTokensOff)));
    let session = token(json!({ "account": account, "from": "quill://app", "otherKey": true }));
    let rejected = sign_in_token(&hub(), "quill", &session).await;
    assert_eq!(rejected, Err(Error::Service(ServiceError::LoginRejected)));
}
