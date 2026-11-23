//! Signing in with a code (spec/service.md, "Signing in with a code"):
//! Clerk's device authorization grant, at the app's Clerk instance and
//! under its OAuth client, for a device with no browser to sign in
//! through. The access token it ends with is a login token for
//! enrollment, and [`sign_in_token`] trades it for a Clerk sign-in token.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use reqwest::Method;
use serde::Deserialize;
use serde_json::Value;
use tokio::time::{sleep, timeout};

use crate::errors::Error;
use crate::hub_api::{ANSWER_WITHIN, HubApi};
use crate::hub_socket::percent_encode;
use crate::runtime::on_own_runtime;
use crate::testing::{self, Hooks};

const DEVICE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";
/// What `slow_down` adds to the wait, from then on.
const SLOW_DOWN: Duration = Duration::from_secs(5);
const DEFAULT_INTERVAL_SECONDS: f64 = 5.0;

/// Why signing in with a code failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodeSignInError {
    /// The Clerk instance has the device grant off.
    Off,
    /// The person denied the code.
    Denied,
    Expired,
    /// Clerk refused, in its own words when it gave any.
    Refused {
        words: Option<String>,
    },
    Unreachable(String),
}

impl fmt::Display for CodeSignInError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodeSignInError::Off => f.write_str("The Clerk instance has signing in with a code off."),
            CodeSignInError::Denied => f.write_str("The code was denied."),
            CodeSignInError::Expired => f.write_str("The code expired."),
            CodeSignInError::Refused { words: Some(words) } => write!(f, "Clerk refused: {words}"),
            CodeSignInError::Refused { words: None } => f.write_str("Clerk refused."),
            CodeSignInError::Unreachable(cause) => write!(f, "Clerk could not be reached: {cause}"),
        }
    }
}

impl std::error::Error for CodeSignInError {}

/// What to show the person, and what finishing needs.
#[derive(Clone, PartialEq)]
pub struct SignInCode {
    pub user_code: String,
    pub verification_uri: String,
    /// The page with the code already in it.
    pub verification_uri_complete: Option<String>,
    pub device_code: String,
    pub interval: Duration,
    pub expires_at: SystemTime,
}

debug_without_secrets!(SignInCode { user_code, verification_uri, verification_uri_complete, interval, expires_at });

/// The app's Clerk instance and OAuth client.
#[derive(Clone)]
pub struct CodeSignIn {
    /// The app's Clerk publishable key, which encodes its Frontend API.
    publishable_key: String,
    client_id: String,
    hooks: Arc<dyn Hooks>,
}

#[derive(Deserialize)]
struct Discovery {
    token_endpoint: String,
    device_authorization_endpoint: Option<String>,
}

#[derive(Deserialize)]
struct DeviceCode {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: Option<String>,
    expires_in: f64,
    interval: Option<f64>,
}

#[derive(Deserialize)]
struct Tokens {
    access_token: String,
}

#[derive(Deserialize)]
struct OAuthError {
    error: String,
    error_description: Option<String>,
}

fn unreachable(cause: impl fmt::Display) -> CodeSignInError {
    CodeSignInError::Unreachable(cause.to_string())
}

fn words_of(body: &Value) -> Option<String> {
    let error = OAuthError::deserialize(body).ok()?;
    Some(error.error_description.unwrap_or(error.error))
}

fn form(fields: &[(&str, &str)]) -> String {
    fields.iter().map(|(name, value)| format!("{name}={}", percent_encode(value))).collect::<Vec<_>>().join("&")
}

/// The answer's status and JSON, whatever the status.
struct Answer {
    ok: bool,
    body: Value,
}

impl CodeSignIn {
    pub fn new(publishable_key: impl Into<String>, client_id: impl Into<String>) -> CodeSignIn {
        CodeSignIn { publishable_key: publishable_key.into(), client_id: client_id.into(), hooks: testing::none() }
    }

    #[doc(hidden)]
    pub fn hooks(mut self, hooks: Arc<dyn Hooks>) -> CodeSignIn {
        self.hooks = hooks;
        self
    }

    /// Asks Clerk for a code to show the person.
    pub async fn start(&self) -> Result<SignInCode, CodeSignInError> {
        let this = self.clone();
        on_own_runtime(async move { this.started().await }).await
    }

    /// Asks Clerk at the pace it set until the person approves or denies
    /// the code, or it expires, and answers the access token. A turn that
    /// fails to reach Clerk asks again at the next.
    pub async fn finish(&self, code: &SignInCode) -> Result<String, CodeSignInError> {
        let (this, code) = (self.clone(), code.clone());
        on_own_runtime(async move { this.finished(code).await }).await
    }

    /// The Frontend API's host: the key is `pk_test_` or `pk_live_` and the
    /// base64 of the host followed by `$`.
    fn frontend_host(&self) -> Result<String, CodeSignInError> {
        let encoded = self.publishable_key.strip_prefix("pk_test_").or(self.publishable_key.strip_prefix("pk_live_"));
        let encoded = encoded.unwrap_or(&self.publishable_key).replace('+', "-").replace('/', "_");
        let decoded = crate::wire::base64url::decode(&encoded).and_then(|bytes| String::from_utf8(bytes).ok());
        let decoded = decoded.ok_or(CodeSignInError::Refused { words: Some("bad publishable key".into()) })?;
        Ok(decoded.strip_suffix('$').unwrap_or(&decoded).to_owned())
    }

    async fn fetch_json(&self, method: Method, url: &str, form: Option<String>) -> Result<Answer, CodeSignInError> {
        let url = self.hooks.divert(method.as_str(), url).unwrap_or_else(|| url.to_owned());
        let mut request = reqwest::Client::new().request(method, url);
        if let Some(form) = form {
            request = request.header("content-type", "application/x-www-form-urlencoded").body(form);
        }
        let answered = async {
            let response = request.send().await.map_err(unreachable)?;
            let ok = response.status().as_u16() < 300;
            let bytes = response.bytes().await.map_err(unreachable)?;
            let body = serde_json::from_slice(&bytes).map_err(unreachable)?;
            Ok(Answer { ok, body })
        };
        timeout(ANSWER_WITHIN, answered).await.unwrap_or_else(|_| Err(unreachable("no answer in time")))
    }

    async fn endpoints(&self) -> Result<(String, String), CodeSignInError> {
        let host = self.frontend_host()?;
        let url = format!("https://{host}/.well-known/openid-configuration");
        let answer = self.fetch_json(Method::GET, &url, None).await?;
        let discovery = Discovery::deserialize(answer.body).map_err(unreachable)?;
        let device = discovery.device_authorization_endpoint.ok_or(CodeSignInError::Off)?;
        Ok((device, discovery.token_endpoint))
    }

    async fn started(&self) -> Result<SignInCode, CodeSignInError> {
        let (device, _) = self.endpoints().await?;
        let asked_at = SystemTime::now();
        let answer = self.fetch_json(Method::POST, &device, Some(form(&[("client_id", &self.client_id)]))).await?;
        if !answer.ok {
            return Err(CodeSignInError::Refused { words: words_of(&answer.body) });
        }
        let code = DeviceCode::deserialize(answer.body).map_err(unreachable)?;
        let seconds = |value: f64| Duration::try_from_secs_f64(value).map_err(unreachable);
        Ok(SignInCode {
            user_code: code.user_code,
            verification_uri: code.verification_uri,
            verification_uri_complete: code.verification_uri_complete,
            device_code: code.device_code,
            interval: seconds(code.interval.unwrap_or(DEFAULT_INTERVAL_SECONDS))?,
            expires_at: asked_at
                .checked_add(seconds(code.expires_in)?)
                .ok_or_else(|| unreachable("a code that outlives time"))?,
        })
    }

    async fn finished(&self, code: SignInCode) -> Result<String, CodeSignInError> {
        let (_, token) = before_expiry(&code, self.endpoints()).await??;
        let mut interval = code.interval;
        let asking =
            form(&[("grant_type", DEVICE_GRANT), ("device_code", &code.device_code), ("client_id", &self.client_id)]);
        loop {
            before_expiry(&code, sleep(interval)).await?;
            let Ok(answer) = before_expiry(&code, self.fetch_json(Method::POST, &token, Some(asking.clone()))).await?
            else {
                continue;
            };
            if answer.ok {
                return Tokens::deserialize(answer.body).map(|tokens| tokens.access_token).map_err(unreachable);
            }
            match OAuthError::deserialize(&answer.body).map(|error| error.error).as_deref() {
                Ok("authorization_pending") => {}
                Ok("slow_down") => interval += SLOW_DOWN,
                Ok("access_denied") => return Err(CodeSignInError::Denied),
                Ok("expired_token") => return Err(CodeSignInError::Expired),
                _ => return Err(CodeSignInError::Refused { words: words_of(&answer.body) }),
            }
        }
    }
}

/// `step`, ended as the code expires.
async fn before_expiry<F: std::future::Future>(code: &SignInCode, step: F) -> Result<F::Output, CodeSignInError> {
    let left = code.expires_at.duration_since(SystemTime::now()).map_err(|_| CodeSignInError::Expired)?;
    timeout(left, step).await.map_err(|_| CodeSignInError::Expired)
}

/// `POST /sign-in-token`: a Clerk sign-in token for the account the OAuth
/// access token names, which the app redeems with Clerk for a session.
pub async fn sign_in_token(hub: &str, app: &str, access_token: &str) -> Result<String, Error> {
    let (api, app, access_token) = (HubApi::new(hub, testing::none()), app.to_owned(), access_token.to_owned());
    on_own_runtime(async move { api.sign_in_token(&app, &access_token).await }).await
}
