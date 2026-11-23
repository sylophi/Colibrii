//! The service's HTTP API as the device calls it: every request names the
//! wire version, under the bearer the route takes. A failure to reach the
//! hub, no answer in time, or an answer the wire does not describe, reads
//! as HubUnreachable, and a 429 as RateLimited.

use std::sync::Arc;
use std::time::Duration;

use reqwest::Method;
use serde::Serialize;
use serde_json::Value;

use crate::errors::Error;
use crate::testing::Hooks;
use crate::wire::api::{ServiceError, VERSION_HEADER, WIRE_VERSION};

/// How long a call waits for its answer.
pub(crate) const ANSWER_WITHIN: Duration = Duration::from_secs(15);

/// Without a readable `retry-after`, a 429 waits this long.
const DEFAULT_RETRY_AFTER_SECONDS: f64 = 60.0;

pub(crate) fn retry_after_of(header: Option<&str>) -> f64 {
    header
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .and_then(|text| text.parse::<f64>().ok())
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
        .unwrap_or(DEFAULT_RETRY_AFTER_SECONDS)
}

// The errors each route may answer, its own and those of its middleware:
// the version floor on every route, and the credential's check on the
// device routes.
pub(crate) const ENROLL: &[&str] =
    &["UpdateRequired", "LoginRejected", "UnknownApp", "DeviceEnrolledElsewhere", "AccountFull", "Unavailable"];
pub(crate) const SIGN_IN_TOKEN: &[&str] =
    &["UpdateRequired", "LoginRejected", "UnknownApp", "SignInTokensOff", "Unavailable"];
pub(crate) const DEVICE: &[&str] = &["UpdateRequired", "CredentialRejected", "DeviceRevoked", "Unavailable"];
pub(crate) const DEVICE_BY_ID: &[&str] =
    &["UpdateRequired", "CredentialRejected", "DeviceRevoked", "Unavailable", "UnknownDevice"];

#[derive(Clone)]
pub(crate) struct HubApi {
    client: reqwest::Client,
    hub: String,
    hooks: Arc<dyn Hooks>,
}

impl HubApi {
    pub fn new(hub: &str, hooks: Arc<dyn Hooks>) -> HubApi {
        HubApi { client: reqwest::Client::new(), hub: hub.trim_end_matches('/').to_owned(), hooks }
    }

    pub fn hub(&self) -> &str {
        &self.hub
    }

    /// One call under `bearer`, a login token or the device credential,
    /// answering the body's JSON, or null for none.
    pub async fn call(
        &self,
        method: Method,
        path: &str,
        bearer: &str,
        body: Option<&(impl Serialize + Sync)>,
        errors: &[&str],
    ) -> Result<Value, Error> {
        let url = format!("{}{path}", self.hub);
        let url = self.hooks.divert(method.as_str(), &url).unwrap_or(url);
        let mut request = self
            .client
            .request(method, url)
            .header(VERSION_HEADER, WIRE_VERSION.to_string())
            .header("authorization", format!("Bearer {bearer}"));
        for (name, value) in self.hooks.headers() {
            request = request.header(name, value);
        }
        if let Some(body) = body {
            request = request
                .header("content-type", "application/json")
                .body(serde_json::to_vec(body).expect("a request body encodes"));
        }
        let answered = async {
            let response = request.send().await.map_err(|error| Error::HubUnreachable(error.to_string()))?;
            let status = response.status().as_u16();
            let retry_after =
                response.headers().get("retry-after").and_then(|value| value.to_str().ok()).map(str::to_owned);
            let bytes = response.bytes().await.map_err(|error| Error::HubUnreachable(error.to_string()))?;
            Ok::<_, Error>((status, retry_after, bytes))
        };
        let (status, retry_after, bytes) = tokio::time::timeout(ANSWER_WITHIN, answered)
            .await
            .map_err(|_| Error::HubUnreachable("no answer in time".into()))??;
        if status == 429 {
            return Err(Error::RateLimited { retry_after_seconds: retry_after_of(retry_after.as_deref()) });
        }
        let json = if bytes.is_empty() { Some(Value::Null) } else { serde_json::from_slice(&bytes).ok() };
        if (200..300).contains(&status) {
            return json.ok_or_else(|| Error::HubUnreachable(format!("an unreadable answer ({status})")));
        }
        match json.as_ref().and_then(ServiceError::decode) {
            Some(error) if error.status() == status && errors.contains(&error.tag()) => Err(Error::Service(error)),
            _ => Err(Error::HubUnreachable(format!("an unreadable answer ({status})"))),
        }
    }

    /// `POST /sign-in-token`.
    pub async fn sign_in_token(&self, app: &str, access_token: &str) -> Result<String, Error> {
        let answer = self
            .call(
                Method::POST,
                "/sign-in-token",
                access_token,
                Some(&crate::wire::api::SignInTokenRequest { app: app.to_owned() }),
                SIGN_IN_TOKEN,
            )
            .await?;
        answer
            .get("token")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| Error::HubUnreachable("an unreadable answer".into()))
    }
}

/// A call's error that stops the device as its close code would.
pub(crate) fn stop_of_error(error: &Error) -> Option<crate::device::Stop> {
    use crate::device::Stop;
    match error {
        Error::Service(ServiceError::DeviceRevoked) => Some(Stop::Removed),
        Error::Service(ServiceError::CredentialRejected) => Some(Stop::CredentialRejected),
        Error::Service(ServiceError::UpdateRequired { .. }) => Some(Stop::UpdateRequired),
        _ => None,
    }
}
