//! The service's HTTP API (spec/service.md): its bodies and its errors,
//! each of which crosses the wire as its `_tag` and fields.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::hub::{Connections, DeviceRecord, readable};

pub const VERSION_HEADER: &str = "colibrii-version";
pub const WIRE_VERSION: u32 = 1;

pub const DEVICE_CREDENTIAL_PREFIX: &str = "colibrii_dc_";
pub const CONNECT_TICKET_PREFIX: &str = "colibrii_ct_";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollRequest {
    pub app: String,
    pub device_id: String,
    pub name: String,
    pub public_key: String,
    pub connections: Connections,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollResponse {
    pub credential: String,
    pub account_id: String,
    pub device: DeviceRecord,
}

impl EnrollResponse {
    pub fn decode(value: &Value) -> Option<EnrollResponse> {
        let response = EnrollResponse {
            credential: value.get("credential")?.as_str()?.to_owned(),
            account_id: value.get("accountId")?.as_str()?.to_owned(),
            device: DeviceRecord::decode(value.get("device")?)?,
        };
        (!response.account_id.is_empty()).then_some(response)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignInTokenRequest {
    pub app: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignInTokenResponse {
    pub token: String,
}

/// A device leaves out an entry it cannot read.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeviceList {
    pub devices: Vec<DeviceRecord>,
}

impl DeviceList {
    pub fn decode(value: &Value) -> Option<DeviceList> {
        Some(DeviceList { devices: readable(value.get("devices")?, DeviceRecord::decode)? })
    }
}

/// `name`, `data`, or both.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DevicePatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TicketRequest {
    pub connection_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketResponse {
    pub ticket: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunnelRequest {
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TunnelResponse {
    pub hostname: String,
    pub connector_token: String,
    pub fresh: bool,
}

/// An error the service answers with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "_tag")]
pub enum ServiceError {
    /// The request named no version, or one below `floor`.
    UpdateRequired {
        floor: i64,
    },
    /// The login token is missing, invalid, expired, or not issued for the
    /// app named.
    LoginRejected,
    UnknownApp {
        app: String,
    },
    /// The device credential is malformed or matches no device.
    CredentialRejected,
    /// The credential belonged to a device that was removed.
    DeviceRevoked,
    /// The device id is enrolled under another app or account.
    DeviceEnrolledElsewhere,
    /// The app's device cap is reached and every device is online.
    AccountFull {
        limit: i64,
    },
    /// No such device in the caller's app and account.
    UnknownDevice {
        #[serde(rename = "deviceId")]
        device_id: String,
    },
    TicketMalformed,
    UpgradeRequired,
    /// The app registered no Clerk secret key.
    SignInTokensOff,
    /// Storage, Cloudflare, or Clerk failed.
    Unavailable {
        operation: String,
    },
}

const OPERATIONS: [&str; 8] = ["enroll", "credential", "list", "revoke", "update", "ticket", "tunnel", "sign-in-token"];

impl ServiceError {
    pub fn tag(&self) -> &'static str {
        match self {
            ServiceError::UpdateRequired { .. } => "UpdateRequired",
            ServiceError::LoginRejected => "LoginRejected",
            ServiceError::UnknownApp { .. } => "UnknownApp",
            ServiceError::CredentialRejected => "CredentialRejected",
            ServiceError::DeviceRevoked => "DeviceRevoked",
            ServiceError::DeviceEnrolledElsewhere => "DeviceEnrolledElsewhere",
            ServiceError::AccountFull { .. } => "AccountFull",
            ServiceError::UnknownDevice { .. } => "UnknownDevice",
            ServiceError::TicketMalformed => "TicketMalformed",
            ServiceError::UpgradeRequired => "UpgradeRequired",
            ServiceError::SignInTokensOff => "SignInTokensOff",
            ServiceError::Unavailable { .. } => "Unavailable",
        }
    }

    pub fn status(&self) -> u16 {
        match self {
            ServiceError::LoginRejected | ServiceError::CredentialRejected => 401,
            ServiceError::UpdateRequired { .. }
            | ServiceError::DeviceRevoked
            | ServiceError::TicketMalformed
            | ServiceError::SignInTokensOff => 403,
            ServiceError::UnknownApp { .. } | ServiceError::UnknownDevice { .. } => 404,
            ServiceError::DeviceEnrolledElsewhere | ServiceError::AccountFull { .. } => 409,
            ServiceError::UpgradeRequired => 426,
            ServiceError::Unavailable { .. } => 502,
        }
    }

    /// Nothing for a `_tag` this reader does not know or a field it cannot
    /// read, which reads as an answer it cannot read.
    pub fn decode(value: &Value) -> Option<ServiceError> {
        let int = |field: &str| value.get(field)?.as_i64();
        let string = |field: &str| value.get(field)?.as_str().map(str::to_owned);
        Some(match value.get("_tag")?.as_str()? {
            "UpdateRequired" => ServiceError::UpdateRequired { floor: int("floor")? },
            "LoginRejected" => ServiceError::LoginRejected,
            "UnknownApp" => ServiceError::UnknownApp { app: string("app")? },
            "CredentialRejected" => ServiceError::CredentialRejected,
            "DeviceRevoked" => ServiceError::DeviceRevoked,
            "DeviceEnrolledElsewhere" => ServiceError::DeviceEnrolledElsewhere,
            "AccountFull" => ServiceError::AccountFull { limit: int("limit")? },
            "UnknownDevice" => ServiceError::UnknownDevice { device_id: string("deviceId")? },
            "TicketMalformed" => ServiceError::TicketMalformed,
            "UpgradeRequired" => ServiceError::UpgradeRequired,
            "SignInTokensOff" => ServiceError::SignInTokensOff,
            "Unavailable" => {
                let operation = string("operation")?;
                OPERATIONS.contains(&operation.as_str()).then_some(())?;
                ServiceError::Unavailable { operation }
            }
            _ => return None,
        })
    }
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceError::UpdateRequired { floor } => write!(f, "The service needs wire version {floor} or later."),
            ServiceError::LoginRejected => f.write_str("The service did not accept this sign-in."),
            ServiceError::UnknownApp { app } => write!(f, "No app is registered as {app}."),
            ServiceError::CredentialRejected => f.write_str("The service does not accept this device credential."),
            ServiceError::DeviceRevoked => f.write_str("This device was removed."),
            ServiceError::DeviceEnrolledElsewhere => {
                f.write_str("This device is enrolled under another app or account.")
            }
            ServiceError::AccountFull { limit } => write!(f, "The account has {limit} devices, all online."),
            ServiceError::UnknownDevice { .. } => f.write_str("The account has no such device."),
            ServiceError::TicketMalformed => f.write_str("The connect ticket is malformed."),
            ServiceError::UpgradeRequired => f.write_str("The connect route takes a websocket upgrade."),
            ServiceError::SignInTokensOff => f.write_str("The service mints no sign-in tokens for this app."),
            ServiceError::Unavailable { operation } => {
                write!(f, "The service could not complete the {operation} request.")
            }
        }
    }
}
