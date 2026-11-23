//! The SDK's own errors. The service's errors are the wire's
//! (`wire::api::ServiceError`), and cross into the SDK as they are.

use std::fmt;

pub use crate::wire::api::ServiceError;
use crate::wire::relay::CandidateKind;

/// Input the SDK refuses before it sends anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidInput {
    /// A device name is 1 to 256 bytes of UTF-8.
    Name,
    /// Device data is a JSON object of at most 1024 bytes.
    Data,
    /// A device id is a UUID in lowercase canonical form.
    DeviceId,
    /// An app hello is at most 16 KiB.
    AppHello,
}

impl fmt::Display for InvalidInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            InvalidInput::Name => "A device name is 1 to 256 bytes of UTF-8.",
            InvalidInput::Data => "Device data is a JSON object of at most 1024 bytes.",
            InvalidInput::DeviceId => "A device id is a UUID in lowercase canonical form.",
            InvalidInput::AppHello => "An app hello is at most 16 KiB.",
        })
    }
}

/// What failed in the credential store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOperation {
    Read,
    Write,
    Remove,
}

/// Why a device's call failed.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// The service's answer.
    Service(ServiceError),
    /// The hub could not be reached, gave no answer in time, or answered
    /// something unreadable.
    HubUnreachable(String),
    /// The hub asks to wait this many seconds.
    RateLimited {
        retry_after_seconds: f64,
    },
    InvalidInput(InvalidInput),
    /// This device is not enrolled.
    NotEnrolled,
    /// The credential store failed.
    Store {
        operation: StoreOperation,
        message: String,
    },
    /// The listener could not start.
    Listen(String),
}

impl Error {
    pub(crate) fn store(operation: StoreOperation, error: std::io::Error) -> Error {
        Error::Store { operation, message: error.to_string() }
    }

    /// An answer the wire does not describe.
    pub(crate) fn unreadable() -> Error {
        Error::HubUnreachable("an unreadable answer".into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Service(error) => error.fmt(f),
            Error::HubUnreachable(cause) => {
                write!(f, "The hub could not be reached, or answered something unreadable: {cause}")
            }
            Error::RateLimited { retry_after_seconds } => {
                write!(f, "The hub asks to wait {retry_after_seconds} seconds.")
            }
            Error::InvalidInput(input) => input.fmt(f),
            Error::NotEnrolled => f.write_str("This device is not enrolled."),
            Error::Store { operation, message } => {
                let operation = match operation {
                    StoreOperation::Read => "read",
                    StoreOperation::Write => "write",
                    StoreOperation::Remove => "remove",
                };
                write!(f, "Could not {operation} the device's credential: {message}")
            }
            Error::Listen(cause) => write!(f, "The listener could not start: {cause}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<ServiceError> for Error {
    fn from(error: ServiceError) -> Error {
        Error::Service(error)
    }
}

impl From<InvalidInput> for Error {
    fn from(input: InvalidInput) -> Error {
        Error::InvalidInput(input)
    }
}

/// Who refused a link: Colibrii's checks, or the listening app.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusedBy {
    Colibrii,
    App,
}

/// One candidate a dial tried and lost, for a dial that got no link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateFailure {
    pub kind: CandidateKind,
    pub url: String,
    /// The close code its socket ended with, when it opened and closed.
    pub close_code: Option<u16>,
    pub reason: String,
}

/// A dial that got no link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DialError {
    /// This device's hub socket is down, or it is not enrolled.
    NotConnected,
    /// The peer is not in the roster, or left it or changed its key while
    /// dialed.
    Offline,
    /// The peer never listens, such as a browser.
    NoListener,
    /// The peer listens, but offers nothing this device can dial now.
    Unavailable,
    /// No answer to the ask, or no welcome, in time.
    TimedOut,
    /// The answer could not be read, or every candidate failed before its
    /// welcome.
    Unreachable {
        cause: String,
        failures: Vec<CandidateFailure>,
    },
    /// The listener refused the link: Colibrii's checks, or its app, with
    /// the app's own bytes.
    Refused {
        by: RefusedBy,
        reason: String,
        app_refusal: Option<Vec<u8>>,
    },
    InvalidInput(InvalidInput),
}

impl DialError {
    pub(crate) fn unreachable(cause: impl Into<String>) -> DialError {
        DialError::Unreachable { cause: cause.into(), failures: Vec::new() }
    }
}

impl fmt::Display for DialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DialError::NotConnected => f.write_str("Could not dial: this device is not connected."),
            DialError::Offline => f.write_str("Could not dial: the device is offline."),
            DialError::NoListener => f.write_str("Could not dial: the device does not listen."),
            DialError::Unavailable => f.write_str("Could not dial: the device offers nothing to dial now."),
            DialError::TimedOut => f.write_str("Could not dial: no answer in time."),
            DialError::Unreachable { cause, .. } => write!(f, "Could not dial: {cause}"),
            DialError::Refused { by, reason, .. } => {
                let by = match by {
                    RefusedBy::Colibrii => "colibrii",
                    RefusedBy::App => "app",
                };
                write!(f, "The device refused the link ({by}): {reason}")
            }
            DialError::InvalidInput(input) => input.fmt(f),
        }
    }
}

impl std::error::Error for DialError {}

/// A link's `send` that could not go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    /// The link is closed.
    Closed,
    /// An app message is at most 32 MiB.
    TooBig,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SendError::Closed => "The link is closed.",
            SendError::TooBig => "An app message is at most 32 MiB.",
        })
    }
}

impl std::error::Error for SendError {}
