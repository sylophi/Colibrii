//! Where a device keeps its id, credential and private key between runs
//! (spec/service.md, "What a device keeps"), as one text document.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::Serialize;
use serde_json::Value;

use crate::wire::base64url;
use crate::wire::ids::is_device_id;

/// Keeps the device's document. Only the person's own user account should
/// be able to read it: an app that has a keychain or credential store of
/// the system's keeps it there, and otherwise in a [`FileStore`].
///
/// Each call is short and may block, as a keychain does.
pub trait CredentialStore: Send + Sync + 'static {
    fn read(&self) -> io::Result<Option<String>>;
    fn write(&self, text: &str) -> io::Result<()>;
    fn remove(&self) -> io::Result<()>;
}

/// The document as one file, readable only by its owner. A write is whole
/// or not at all. On Windows a file's mode does not limit who reads it, so
/// an app there keeps the document in the system's credential store
/// through a [`CredentialStore`] of its own.
#[derive(Debug, Clone)]
pub struct FileStore {
    path: PathBuf,
}

impl FileStore {
    pub fn new(path: impl Into<PathBuf>) -> FileStore {
        FileStore { path: path.into() }
    }
}

#[cfg(unix)]
fn create_private_dir(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new().recursive(true).mode(0o700).create(dir)
}

#[cfg(not(unix))]
fn create_private_dir(dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)
}

#[cfg(unix)]
fn write_private(path: &Path, text: &str) -> io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(path)?;
    file.write_all(text.as_bytes())?;
    file.sync_all()
}

#[cfg(not(unix))]
fn write_private(path: &Path, text: &str) -> io::Result<()> {
    fs::write(path, text)
}

impl CredentialStore for FileStore {
    fn read(&self) -> io::Result<Option<String>> {
        match fs::read_to_string(&self.path) {
            Ok(text) => Ok(Some(text)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn write(&self, text: &str) -> io::Result<()> {
        if let Some(dir) = self.path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
            create_private_dir(dir)?;
        }
        let mut next = self.path.clone().into_os_string();
        next.push(".next");
        write_private(Path::new(&next), text)?;
        fs::rename(&next, &self.path)
    }

    fn remove(&self) -> io::Result<()> {
        match fs::remove_file(&self.path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }
}

/// The document in memory, for a device that keeps nothing between runs.
#[derive(Default)]
pub struct MemoryStore {
    text: Mutex<Option<String>>,
}

debug_without_secrets!(MemoryStore {});

impl MemoryStore {
    pub fn new() -> MemoryStore {
        MemoryStore::default()
    }
}

impl CredentialStore for MemoryStore {
    fn read(&self) -> io::Result<Option<String>> {
        Ok(self.text.lock().unwrap().clone())
    }

    fn write(&self, text: &str) -> io::Result<()> {
        *self.text.lock().unwrap() = Some(text.to_owned());
        Ok(())
    }

    fn remove(&self) -> io::Result<()> {
        *self.text.lock().unwrap() = None;
        Ok(())
    }
}

// ---- The document ----

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Enrollment {
    pub device_id: String,
    pub account_id: String,
    pub credential: String,
    pub private_key: [u8; 32],
    /// The port the listener last listened on, so a peer's LAN address for
    /// it and the tunnel's ingress stay right across runs.
    pub port: Option<u16>,
}

debug_without_secrets!(Enrollment { device_id, account_id, port });

/// Signed out before the service heard of it: the device removes itself
/// with the credential once it can.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Owed {
    pub device_id: String,
    pub credential: String,
}

debug_without_secrets!(Owed { device_id });

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Stored {
    Enrollment(Enrollment),
    Owed(Owed),
    /// Removed from the account, by another device or past its cap: the
    /// device reads as removed until the app enrolls it or signs it out.
    Removed {
        device_id: String,
    },
}

impl Stored {
    pub fn device_id(&self) -> &str {
        match self {
            Stored::Enrollment(enrollment) => &enrollment.device_id,
            Stored::Owed(owed) => &owed.device_id,
            Stored::Removed { device_id } => device_id,
        }
    }

    pub fn enrollment(&self) -> Option<&Enrollment> {
        match self {
            Stored::Enrollment(enrollment) => Some(enrollment),
            _ => None,
        }
    }

    pub fn owed(&self) -> Option<&Owed> {
        match self {
            Stored::Owed(owed) => Some(owed),
            _ => None,
        }
    }

    /// The same document the TypeScript SDK keeps.
    pub fn decode(text: &str) -> Option<Stored> {
        let value: Value = serde_json::from_str(text).ok()?;
        let string = |field: &str| value.get(field)?.as_str().map(str::to_owned);
        let device_id = string("deviceId").filter(|id| is_device_id(id))?;
        if value.get("removed") == Some(&Value::Bool(true)) {
            return Some(Stored::Removed { device_id });
        }
        let credential = string("credential")?;
        if let (Some(account_id), Some(private_key)) = (string("accountId"), string("privateKey")) {
            let port = match value.get("port") {
                None => None,
                Some(port) => Some(u16::try_from(port.as_u64()?).ok()?),
            };
            return Some(Stored::Enrollment(Enrollment {
                device_id,
                account_id: Some(account_id).filter(|id| !id.is_empty())?,
                credential,
                private_key: base64url::decode(&private_key)?.try_into().ok()?,
                port,
            }));
        }
        (value.get("signedOut")? == &Value::Bool(true)).then_some(Stored::Owed(Owed { device_id, credential }))
    }

    pub fn encode(&self) -> String {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct EnrollmentOut<'a> {
            device_id: &'a str,
            account_id: &'a str,
            credential: &'a str,
            private_key: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            port: Option<u16>,
        }
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct OwedOut<'a> {
            device_id: &'a str,
            credential: &'a str,
            signed_out: bool,
        }
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct RemovedOut<'a> {
            device_id: &'a str,
            removed: bool,
        }
        match self {
            Stored::Enrollment(enrollment) => serde_json::to_string(&EnrollmentOut {
                device_id: &enrollment.device_id,
                account_id: &enrollment.account_id,
                credential: &enrollment.credential,
                private_key: base64url::encode(&enrollment.private_key),
                port: enrollment.port,
            }),
            Stored::Owed(owed) => serde_json::to_string(&OwedOut {
                device_id: &owed.device_id,
                credential: &owed.credential,
                signed_out: true,
            }),
            Stored::Removed { device_id } => serde_json::to_string(&RemovedOut { device_id, removed: true }),
        }
        .expect("the document encodes")
    }
}
