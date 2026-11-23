//! Colibrii's device side in Rust: a device enrolls under its person's
//! account, follows which of their other devices are online, and opens end
//! to end encrypted links to them.

mod conn;
mod device;
mod errors;
mod hub_api;
mod hub_socket;
mod liveness;
mod runtime;
mod session;
mod store;
#[doc(hidden)]
pub mod testing;
mod wakes;
mod watch;
pub mod wire;

pub use device::{AccountState, Device, Devices, Options, Peer, StopReason, TunnelState};
pub use errors::{
    CandidateFailure, DialError, Error, InvalidInput, RefusedBy, SendError, ServiceError, StoreOperation,
};
pub use runtime::block_on;
pub use store::{CredentialStore, FileStore, MemoryStore};
pub use watch::Watch;
pub use wire::hub::{Connections, DeviceRecord};
