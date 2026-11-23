//! Colibrii's device side in Rust: a device enrolls under its person's
//! account, follows which of their other devices are online, and opens end
//! to end encrypted links to them.

mod cloudflared;
mod conn;
mod device;
mod dial;
mod errors;
#[cfg(test)]
mod fakes;
mod hub_api;
mod hub_socket;
mod link;
mod listener;
mod liveness;
mod runtime;
mod session;
mod sign_in;
mod store;
#[doc(hidden)]
pub mod testing;
mod tickets;
mod tunnel;
mod wakes;
mod watch;
pub mod wire;

pub use cloudflared::{CLOUDFLARED_VERSION, FetchError, fetch_cloudflared};
pub use device::{AccountState, Device, Devices, Incoming, Options, Peer, StopReason, TunnelState};
pub use errors::{
    CandidateFailure, DialError, Error, InvalidInput, RefusedBy, SendError, ServiceError, StoreOperation,
};
pub use link::{Link, LinkCloseCause, LinkClosed, Role};
pub use listener::{IncomingHello, Listen};
pub use runtime::block_on;
pub use sign_in::{CodeSignIn, CodeSignInError, SignInCode, sign_in_token};
pub use store::{CredentialStore, FileStore, MemoryStore};
pub use tunnel::{TunnelOptions, end_leftover_cloudflared};
pub use watch::Watch;
pub use wire::hub::{Connections, DeviceRecord};
pub use wire::relay::CandidateKind;
