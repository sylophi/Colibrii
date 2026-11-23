//! Colibrii's device side in Rust: a device enrolls under its person's
//! account, follows which of their other devices are online, and opens end
//! to end encrypted links to them.

/// `Debug` showing only the fields named, for a type that holds a secret.
macro_rules! debug_without_secrets {
    ($type:ident { $($field:ident),* }) => {
        impl std::fmt::Debug for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($type))$(.field(stringify!($field), &self.$field))*.finish_non_exhaustive()
            }
        }
    };
}

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

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use crate::store::{CredentialStore, Enrollment, MemoryStore, Owed, Stored};
    use crate::wire::api::{EnrollResponse, SignInTokenResponse, TicketResponse, TunnelResponse};
    use crate::wire::hub::{Connections, DeviceRecord};
    use crate::wire::noise::KeyPair;
    use crate::wire::relay::{Candidate, CandidateKind};

    const SECRET: &str = "s3cr3t";

    #[test]
    fn debug_prints_no_secret() {
        let key = KeyPair::from_private_key([7; 32]);
        let enrollment = Enrollment {
            device_id: "device".into(),
            account_id: "account".into(),
            credential: SECRET.into(),
            private_key: key.private_key,
            port: None,
        };
        let store = MemoryStore::new();
        store.write(SECRET).unwrap();
        let device = DeviceRecord {
            device_id: "device".into(),
            name: "desk".into(),
            data: Default::default(),
            connections: Connections::Single,
            created_at: 0,
            last_seen_at: None,
            online: true,
        };
        let printed = [
            format!("{key:?}"),
            format!("{store:?}"),
            format!("{:?}", Stored::Enrollment(enrollment)),
            format!("{:?}", Stored::Owed(Owed { device_id: "device".into(), credential: SECRET.into() })),
            format!("{:?}", EnrollResponse { credential: SECRET.into(), account_id: "account".into(), device }),
            format!("{:?}", SignInTokenResponse { token: SECRET.into() }),
            format!("{:?}", TicketResponse { ticket: SECRET.into() }),
            format!("{:?}", TunnelResponse { hostname: "host".into(), connector_token: SECRET.into(), fresh: true }),
            format!("{:?}", Candidate { kind: CandidateKind::Lan, url: "ws://host".into(), ticket: SECRET.into() }),
            format!(
                "{:?}",
                crate::SignInCode {
                    user_code: "ABCD-EFGH".into(),
                    verification_uri: "https://clerk".into(),
                    verification_uri_complete: None,
                    device_code: SECRET.into(),
                    interval: Duration::from_secs(5),
                    expires_at: SystemTime::UNIX_EPOCH,
                }
            ),
        ];
        let private = format!("{:?}", key.private_key);
        for printed in printed {
            assert!(!printed.contains(SECRET) && !printed.contains(&private), "{printed}");
        }
    }
}
