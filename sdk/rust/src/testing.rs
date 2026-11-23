//! What the SDK's own tests put in place of the network. Not for apps.

use std::net::IpAddr;
use std::sync::Arc;

pub use crate::conn::{Close, Msg, Opened};
pub use crate::tunnel::Timings;
use crate::wire::relay::CandidateKind;

pub trait Hooks: Send + Sync + 'static {
    /// The URL an HTTP request goes to instead, if any.
    fn divert(&self, _method: &str, _url: &str) -> Option<String> {
        None
    }

    /// Headers added to every HTTP request this device makes, and to its
    /// hub socket's upgrade.
    fn headers(&self) -> Vec<(String, String)> {
        Vec::new()
    }

    /// Before a socket opens: false refuses it.
    fn open(&self, _url: &str) -> bool {
        true
    }

    /// Each socket this device opened.
    fn opened(&self, _socket: Opened) {}

    fn dialable_kinds(&self) -> Vec<CandidateKind> {
        vec![CandidateKind::Lan, CandidateKind::Tunnel]
    }

    /// The listener's addresses, as found.
    fn addresses(&self, found: Vec<IpAddr>) -> Vec<IpAddr> {
        found
    }

    /// The port the listener bound.
    fn bound(&self, _port: u16) {}

    /// The `cloudflared` the tunnel runner looks for on the `PATH`.
    fn cloudflared_on_path(&self) -> String {
        "cloudflared".into()
    }

    /// Where the tunnel runner fetches its hostname.
    fn probe_url(&self, hostname: &str) -> String {
        format!("https://{hostname}")
    }
}

pub(crate) struct NoHooks;

impl Hooks for NoHooks {}

pub(crate) fn none() -> Arc<dyn Hooks> {
    Arc::new(NoHooks)
}
