//! The wire itself (spec/ in the repository's root) as pure code: the
//! service's HTTP bodies and errors, the hub socket's messages, Noise, the
//! relay's sealing, and the peer link's handshake and messages. The
//! device runtime is built on it, and the test vectors in `spec/vectors/`
//! check it.

pub mod api;
pub mod base64url;
pub mod hub;
pub mod ids;
pub mod link;
pub mod noise;
pub mod relay;

/// `length` bytes from the system's random source.
pub fn random_bytes(length: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; length];
    getrandom::fill(&mut bytes).expect("the system's random source");
    bytes
}

/// Public keys, so no constant-time compare is needed.
pub fn same_key(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a == b
}

/// A device key's public half as the service and the roster carry it.
pub fn decode_public_key(text: &str) -> Option<[u8; 32]> {
    base64url::decode(text)?.try_into().ok()
}
