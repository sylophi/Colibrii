//! The ids the wire carries, and the checks on their form.

/// 1 to 32 characters, each a lowercase letter, a digit, or `-`.
pub fn is_app_id(text: &str) -> bool {
    (1..=32).contains(&text.len()) && text.bytes().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}

fn is_lower_hex(text: &str) -> bool {
    text.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// A UUID in lowercase canonical form.
pub fn is_device_id(text: &str) -> bool {
    let parts: Vec<&str> = text.split('-').collect();
    parts.iter().map(|part| part.len()).eq([8, 4, 4, 4, 12]) && parts.iter().all(|part| is_lower_hex(part))
}

/// 16 bytes as 32 lowercase hex characters.
pub fn is_connection_id(text: &str) -> bool {
    text.len() == 32 && is_lower_hex(text)
}

/// An X25519 public key, 32 bytes as base64url.
pub fn is_public_key(text: &str) -> bool {
    text.len() == 43 && super::base64url::decode(text).is_some()
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn new_connection_id() -> String {
    hex(&super::random_bytes(16))
}

/// A random (version 4) UUID.
pub fn new_device_id() -> String {
    let mut bytes = super::random_bytes(16);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let text = hex(&bytes);
    format!("{}-{}-{}-{}-{}", &text[..8], &text[8..12], &text[12..16], &text[16..20], &text[20..])
}
