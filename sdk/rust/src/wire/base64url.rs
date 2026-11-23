//! Base64url (RFC 4648, section 5). Encoding never pads. Decoding takes
//! the input with or without its padding, and nothing outside the
//! URL-safe alphabet.

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

pub fn encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for i in 0..=chunk.len() {
            out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
        }
    }
    out
}

fn value(c: u8) -> Option<u32> {
    match c {
        b'A'..=b'Z' => Some(u32::from(c - b'A')),
        b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
        b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
}

/// Nothing for text that is not base64url: a last group of one
/// character, padding anywhere but the end or of the wrong length, or a
/// character outside the alphabet.
pub fn decode(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    let unpadded = match bytes.len() % 4 {
        0 if bytes.ends_with(b"==") => &bytes[..bytes.len() - 2],
        0 if bytes.ends_with(b"=") => &bytes[..bytes.len() - 1],
        _ => bytes,
    };
    if unpadded.len() % 4 == 1 {
        return None;
    }
    // Padding, when there is any, completes the last group exactly.
    if unpadded.len() != bytes.len() && unpadded.len() % 4 + (bytes.len() - unpadded.len()) != 4 {
        return None;
    }
    let mut out = Vec::with_capacity(unpadded.len() * 3 / 4);
    for chunk in unpadded.chunks(4) {
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= value(c)? << (18 - 6 * i);
        }
        out.extend_from_slice(&n.to_be_bytes()[1..chunk.len()]);
    }
    Some(out)
}

/// Base64url of `byte_length` random bytes.
pub fn random(byte_length: usize) -> String {
    encode(&super::random_bytes(byte_length))
}
