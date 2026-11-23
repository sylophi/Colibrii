//! The Noise Protocol Framework (revision 34, noiseprotocol.org/noise.html)
//! as the one protocol the wire uses: `Noise_IK_25519_ChaChaPoly_SHA256`,
//! without extensions. The types are the specification's CipherState,
//! SymmetricState and HandshakeState (its section 5), with its method
//! names, and the comments cite its sections. The primitives come from the
//! RustCrypto and dalek crates.
//!
//! IK (section 7.5): the initiator knows the responder's static key before
//! it starts, and sends its own, encrypted, in the first message. Two
//! messages, after which each side holds a CipherState per direction. The
//! responder reads the initiator's static key off the handshake
//! (`remote_static_key`), and checking it is the caller's job.

use chacha20poly1305::ChaCha20Poly1305;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use hkdf::Hkdf;
use sha2::{Digest, Sha256};
use x25519_dalek::{X25519_BASEPOINT_BYTES, x25519};

pub const PROTOCOL_NAME: &str = "Noise_IK_25519_ChaChaPoly_SHA256";

// Section 4: DHLEN for 25519, and the AEAD tag.
const DHLEN: usize = 32;
pub const TAG_BYTES: usize = 16;

/// Section 3.
pub const MAX_MESSAGE_BYTES: usize = 65_535;

/// A handshake or transport message that cannot go on. The caller drops
/// the connection, and tells the far end nothing about why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoiseError {
    Authentication,
    Length,
    Turn,
    Nonce,
    Key,
}

impl std::fmt::Display for NoiseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            NoiseError::Authentication => "A Noise message failed authentication.",
            NoiseError::Length => "A Noise message has the wrong length.",
            NoiseError::Turn => "A Noise handshake message came out of turn.",
            NoiseError::Nonce => "A Noise cipher ran out of nonces.",
            NoiseError::Key => "A Noise public key is not usable.",
        })
    }
}

impl std::error::Error for NoiseError {}

#[derive(Clone)]
pub struct KeyPair {
    pub public_key: [u8; 32],
    pub private_key: [u8; 32],
}

impl std::fmt::Debug for KeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyPair").field("public_key", &self.public_key).finish_non_exhaustive()
    }
}

// ---- Section 4.1: DH functions ----

impl KeyPair {
    /// GENERATE_KEYPAIR().
    pub fn generate() -> KeyPair {
        let mut private_key = [0u8; 32];
        private_key.copy_from_slice(&super::random_bytes(32));
        KeyPair::from_private_key(private_key)
    }

    pub fn from_private_key(private_key: [u8; 32]) -> KeyPair {
        KeyPair { public_key: x25519(private_key, X25519_BASEPOINT_BYTES), private_key }
    }
}

/// DH(key_pair, public_key). A public key whose result is all zeros (a
/// low-order point) is refused, which section 12.1 allows.
fn dh(key_pair: &KeyPair, public_key: &[u8; 32]) -> Result<[u8; 32], NoiseError> {
    let shared = x25519(key_pair.private_key, *public_key);
    if shared.iter().all(|&byte| byte == 0) {
        return Err(NoiseError::Key);
    }
    Ok(shared)
}

// ---- Section 4.2: cipher functions ----

/// ChaChaPoly's 96-bit nonce: 32 bits of zeros, then n as a 64-bit
/// little-endian integer.
fn nonce_bytes(n: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(&n.to_le_bytes());
    nonce
}

// ---- Section 4.3: hash functions ----

/// HKDF(chaining_key, input_key_material, 2): RFC 5869's HKDF with the
/// chaining key as the salt and no info gives output1 and output2 as its
/// first 64 bytes.
fn hkdf2(chaining_key: &[u8; 32], input_key_material: &[u8]) -> ([u8; 32], [u8; 32]) {
    let mut output = [0u8; 64];
    Hkdf::<Sha256>::new(Some(chaining_key), input_key_material)
        .expand(&[], &mut output)
        .expect("64 bytes is a valid HKDF length");
    let mut first = [0u8; 32];
    let mut second = [0u8; 32];
    first.copy_from_slice(&output[..32]);
    second.copy_from_slice(&output[32..]);
    (first, second)
}

fn sha256(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

// ---- Section 5.1: CipherState ----

#[derive(Clone, Default)]
pub struct CipherState {
    k: Option<[u8; 32]>,
    n: u64,
}

impl CipherState {
    fn initialize_key(&mut self, key: [u8; 32]) {
        self.k = Some(key);
        self.n = 0;
    }

    fn has_key(&self) -> bool {
        self.k.is_some()
    }

    pub fn encrypt_with_ad(&mut self, ad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, NoiseError> {
        let Some(k) = self.k else { return Ok(plaintext.to_vec()) };
        if plaintext.len() + TAG_BYTES > MAX_MESSAGE_BYTES {
            return Err(NoiseError::Length);
        }
        // Section 5.1 reserves 2^64-1.
        if self.n == u64::MAX {
            return Err(NoiseError::Nonce);
        }
        let cipher = ChaCha20Poly1305::new_from_slice(&k).expect("a 32-byte key");
        let ciphertext = cipher
            .encrypt(&nonce_bytes(self.n).into(), Payload { msg: plaintext, aad: ad })
            .map_err(|_| NoiseError::Length)?;
        self.n += 1;
        Ok(ciphertext)
    }

    /// A failed decryption leaves n where it was (section 5.1).
    pub fn decrypt_with_ad(&mut self, ad: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, NoiseError> {
        let Some(k) = self.k else { return Ok(ciphertext.to_vec()) };
        if ciphertext.len() > MAX_MESSAGE_BYTES {
            return Err(NoiseError::Length);
        }
        if self.n == u64::MAX {
            return Err(NoiseError::Nonce);
        }
        let cipher = ChaCha20Poly1305::new_from_slice(&k).expect("a 32-byte key");
        let plaintext = cipher
            .decrypt(&nonce_bytes(self.n).into(), Payload { msg: ciphertext, aad: ad })
            .map_err(|_| NoiseError::Authentication)?;
        self.n += 1;
        Ok(plaintext)
    }
}

// ---- Section 5.2: SymmetricState ----

#[derive(Clone)]
struct SymmetricState {
    cipher_state: CipherState,
    ck: [u8; 32],
    h: [u8; 32],
}

impl SymmetricState {
    /// InitializeSymmetric(protocol_name). The name is exactly HASHLEN
    /// bytes, so it is h as it is.
    fn new() -> SymmetricState {
        let mut h = [0u8; 32];
        h.copy_from_slice(PROTOCOL_NAME.as_bytes());
        SymmetricState { cipher_state: CipherState::default(), ck: h, h }
    }

    /// HASHLEN is 32, so temp_k needs no truncation.
    fn mix_key(&mut self, input_key_material: &[u8]) {
        let (ck, temp_k) = hkdf2(&self.ck, input_key_material);
        self.ck = ck;
        self.cipher_state.initialize_key(temp_k);
    }

    fn mix_hash(&mut self, data: &[u8]) {
        self.h = sha256(&[&self.h, data]);
    }

    fn encrypt_and_hash(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, NoiseError> {
        let ciphertext = self.cipher_state.encrypt_with_ad(&self.h, plaintext)?;
        self.mix_hash(&ciphertext);
        Ok(ciphertext)
    }

    fn decrypt_and_hash(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, NoiseError> {
        let plaintext = self.cipher_state.decrypt_with_ad(&self.h, ciphertext)?;
        self.mix_hash(ciphertext);
        Ok(plaintext)
    }

    fn split(&self) -> (CipherState, CipherState) {
        let (temp_k1, temp_k2) = hkdf2(&self.ck, &[]);
        let mut c1 = CipherState::default();
        let mut c2 = CipherState::default();
        c1.initialize_key(temp_k1);
        c2.initialize_key(temp_k2);
        (c1, c2)
    }
}

// ---- Section 7.5: the IK pattern ----
//
//   IK:
//     <- s
//     ...
//     -> e, es, s, ss
//     <- e, ee, se

#[derive(Clone, Copy)]
enum Token {
    E,
    S,
    Ee,
    Es,
    Se,
    Ss,
}

const IK: [&[Token]; 2] = [&[Token::E, Token::Es, Token::S, Token::Ss], &[Token::E, Token::Ee, Token::Se]];

/// A finished handshake's ciphers for one side: Split's c1 is the
/// initiator's sending cipher, c2 the responder's (section 5.3).
#[derive(Clone)]
pub struct Transport {
    pub send: CipherState,
    pub receive: CipherState,
}

// ---- Section 5.3: HandshakeState ----

#[derive(Clone)]
pub struct HandshakeState {
    symmetric_state: SymmetricState,
    initiator: bool,
    s: KeyPair,
    e: Option<KeyPair>,
    rs: Option<[u8; 32]>,
    re: Option<[u8; 32]>,
    // The index of the next message pattern. The initiator writes the even
    // ones.
    next: usize,
}

impl HandshakeState {
    /// Initialize(handshake_pattern, initiator, prologue, s, e, rs, re) for
    /// the initiator, who knows rs, the responder's static public key. Only
    /// the test vectors pass e.
    pub fn initiator(prologue: &[u8], s: KeyPair, rs: [u8; 32], e: Option<KeyPair>) -> HandshakeState {
        HandshakeState::new(true, prologue, s, Some(rs), e)
    }

    /// The same for the responder.
    pub fn responder(prologue: &[u8], s: KeyPair, e: Option<KeyPair>) -> HandshakeState {
        HandshakeState::new(false, prologue, s, None, e)
    }

    fn new(initiator: bool, prologue: &[u8], s: KeyPair, rs: Option<[u8; 32]>, e: Option<KeyPair>) -> HandshakeState {
        let mut symmetric_state = SymmetricState::new();
        symmetric_state.mix_hash(prologue);
        // The pre-message "<- s".
        symmetric_state.mix_hash(rs.as_ref().unwrap_or(&s.public_key));
        HandshakeState { symmetric_state, initiator, s, e, rs, re: None, next: 0 }
    }

    /// The far end's static public key, which the responder learns from the
    /// first message.
    pub fn remote_static_key(&self) -> Option<[u8; 32]> {
        self.rs
    }

    pub fn handshake_hash(&self) -> [u8; 32] {
        self.symmetric_state.h
    }

    /// WriteMessage(payload, message_buffer), with this side's transport
    /// after the last message.
    pub fn write_message(&mut self, payload: &[u8]) -> Result<(Vec<u8>, Option<Transport>), NoiseError> {
        let mut message = Vec::new();
        for &token in self.take_pattern(true)? {
            match token {
                Token::E => {
                    let e = self.e.get_or_insert_with(KeyPair::generate).public_key;
                    message.extend_from_slice(&e);
                    self.symmetric_state.mix_hash(&e);
                }
                Token::S => {
                    let s = self.s.public_key;
                    message.extend(self.symmetric_state.encrypt_and_hash(&s)?);
                }
                dh_token => {
                    let shared = self.dh_for(dh_token)?;
                    self.symmetric_state.mix_key(&shared);
                }
            }
        }
        message.extend(self.symmetric_state.encrypt_and_hash(payload)?);
        if message.len() > MAX_MESSAGE_BYTES {
            return Err(NoiseError::Length);
        }
        Ok((message, self.transport_if_done()))
    }

    /// ReadMessage(message, payload_buffer), with this side's transport
    /// after the last message.
    pub fn read_message(&mut self, message: &[u8]) -> Result<(Vec<u8>, Option<Transport>), NoiseError> {
        if message.len() > MAX_MESSAGE_BYTES {
            return Err(NoiseError::Length);
        }
        let mut offset = 0;
        let mut take = |length: usize| -> Result<&[u8], NoiseError> {
            let bytes = message.get(offset..offset + length).ok_or(NoiseError::Length)?;
            offset += length;
            Ok(bytes)
        };
        for &token in self.take_pattern(false)? {
            match token {
                Token::E => {
                    let re: [u8; 32] = take(DHLEN)?.try_into().expect("DHLEN bytes");
                    self.re = Some(re);
                    self.symmetric_state.mix_hash(&re);
                }
                Token::S => {
                    let length = if self.symmetric_state.cipher_state.has_key() { DHLEN + TAG_BYTES } else { DHLEN };
                    let rs = self.symmetric_state.decrypt_and_hash(take(length)?)?;
                    self.rs = Some(rs.try_into().map_err(|_| NoiseError::Length)?);
                }
                dh_token => {
                    let shared = self.dh_for(dh_token)?;
                    self.symmetric_state.mix_key(&shared);
                }
            }
        }
        let payload = self.symmetric_state.decrypt_and_hash(&message[offset..])?;
        Ok((payload, self.transport_if_done()))
    }

    fn take_pattern(&mut self, writing: bool) -> Result<&'static [Token], NoiseError> {
        let pattern = IK.get(self.next).ok_or(NoiseError::Turn)?;
        if writing != (self.next.is_multiple_of(2) == self.initiator) {
            return Err(NoiseError::Turn);
        }
        self.next += 1;
        Ok(pattern)
    }

    /// The DH a token names, from this side: "es" is the initiator's e with
    /// the responder's s, so the initiator computes DH(e, rs) and the
    /// responder DH(s, re).
    fn dh_for(&self, token: Token) -> Result<[u8; 32], NoiseError> {
        let e = || self.e.as_ref().ok_or(NoiseError::Turn);
        let re = || self.re.as_ref().ok_or(NoiseError::Turn);
        let rs = || self.rs.as_ref().ok_or(NoiseError::Turn);
        match (token, self.initiator) {
            (Token::Ee, _) => dh(e()?, re()?),
            (Token::Es, true) => dh(e()?, rs()?),
            (Token::Es, false) => dh(&self.s, re()?),
            (Token::Se, true) => dh(&self.s, re()?),
            (Token::Se, false) => dh(e()?, rs()?),
            (Token::Ss, _) => dh(&self.s, rs()?),
            (Token::E | Token::S, _) => Err(NoiseError::Turn),
        }
    }

    fn transport_if_done(&self) -> Option<Transport> {
        if self.next < IK.len() {
            return None;
        }
        let (c1, c2) = self.symmetric_state.split();
        Some(if self.initiator { Transport { send: c1, receive: c2 } } else { Transport { send: c2, receive: c1 } })
    }
}
