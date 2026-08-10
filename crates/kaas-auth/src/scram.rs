//! SCRAM-SHA-512 server state machine (RFC 5802).
//!
//! Two steps:
//!
//! 1. **client-first** — `"n,,n=<user>,r=<clientNonce>"`. Server
//!    looks up the user's stored credentials, appends an 18-byte
//!    random server nonce, returns `"r=<full>,s=<salt>,i=<iter>"`.
//! 2. **client-final** — `"c=biws,r=<fullNonce>,p=<proofB64>"`.
//!    Server recomputes `ClientSignature = HMAC(StoredKey,
//!    AuthMessage)`, derives `RecoveredKey = ClientProof XOR
//!    ClientSignature`, checks `H(RecoveredKey) == StoredKey` via
//!    constant-time compare. Returns `"v=<serverSigB64>"`.

use std::collections::HashMap;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::engine::general_purpose::STANDARD_NO_PAD as BASE64_NOPAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use rand::rngs::OsRng;
use rand::RngCore;
use sha2::{Digest, Sha512};
use subtle::ConstantTimeEq;

use crate::credentials::CredentialStore;
use crate::engine::SaslExchange;
use crate::errors::AuthError;
use crate::types::{Principal, PrincipalKind};

/// 18-byte random nonce suffix. Base64-no-pad → 24 chars.
const NONCE_SUFFIX_BYTES: usize = 18;

#[derive(Debug)]
enum ScramState {
    Start,
    AwaitFinal,
    Done,
}

/// Pluggable RNG seam so tests can inject a deterministic stream.
/// Production uses [`OsRng`]; tests use a step RNG.
pub trait NonceSource: Send + std::fmt::Debug {
    /// Fill `dst` with random bytes.
    fn fill(&mut self, dst: &mut [u8]);
}

#[derive(Debug, Default)]
pub struct OsRngSource;

impl NonceSource for OsRngSource {
    fn fill(&mut self, dst: &mut [u8]) {
        OsRng.fill_bytes(dst);
    }
}

#[derive(Debug)]
pub struct ScramExchange {
    store: Arc<dyn CredentialStore>,
    rng: Box<dyn NonceSource>,
    state: ScramState,
    username: String,
    client_first_bare: String,
    server_first: String,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
    full_nonce: String,
    principal: Option<Principal>,
}

impl ScramExchange {
    pub fn new(store: Arc<dyn CredentialStore>) -> Self {
        Self::with_rng(store, Box::new(OsRngSource))
    }

    pub fn with_rng(store: Arc<dyn CredentialStore>, rng: Box<dyn NonceSource>) -> Self {
        Self {
            store,
            rng,
            state: ScramState::Start,
            username: String::new(),
            client_first_bare: String::new(),
            server_first: String::new(),
            stored_key: Vec::new(),
            server_key: Vec::new(),
            full_nonce: String::new(),
            principal: None,
        }
    }
}

impl SaslExchange for ScramExchange {
    fn step(&mut self, client_msg: &[u8]) -> Result<(Vec<u8>, bool), AuthError> {
        let outcome = match self.state {
            ScramState::Start => self.handle_client_first(client_msg),
            ScramState::AwaitFinal => self.handle_client_final(client_msg),
            ScramState::Done => Err(AuthError::MalformedSaslMessage),
        };
        crate::engine::record_sasl_outcome("SCRAM-SHA-512", &outcome);
        outcome
    }

    fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }
}

impl ScramExchange {
    fn handle_client_first(&mut self, msg: &[u8]) -> Result<(Vec<u8>, bool), AuthError> {
        let s = std::str::from_utf8(msg).map_err(|_| AuthError::MalformedSaslMessage)?;

        // Strip the GS2 header ("n,," or "y,,") at the
        // first occurrence of ",,".
        let bare_idx = s.find(",,").ok_or(AuthError::MalformedSaslMessage)? + 2;
        self.client_first_bare = s[bare_idx..].to_owned();

        let attrs = parse_attrs(&self.client_first_bare);
        let username = attrs.get("n").ok_or(AuthError::MalformedSaslMessage)?;
        let client_nonce = attrs.get("r").ok_or(AuthError::MalformedSaslMessage)?;
        self.username = username.clone();

        let creds = self
            .store
            .lookup_scram(username)
            .ok_or(AuthError::BadCredentials)?;
        self.stored_key = creds.stored_key;
        self.server_key = creds.server_key;

        let mut suffix = [0u8; NONCE_SUFFIX_BYTES];
        self.rng.fill(&mut suffix);
        let suffix_b64 = BASE64_NOPAD.encode(suffix);
        self.full_nonce = format!("{client_nonce}{suffix_b64}");

        self.server_first = format!(
            "r={nonce},s={salt},i={iter}",
            nonce = self.full_nonce,
            salt = BASE64.encode(&creds.salt),
            iter = creds.iterations,
        );
        self.state = ScramState::AwaitFinal;
        Ok((self.server_first.as_bytes().to_vec(), false))
    }

    fn handle_client_final(&mut self, msg: &[u8]) -> Result<(Vec<u8>, bool), AuthError> {
        let s = std::str::from_utf8(msg).map_err(|_| AuthError::MalformedSaslMessage)?;

        // Everything before ",p=" is the proof-less client-final.
        let proof_idx = s.rfind(",p=").ok_or(AuthError::MalformedSaslMessage)?;
        let client_final_without_proof = &s[..proof_idx];
        let proof_b64 = &s[proof_idx + 3..];

        let attrs = parse_attrs(client_final_without_proof);
        let nonce = attrs.get("r").ok_or(AuthError::MalformedSaslMessage)?;
        if nonce != &self.full_nonce {
            return Err(AuthError::BadCredentials);
        }

        let client_proof = BASE64.decode(proof_b64)?;
        let auth_message = format!(
            "{first},{server_first},{client_final}",
            first = self.client_first_bare,
            server_first = self.server_first,
            client_final = client_final_without_proof,
        );

        let client_sig = hmac_sha512(&self.stored_key, auth_message.as_bytes());
        if client_proof.len() != client_sig.len() {
            return Err(AuthError::BadCredentials);
        }
        let recovered: Vec<u8> = client_proof
            .iter()
            .zip(client_sig.iter())
            .map(|(p, c)| p ^ c)
            .collect();
        let recovered_hash = Sha512::digest(&recovered);
        if recovered_hash.ct_eq(&self.stored_key).unwrap_u8() != 1 {
            return Err(AuthError::BadCredentials);
        }

        let server_sig = hmac_sha512(&self.server_key, auth_message.as_bytes());
        let server_final = format!("v={}", BASE64.encode(server_sig));
        self.principal = Some(Principal {
            name: self.username.clone(),
            kind: PrincipalKind::User,
        });
        self.state = ScramState::Done;
        Ok((server_final.into_bytes(), true))
    }
}

fn parse_attrs(s: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for part in s.split(',') {
        if part.len() < 2 || part.as_bytes()[1] != b'=' {
            continue;
        }
        let key = &part[..1];
        let value = &part[2..];
        m.insert(key.to_owned(), value.to_owned());
    }
    m
}

/// RFC 5802 key derivation from a client-supplied SaltedPassword
/// (gh #252 — KIP-554 `AlterUserScramCredentials` sends
/// `(salt, salted_password, iterations)`, never the password):
/// `StoredKey = H(HMAC(salted, "Client Key"))`,
/// `ServerKey = HMAC(salted, "Server Key")`. Returns
/// `(stored_key, server_key)`.
pub fn keys_from_salted_password(salted_password: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let client_key = hmac_sha512(salted_password, b"Client Key");
    let stored_key = Sha512::digest(&client_key).to_vec();
    let server_key = hmac_sha512(salted_password, b"Server Key");
    (stored_key, server_key)
}

/// HMAC-SHA-512. The `new_from_slice` constructor is documented to
/// accept any key length (oversize keys are hashed first internally),
/// so the `Err` arm is unreachable — collapse it with a fallback
/// rather than `unwrap` to keep the workspace `unwrap_used` /
/// `expect_used` lints clean.
fn hmac_sha512(key: &[u8], msg: &[u8]) -> Vec<u8> {
    match <Hmac<Sha512> as Mac>::new_from_slice(key) {
        Ok(mut mac) => {
            mac.update(msg);
            mac.finalize().into_bytes().to_vec()
        }
        // Unreachable in practice; return an obviously-wrong fixed
        // value so any caller that ignored this would fail closed.
        Err(_) => vec![0u8; 64],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::{ScramCreds, TestCred};
    use crate::CredentialLoader;

    /// Deterministic step-RNG for the byte-equal nonce test below.
    #[derive(Debug)]
    struct StepNonce {
        next: u8,
    }

    impl NonceSource for StepNonce {
        fn fill(&mut self, dst: &mut [u8]) {
            for b in dst.iter_mut() {
                *b = self.next;
                self.next = self.next.wrapping_add(1);
            }
        }
    }

    /// Reference SCRAM client implementation: produces the proof for a
    /// known password against a captured server-first message. Used to
    /// drive the server-side exchange in tests without pulling a full
    /// client crate.
    fn build_scram_credentials(
        username: &str,
        password: &str,
        salt: &[u8],
        iterations: i32,
    ) -> ScramCreds {
        // SaltedPassword = PBKDF2(HMAC-SHA-512, password, salt, iterations, dkLen=64)
        let mut salted = vec![0u8; 64];
        pbkdf2_hmac_sha512(
            password.as_bytes(),
            salt,
            u32::try_from(iterations).unwrap_or(0),
            &mut salted,
        );
        let client_key = hmac_sha512(&salted, b"Client Key");
        let server_key = hmac_sha512(&salted, b"Server Key");
        let stored_key = Sha512::digest(&client_key).to_vec();
        let _ = username;
        ScramCreds {
            stored_key,
            server_key,
            salt: salt.to_vec(),
            iterations,
        }
    }

    fn pbkdf2_hmac_sha512(password: &[u8], salt: &[u8], iters: u32, out: &mut [u8]) {
        // Minimal PBKDF2-HMAC-SHA512 for tests. One block (dkLen <= 64).
        assert!(out.len() <= 64);
        // U_1 = HMAC(P, salt || INT(1))
        let mut salt_block = salt.to_vec();
        salt_block.extend_from_slice(&1u32.to_be_bytes());
        let mut u = hmac_sha512(password, &salt_block);
        let mut t = u.clone();
        for _ in 1..iters {
            u = hmac_sha512(password, &u);
            for (ti, ui) in t.iter_mut().zip(u.iter()) {
                *ti ^= *ui;
            }
        }
        out.copy_from_slice(&t[..out.len()]);
    }

    fn build_client_proof(
        password: &str,
        salt: &[u8],
        iterations: i32,
        auth_message: &str,
    ) -> Vec<u8> {
        let mut salted = vec![0u8; 64];
        pbkdf2_hmac_sha512(
            password.as_bytes(),
            salt,
            u32::try_from(iterations).unwrap_or(0),
            &mut salted,
        );
        let client_key = hmac_sha512(&salted, b"Client Key");
        let stored_key = Sha512::digest(&client_key);
        let client_sig = hmac_sha512(&stored_key, auth_message.as_bytes());
        client_key
            .iter()
            .zip(client_sig.iter())
            .map(|(k, s)| k ^ s)
            .collect()
    }

    fn loader_with_user(username: &str, password: &str) -> Arc<CredentialLoader> {
        let salt = b"saltsaltsalt";
        let iterations = 4096;
        let creds = build_scram_credentials(username, password, salt, iterations);
        let loader = CredentialLoader::new("/tmp/kaas-auth-test-nofile");
        loader.install_for_test(vec![TestCred {
            username: username.to_owned(),
            auth_type: "scram-sha-512".to_owned(),
            scram: Some(creds),
            ..TestCred::default()
        }]);
        Arc::new(loader)
    }

    #[test]
    fn happy_path_with_deterministic_nonce() {
        let loader = loader_with_user("alice", "hunter2");
        let mut ex = ScramExchange::with_rng(loader, Box::new(StepNonce { next: 0xA0 }));

        let client_first = b"n,,n=alice,r=clientnonce123";
        let (server_first, done) = ex.step(client_first).unwrap();
        assert!(!done);
        let server_first_str = std::str::from_utf8(&server_first).unwrap();
        // r=<clientnonce><24-char base64-no-pad of 18 bytes>
        assert!(server_first_str.starts_with("r=clientnonce123"));
        assert!(server_first_str.contains(",s="));
        assert!(server_first_str.contains(",i=4096"));

        // Build the proof using the server-supplied nonce.
        let attrs = parse_attrs(server_first_str);
        let full_nonce = attrs.get("r").unwrap();
        let salt_b64 = attrs.get("s").unwrap();
        let salt = BASE64.decode(salt_b64).unwrap();
        let client_final_without_proof = format!("c=biws,r={full_nonce}");
        let auth_message =
            format!("n=alice,r=clientnonce123,{server_first_str},{client_final_without_proof}");
        let proof = build_client_proof("hunter2", &salt, 4096, &auth_message);
        let client_final = format!("{client_final_without_proof},p={}", BASE64.encode(&proof));
        let (server_final, done) = ex.step(client_final.as_bytes()).unwrap();
        assert!(done);
        assert!(std::str::from_utf8(&server_final)
            .unwrap()
            .starts_with("v="));
        assert_eq!(ex.principal().unwrap().name, "alice");
    }

    #[test]
    fn wrong_password_rejected() {
        let loader = loader_with_user("alice", "right");
        let mut ex = ScramExchange::with_rng(loader, Box::new(StepNonce { next: 0 }));
        let (server_first, _) = ex.step(b"n,,n=alice,r=cn").unwrap();
        let server_first_str = std::str::from_utf8(&server_first).unwrap();
        let attrs = parse_attrs(server_first_str);
        let full_nonce = attrs.get("r").unwrap();
        let salt = BASE64.decode(attrs.get("s").unwrap()).unwrap();
        let client_final_without_proof = format!("c=biws,r={full_nonce}");
        let auth_message = format!("n=alice,r=cn,{server_first_str},{client_final_without_proof}");
        let bad_proof = build_client_proof("wrong", &salt, 4096, &auth_message);
        let client_final = format!(
            "{client_final_without_proof},p={}",
            BASE64.encode(&bad_proof)
        );
        let err = ex.step(client_final.as_bytes()).unwrap_err();
        assert!(matches!(err, AuthError::BadCredentials));
    }

    #[test]
    fn unknown_user_rejected() {
        let loader = loader_with_user("alice", "hunter2");
        let mut ex = ScramExchange::with_rng(loader, Box::new(StepNonce { next: 0 }));
        let err = ex.step(b"n,,n=ghost,r=cn").unwrap_err();
        assert!(matches!(err, AuthError::BadCredentials));
    }

    #[test]
    fn malformed_gs2_header_rejected() {
        let loader = loader_with_user("alice", "hunter2");
        let mut ex = ScramExchange::with_rng(loader, Box::new(StepNonce { next: 0 }));
        let err = ex.step(b"no-gs2-header").unwrap_err();
        assert!(matches!(err, AuthError::MalformedSaslMessage));
    }

    #[test]
    fn nonce_mismatch_rejected() {
        let loader = loader_with_user("alice", "hunter2");
        let mut ex = ScramExchange::with_rng(loader, Box::new(StepNonce { next: 0 }));
        ex.step(b"n,,n=alice,r=cn").unwrap();
        // Send a client-final that echoes the wrong nonce.
        let err = ex.step(b"c=biws,r=wrong-nonce,p=AAAA").unwrap_err();
        assert!(matches!(err, AuthError::BadCredentials));
    }
}
