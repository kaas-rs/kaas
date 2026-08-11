//! `AuthEngine` — per-listener authentication surface.
//!
//! Authentication and authorization are split (gh #126). The engine
//! handles SASL handshake mechanics and mTLS principal extraction;
//! cluster-wide authorization runs through [`crate::Authorizer`].
//!
//! Two impls ship in Phase 4:
//!
//! - [`AllowAllAuthEngine`] — anonymous listener; SASL exchange
//!   completes immediately as `User:ANONYMOUS`; `requires_pre_auth`
//!   returns `false`.
//! - [`RealAuthEngine`] — backed by a [`CredentialStore`] +
//!   [`PrincipalMapper`]; provides SCRAM-SHA-512, PLAIN-static, and
//!   mTLS principal lookup; `requires_pre_auth` returns `true`.

use std::sync::Arc;

use crate::credentials::CredentialStore;
use crate::errors::AuthError;
use crate::plain::PlainExchange;
use crate::principal_mapping::PrincipalMapper;
use crate::scram::ScramExchange;
use crate::types::{Principal, PrincipalKind};

/// Server side of one SASL mechanism's state machine. Created once
/// per connection after the `SaslHandshake`; `step` runs for each
/// `SaslAuthenticate` message until `done = true`, at which point
/// `principal()` is valid.
pub trait SaslExchange: Send + std::fmt::Debug {
    /// Process the next client payload. Returns `(server_payload,
    /// done)`. When `done = true`, `principal()` is set.
    fn step(&mut self, client_msg: &[u8]) -> Result<(Vec<u8>, bool), AuthError>;

    /// Authenticated principal (only valid after `done = true`).
    fn principal(&self) -> Option<&Principal>;

    /// KIP-368: how long the authenticated session may live before
    /// the client must re-authenticate, in milliseconds. `0` = no
    /// re-authentication demanded (the default for every mechanism
    /// except OAUTHBEARER with `maxSecondsWithoutReauthentication`
    /// set). Only meaningful once `done = true`.
    fn session_lifetime_ms(&self) -> i64 {
        0
    }
}

pub trait AuthEngine: Send + Sync + std::fmt::Debug + 'static {
    /// Build a fresh SASL exchange for the chosen mechanism. Unknown
    /// mechanism → [`AuthError::UnknownMechanism`].
    fn new_sasl_exchange(&self, mechanism: &str) -> Result<Box<dyn SaslExchange>, AuthError>;

    /// Resolve a TLS subject CN (already passed through the principal
    /// mapper) to a `Principal`. Used by the mTLS path in the server.
    fn authenticate_tls(&self, cn: &str) -> Result<Principal, AuthError>;

    /// Whether the dispatcher must reject non-pre-SASL APIs on this
    /// listener until SASL completes. `AllowAllAuthEngine` returns
    /// `false`; `RealAuthEngine` returns `true`.
    fn requires_pre_auth(&self) -> bool;

    /// SASL mechanisms this engine serves, in `SaslHandshake`
    /// advertisement order (clients pick the first match). The
    /// default is the credential-store pair; `OauthEngine` overrides
    /// with `["OAUTHBEARER"]` so an oauth listener never advertises a
    /// mechanism it would reject at authenticate time.
    fn mechanisms(&self) -> &'static [&'static str] {
        &["SCRAM-SHA-512", "PLAIN"]
    }
}

#[derive(Debug)]
pub struct AllowAllAuthEngine;

impl AuthEngine for AllowAllAuthEngine {
    fn new_sasl_exchange(&self, _mechanism: &str) -> Result<Box<dyn SaslExchange>, AuthError> {
        Ok(Box::new(AnonymousExchange::default()))
    }

    fn authenticate_tls(&self, _cn: &str) -> Result<Principal, AuthError> {
        // Anonymous listener with TLS but no client-cert authentication
        // simply maps every connection to the anonymous principal.
        Ok(Principal::anonymous())
    }

    fn requires_pre_auth(&self) -> bool {
        false
    }
}

#[derive(Debug, Default)]
struct AnonymousExchange {
    done: bool,
    principal: Option<Principal>,
}

impl SaslExchange for AnonymousExchange {
    fn step(&mut self, _client_msg: &[u8]) -> Result<(Vec<u8>, bool), AuthError> {
        // One-shot completion: ignore whatever the client sent, stamp
        // anonymous, return empty challenge + done.
        self.done = true;
        self.principal = Some(Principal::anonymous());
        Ok((Vec::new(), true))
    }

    fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }
}

#[derive(Debug)]
pub struct RealAuthEngine {
    creds: Arc<dyn CredentialStore>,
    mapper: Arc<PrincipalMapper>,
}

impl RealAuthEngine {
    pub fn new(creds: Arc<dyn CredentialStore>, mapper: Arc<PrincipalMapper>) -> Self {
        Self { creds, mapper }
    }

    pub fn credentials(&self) -> &Arc<dyn CredentialStore> {
        &self.creds
    }

    pub fn principal_mapper(&self) -> &Arc<PrincipalMapper> {
        &self.mapper
    }
}

impl AuthEngine for RealAuthEngine {
    fn new_sasl_exchange(&self, mechanism: &str) -> Result<Box<dyn SaslExchange>, AuthError> {
        match mechanism {
            "SCRAM-SHA-512" => Ok(Box::new(ScramExchange::new(self.creds.clone()))),
            "PLAIN" => Ok(Box::new(PlainExchange::new(self.creds.clone()))),
            other => Err(AuthError::UnknownMechanism(other.to_owned())),
        }
    }

    fn authenticate_tls(&self, cn: &str) -> Result<Principal, AuthError> {
        let out = match self.creds.lookup_tls(cn) {
            Some(username) => Ok(Principal {
                name: username,
                kind: PrincipalKind::User,
            }),
            None => Err(AuthError::BadCertificate),
        };
        record_auth_outcome("mtls", out.is_ok());
        out
    }

    fn requires_pre_auth(&self) -> bool {
        true
    }
}

/// Bump `kaas.auth.success` / `kaas.auth.failure` on any
/// authentication attempt. Used by the SASL exchanges and mTLS
/// path — one call site per completed authentication decision.
pub(crate) fn record_auth_outcome(mechanism: &str, ok: bool) {
    let m = kaas_observability::metrics::global();
    let labels = [kaas_observability::KeyValue::new(
        "mechanism",
        mechanism.to_string(),
    )];
    if ok {
        m.auth_success.add(1, &labels);
    } else {
        m.auth_failure.add(1, &labels);
    }
}

/// Instrument a SASL `step()` outcome. Only bumps a counter when the
/// exchange terminates — the SCRAM two-round-trip path calls this
/// twice (server_first → server_final); the first is `Ok((_, false))`
/// and skipped, the second is `Ok((_, true))` and counted as success.
pub(crate) fn record_sasl_outcome(mechanism: &str, outcome: &Result<(Vec<u8>, bool), AuthError>) {
    match outcome {
        Ok((_, true)) => record_auth_outcome(mechanism, true),
        Ok(_) => {}
        Err(_) => record_auth_outcome(mechanism, false),
    }
}
