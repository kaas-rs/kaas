//! `AuthError` — the shared error type for every kaas-auth surface.

use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("auth: unknown SASL mechanism {0:?}")]
    UnknownMechanism(String),

    #[error("auth: malformed SASL message")]
    MalformedSaslMessage,

    #[error("auth: bad credentials")]
    BadCredentials,

    #[error("auth: bad certificate")]
    BadCertificate,

    /// OAUTHBEARER token rejection. The reason is deliberately coarse
    /// — it is client-visible via `SaslAuthenticate.error_message`,
    /// so it names the check that failed, never key material or
    /// issuer internals.
    #[error("auth: invalid token: {0}")]
    InvalidToken(String),

    #[error("auth: io: {0}")]
    Io(#[from] io::Error),

    #[error("auth: json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("auth: regex: {0}")]
    Regex(#[from] regex::Error),

    #[error("auth: base64: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error("auth: principal-mapping parse: {0}")]
    PrincipalMappingParse(String),

    #[error("auth: x509: {0}")]
    X509(String),
}
