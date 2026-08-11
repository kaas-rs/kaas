//! kaas-auth — SCRAM-SHA-512 + SASL PLAIN, mTLS, ACLs, quotas, principal mapping.
//!
//! Module layout:
//!
//! - [`credentials`] — Strimzi-shape `credentials.json` loader +
//!   `CredentialStore` trait.
//! - [`acls`] — `acls.json` loader + `AclEngine` (deny-overrides-allow).
//! - [`authorizer`] — `Authorizer` trait + `AllowAllAuthorizer` +
//!   `SuperUserAuthorizer` wrapper.
//! - [`scram`] — SCRAM-SHA-512 server state machine.
//! - [`plain`] — SASL PLAIN (static-credential path; K8s SA is Phase 7).
//! - [`oauth`] — SASL/OAUTHBEARER + OIDC JWT validation against a
//!   hot-swapped JWKS (gh #42; the fetch loop lives in `bins/kaas`).
//! - [`mtls`] — peer-cert principal extraction.
//! - [`principal_mapping`] — `ssl.principal.mapping.rules` parser
//!   (gh #43, KIP-371).
//! - [`quota`] — token-bucket quotas with debt-carry (gh #125).
//! - [`engine`] — `AuthEngine` trait + `AllowAllAuthEngine` +
//!   `RealAuthEngine`.
//! - [`selector`] — per-listener `AuthEngineSelector`.
//! - [`types`] — `Principal`, `Resource`, `Operation`, `Quotas`.
//! - [`errors`] — `AuthError`.

pub mod acls;
pub mod authorizer;
pub mod credentials;
pub mod engine;
pub mod errors;
pub mod mtls;
pub mod oauth;
pub mod plain;
pub mod principal_mapping;
pub mod quota;
pub mod scram;
pub mod selector;
pub mod types;

pub use acls::AclEngine;
pub use authorizer::{AllowAllAuthorizer, Authorizer, SuperUserAuthorizer};
pub use credentials::{CredentialLoader, CredentialStore, ScramInfo};
pub use engine::{AllowAllAuthEngine, AuthEngine, RealAuthEngine, SaslExchange};
pub use errors::AuthError;
pub use mtls::extract_principal as extract_mtls_principal;
pub use oauth::{OauthConfig, OauthEngine, OauthValidator};
pub use plain::PlainExchange;
pub use principal_mapping::PrincipalMapper;
pub use quota::{Clock, NoQuotaChecker, QuotaChecker, QuotaEnforcer, RealClock};
pub use scram::ScramExchange;
pub use selector::{AuthEngineSelector, PerListenerAuthEngine, SingleAuthEngine};
pub use types::{Operation, PatternType, Principal, PrincipalKind, Quotas, Resource, ResourceKind};
