//! SASL/OAUTHBEARER (RFC 7628) — OIDC bearer-token listener auth (gh #42).
//!
//! Strimzi-style `authentication.type: oauth`: the client presents an
//! OAuth 2 access token (a JWT) minted by an external issuer; the
//! broker validates it locally against the issuer's JWKS — signature,
//! `exp`/`nbf`, `iss`, optionally `aud` — and derives the principal
//! from a configurable claim (default `sub`). No introspection
//! endpoint, no client secret on the broker: fast local validation,
//! same as Strimzi's `oauth.jwks.endpoint.uri` path.
//!
//! Split of responsibilities, mirroring the credentials/ACL
//! hot-reload pattern: this module owns *parsing and verification*
//! and is pure-sync (the [`crate::engine::SaslExchange::step`] call
//! sits on the request path); the JWKS **fetch loop** lives in
//! `bins/kaas` and pushes fresh key material in via
//! [`OauthValidator::install_jwks`]. Until the first successful
//! install every token is rejected — the failure mode of an
//! unreachable issuer is "clients can't authenticate", never "clients
//! skip validation".
//!
//! Four wire details worth naming (they are the interop surface):
//!
//! * The initial client response is `n,,` `%x01` `auth=Bearer <tok>`
//!   `%x01` … `%x01%x01` — the separators are the format (see
//!   KAFKA-7182 for what shipping them wrong looks like).
//! * A rejected token is a **two-step** failure: the server answers
//!   with an RFC 7628 §3.2.2 JSON document as a challenge, the client
//!   acks with a single `%x01`, and only then does the exchange fail
//!   with `SASL_AUTHENTICATION_FAILED` (58). Failing on the first
//!   round trip hangs clients that wait for the ack window.
//! * A non-empty gs2 `authzid` must equal the token's own principal
//!   (Apache rejects a mismatch; so do we).
//! * `alg` is an allowlist (`RS256/RS384/RS512/ES256`), never read
//!   from the token to pick semantics — `none` and the HMAC family
//!   are rejected outright, which is what kills the classic
//!   alg-confusion attack (an HS256 token "signed" with the public
//!   JWKS bytes).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use ring::signature;

use crate::engine::{record_sasl_outcome, AuthEngine, SaslExchange};
use crate::errors::AuthError;
use crate::types::{Principal, PrincipalKind};

/// Clock-skew allowance for `exp` / `nbf`, in seconds. Matches the
/// Java client stack's default tolerance.
const CLOCK_SKEW_SECS: i64 = 60;

/// RFC 7628 §3.2.2 failure body. Deliberately static and content-free:
/// the *reason* goes to the broker log, not to the unauthenticated
/// peer. Same body Apache's `OAuthBearerSaslServer` sends.
const FAILURE_CHALLENGE: &[u8] = b"{\"status\":\"invalid_token\"}";

/// Per-listener OAuth validation config. Field names mirror Strimzi's
/// `KafkaListenerAuthenticationOAuth` 1:1 (the chart passes them
/// through `KAAS_LISTENERS` verbatim).
#[derive(Debug, Clone)]
pub struct OauthConfig {
    /// Exact-match `iss` claim. Always checked.
    pub valid_issuer_uri: String,
    /// Where the fetch loop in `bins/kaas` pulls key material from.
    pub jwks_endpoint_uri: String,
    /// Claim that becomes `Principal.name`. Default `sub`.
    pub user_name_claim: Option<String>,
    /// Tried when `user_name_claim` is absent from the token.
    pub fallback_user_name_claim: Option<String>,
    /// When `true`, the token's `aud` must contain `client_id`.
    pub check_audience: bool,
    /// The audience value `check_audience` looks for.
    pub client_id: Option<String>,
    /// JWKS re-fetch interval for the loop in `bins/kaas`.
    pub jwks_refresh_seconds: u64,
    /// KIP-368: when set, a successful authentication advertises
    /// `session_lifetime_ms = min(this, token remaining lifetime)`
    /// and the dispatcher enforces the deadline. `None` = sessions
    /// outlive their token (Apache's own default with
    /// `connections.max.reauth.ms=0`).
    pub max_seconds_without_reauthentication: Option<u64>,
}

/// One JWKS key, pre-decoded into what `ring` verifies with.
enum VerifyKey {
    Rsa {
        kid: Option<String>,
        n: Vec<u8>,
        e: Vec<u8>,
    },
    /// Uncompressed SEC1 point (`0x04 || x || y`), P-256.
    P256 { kid: Option<String>, point: Vec<u8> },
}

impl VerifyKey {
    fn kid(&self) -> Option<&str> {
        match self {
            VerifyKey::Rsa { kid, .. } | VerifyKey::P256 { kid, .. } => kid.as_deref(),
        }
    }
}

impl std::fmt::Debug for VerifyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Public key material only, but the moduli are long and
        // useless in logs — print kind + kid.
        match self {
            VerifyKey::Rsa { kid, .. } => write!(f, "Rsa(kid={kid:?})"),
            VerifyKey::P256 { kid, .. } => write!(f, "P256(kid={kid:?})"),
        }
    }
}

/// Outcome of a successful validation.
#[derive(Debug, Clone)]
pub struct ValidatedToken {
    pub principal: String,
    pub exp_unix: i64,
}

/// JWT validator + hot-swapped JWKS key set for one oauth listener.
#[derive(Debug)]
pub struct OauthValidator {
    cfg: OauthConfig,
    keys: ArcSwap<Vec<VerifyKey>>,
    /// Set on a kid-miss so the fetch loop can re-pull ahead of its
    /// scheduled interval (issuer key rotation).
    refresh_hint: AtomicBool,
}

impl OauthValidator {
    pub fn new(cfg: OauthConfig) -> Self {
        Self {
            cfg,
            keys: ArcSwap::from_pointee(Vec::new()),
            refresh_hint: AtomicBool::new(false),
        }
    }

    pub fn config(&self) -> &OauthConfig {
        &self.cfg
    }

    /// Parse a JWKS document and swap it in. Unusable keys (unknown
    /// `kty`, `use != sig`, undecodable material) are skipped, not
    /// fatal — issuers routinely mix signing and encryption keys in
    /// one document. Returns how many keys were installed.
    pub fn install_jwks(&self, jwks_json: &str) -> Result<usize, AuthError> {
        let doc: serde_json::Value = serde_json::from_str(jwks_json)?;
        let raw_keys = doc
            .get("keys")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| AuthError::InvalidToken("JWKS document has no `keys` array".into()))?;

        let mut keys = Vec::new();
        for k in raw_keys {
            if let Some(parsed) = parse_jwk(k) {
                keys.push(parsed);
            }
        }
        let n = keys.len();
        self.keys.store(Arc::new(keys));
        Ok(n)
    }

    pub fn has_keys(&self) -> bool {
        !self.keys.load().is_empty()
    }

    /// True once per kid-miss burst; the fetch loop calls this to
    /// decide whether to re-pull early.
    pub fn take_refresh_hint(&self) -> bool {
        self.refresh_hint.swap(false, Ordering::Relaxed)
    }

    /// Validate a compact-serialized JWT. `now_unix` is a parameter
    /// (not sampled here) so tests pin the clock.
    pub fn validate(&self, token: &str, now_unix: i64) -> Result<ValidatedToken, AuthError> {
        let mut parts = token.split('.');
        let (Some(h), Some(p), Some(s), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(reject("token is not a three-part compact JWT"));
        };
        let header_bytes = URL_SAFE_NO_PAD
            .decode(h)
            .map_err(|_| reject("token header is not base64url"))?;
        let payload_bytes = URL_SAFE_NO_PAD
            .decode(p)
            .map_err(|_| reject("token payload is not base64url"))?;
        let sig = URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|_| reject("token signature is not base64url"))?;

        let header: serde_json::Value = serde_json::from_slice(&header_bytes)
            .map_err(|_| reject("token header is not JSON"))?;
        let alg = header
            .get("alg")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| reject("token header has no alg"))?;
        let kid = header.get("kid").and_then(serde_json::Value::as_str);

        // Signed portion is the raw base64 text, not the decoded bytes.
        let msg_len = h.len() + 1 + p.len();
        let msg = token
            .as_bytes()
            .get(..msg_len)
            .ok_or_else(|| reject("token framing error"))?;

        if !self.verify_signature(alg, kid, msg, &sig)? {
            return Err(reject("token signature verification failed"));
        }

        let claims: serde_json::Value = serde_json::from_slice(&payload_bytes)
            .map_err(|_| reject("token payload is not JSON"))?;
        self.validate_claims(&claims, now_unix)
    }

    /// Signature check against the current key set. `Ok(false)` =
    /// tried and failed; `Err` = structurally impossible (unsupported
    /// alg, no candidate keys).
    fn verify_signature(
        &self,
        alg: &str,
        kid: Option<&str>,
        msg: &[u8],
        sig: &[u8],
    ) -> Result<bool, AuthError> {
        let rsa_params: Option<&'static signature::RsaParameters> = match alg {
            "RS256" => Some(&signature::RSA_PKCS1_2048_8192_SHA256),
            "RS384" => Some(&signature::RSA_PKCS1_2048_8192_SHA384),
            "RS512" => Some(&signature::RSA_PKCS1_2048_8192_SHA512),
            "ES256" => None,
            _ => {
                // Allowlist, and only asymmetric algorithms. `none` and
                // HS* land here by design.
                return Err(reject(
                    "token alg is not in the RS256/RS384/RS512/ES256 allowlist",
                ));
            }
        };

        let keys = self.keys.load();
        if keys.is_empty() {
            self.refresh_hint.store(true, Ordering::Relaxed);
            return Err(reject("no JWKS keys loaded yet"));
        }

        let mut candidates = 0usize;
        for key in keys.iter() {
            // kid-pinning: a token that names a key only matches that
            // key; a token without kid tries every key of its family.
            if let Some(want) = kid {
                if key.kid() != Some(want) {
                    continue;
                }
            }
            let ok = match (key, rsa_params) {
                (VerifyKey::Rsa { n, e, .. }, Some(params)) => {
                    candidates += 1;
                    signature::RsaPublicKeyComponents { n, e }
                        .verify(params, msg, sig)
                        .is_ok()
                }
                (VerifyKey::P256 { point, .. }, None) => {
                    candidates += 1;
                    signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, point)
                        .verify(msg, sig)
                        .is_ok()
                }
                // Family mismatch (RSA token vs EC key or vice versa).
                _ => false,
            };
            if ok {
                return Ok(true);
            }
        }

        if candidates == 0 {
            // Unknown kid → likely key rotation; wave the fetch loop.
            self.refresh_hint.store(true, Ordering::Relaxed);
            return Err(reject("token kid matches no loaded JWKS key"));
        }
        Ok(false)
    }

    fn validate_claims(
        &self,
        claims: &serde_json::Value,
        now_unix: i64,
    ) -> Result<ValidatedToken, AuthError> {
        let exp = claims
            .get("exp")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| reject("token has no exp claim"))?;
        if now_unix - CLOCK_SKEW_SECS >= exp {
            return Err(reject("token is expired"));
        }
        if let Some(nbf) = claims.get("nbf").and_then(serde_json::Value::as_i64) {
            if now_unix + CLOCK_SKEW_SECS < nbf {
                return Err(reject("token is not valid yet (nbf)"));
            }
        }

        let iss = claims
            .get("iss")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| reject("token has no iss claim"))?;
        if iss != self.cfg.valid_issuer_uri {
            return Err(reject("token issuer does not match validIssuerUri"));
        }

        if self.cfg.check_audience {
            let want = self
                .cfg
                .client_id
                .as_deref()
                .ok_or_else(|| reject("checkAudience is set but clientId is not"))?;
            let aud_ok = match claims.get("aud") {
                Some(serde_json::Value::String(a)) => a == want,
                Some(serde_json::Value::Array(list)) => list
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .any(|a| a == want),
                _ => false,
            };
            if !aud_ok {
                return Err(reject("token aud does not contain the configured clientId"));
            }
        }

        let primary = self.cfg.user_name_claim.as_deref().unwrap_or("sub");
        let name = claims
            .get(primary)
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                self.cfg
                    .fallback_user_name_claim
                    .as_deref()
                    .and_then(|c| claims.get(c))
                    .and_then(serde_json::Value::as_str)
            })
            .ok_or_else(|| reject("token carries no usable user-name claim"))?;
        if name.is_empty() {
            return Err(reject("token user-name claim is empty"));
        }

        Ok(ValidatedToken {
            principal: name.to_owned(),
            exp_unix: exp,
        })
    }
}

fn reject(reason: &str) -> AuthError {
    AuthError::InvalidToken(reason.to_owned())
}

/// Parse one JWK object. `None` = unusable (skipped), with the reason
/// traced at debug — a JWKS mixing `enc` keys in is normal, not an
/// error.
fn parse_jwk(k: &serde_json::Value) -> Option<VerifyKey> {
    let use_ok = k
        .get("use")
        .and_then(serde_json::Value::as_str)
        .map(|u| u == "sig")
        .unwrap_or(true);
    if !use_ok {
        return None;
    }
    let kid = k
        .get("kid")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let field = |name: &str| -> Option<Vec<u8>> {
        k.get(name)
            .and_then(serde_json::Value::as_str)
            .and_then(|v| URL_SAFE_NO_PAD.decode(v).ok())
    };
    match k.get("kty").and_then(serde_json::Value::as_str) {
        Some("RSA") => {
            let n = field("n")?;
            let e = field("e")?;
            Some(VerifyKey::Rsa { kid, n, e })
        }
        Some("EC") => {
            if k.get("crv").and_then(serde_json::Value::as_str) != Some("P-256") {
                return None;
            }
            let x = field("x")?;
            let y = field("y")?;
            if x.len() > 32 || y.len() > 32 {
                return None;
            }
            // Left-pad to 32 bytes each; SEC1 uncompressed form.
            let mut point = Vec::with_capacity(65);
            point.push(0x04);
            point.extend(std::iter::repeat_n(0u8, 32 - x.len()));
            point.extend_from_slice(&x);
            point.extend(std::iter::repeat_n(0u8, 32 - y.len()));
            point.extend_from_slice(&y);
            Some(VerifyKey::P256 { kid, point })
        }
        _ => None,
    }
}

/// Parsed RFC 7628 §3.1 initial client response.
#[derive(Debug, PartialEq, Eq)]
struct InitialResponse {
    authzid: Option<String>,
    token: String,
}

/// `n,[a=authzid],` `%x01` (`key=value` `%x01`)* `%x01`
fn parse_initial_response(msg: &[u8]) -> Result<InitialResponse, AuthError> {
    let sep = msg
        .iter()
        .position(|b| *b == 0x01)
        .ok_or(AuthError::MalformedSaslMessage)?;
    let gs2 = std::str::from_utf8(&msg[..sep]).map_err(|_| AuthError::MalformedSaslMessage)?;
    let rest = &msg[sep + 1..];

    // gs2: only "no channel binding" is legal here ("y,"/"p=" rejected).
    let authzid = gs2
        .strip_prefix("n,")
        .and_then(|t| t.strip_suffix(','))
        .ok_or(AuthError::MalformedSaslMessage)
        .and_then(|mid| match mid {
            "" => Ok(None),
            _ => mid
                .strip_prefix("a=")
                .filter(|z| !z.is_empty())
                .map(|z| Some(z.to_owned()))
                .ok_or(AuthError::MalformedSaslMessage),
        })?;

    // kvpairs terminated by one extra %x01.
    if rest.last() != Some(&0x01) {
        return Err(AuthError::MalformedSaslMessage);
    }
    let mut token = None;
    for part in rest.split(|b| *b == 0x01).filter(|p| !p.is_empty()) {
        let part = std::str::from_utf8(part).map_err(|_| AuthError::MalformedSaslMessage)?;
        let (key, value) = part
            .split_once('=')
            .ok_or(AuthError::MalformedSaslMessage)?;
        if key.eq_ignore_ascii_case("auth") {
            if token.is_some() {
                // Two auth pairs — refuse to guess which one counts.
                return Err(AuthError::MalformedSaslMessage);
            }
            let (scheme, tok) = value
                .split_once(' ')
                .ok_or(AuthError::MalformedSaslMessage)?;
            if !scheme.eq_ignore_ascii_case("bearer") || tok.is_empty() {
                return Err(AuthError::MalformedSaslMessage);
            }
            token = Some(tok.to_owned());
        }
        // Other extensions (RFC 7628 SaslExtensions) are ignored.
    }
    let token = token.ok_or(AuthError::MalformedSaslMessage)?;
    Ok(InitialResponse { authzid, token })
}

#[derive(Debug)]
enum ExchangeStep {
    AwaitInitial,
    /// Token was rejected; the JSON challenge went out and we owe the
    /// client one more round trip (its `%x01` ack) before failing.
    AwaitErrorAck {
        reason: String,
    },
    Done,
}

#[derive(Debug)]
pub struct OauthBearerExchange {
    validator: Arc<OauthValidator>,
    step: ExchangeStep,
    principal: Option<Principal>,
    session_lifetime_ms: i64,
}

impl OauthBearerExchange {
    pub fn new(validator: Arc<OauthValidator>) -> Self {
        Self {
            validator,
            step: ExchangeStep::AwaitInitial,
            principal: None,
            session_lifetime_ms: 0,
        }
    }

    fn step_inner(&mut self, client_msg: &[u8]) -> Result<(Vec<u8>, bool), AuthError> {
        match std::mem::replace(&mut self.step, ExchangeStep::Done) {
            ExchangeStep::AwaitInitial => {
                let attempt = parse_initial_response(client_msg).and_then(|resp| {
                    let now = now_unix();
                    let validated = self.validator.validate(&resp.token, now)?;
                    if let Some(z) = &resp.authzid {
                        if z != &validated.principal {
                            return Err(reject("authzid does not match the token principal"));
                        }
                    }
                    Ok((validated, now))
                });
                match attempt {
                    Ok((validated, now)) => {
                        self.session_lifetime_ms = session_lifetime_ms(
                            self.validator.cfg.max_seconds_without_reauthentication,
                            validated.exp_unix,
                            now,
                        );
                        self.principal = Some(Principal {
                            name: validated.principal,
                            kind: PrincipalKind::User,
                        });
                        self.step = ExchangeStep::Done;
                        Ok((Vec::new(), true))
                    }
                    Err(err) => {
                        // Reason to the log; a fixed JSON body to the
                        // unauthenticated peer, then one ack round trip.
                        tracing::warn!(%err, "oauth: token rejected");
                        self.step = ExchangeStep::AwaitErrorAck {
                            reason: err.to_string(),
                        };
                        Ok((FAILURE_CHALLENGE.to_vec(), false))
                    }
                }
            }
            ExchangeStep::AwaitErrorAck { reason } => Err(AuthError::InvalidToken(reason)),
            ExchangeStep::Done => Err(AuthError::MalformedSaslMessage),
        }
    }
}

impl SaslExchange for OauthBearerExchange {
    fn step(&mut self, client_msg: &[u8]) -> Result<(Vec<u8>, bool), AuthError> {
        let outcome = self.step_inner(client_msg);
        record_sasl_outcome("OAUTHBEARER", &outcome);
        outcome
    }

    fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }

    fn session_lifetime_ms(&self) -> i64 {
        self.session_lifetime_ms
    }
}

/// `min(configured max, token remaining lifetime)`, in ms; 0 when no
/// max is configured (no re-auth demanded, Apache's default).
fn session_lifetime_ms(max_secs: Option<u64>, exp_unix: i64, now_unix: i64) -> i64 {
    let Some(max) = max_secs else { return 0 };
    let remaining_ms = exp_unix
        .saturating_sub(now_unix)
        .max(0)
        .saturating_mul(1000);
    let max_ms = i64::try_from(max.saturating_mul(1000)).unwrap_or(i64::MAX);
    remaining_ms.min(max_ms)
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_secs()).ok())
        .unwrap_or(0)
}

/// Per-listener engine for `authentication.type: oauth`.
#[derive(Debug)]
pub struct OauthEngine {
    validator: Arc<OauthValidator>,
}

impl OauthEngine {
    pub fn new(validator: Arc<OauthValidator>) -> Self {
        Self { validator }
    }

    pub fn validator(&self) -> &Arc<OauthValidator> {
        &self.validator
    }
}

impl AuthEngine for OauthEngine {
    fn new_sasl_exchange(&self, mechanism: &str) -> Result<Box<dyn SaslExchange>, AuthError> {
        match mechanism {
            "OAUTHBEARER" => Ok(Box::new(OauthBearerExchange::new(self.validator.clone()))),
            other => Err(AuthError::UnknownMechanism(other.to_owned())),
        }
    }

    fn authenticate_tls(&self, _cn: &str) -> Result<Principal, AuthError> {
        // An oauth listener authenticates by token, never by client
        // cert. A listener configured with both is a misconfiguration;
        // deny rather than invent a principal.
        Err(AuthError::BadCertificate)
    }

    fn requires_pre_auth(&self) -> bool {
        true
    }

    fn mechanisms(&self) -> &'static [&'static str] {
        &["OAUTHBEARER"]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};

    // Static RS256 vector: throwaway 2048-bit key generated for this
    // test alone; token exp = 4102444800 (2100-01-01).
    const RS_N: &str = "w4k8mXMn9DOSldw_j28hHWtZLCFxC31qNAzKEt7Bb084kV_z6kBf7ir19AIQ8WvsH5XWSgsdmgd1EkuBcoYbU6K7eFpMVIKt1LDgx6VLtWnbCmgymSgRpyhb3_LXtUYHFkDZymKAGj7QDDqBi582tjrHfVS_yGAl7ue3mf-d7ZrHj0qlePm_lUnyWcfbzpdxTIQHwI36SF1WemPWb4ZfMc4Vk5D67cHm7tmdBXrfVpNet1j68TQDVjhPlzmQpCUseU1u13qszn3XV60ixGHxDHKTNYWz7iqNYER90u5nc1pHMsCLtElE2MT4vWj05QVHWdHfyDu398eMvRsEZQ9pRw";
    const RS_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InRlc3QtcnMyNTYifQ.eyJpc3MiOiJodHRwczovL2lzc3Vlci50ZXN0L3YyLjAiLCJzdWIiOiJiNGJlOTcwNi0wMDAwLTRmNTItODQ4Mi02YTZlNWNjMmZmZmYiLCJhdWQiOiJhcGk6Ly9rYWFzLXRlc3QiLCJleHAiOjQxMDI0NDQ4MDAsIm5iZiI6MTAwMDAwMDAwMCwicHJlZmVycmVkX3VzZXJuYW1lIjoiY2FuYXJ5QHRlc3QifQ.EdFx4jekrMMyi8aLytl9eguSnnPab6jdHcsgmR15TP9ugkNP-_Xz2Mq3nV95EhuHObZ_4esrbnGFSdwLQUzluiR3ZSoRrxDtDn6JKkoZNjqdgXuFT8Uj2SIfqH-KPMEjfMlgfiQUVKY0VN7JgS2JVda9tRN0_OW3pcvRdZVcl1Pr8ubaeUX12ZSH1bav-AzNkeDlmT9bDygGKgHjJRYd1gRSX16AxOsxOQBJ_clB395xOY_nP6nVd9nDnqU-KvP9g7PB9nYLyZlP8PyghA5BrbKadHTGhuRv33aJq0EkcClvuwTYfYyXYzko79sA1_wj3e09_B-7J0hHXSgI8H9vSg";
    const RS_ISSUER: &str = "https://issuer.test/v2.0";
    const RS_SUB: &str = "b4be9706-0000-4f52-8482-6a6e5cc2ffff";
    /// Any instant inside the vector's validity window.
    const RS_NOW: i64 = 1_754_900_000;

    fn rs_jwks() -> String {
        format!(
            r#"{{"keys":[{{"kty":"RSA","kid":"test-rs256","use":"sig","alg":"RS256","n":"{RS_N}","e":"AQAB"}}]}}"#
        )
    }

    fn cfg(issuer: &str) -> OauthConfig {
        OauthConfig {
            valid_issuer_uri: issuer.to_owned(),
            jwks_endpoint_uri: "http://unused.test/jwks".to_owned(),
            user_name_claim: None,
            fallback_user_name_claim: None,
            check_audience: false,
            client_id: None,
            jwks_refresh_seconds: 300,
            max_seconds_without_reauthentication: None,
        }
    }

    fn rs_validator() -> OauthValidator {
        let v = OauthValidator::new(cfg(RS_ISSUER));
        assert_eq!(v.install_jwks(&rs_jwks()).unwrap(), 1);
        v
    }

    // --- ES256 minting helpers (negative-path tests want arbitrary claims) ---

    struct EcIssuer {
        kp: EcdsaKeyPair,
        rng: SystemRandom,
        kid: &'static str,
    }

    impl EcIssuer {
        fn new(kid: &'static str) -> Self {
            let rng = SystemRandom::new();
            let pkcs8 =
                EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
            let kp =
                EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
                    .unwrap();
            Self { kp, rng, kid }
        }

        fn jwks(&self) -> String {
            let point = self.kp.public_key().as_ref();
            let x = URL_SAFE_NO_PAD.encode(&point[1..33]);
            let y = URL_SAFE_NO_PAD.encode(&point[33..65]);
            format!(
                r#"{{"keys":[{{"kty":"EC","crv":"P-256","kid":"{}","use":"sig","x":"{x}","y":"{y}"}}]}}"#,
                self.kid
            )
        }

        fn mint(&self, claims: &serde_json::Value) -> String {
            let header = format!(r#"{{"alg":"ES256","kid":"{}"}}"#, self.kid);
            let msg = format!(
                "{}.{}",
                URL_SAFE_NO_PAD.encode(header.as_bytes()),
                URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes())
            );
            let sig = self.kp.sign(&self.rng, msg.as_bytes()).unwrap();
            format!("{msg}.{}", URL_SAFE_NO_PAD.encode(sig.as_ref()))
        }
    }

    const NOW: i64 = 1_754_900_000;

    fn base_claims() -> serde_json::Value {
        serde_json::json!({
            "iss": "https://ec.test",
            "sub": "svc-canary",
            "aud": "kaas",
            "exp": NOW + 3600,
        })
    }

    fn ec_validator(issuer: &EcIssuer) -> OauthValidator {
        let v = OauthValidator::new(cfg("https://ec.test"));
        assert_eq!(v.install_jwks(&issuer.jwks()).unwrap(), 1);
        v
    }

    // --- message parsing ---

    fn initial(token: &str) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(b"n,,");
        v.push(0x01);
        v.extend_from_slice(format!("auth=Bearer {token}").as_bytes());
        v.push(0x01);
        v.push(0x01);
        v
    }

    #[test]
    fn initial_response_parses() {
        let r = parse_initial_response(&initial("tok123")).unwrap();
        assert_eq!(r.token, "tok123");
        assert_eq!(r.authzid, None);
    }

    #[test]
    fn initial_response_with_authzid_and_extensions() {
        let mut v = Vec::new();
        v.extend_from_slice(b"n,a=alice,");
        v.push(0x01);
        v.extend_from_slice(b"host=broker1");
        v.push(0x01);
        v.extend_from_slice(b"auth=Bearer tok");
        v.push(0x01);
        v.push(0x01);
        let r = parse_initial_response(&v).unwrap();
        assert_eq!(r.authzid.as_deref(), Some("alice"));
        assert_eq!(r.token, "tok");
    }

    #[test]
    fn initial_response_rejects_bad_shapes() {
        // channel binding flag we don't support
        assert!(parse_initial_response(b"y,,\x01auth=Bearer t\x01\x01").is_err());
        // no auth kv at all
        assert!(parse_initial_response(b"n,,\x01host=x\x01\x01").is_err());
        // wrong scheme
        assert!(parse_initial_response(b"n,,\x01auth=Basic dXNlcg==\x01\x01").is_err());
        // duplicate auth
        assert!(parse_initial_response(b"n,,\x01auth=Bearer a\x01auth=Bearer b\x01\x01").is_err());
        // missing terminator
        assert!(parse_initial_response(b"n,,\x01auth=Bearer t").is_err());
        // no separators at all
        assert!(parse_initial_response(b"garbage").is_err());
    }

    // --- signature + claims ---

    #[test]
    fn rs256_vector_validates() {
        let v = rs_validator();
        let out = v.validate(RS_TOKEN, RS_NOW).unwrap();
        assert_eq!(out.principal, RS_SUB);
        assert_eq!(out.exp_unix, 4_102_444_800);
    }

    #[test]
    fn rs256_vector_respects_user_name_claim() {
        let mut c = cfg(RS_ISSUER);
        c.user_name_claim = Some("preferred_username".to_owned());
        let v = OauthValidator::new(c);
        v.install_jwks(&rs_jwks()).unwrap();
        let out = v.validate(RS_TOKEN, RS_NOW).unwrap();
        assert_eq!(out.principal, "canary@test");
    }

    #[test]
    fn rs256_vector_audience_check() {
        let mut c = cfg(RS_ISSUER);
        c.check_audience = true;
        c.client_id = Some("api://kaas-test".to_owned());
        let v = OauthValidator::new(c);
        v.install_jwks(&rs_jwks()).unwrap();
        assert!(v.validate(RS_TOKEN, RS_NOW).is_ok());

        let mut c2 = cfg(RS_ISSUER);
        c2.check_audience = true;
        c2.client_id = Some("api://other".to_owned());
        let v2 = OauthValidator::new(c2);
        v2.install_jwks(&rs_jwks()).unwrap();
        assert!(v2.validate(RS_TOKEN, RS_NOW).is_err());
    }

    #[test]
    fn tampered_payload_rejected() {
        let v = rs_validator();
        // Swap the payload for one claiming a different sub; signature
        // no longer matches.
        let parts: Vec<&str> = RS_TOKEN.split('.').collect();
        let forged_claims = serde_json::json!({
            "iss": RS_ISSUER, "sub": "attacker", "exp": 4_102_444_800i64,
        });
        let forged = format!(
            "{}.{}.{}",
            parts[0],
            URL_SAFE_NO_PAD.encode(forged_claims.to_string().as_bytes()),
            parts[2]
        );
        assert!(v.validate(&forged, RS_NOW).is_err());
    }

    #[test]
    fn expired_token_rejected() {
        let issuer = EcIssuer::new("k1");
        let v = ec_validator(&issuer);
        let mut claims = base_claims();
        claims["exp"] = serde_json::json!(NOW - 120);
        let tok = issuer.mint(&claims);
        let err = v.validate(&tok, NOW).unwrap_err();
        assert!(err.to_string().contains("expired"), "{err}");
    }

    #[test]
    fn expiry_leeway_tolerates_small_skew() {
        let issuer = EcIssuer::new("k1");
        let v = ec_validator(&issuer);
        let mut claims = base_claims();
        claims["exp"] = serde_json::json!(NOW - 30); // inside 60s skew
        assert!(v.validate(&issuer.mint(&claims), NOW).is_ok());
    }

    #[test]
    fn nbf_in_future_rejected() {
        let issuer = EcIssuer::new("k1");
        let v = ec_validator(&issuer);
        let mut claims = base_claims();
        claims["nbf"] = serde_json::json!(NOW + 600);
        assert!(v.validate(&issuer.mint(&claims), NOW).is_err());
    }

    #[test]
    fn wrong_issuer_rejected() {
        let issuer = EcIssuer::new("k1");
        let v = ec_validator(&issuer);
        let mut claims = base_claims();
        claims["iss"] = serde_json::json!("https://evil.test");
        assert!(v.validate(&issuer.mint(&claims), NOW).is_err());
    }

    #[test]
    fn missing_exp_rejected() {
        let issuer = EcIssuer::new("k1");
        let v = ec_validator(&issuer);
        let claims = serde_json::json!({"iss": "https://ec.test", "sub": "x"});
        assert!(v.validate(&issuer.mint(&claims), NOW).is_err());
    }

    #[test]
    fn fallback_user_name_claim_used() {
        let issuer = EcIssuer::new("k1");
        let mut c = cfg("https://ec.test");
        c.user_name_claim = Some("preferred_username".to_owned());
        c.fallback_user_name_claim = Some("sub".to_owned());
        let v = OauthValidator::new(c);
        v.install_jwks(&issuer.jwks()).unwrap();
        // No preferred_username in claims → falls back to sub.
        assert_eq!(
            v.validate(&issuer.mint(&base_claims()), NOW)
                .unwrap()
                .principal,
            "svc-canary"
        );
    }

    #[test]
    fn alg_none_and_hs256_rejected() {
        let v = rs_validator();
        let claims = URL_SAFE_NO_PAD.encode(base_claims().to_string().as_bytes());
        for alg in ["none", "HS256", "HS512"] {
            let header = URL_SAFE_NO_PAD.encode(format!(r#"{{"alg":"{alg}"}}"#).as_bytes());
            let tok = format!("{header}.{claims}.AAAA");
            let err = v.validate(&tok, NOW).unwrap_err();
            assert!(err.to_string().contains("allowlist"), "{alg}: {err}");
        }
    }

    #[test]
    fn unknown_kid_rejected_and_hints_refresh() {
        let issuer = EcIssuer::new("k1");
        let other = EcIssuer::new("k2");
        let v = ec_validator(&issuer);
        assert!(!v.take_refresh_hint());
        // Signed by a key the validator has never seen.
        assert!(v.validate(&other.mint(&base_claims()), NOW).is_err());
        assert!(v.take_refresh_hint());
        assert!(!v.take_refresh_hint(), "hint is one-shot");
    }

    #[test]
    fn empty_jwks_rejects_and_hints() {
        let v = OauthValidator::new(cfg(RS_ISSUER));
        assert!(!v.has_keys());
        assert!(v.validate(RS_TOKEN, RS_NOW).is_err());
        assert!(v.take_refresh_hint());
    }

    #[test]
    fn jwks_skips_unusable_keys() {
        let v = OauthValidator::new(cfg(RS_ISSUER));
        let jwks = format!(
            r#"{{"keys":[
                {{"kty":"RSA","kid":"enc-key","use":"enc","n":"{RS_N}","e":"AQAB"}},
                {{"kty":"oct","kid":"symmetric","k":"c2VjcmV0"}},
                {{"kty":"EC","crv":"P-384","kid":"wrong-curve","x":"AA","y":"AA"}},
                {{"kty":"RSA","kid":"test-rs256","use":"sig","n":"{RS_N}","e":"AQAB"}}
            ]}}"#
        );
        assert_eq!(v.install_jwks(&jwks).unwrap(), 1);
        assert!(v.validate(RS_TOKEN, RS_NOW).is_ok());
    }

    // --- exchange state machine ---

    #[test]
    fn exchange_happy_path() {
        let issuer = EcIssuer::new("k1");
        let v = Arc::new(ec_validator(&issuer));
        // Freshly minted with real wall-clock validity so the
        // exchange's own clock accepts it.
        let mut claims = base_claims();
        claims["exp"] = serde_json::json!(now_unix() + 3600);
        let tok = issuer.mint(&claims);
        let mut ex = OauthBearerExchange::new(v);
        let (out, done) = ex.step(&initial(&tok)).unwrap();
        assert!(done);
        assert!(out.is_empty());
        assert_eq!(ex.principal().unwrap().name, "svc-canary");
        assert_eq!(ex.session_lifetime_ms(), 0, "no max configured → 0");
    }

    #[test]
    fn exchange_session_lifetime_capped() {
        let issuer = EcIssuer::new("k1");
        let mut c = cfg("https://ec.test");
        c.max_seconds_without_reauthentication = Some(300);
        let v = OauthValidator::new(c);
        v.install_jwks(&issuer.jwks()).unwrap();
        let mut claims = base_claims();
        claims["exp"] = serde_json::json!(now_unix() + 3600);
        let tok = issuer.mint(&claims);
        let mut ex = OauthBearerExchange::new(Arc::new(v));
        let (_, done) = ex.step(&initial(&tok)).unwrap();
        assert!(done);
        // Token has ~3600s left; cap is 300s.
        let ms = ex.session_lifetime_ms();
        assert!(ms > 0 && ms <= 300_000, "{ms}");
    }

    #[test]
    fn exchange_failure_is_two_step() {
        let issuer = EcIssuer::new("k1");
        let v = Arc::new(ec_validator(&issuer));
        let mut ex = OauthBearerExchange::new(v);
        // Garbage token → JSON challenge, not yet an error.
        let (challenge, done) = ex.step(&initial("not-a-jwt")).unwrap();
        assert!(!done);
        assert_eq!(challenge, FAILURE_CHALLENGE);
        // Client acks with %x01 → now it fails.
        let err = ex.step(&[0x01]).unwrap_err();
        assert!(matches!(err, AuthError::InvalidToken(_)));
        assert!(ex.principal().is_none());
    }

    #[test]
    fn exchange_authzid_mismatch_rejected() {
        let issuer = EcIssuer::new("k1");
        let v = Arc::new(ec_validator(&issuer));
        let mut claims = base_claims();
        claims["exp"] = serde_json::json!(now_unix() + 3600);
        let tok = issuer.mint(&claims);
        let mut msg = Vec::new();
        msg.extend_from_slice(b"n,a=somebody-else,");
        msg.push(0x01);
        msg.extend_from_slice(format!("auth=Bearer {tok}").as_bytes());
        msg.push(0x01);
        msg.push(0x01);
        let mut ex = OauthBearerExchange::new(v);
        let (_, done) = ex.step(&msg).unwrap();
        assert!(!done, "mismatch takes the two-step failure path");
        assert!(ex.step(&[0x01]).is_err());
    }

    #[test]
    fn engine_serves_only_oauthbearer() {
        let issuer = EcIssuer::new("k1");
        let eng = OauthEngine::new(Arc::new(ec_validator(&issuer)));
        assert!(eng.new_sasl_exchange("OAUTHBEARER").is_ok());
        assert!(matches!(
            eng.new_sasl_exchange("SCRAM-SHA-512"),
            Err(AuthError::UnknownMechanism(_))
        ));
        assert_eq!(eng.mechanisms(), &["OAUTHBEARER"]);
        assert!(eng.requires_pre_auth());
        assert!(eng.authenticate_tls("CN=x").is_err());
    }
}
