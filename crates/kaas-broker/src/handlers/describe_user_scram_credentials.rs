//! DescribeUserScramCredentials handler — API key 50 (gh #252,
//! KIP-554).
//!
//! Answers from the live [`CredentialStore`] (the operator-written
//! `credentials.json`, hot-reloaded) — mechanism + iteration count
//! only, never salts or keys. `users: null` describes every user
//! with SCRAM credentials; a named user without any answers
//! `RESOURCE_NOT_FOUND` (83); a user named twice answers
//! `DUPLICATE_RESOURCE` (81), both per Apache.
//!
//! Authorization: `Operation::Describe` on the cluster resource —
//! denial is the *top-level* `CLUSTER_AUTHORIZATION_FAILED` (31),
//! matching Apache's shape for this API.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{CredentialStore, Operation, Resource};
use kaas_codec::api::describe_user_scram_credentials::{
    self, mechanism, CredentialInfo, DescribeUserResult, Response,
};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NONE: i16 = 0;
const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;
const ERR_DUPLICATE_RESOURCE: i16 = 81;
const ERR_RESOURCE_NOT_FOUND: i16 = 83;

#[derive(Debug)]
pub struct DescribeUserScramCredentialsHandler {
    broker: Arc<Broker>,
    /// `None` in dev mode / auth-disabled — every lookup answers
    /// RESOURCE_NOT_FOUND rather than pretending an empty universe
    /// is an error.
    creds: Option<Arc<dyn CredentialStore>>,
}

impl DescribeUserScramCredentialsHandler {
    pub fn new(broker: Arc<Broker>, creds: Option<Arc<dyn CredentialStore>>) -> Self {
        Self { broker, creds }
    }
}

#[async_trait]
impl Handler for DescribeUserScramCredentialsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = describe_user_scram_credentials::decode_request(&mut body, version)?;

        let principal = principal_from(conn);
        if !self
            .broker
            .authorizer
            .authorize(&principal, &Resource::cluster(), Operation::Describe)
        {
            let resp = Response {
                throttle_time_ms: 0,
                error_code: ERR_CLUSTER_AUTHZ_FAILED,
                error_message: Some("Cluster authorization failed.".into()),
                results: vec![],
            };
            let mut out = BytesMut::new();
            describe_user_scram_credentials::encode_response(&mut out, &resp, version)?;
            return Ok(out);
        }

        let all = self
            .creds
            .as_ref()
            .map(|c| c.list_all_scram_users())
            .unwrap_or_default();

        let results: Vec<DescribeUserResult> = match req.users {
            // null → everything with SCRAM credentials, sorted for a
            // stable wire order.
            None => {
                let mut users: Vec<_> = all.into_iter().collect();
                users.sort_by(|a, b| a.0.cmp(&b.0));
                users
                    .into_iter()
                    .map(|(user, info)| DescribeUserResult {
                        user,
                        error_code: ERR_NONE,
                        error_message: None,
                        credential_infos: vec![CredentialInfo {
                            mechanism: mechanism::SCRAM_SHA_512,
                            iterations: info.iterations,
                        }],
                    })
                    .collect()
            }
            Some(requested) => {
                let mut seen = HashSet::new();
                let mut dups = HashSet::new();
                for u in &requested {
                    if !seen.insert(u.clone()) {
                        dups.insert(u.clone());
                    }
                }
                // One result row per distinct requested user.
                let mut emitted = HashSet::new();
                requested
                    .into_iter()
                    .filter(|u| emitted.insert(u.clone()))
                    .map(|user| {
                        if dups.contains(&user) {
                            return DescribeUserResult {
                                user,
                                error_code: ERR_DUPLICATE_RESOURCE,
                                error_message: Some(
                                    "user appears more than once in the request".into(),
                                ),
                                credential_infos: vec![],
                            };
                        }
                        match all.get(&user) {
                            Some(info) => DescribeUserResult {
                                user,
                                error_code: ERR_NONE,
                                error_message: None,
                                credential_infos: vec![CredentialInfo {
                                    mechanism: mechanism::SCRAM_SHA_512,
                                    iterations: info.iterations,
                                }],
                            },
                            None => DescribeUserResult {
                                user,
                                error_code: ERR_RESOURCE_NOT_FOUND,
                                error_message: Some(
                                    "attempt to describe a user with no SCRAM credentials".into(),
                                ),
                                credential_infos: vec![],
                            },
                        }
                    })
                    .collect()
            }
        };

        let resp = Response {
            throttle_time_ms: 0,
            error_code: ERR_NONE,
            error_message: None,
            results,
        };
        let mut out = BytesMut::new();
        describe_user_scram_credentials::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
