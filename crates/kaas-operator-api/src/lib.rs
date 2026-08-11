//! kaas-operator-api — CRD type definitions used by the operator and the broker.
//!
//! Four CRD types
//! plus a shared scheme module:
//!
//! - [`kafkacluster::KafkaCluster`] — top-level cluster config, drives
//!   external-listener plumbing (cert-manager + per-broker Services +
//!   Gateway-API TLSRoutes).
//! - [`kafkatopic::KafkaTopic`] — partition-dir + topic-config CR. The
//!   operator materialises partition directories on the shared PVC and
//!   mints `Status.TopicID` (gh #105, KIP-516) on first reconcile.
//! - [`kafkauser::KafkaUser`] — Strimzi-shape user CR: authentication +
//!   inline `spec.authorization.acls` (gh #135) + per-broker quotas
//!   (gh #126, named-honestly `producerMaxByteRatePerBroker`).
//!
//! Every CR derives `kube::CustomResource` so callers get
//! `<T>::api(client)`, `<T>::crd()`, and the apiVersion/kind metadata
//! for free, and `schemars::JsonSchema` so `xtask gen-crds`
//! (workstream E) can walk the type and emit YAML. Field-level
//! validation annotations preserve the v0.1 CRD schema — see each
//! module for the field-by-field mapping.

#![allow(missing_debug_implementations)]

pub mod condition;
pub mod kafkacluster;
pub mod kafkatopic;
pub mod kafkauser;
pub mod scheme;

pub use condition::Condition;
pub use kafkacluster::{
    CertManagerConfig, ExternalListener, GatewayConfig, GatewayRef, InternalListener, IssuerRef,
    KafkaCluster, KafkaClusterListeners, KafkaClusterSpec, KafkaClusterStatus, KafkaClusterStorage,
    ServiceConfig, TlsConfig,
};
pub use kafkatopic::{
    KafkaTopic, KafkaTopicConfig, KafkaTopicSpec, KafkaTopicStatus, KafkaTopicStorage,
};
pub use kafkauser::{
    KafkaUser, KafkaUserAcl, KafkaUserAclResource, KafkaUserAuthentication, KafkaUserAuthorization,
    KafkaUserQuotas, KafkaUserScramCredential, KafkaUserSpec, KafkaUserStatus, LocalObjectRef,
    SecretKeyRef, ServiceAccountRef,
};
pub use scheme::{GROUP, VERSION};

// Re-export CustomResourceExt so callers don't need a direct `kube` dep
// just to call `<T>::crd()`.
pub use kube::CustomResourceExt;
