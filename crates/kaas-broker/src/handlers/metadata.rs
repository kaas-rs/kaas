//! Metadata handler (key 3).
//!
//! Single-broker shape: every partition leads on `broker_id = self`,
//! every topic carries the all-zero `topic_id` sentinel until Phase 7
//! mints real UUIDs.
//!
//! Per-listener port advertisement (gh #125): the handler picks the
//! port matching `ConnState::listener_name` from the listener table
//! it was constructed with, so a client that bootstrapped on
//! the authed listener gets back the authed port, not the anonymous
//! one. Phase 3 single-listener clusters resolve to the single entry.
//!
//! ## Topic auto-creation (gh #109, gh #242)
//!
//! Apache `auto.create.topics.enable`. An unknown topic named by a
//! request that set `allowAutoTopicCreation` mints a `KafkaTopic` CR
//! through the installed [`TopicCRWriter`] and answers
//! `LEADER_NOT_AVAILABLE` (5) — a retriable code — because the CR
//! exists but the operator hasn't materialised the partition dirs and
//! the controller hasn't assigned a leader, so there is no truthful
//! leader to advertise yet. The client re-requests metadata and gets
//! the real thing.
//!
//! This matters beyond convenience: Kafka Streams' DSL `.to(sink)`
//! never calls `AdminClient.createTopics` for its sink topic (only for
//! changelog/repartition topics), so without this path a Streams app
//! whose output topic doesn't already exist can never make progress.
//!
//! Four guards, each with a test: the broker-side switch must be on,
//! the client must not have opted out (the flag exists in v4+; v0-v3
//! carry no field and decode to Apache's `true` default, so the broker
//! switch is the only gate for those), the `__` prefix is reserved, and
//! the principal needs `Create` on the topic — otherwise Metadata would
//! be a way around the authorizer that `CreateTopics` enforces.
//!
//! A request with an empty topic list can't trip any of this: it's the
//! "all topics" form, so every name comes from the registry and the
//! unknown-topic branch is unreachable. Apache carves the same case out
//! explicitly via `!metadataRequest.isAllTopics`.
//!
//! [`TopicCRWriter`]: crate::topic_cr_writer::TopicCRWriter

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Principal, Resource};
use kaas_codec::api::metadata;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use crate::broker::Broker;
use crate::cli::ListenerEntry;
use crate::topic_cr_writer::TopicWriteError;

const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERR_LEADER_NOT_AVAILABLE: i16 = 5;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;

/// Apache reserves the `__` prefix for internal topics
/// (`__consumer_offsets`, `__transaction_state`). kaas has no such
/// topics on disk — the equivalent state lives under `__cluster/`
/// (see the non-goals in CLAUDE.md) — but a client asking for one
/// must never mint a CR for it.
fn is_reserved_internal(name: &str) -> bool {
    name.starts_with("__")
}

/// Per-listener advertised endpoint precomputed at handler-build
/// time. Keyed by the listener `name` stored on each connection.
#[derive(Debug, Clone)]
struct ListenerAdvert {
    name: String,
    host: String,
    port: i32,
}

#[derive(Debug)]
pub struct MetadataHandler {
    broker: Arc<Broker>,
    listeners: Vec<ListenerAdvert>,
    /// Apache `auto.create.topics.enable` (gh #242). Off unless
    /// [`with_auto_create`](Self::with_auto_create) says otherwise, so
    /// tests and dev-mode wiring keep the plain
    /// `UNKNOWN_TOPIC_OR_PARTITION` behaviour; `bins/kaas` turns it on
    /// from `Cli`.
    auto_create: bool,
    /// Apache `num.partitions` — partition count for an auto-created
    /// topic. Only read when `auto_create` is set.
    num_partitions: i32,
}

impl MetadataHandler {
    pub fn new(broker: Arc<Broker>, listeners: &[ListenerEntry]) -> Self {
        let listeners = listeners.iter().map(advert_from).collect();
        Self {
            broker,
            listeners,
            auto_create: false,
            num_partitions: 1,
        }
    }

    /// Enable broker-side topic auto-creation. `num_partitions` is
    /// clamped to at least 1 — a zero-partition topic is unservable
    /// and the CR would be rejected downstream anyway.
    #[must_use]
    pub fn with_auto_create(mut self, enabled: bool, num_partitions: i32) -> Self {
        self.auto_create = enabled;
        self.num_partitions = num_partitions.max(1);
        self
    }

    /// Mint a `KafkaTopic` CR for an unknown topic named by a Metadata
    /// request that opted into auto-creation, and return the error code
    /// to report for it.
    ///
    /// `LEADER_NOT_AVAILABLE` is the success answer, not an error: the
    /// CR exists but the operator hasn't created the partition dirs and
    /// the controller hasn't assigned a leader yet, so there is nothing
    /// truthful to advertise. Java clients treat code 5 as retriable and
    /// re-request metadata, which is exactly the handshake Apache uses.
    /// `AlreadyExists` gets the same answer — a concurrent request (or
    /// an operator-authored CR the watch hasn't delivered yet) won the
    /// race, which is success for this caller.
    async fn auto_create_topic(&self, principal: &Principal, name: &str) -> i16 {
        if is_reserved_internal(name) {
            return ERR_UNKNOWN_TOPIC_OR_PARTITION;
        }
        // Auto-creation is a create, so it takes the same ACL as
        // CreateTopics — otherwise Metadata would be a way around the
        // authorizer.
        if !self
            .broker
            .authorizer
            .authorize(principal, &Resource::topic(name), Operation::Create)
        {
            return ERR_TOPIC_AUTHZ_FAILED;
        }
        let Some(writer) = self.broker.cr_writer() else {
            // Dev mode / no kube client: nothing can create the topic,
            // so the honest answer is that it doesn't exist.
            return ERR_UNKNOWN_TOPIC_OR_PARTITION;
        };
        match writer.create_topic(name, self.num_partitions).await {
            Ok(()) | Err(TopicWriteError::AlreadyExists(_)) => {
                tracing::info!(topic = %name, partitions = self.num_partitions, "auto-created topic");
                ERR_LEADER_NOT_AVAILABLE
            }
            Err(e) => {
                tracing::warn!(topic = %name, error = %e, "topic auto-create failed");
                ERR_UNKNOWN_TOPIC_OR_PARTITION
            }
        }
    }

    fn advert_for(&self, listener_name: &str) -> ListenerAdvert {
        self.listeners
            .iter()
            .find(|l| l.name == listener_name)
            .cloned()
            .unwrap_or_else(|| {
                // The connection's listener tag didn't match any
                // configured entry — should only happen with a
                // programming error in main.rs. Fall back to the
                // first listener so the response is still well-formed.
                self.listeners.first().cloned().unwrap_or(ListenerAdvert {
                    name: "internal".to_owned(),
                    host: "127.0.0.1".to_owned(),
                    port: 9092,
                })
            })
    }
}

fn self_broker_row(node_id: i32, advert: &ListenerAdvert) -> metadata::Broker {
    metadata::Broker {
        node_id,
        host: advert.host.clone(),
        port: advert.port,
        rack: None,
    }
}

/// `"kaas-2"` → `2`. Broker identity strings carry the ordinal as
/// the trailing hyphen segment (StatefulSet pod-name shape); a
/// malformed id yields `None` and the caller falls back to self.
fn trailing_ordinal(id: &str) -> Option<i32> {
    id.rsplit('-').next()?.parse().ok()
}

fn advert_from(entry: &ListenerEntry) -> ListenerAdvert {
    // Best-effort parse: bad addrs (which shouldn't occur — `Cli`
    // validates earlier) degrade to localhost:9092 so the Metadata
    // response stays well-formed.
    let addr: std::net::SocketAddr = entry
        .addr
        .parse()
        .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 9092)));
    let port = i32::from(addr.port());
    let host = match entry.advertised_host.as_deref() {
        Some(h) if !h.is_empty() => h.to_owned(),
        // 0.0.0.0 is a wildcard bind, not a routable target.
        // For dev clients connecting on the same box, localhost is
        // the right echo.
        _ if addr.ip().is_unspecified() => "127.0.0.1".to_owned(),
        _ => addr.ip().to_string(),
    };
    ListenerAdvert {
        name: entry.name.clone(),
        host,
        port,
    }
}

#[async_trait]
impl Handler for MetadataHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = metadata::decode_request(&mut body, version)?;
        let (listener_name, principal) = {
            let c = conn.lock();
            (
                c.listener_name.clone(),
                c.principal.clone().unwrap_or_else(Principal::anonymous),
            )
        };
        let advert = self.advert_for(&listener_name);

        // Cluster mode: advertise the live broker set. Peers run the
        // same chart, so each peer is advertised at its stable FQDN
        // with the port of the listener this client connected on
        // (gh #125 symmetry). Self keeps the listener's own
        // advertised host so external hostname templates still win.
        // (External per-broker hostname templates for peers are a
        // follow-up — the external listener ships disabled.)
        let brokers = match self.broker.broker_view() {
            Some(view) => {
                let mut v: Vec<metadata::Broker> = view
                    .brokers()
                    .into_iter()
                    .map(|b| metadata::Broker {
                        node_id: b.node_id,
                        host: if b.node_id == self.broker.broker_id {
                            advert.host.clone()
                        } else {
                            b.host
                        },
                        port: advert.port,
                        rack: None,
                    })
                    .collect();
                if v.is_empty() {
                    v.push(self_broker_row(self.broker.broker_id, &advert));
                }
                v
            }
            None => vec![self_broker_row(self.broker.broker_id, &advert)],
        };

        // Per-partition leader from the applied assignment; self
        // when no coordinator is wired (dev) or the partition is
        // missing from the assignment (fresh topic, next recompute
        // pending).
        let coord = self.broker.coordinator();
        let controller_id = coord
            .as_ref()
            .and_then(|c| c.snapshot())
            .and_then(|a| trailing_ordinal(&a.controller))
            .unwrap_or(self.broker.broker_id);

        // If the request topic list is empty, return every known topic
        // (Apache: an empty list means "all topics"). If non-empty,
        // return exactly the requested topics, with UNKNOWN_TOPIC_OR_PARTITION
        // for any that aren't in our registry.
        let topic_names: Vec<String> = if req.topics.is_empty() {
            self.broker
                .topics
                .all()
                .into_iter()
                .map(|t| t.name)
                .collect()
        } else {
            req.topics
        };

        let mut topics = Vec::with_capacity(topic_names.len());
        for name in topic_names {
            match self.broker.topics.get(&name) {
                Some(meta) => {
                    let mut partitions =
                        Vec::with_capacity(usize::try_from(meta.partition_count).unwrap_or(0));
                    for i in 0..meta.partition_count {
                        let leader_id = coord
                            .as_ref()
                            .and_then(|c| c.leader_for(&name, i))
                            .and_then(|owner| trailing_ordinal(&owner))
                            .unwrap_or(self.broker.broker_id);
                        partitions.push(metadata::Partition {
                            error_code: 0,
                            partition_index: i,
                            leader_id,
                            leader_epoch: 0,
                            replica_nodes: vec![leader_id],
                            isr_nodes: vec![leader_id],
                            offline_replicas: Vec::new(),
                        });
                    }
                    topics.push(metadata::Topic {
                        error_code: 0,
                        name: meta.name,
                        topic_id: meta.topic_id,
                        is_internal: false,
                        partitions,
                        topic_authorized_operations: 0,
                    });
                }
                None => {
                    // gh #242: only when the broker has the feature on
                    // AND the client asked for it. A client that didn't
                    // set the flag (or a v0-v3 request, where the field
                    // doesn't exist and decodes false) keeps the plain
                    // UNKNOWN_TOPIC_OR_PARTITION answer.
                    let error_code = if self.auto_create && req.allow_auto_topic_creation {
                        self.auto_create_topic(&principal, &name).await
                    } else {
                        ERR_UNKNOWN_TOPIC_OR_PARTITION
                    };
                    topics.push(metadata::Topic {
                        error_code,
                        name,
                        topic_id: [0; 16],
                        is_internal: false,
                        partitions: Vec::new(),
                        topic_authorized_operations: 0,
                    });
                }
            }
        }

        let resp = metadata::Response {
            throttle_time_ms: 0,
            brokers,
            cluster_id: Some(self.broker.cluster_id.clone()),
            controller_id,
            topics,
            cluster_authorized_operations: 0,
        };

        let mut out = BytesMut::new();
        metadata::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ListenerEntry;
    use crate::topic_cr_writer::TopicCRWriter;
    use crate::topic_registry::{TopicMeta, TopicRegistry};
    use kaas_codec::api::common::{write_array_len, write_str};
    use kaas_codec::primitives::write_i8;
    use kaas_codec::tagged;
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn(listener: &str) -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            listener,
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker_with(topics: Vec<(&str, i32)>) -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let r = Arc::new(TopicRegistry::new());
        for (n, p) in topics {
            r.insert(TopicMeta {
                name: n.to_owned(),
                partition_count: p,
                topic_id: [0; 16],
            });
        }
        Arc::new(Broker::new(engine, r, "kaas-dev", 0))
    }

    fn listeners() -> Vec<ListenerEntry> {
        vec![
            ListenerEntry {
                name: "internal".to_owned(),
                addr: "0.0.0.0:9092".to_owned(),
                advertised_host: None,
                tls: None,
                authentication_type: None,
            },
            ListenerEntry {
                name: "external".to_owned(),
                addr: "0.0.0.0:9094".to_owned(),
                advertised_host: Some("broker-0.cluster.local".to_owned()),
                tls: None,
                authentication_type: None,
            },
        ]
    }

    fn encode_request_v9(topics: &[&str]) -> Bytes {
        encode_request_v9_auto(topics, false)
    }

    fn encode_request_v9_auto(topics: &[&str], allow_auto_create: bool) -> Bytes {
        let flexible = true; // v9 flexible
        let mut w = BytesMut::new();
        write_array_len(&mut w, topics.len(), flexible).unwrap();
        for n in topics {
            write_str(&mut w, n, flexible).unwrap();
            tagged::write_empty(&mut w);
        }
        write_i8(&mut w, i8::from(allow_auto_create)); // allow_auto_topic_creation (v4+)
        write_i8(&mut w, 0); // include_cluster_authorized_operations (v8-10)
        write_i8(&mut w, 0); // include_topic_authorized_operations (v8+)
        tagged::write_empty(&mut w);
        w.freeze()
    }

    /// Records every `create_topic` call so a test can assert both
    /// that the CR write happened and what partition count it carried.
    #[derive(Debug, Default)]
    struct RecordingWriter {
        calls: Mutex<Vec<(String, i32)>>,
        already_exists: bool,
    }

    impl RecordingWriter {
        fn calls(&self) -> Vec<(String, i32)> {
            self.calls.lock().clone()
        }
    }

    #[async_trait]
    impl TopicCRWriter for RecordingWriter {
        async fn create_topic(&self, name: &str, n: i32) -> Result<(), TopicWriteError> {
            self.calls.lock().push((name.to_owned(), n));
            if self.already_exists {
                return Err(TopicWriteError::AlreadyExists(name.into()));
            }
            Ok(())
        }
        async fn expand_topic(&self, _: &str, _: i32) -> Result<(), TopicWriteError> {
            unreachable!("Metadata never expands a topic")
        }
        async fn update_topic_config(
            &self,
            _: &str,
            _: &[crate::topic_cr_writer::ConfigOpWithValue],
        ) -> Result<(), TopicWriteError> {
            unreachable!("Metadata never edits topic config")
        }
        async fn delete_topic(&self, _: &str) -> Result<(), TopicWriteError> {
            unreachable!("Metadata never deletes a topic")
        }
        async fn set_partition_log_dir(
            &self,
            _: &str,
            _: i32,
            _: &str,
        ) -> Result<(), TopicWriteError> {
            unreachable!("Metadata never moves a partition")
        }
    }

    /// Drive one v9 Metadata request and return the per-topic error
    /// code for the single requested topic.
    async fn error_code_for(h: &MetadataHandler, topic: &str, allow_auto: bool) -> i16 {
        let body = encode_request_v9_auto(&[topic], allow_auto);
        let out = h.handle(&conn("internal"), 9, body).await.unwrap();
        let mut r = out.freeze();
        metadata::decode_response(&mut r, 9).unwrap().topics[0].error_code
    }

    #[tokio::test]
    async fn returns_self_as_only_broker_and_leader() {
        let h = MetadataHandler::new(broker_with(vec![("events", 3)]), &listeners());
        let body = encode_request_v9(&["events"]);
        let out = h.handle(&conn("internal"), 9, body).await.unwrap();
        let mut r = out.freeze();
        let resp = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].node_id, 0);
        assert_eq!(resp.cluster_id.as_deref(), Some("kaas-dev"));
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "events");
        assert_eq!(resp.topics[0].partitions.len(), 3);
        for p in &resp.topics[0].partitions {
            assert_eq!(p.leader_id, 0);
            assert_eq!(p.replica_nodes, vec![0]);
            assert_eq!(p.isr_nodes, vec![0]);
        }
    }

    #[tokio::test]
    async fn per_listener_port_echoed_back() {
        let h = MetadataHandler::new(broker_with(vec![("events", 1)]), &listeners());
        let body = encode_request_v9(&["events"]);

        let internal = h.handle(&conn("internal"), 9, body.clone()).await.unwrap();
        let mut r = internal.freeze();
        let resp_int = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp_int.brokers[0].port, 9092);
        assert_eq!(resp_int.brokers[0].host, "127.0.0.1");

        let external = h.handle(&conn("external"), 9, body).await.unwrap();
        let mut r = external.freeze();
        let resp_ext = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp_ext.brokers[0].port, 9094);
        assert_eq!(resp_ext.brokers[0].host, "broker-0.cluster.local");
    }

    #[tokio::test]
    async fn unknown_topic_returns_per_topic_error_3() {
        let h = MetadataHandler::new(broker_with(vec![("events", 1)]), &listeners());
        let body = encode_request_v9(&["nope"]);
        let out = h.handle(&conn("internal"), 9, body).await.unwrap();
        let mut r = out.freeze();
        let resp = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp.topics[0].error_code, ERR_UNKNOWN_TOPIC_OR_PARTITION);
        assert!(resp.topics[0].partitions.is_empty());
    }

    // --- gh #242: auto.create.topics.enable -----------------------

    #[tokio::test]
    async fn auto_create_mints_cr_and_answers_leader_not_available() {
        let broker = broker_with(vec![("events", 1)]);
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 3);

        assert_eq!(
            error_code_for(&h, "brand-new", true).await,
            ERR_LEADER_NOT_AVAILABLE
        );
        // The CR carries the configured num.partitions, not the
        // requesting client's guess (Metadata has no such field).
        assert_eq!(writer.calls(), vec![("brand-new".to_owned(), 3)]);
    }

    #[tokio::test]
    async fn auto_create_requires_the_client_to_ask() {
        let broker = broker_with(vec![("events", 1)]);
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 1);

        // Feature on, but the v4+ client explicitly opted out. An
        // opt-out must be honoured even with the broker switch on,
        // or `allow.auto.create.topics=false` on the consumer means
        // nothing.
        assert_eq!(
            error_code_for(&h, "nope", false).await,
            ERR_UNKNOWN_TOPIC_OR_PARTITION
        );
        assert!(writer.calls().is_empty());
    }

    #[tokio::test]
    async fn auto_create_disabled_never_writes() {
        let broker = broker_with(vec![("events", 1)]);
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners());

        assert_eq!(
            error_code_for(&h, "nope", true).await,
            ERR_UNKNOWN_TOPIC_OR_PARTITION
        );
        assert!(writer.calls().is_empty());
    }

    #[tokio::test]
    async fn auto_create_refuses_reserved_internal_names() {
        let broker = broker_with(vec![("events", 1)]);
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 1);

        assert_eq!(
            error_code_for(&h, "__consumer_offsets", true).await,
            ERR_UNKNOWN_TOPIC_OR_PARTITION
        );
        assert!(writer.calls().is_empty());
    }

    #[tokio::test]
    async fn auto_create_without_a_writer_reports_unknown() {
        // Dev mode: no kube client, so nothing can mint the CR and
        // claiming LEADER_NOT_AVAILABLE would loop the client forever.
        let h = MetadataHandler::new(broker_with(vec![("events", 1)]), &listeners())
            .with_auto_create(true, 1);
        assert_eq!(
            error_code_for(&h, "nope", true).await,
            ERR_UNKNOWN_TOPIC_OR_PARTITION
        );
    }

    #[tokio::test]
    async fn auto_create_treats_already_exists_as_success() {
        let broker = broker_with(vec![("events", 1)]);
        broker.install_cr_writer(Arc::new(RecordingWriter {
            calls: Mutex::new(Vec::new()),
            already_exists: true,
        }));
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 1);
        // A concurrent creator won the race; the topic exists, so the
        // caller should retry rather than be told it's unknown.
        assert_eq!(
            error_code_for(&h, "raced", true).await,
            ERR_LEADER_NOT_AVAILABLE
        );
    }

    #[tokio::test]
    async fn auto_create_honours_the_authorizer() {
        // Auto-creation is a create. If Metadata skipped the ACL check,
        // an unauthorized client could mint topics through it even
        // though CreateTopics would refuse.
        #[derive(Debug)]
        struct DenyAll;
        impl kaas_auth::Authorizer for DenyAll {
            fn authorize(&self, _: &Principal, _: &Resource, _: Operation) -> bool {
                false
            }
        }

        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let broker = Arc::new(Broker::with_auth(
            engine,
            Arc::new(TopicRegistry::new()),
            "kaas-dev",
            0,
            Arc::new(DenyAll),
            Arc::new(kaas_auth::NoQuotaChecker),
        ));
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 1);

        assert_eq!(
            error_code_for(&h, "forbidden-topic", true).await,
            ERR_TOPIC_AUTHZ_FAILED
        );
        assert!(writer.calls().is_empty());
    }

    #[tokio::test]
    async fn auto_create_leaves_known_topics_alone() {
        let broker = broker_with(vec![("events", 2)]);
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 9);

        assert_eq!(error_code_for(&h, "events", true).await, 0);
        assert!(writer.calls().is_empty());
    }

    #[tokio::test]
    async fn empty_topic_list_returns_all_known() {
        let h = MetadataHandler::new(broker_with(vec![("a", 1), ("b", 2)]), &listeners());
        let body = encode_request_v9(&[]);
        let out = h.handle(&conn("internal"), 9, body).await.unwrap();
        let mut r = out.freeze();
        let resp = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp.topics.len(), 2);
        let names: Vec<&str> = resp.topics.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
    }
}
