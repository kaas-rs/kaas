//! Smoke tests: every CR type can produce a `CustomResourceDefinition`
//! and serialise it to YAML.
//!
//! Workstream E (`xtask gen-crds`) will diff this YAML against the
//! committed CRD fixtures under `deploy/crds/`; those tests don't
//! land here. The point of this file is to catch
//! `kube-derive` macro mis-uses (missing fields, malformed
//! `printcolumn` JSON, etc.) at the crate gate, not at the workspace
//! gate.

use kaas_operator_api::{KafkaCluster, KafkaTopic, KafkaUser};
use kube::CustomResourceExt;

#[test]
fn kafkatopic_crd_serialises() {
    let crd = KafkaTopic::crd();
    let yaml = serde_yaml::to_string(&crd).expect("crd serialises");
    assert!(yaml.contains("kind: CustomResourceDefinition"));
    assert!(yaml.contains("name: kafkatopics.kaas.rs"));
    assert!(yaml.contains("partitions"));
    assert!(yaml.contains("topicId"));
}

#[test]
fn kafkauser_crd_serialises() {
    let crd = KafkaUser::crd();
    let yaml = serde_yaml::to_string(&crd).expect("crd serialises");
    assert!(yaml.contains("name: kafkausers.kaas.rs"));
    assert!(yaml.contains("authentication"));
    assert!(yaml.contains("authorization"));
    assert!(yaml.contains("producerMaxByteRatePerBroker"));
}

#[test]
fn kafkacluster_crd_serialises() {
    let crd = KafkaCluster::crd();
    let yaml = serde_yaml::to_string(&crd).expect("crd serialises");
    assert!(yaml.contains("name: kafkaclusters.kaas.rs"));
    // The two default ports we explicitly carry through schemars.
    assert!(yaml.contains("9092"), "internal port default present");
    assert!(yaml.contains("9093"), "external port default present");
}

#[test]
fn kafkatopic_effective_topic_name_falls_back_to_metadata_name() {
    use kaas_operator_api::{KafkaTopicConfig, KafkaTopicSpec};
    use kube::api::ObjectMeta;

    // spec.topic_name set → wins
    let with_topic_name = KafkaTopic {
        metadata: ObjectMeta {
            name: Some("topic-name-from-meta".into()),
            ..ObjectMeta::default()
        },
        spec: KafkaTopicSpec {
            topic_name: "explicit-Kafka-Name".into(),
            partitions: 3,
            config: KafkaTopicConfig::default(),
            storage: None,
        },
        status: None,
    };
    assert_eq!(
        with_topic_name.effective_topic_name(),
        "explicit-Kafka-Name"
    );

    // spec.topic_name empty → falls back to metadata.name
    let without_topic_name = KafkaTopic {
        metadata: ObjectMeta {
            name: Some("only-meta".into()),
            ..ObjectMeta::default()
        },
        spec: KafkaTopicSpec {
            topic_name: String::new(),
            partitions: 1,
            config: KafkaTopicConfig::default(),
            storage: None,
        },
        status: None,
    };
    assert_eq!(without_topic_name.effective_topic_name(), "only-meta");
}

#[test]
fn group_version_constants_match_crd_attributes() {
    assert_eq!(kaas_operator_api::GROUP, "kaas.rs");
    assert_eq!(kaas_operator_api::VERSION, "v1alpha1");
    let yaml = serde_yaml::to_string(&KafkaTopic::crd()).expect("crd serialises");
    assert!(yaml.contains("group: kaas.rs"));
    assert!(yaml.contains("name: v1alpha1"));
}
