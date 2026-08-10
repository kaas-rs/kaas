//! ArgoCD coexistence annotations for admin-protocol-created CRs.
//!
//! Port of the Go broker's `internal/k8s/argocd.go` (gh #84 + gh
//! #106), dropped in the Rust rewrite while the chart kept shipping
//! the `admin.argocd.*` values and `KAAS_ARGOCD_*` env — dead knobs
//! until this module re-wired them.
//!
//! The problem: a CR the broker mints from the Kafka admin protocol
//! (`kafka-topics.sh --create` → `KafkaTopic`) carries no ArgoCD
//! tracking metadata and no ownerReferences, so it is invisible in
//! the ArgoCD Application tree — and if someone hand-adds the
//! tracking label instead, ArgoCD sees a resource that is "part of
//! the app but absent from git" and prunes it. The Go answer, kept
//! here 1:1:
//!
//! - `argocd.argoproj.io/tracking-id: <app>:<group>/<kind>:<ns>/<name>`
//!   claims the CR into the named Application so it renders in the
//!   UI tree alongside the git-managed CRs (the gh #106 improvement
//!   over gh #84's silent coexistence).
//! - `argocd.argoproj.io/compare-options` (default
//!   `IgnoreExtraneous`) keeps ArgoCD from diffing it against git —
//!   no drift, no selfHeal prune. Empty skips the annotation, for
//!   operators who *want* "this topic isn't in git" surfaced as
//!   drift.
//! - `argocd.argoproj.io/sync-options` (chart default
//!   `Delete=false`) lets runtime-created topics survive a parent
//!   Application delete. Empty skips it.
//!
//! Deployments not using ArgoCD get plain CRs: both `enabled` and a
//! non-empty `application_name` must hold before any annotation is
//! emitted. The broker cannot detect ArgoCD at runtime, so the
//! operator opts in explicitly via `admin.argocd.enabled` on the
//! chart.
//!
//! Scope: only writers that **create** CRs consult this — today
//! that is `KubeTopicCRWriter::create_topic` (and through it the
//! Metadata auto-create path). The ACL writer edits `KafkaUser` CRs
//! that must already exist (404 → `UnknownPrincipal`), and stamping
//! ArgoCD metadata onto a git-managed resource would *cause* the
//! drift this module exists to avoid — so it deliberately does not.
//! A future surface that creates users at runtime (e.g.
//! AlterUserScramCredentials minting a `KafkaUser`) should reuse
//! [`ArgoCdConfig::annotations`] unchanged.

use std::collections::BTreeMap;

const TRACKING_ID_ANNOTATION: &str = "argocd.argoproj.io/tracking-id";
const COMPARE_OPTIONS_ANNOTATION: &str = "argocd.argoproj.io/compare-options";
const SYNC_OPTIONS_ANNOTATION: &str = "argocd.argoproj.io/sync-options";

/// Optional ArgoCD annotation config for CR-creating writers. The
/// zero value ([`Default`]) is fully disabled and produces no
/// annotations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArgoCdConfig {
    /// Explicit primary gate. When `false`, [`Self::annotations`]
    /// returns `None` regardless of every other field. Defensive: an
    /// operator who flips `admin.argocd.enabled: false` must NEVER
    /// see `argocd.argoproj.io/*` annotations on runtime-created
    /// CRs, even if something out-of-band set the other env vars.
    pub enabled: bool,

    /// The ArgoCD Application name (the chart defaults it to the
    /// Helm release name). Empty disables all annotations — both
    /// this and `enabled` must hold.
    pub application_name: String,

    /// Value for `argocd.argoproj.io/compare-options`. Empty skips
    /// the annotation (surfacing the CR as deliberate drift);
    /// the chart default is `IgnoreExtraneous`.
    pub compare_options: String,

    /// Value for `argocd.argoproj.io/sync-options`, passed through
    /// verbatim (`Prune=false`, `Delete=false`, or both,
    /// comma-separated). Empty skips the annotation; the chart
    /// default is `Delete=false`.
    pub sync_options: String,
}

impl ArgoCdConfig {
    /// Read the chart-emitted `KAAS_ARGOCD_*` env. `enabled`
    /// requires the literal `"true"` (the chart's documented
    /// contract); the option strings distinguish "set to empty"
    /// (skip that annotation) from "not set" only in that both mean
    /// skip — pass-through either way.
    pub fn from_env() -> Self {
        Self {
            enabled: std::env::var("KAAS_ARGOCD_ENABLED").as_deref() == Ok("true"),
            application_name: std::env::var("KAAS_ARGOCD_APPLICATION_NAME").unwrap_or_default(),
            compare_options: std::env::var("KAAS_ARGOCD_COMPARE_OPTIONS").unwrap_or_default(),
            sync_options: std::env::var("KAAS_ARGOCD_SYNC_OPTIONS").unwrap_or_default(),
        }
    }

    /// The `argocd.argoproj.io/*` annotations a CR writer should
    /// stamp on a resource it creates, or `None` when the
    /// integration is off (kube's `ObjectMeta.annotations` takes the
    /// `Option` directly).
    ///
    /// `meta_name` is the resource's `metadata.name` and must
    /// already account for the gh #86 synthesised-name path —
    /// ArgoCD's tree is keyed by `metadata.name`, so the tracking-id
    /// must reference `kaas-topic-<hex>`, not the human-friendly
    /// Kafka name it stands in for.
    pub fn annotations(
        &self,
        group: &str,
        kind: &str,
        namespace: &str,
        meta_name: &str,
    ) -> Option<BTreeMap<String, String>> {
        if !self.enabled || self.application_name.is_empty() {
            return None;
        }
        let mut out = BTreeMap::new();
        out.insert(
            TRACKING_ID_ANNOTATION.to_owned(),
            // ArgoCD's own tracking-id format, replicated so
            // admin-created CRs coexist in the same Application
            // resource tree: <app>:<group>/<kind>:<ns>/<name>.
            format!(
                "{}:{group}/{kind}:{namespace}/{meta_name}",
                self.application_name
            ),
        );
        if !self.compare_options.is_empty() {
            out.insert(
                COMPARE_OPTIONS_ANNOTATION.to_owned(),
                self.compare_options.clone(),
            );
        }
        if !self.sync_options.is_empty() {
            out.insert(
                SYNC_OPTIONS_ANNOTATION.to_owned(),
                self.sync_options.clone(),
            );
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full() -> ArgoCdConfig {
        ArgoCdConfig {
            enabled: true,
            application_name: "kaas".into(),
            compare_options: "IgnoreExtraneous".into(),
            sync_options: "Delete=false".into(),
        }
    }

    #[test]
    fn disabled_produces_nothing_regardless_of_other_fields() {
        let cfg = ArgoCdConfig {
            enabled: false,
            ..full()
        };
        assert_eq!(cfg.annotations("kaas.rs", "KafkaTopic", "kaas", "t"), None);
    }

    #[test]
    fn empty_application_name_produces_nothing() {
        let cfg = ArgoCdConfig {
            application_name: String::new(),
            ..full()
        };
        assert_eq!(cfg.annotations("kaas.rs", "KafkaTopic", "kaas", "t"), None);
    }

    #[test]
    fn tracking_id_uses_argocds_format() {
        let ann = full()
            .annotations("kaas.rs", "KafkaTopic", "kaas", "orders")
            .unwrap();
        assert_eq!(
            ann.get("argocd.argoproj.io/tracking-id")
                .map(String::as_str),
            Some("kaas:kaas.rs/KafkaTopic:kaas/orders")
        );
        assert_eq!(
            ann.get("argocd.argoproj.io/compare-options")
                .map(String::as_str),
            Some("IgnoreExtraneous")
        );
        assert_eq!(
            ann.get("argocd.argoproj.io/sync-options")
                .map(String::as_str),
            Some("Delete=false")
        );
    }

    #[test]
    fn empty_option_strings_skip_their_annotations() {
        let cfg = ArgoCdConfig {
            compare_options: String::new(),
            sync_options: String::new(),
            ..full()
        };
        let ann = cfg
            .annotations("kaas.rs", "KafkaTopic", "kaas", "t")
            .unwrap();
        assert_eq!(ann.len(), 1, "only the tracking-id should remain: {ann:?}");
        assert!(ann.contains_key("argocd.argoproj.io/tracking-id"));
    }
}
