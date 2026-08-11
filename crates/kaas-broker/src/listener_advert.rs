//! Advertised endpoints and the live broker catalog.
//!
//! Two APIs answer "who is in this cluster and where do I reach them":
//! Metadata (key 3) and DescribeCluster (key 60). They must agree — a
//! client that bootstrapped on the authed listener and got the
//! anonymous port back from either one loops on SASL retry (gh #125).
//! So the per-listener advertisement rule, the broker-catalog shape,
//! and the controller-id derivation all live here rather than being
//! spelled twice.
//!
//! The rule (Apache's `metadataCache.getAliveBrokerNodes(listenerName)`):
//! every advertised endpoint carries the port of the listener the
//! request arrived on. Peers run the same chart, so a peer's port is
//! this listener's port at that peer's stable FQDN; only self keeps its
//! own advertised host, so external hostname templates still win.

use crate::broker::{Broker, BrokerNode};
use crate::cli::ListenerEntry;

/// Per-listener advertised endpoint, precomputed at handler-build time
/// and keyed by the listener `name` stored on each connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenerAdvert {
    pub name: String,
    pub host: String,
    pub port: i32,
}

/// The configured listener table, resolved to advertised endpoints.
#[derive(Debug, Clone)]
pub struct ListenerAdverts {
    entries: Vec<ListenerAdvert>,
}

impl ListenerAdverts {
    pub fn new(listeners: &[ListenerEntry]) -> Self {
        Self {
            entries: listeners.iter().map(advert_from).collect(),
        }
    }

    /// Advertised endpoint for the listener a connection arrived on.
    ///
    /// An unmatched name should only happen through a wiring bug in
    /// `main.rs`; falling back to the first listener (and finally to
    /// localhost) keeps the response well-formed instead of panicking
    /// mid-request.
    pub fn for_listener(&self, listener_name: &str) -> ListenerAdvert {
        self.entries
            .iter()
            .find(|l| l.name == listener_name)
            .cloned()
            .unwrap_or_else(|| {
                self.entries.first().cloned().unwrap_or(ListenerAdvert {
                    name: "internal".to_owned(),
                    host: "127.0.0.1".to_owned(),
                    port: 9092,
                })
            })
    }
}

fn advert_from(entry: &ListenerEntry) -> ListenerAdvert {
    // Best-effort parse: bad addrs (which shouldn't occur — `Cli`
    // validates earlier) degrade to localhost:9092 so the response
    // stays well-formed.
    let addr: std::net::SocketAddr = entry
        .addr
        .parse()
        .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 9092)));
    let port = i32::from(addr.port());
    let host = match entry.advertised_host.as_deref() {
        Some(h) if !h.is_empty() => h.to_owned(),
        // 0.0.0.0 is a wildcard bind, not a routable target. For dev
        // clients connecting on the same box, localhost is the right
        // echo.
        _ if addr.ip().is_unspecified() => "127.0.0.1".to_owned(),
        _ => addr.ip().to_string(),
    };
    ListenerAdvert {
        name: entry.name.clone(),
        host,
        port,
    }
}

/// The broker catalog to advertise on `advert`'s listener.
///
/// Cluster mode returns the registered broker set with each row's
/// `fenced` flag (gh #249); dev/single-broker mode (no installed
/// view) returns self only. A view that yields no *servable* row
/// degrades to self: the cluster runtime is up but the endpoint watch
/// hasn't delivered yet, or every peer is fenced, and either way a
/// client needs somewhere to go.
///
/// Self is force-unfenced. This broker is answering the request, so a
/// stale not-ready reading of its own endpoint — a readiness blip, or
/// the boot window before takeover completes — must not make it
/// advertise itself as unavailable.
pub fn advertised_brokers(broker: &Broker, advert: &ListenerAdvert) -> Vec<BrokerNode> {
    let self_row = || BrokerNode {
        node_id: broker.broker_id,
        host: advert.host.clone(),
        port: advert.port,
        // Self is never fenced: this broker is answering the request.
        fenced: false,
    };
    match broker.broker_view() {
        Some(view) => {
            let mut v: Vec<BrokerNode> = view
                .brokers()
                .into_iter()
                .map(|b| BrokerNode {
                    node_id: b.node_id,
                    host: if b.node_id == broker.broker_id {
                        advert.host.clone()
                    } else {
                        b.host
                    },
                    port: advert.port,
                    fenced: b.fenced && b.node_id != broker.broker_id,
                })
                .collect();
            if !v.iter().any(|b| !b.fenced) {
                // Every row fenced (or none at all) would leave a
                // client with nowhere to go once Metadata filters.
                // This broker is serving by definition — say so.
                v.retain(|b| b.node_id != broker.broker_id);
                v.push(self_row());
            }
            v
        }
        None => vec![self_row()],
    }
}

/// Node id of the broker holding the `kaas-controller` Lease, per the
/// applied `assignment.json`. Falls back to self when no coordinator is
/// wired (dev mode) or no assignment has been applied yet — Apache's
/// alternative is `-1`, but a client that dials the controller would
/// then have nowhere to go, and on kaas every broker serves the same
/// admin surface anyway.
pub fn controller_id(broker: &Broker) -> i32 {
    broker
        .coordinator()
        .as_ref()
        .and_then(|c| c.snapshot())
        .and_then(|a| trailing_ordinal(&a.controller))
        .unwrap_or(broker.broker_id)
}

/// `"kaas-2"` → `2`. Broker identity strings carry the ordinal as the
/// trailing hyphen segment (StatefulSet pod-name shape); a malformed id
/// yields `None` and the caller falls back to self.
pub fn trailing_ordinal(id: &str) -> Option<i32> {
    id.rsplit('-').next()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, addr: &str, advertised: Option<&str>) -> ListenerEntry {
        ListenerEntry {
            name: name.to_owned(),
            addr: addr.to_owned(),
            advertised_host: advertised.map(str::to_owned),
            tls: None,
            authentication_type: None,
            oauth: None,
        }
    }

    #[test]
    fn wildcard_bind_advertises_localhost() {
        let a = ListenerAdverts::new(&[entry("internal", "0.0.0.0:9092", None)]);
        let got = a.for_listener("internal");
        assert_eq!(got.host, "127.0.0.1");
        assert_eq!(got.port, 9092);
    }

    #[test]
    fn advertised_host_wins_over_bind_addr() {
        let a = ListenerAdverts::new(&[entry(
            "external",
            "0.0.0.0:9094",
            Some("broker-0.example.com"),
        )]);
        assert_eq!(a.for_listener("external").host, "broker-0.example.com");
    }

    #[test]
    fn unknown_listener_falls_back_to_the_first() {
        let a = ListenerAdverts::new(&[
            entry("internal", "0.0.0.0:9092", None),
            entry("authed", "0.0.0.0:9095", None),
        ]);
        assert_eq!(a.for_listener("nonexistent").port, 9092);
    }

    #[test]
    fn empty_listener_table_still_answers() {
        let a = ListenerAdverts::new(&[]);
        let got = a.for_listener("internal");
        assert_eq!((got.host.as_str(), got.port), ("127.0.0.1", 9092));
    }

    #[test]
    fn ordinal_parses_off_the_right() {
        assert_eq!(trailing_ordinal("kaas-2"), Some(2));
        // Slash-bearing / multi-segment names still resolve.
        assert_eq!(trailing_ordinal("kaas-preview-11"), Some(11));
        assert_eq!(trailing_ordinal("kaas"), None);
        assert_eq!(trailing_ordinal(""), None);
    }
}
