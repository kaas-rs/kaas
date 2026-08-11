# kaas-observability

The OTLP metrics + tracing bootstrap, the `/healthz` HTTP handler, and the byte-opacity tripwire counters.

The content of this crate — what gets exported where, the `/healthz`
runtime view, why gauges read through lock-free snapshots — is covered in
the [Observability architecture chapter](../architecture/observability.md);
this page is the code map.

**Module map**: `bootstrap.rs` (OTel SDK bring-up from
`OTEL_EXPORTER_OTLP_*` env; metrics push OTLP/HTTP http/protobuf — the only
dialect Prometheus's native OTLP receiver speaks; traces OTLP/gRPC),
`metrics.rs` (the Arc-shared `Metrics` registry — `global()` returns a
no-op registry before bootstrap, so pre-boot code and tests never
nil-check), `gauges.rs` (the `GaugeSource` seam the broker feeds partition
gauges through), `health.rs` (axum `/healthz` + `/readyz`; the
`RuntimeState` trait), `byteopacity.rs` (the tripwire counters),
`otlp_push_observer.rs` (a `PushMetricExporter` wrapper that makes OTLP
push failures alertable, gh #121), `k8s_api.rs` (K8s API call metrics),
`topic_traffic.rs`, `tracing.rs`
(`tracing-subscriber` + OTel layer; every log line carries
`trace_id`/`span_id` when a span is active).

**Invariant callers must hold**: gauge callbacks and `RuntimeState`
implementations must never take hot-path locks — the gh #134 outage
(a stuck NFS fsync starving the metrics pipeline) is why the storage
engine exposes lock-free snapshots for exactly these readers.

**Start reading at** `health.rs` (the `RuntimeState` trait is a compact
inventory of what the broker considers its own vital signs).
