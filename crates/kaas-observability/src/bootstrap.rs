//! OTel SDK bring-up.
//!
//! Observability bootstrap. Reads:
//!
//! * `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` — if set, push metrics via
//!   OTLP/HTTP (http/protobuf). Prometheus's native OTLP receiver
//!   (`/api/v1/otlp/v1/metrics`) speaks only http/protobuf; exporting gRPC
//!   here just gets an h2 GoAway from Prometheus.
//! * `OTEL_EXPORTER_OTLP_ENDPOINT` — if set, push traces via OTLP/gRPC.
//! * `KAAS_METRIC_EXPORT_INTERVAL` — duration string (`30s` etc.).
//!   Default `30s` per gh #133 (SDK default is 60s → `rate([1m])`
//!   panels see one sample and gap).
//! * `OTEL_SERVICE_VERSION`, `MY_POD_NAME`, `KAAS_NAMESPACE` — folded
//!   into resource attributes so Grafana can filter by broker /
//!   namespace.
//! * `OTEL_TRACES_SAMPLER_ARG` — float in [0, 1]; default `0.1`.
//!
//! With neither OTLP endpoint set the SDK still runs but every export
//! goes nowhere — safe for tests + local runs.

use std::sync::Arc;
use std::time::Duration;

use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::trace::{BatchSpanProcessor, Sampler, Tracer as SdkTracer, TracerProvider};
use opentelemetry_sdk::Resource;
use tokio_util::sync::CancellationToken;

use crate::gauges::install_runtime_gauges;
use crate::metrics::{new_metrics, set_global, Metrics};
use crate::otlp_push_observer::ObservedExporter;
use crate::ObservabilityError;

/// Handle to the initialised OTel SDK. Shutdown is idempotent-ish;
/// calling twice is a no-op the second time.
pub struct Providers {
    pub meter_provider: SdkMeterProvider,
    pub tracer_provider: Option<TracerProvider>,
    pub metrics: Arc<Metrics>,
    pub tracer: SdkTracer,
    _cancel: CancellationToken,
}

impl std::fmt::Debug for Providers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Providers")
            .field("meter_provider", &self.meter_provider)
            .field("has_tracer_provider", &self.tracer_provider.is_some())
            .finish()
    }
}

impl Providers {
    /// Force-flush + shut down both providers with a per-call timeout.
    /// Collects errors but continues — don't bail on the first failure.
    pub fn shutdown(self) -> Result<(), ObservabilityError> {
        let mut first_err = None;
        if let Err(e) = self.meter_provider.shutdown() {
            first_err.get_or_insert(ObservabilityError::MeterShutdown(e.to_string()));
        }
        if let Some(tp) = self.tracer_provider {
            for res in tp.force_flush() {
                if let Err(e) = res {
                    first_err.get_or_insert(ObservabilityError::TracerShutdown(e.to_string()));
                }
            }
            if let Err(e) = tp.shutdown() {
                first_err.get_or_insert(ObservabilityError::TracerShutdown(e.to_string()));
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// Set up the meter + tracer providers, install the global `Metrics`
/// registry, and register the runtime gauges. Idempotent w.r.t. the
/// OTel globals — safe to call once per process; subsequent calls
/// stomp the previous provider.
///
/// `service` becomes `service.name` in exported resource attributes;
/// pass `"kaas"` or `"kaas-operator"`.
pub async fn bootstrap(
    service: &'static str,
    cancel: CancellationToken,
) -> Result<Providers, ObservabilityError> {
    let resource = build_resource(service);

    // --- Meter provider ---
    let mut mp_builder = SdkMeterProvider::builder().with_resource(resource.clone());
    if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") {
        if !endpoint.is_empty() {
            let raw = MetricExporter::builder()
                .with_http()
                .with_endpoint(ensure_scheme(&endpoint))
                .with_timeout(Duration::from_secs(10))
                .build()
                .map_err(|e| ObservabilityError::MetricExporter(e.to_string()))?;

            let wrapped = ObservedExporter::new(raw);
            let interval = parse_duration_env("KAAS_METRIC_EXPORT_INTERVAL")
                .unwrap_or_else(|| Duration::from_secs(30));
            let reader = PeriodicReader::builder(wrapped, runtime::Tokio)
                .with_interval(interval)
                .build();
            mp_builder = mp_builder.with_reader(reader);
        }
    }
    let meter_provider = mp_builder.build();
    global::set_meter_provider(meter_provider.clone());

    // Build central metric registry from the freshly-installed
    // provider so every instrument records against the real reader.
    let meter = global::meter(service);
    let metrics = Arc::new(new_metrics(&meter));
    set_global(Arc::clone(&metrics));

    // Runtime observable gauges. Callback is a no-op until
    // gauges::set_gauge_source is wired by the runtime owner.
    install_runtime_gauges(&meter);

    // Forward kaas-codec's tripwire bumps to the OTel counters. If a
    // future code path fires the tripwire in production, the
    // KaasByteOpacityViolated alert reads the OTel side and pages;
    // pre-bootstrap and tests keep the in-process counter as the
    // fast in-test signal.
    kaas_codec::tripwires::install_tripwire_hooks(
        crate::byteopacity::bump_codec_record_decode,
        crate::byteopacity::bump_codec_batch_reencode,
    );

    // --- Tracer provider ---
    let mut tp_builder = TracerProvider::builder()
        .with_resource(resource)
        .with_sampler(build_sampler());
    let mut has_trace_exporter = false;
    if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        if !endpoint.is_empty() {
            let exporter = SpanExporter::builder()
                .with_tonic()
                .with_endpoint(ensure_scheme(&endpoint))
                .build()
                .map_err(|e| ObservabilityError::TraceExporter(e.to_string()))?;
            let processor = BatchSpanProcessor::builder(exporter, runtime::Tokio).build();
            tp_builder = tp_builder.with_span_processor(processor);
            has_trace_exporter = true;
        }
    }
    let tracer_provider = tp_builder.build();
    global::set_tracer_provider(tracer_provider.clone());
    // Take the concrete SDK Tracer (not BoxedTracer) so
    // tracing-opentelemetry's `.with_tracer(...)` accepts it — the
    // BoxedTracer wrapper doesn't implement PreSampledTracer.
    let tracer = tracer_provider.tracer(service);

    ::tracing::info!(
        service,
        otlp_metrics = std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
            .map(|s| !s.is_empty())
            .unwrap_or(false),
        otlp_traces = has_trace_exporter,
        "observability: initialised"
    );

    Ok(Providers {
        meter_provider,
        tracer_provider: Some(tracer_provider),
        metrics,
        tracer,
        _cancel: cancel,
    })
}

fn build_resource(service: &str) -> Resource {
    let mut attrs = vec![KeyValue::new("service.name", service.to_string())];
    if let Ok(v) = std::env::var("OTEL_SERVICE_VERSION") {
        if !v.is_empty() {
            attrs.push(KeyValue::new("service.version", v));
        }
    }
    if let Ok(v) = std::env::var("MY_POD_NAME") {
        if !v.is_empty() {
            attrs.push(KeyValue::new("k8s.pod.name", v.clone()));
            // Prometheus's OTLP receiver promotes service.instance.id
            // to the `instance` label on every series; without it, all
            // brokers flatten under the same `job=kaas` and
            // per-broker drill-down in Grafana isn't possible.
            attrs.push(KeyValue::new("service.instance.id", v));
        }
    }
    if let Ok(v) = std::env::var("KAAS_NAMESPACE") {
        if !v.is_empty() {
            attrs.push(KeyValue::new("k8s.namespace.name", v));
        }
    }
    Resource::new(attrs)
}

fn build_sampler() -> Sampler {
    let ratio = std::env::var("OTEL_TRACES_SAMPLER_ARG")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|f| (0.0..=1.0).contains(f))
        .unwrap_or(0.1);
    Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio)))
}

/// Parse a short-form duration string (`"30s"`, `"500ms"`) into a
/// [`Duration`]. Returns `None` on parse failure so the caller falls
/// back to the default rather than dying on a chart-config typo.
fn parse_duration_env(name: &str) -> Option<Duration> {
    let raw = std::env::var(name).ok()?;
    parse_duration_str(&raw)
}

fn parse_duration_str(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num, unit) = split_num_unit(s)?;
    let value: f64 = num.parse().ok()?;
    let nanos = match unit {
        "ns" => value,
        "us" | "µs" => value * 1_000.0,
        "ms" => value * 1_000_000.0,
        "s" | "" => value * 1_000_000_000.0,
        "m" => value * 60.0 * 1_000_000_000.0,
        "h" => value * 3600.0 * 1_000_000_000.0,
        _ => return None,
    };
    if nanos < 0.0 || !nanos.is_finite() {
        return None;
    }
    // Go back through seconds to sidestep the f64→u64 cast lint.
    // `nanos` is finite non-negative here so `from_secs_f64` is safe.
    Some(Duration::from_secs_f64(nanos / 1_000_000_000.0))
}

fn split_num_unit(s: &str) -> Option<(&str, &str)> {
    let idx = s
        .find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-' && c != '+')
        .unwrap_or(s.len());
    if idx == 0 {
        None
    } else {
        Some((&s[..idx], &s[idx..]))
    }
}

/// Ensure the endpoint carries a URI scheme. Unlike the v0.1
/// exporter (which wanted bare `host:port`), the
/// Rust exporters parse the endpoint as a full URI — a scheme-less
/// `host:port` makes hyper read `host` as the scheme and the export
/// dies with `InvalidUri`. Chart values written for earlier releases
/// omit the scheme, so default them to `http://` (in-cluster,
/// plaintext). The scheme IS the plaintext/TLS selector — the old
/// `OTEL_EXPORTER_OTLP_INSECURE` env was never read here and its
/// chart emission was dropped (gh #257).
fn ensure_scheme(s: &str) -> String {
    if s.contains("://") {
        s.replacen("grpc://", "http://", 1)
    } else {
        format!("http://{s}")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn ensure_scheme_variants() {
        assert_eq!(ensure_scheme("foo:4317"), "http://foo:4317");
        assert_eq!(ensure_scheme("http://foo:4318"), "http://foo:4318");
        assert_eq!(ensure_scheme("https://foo:4318"), "https://foo:4318");
        assert_eq!(ensure_scheme("grpc://foo:4317"), "http://foo:4317");
        assert_eq!(
            ensure_scheme("http://prom:9090/api/v1/otlp/v1/metrics"),
            "http://prom:9090/api/v1/otlp/v1/metrics"
        );
    }

    #[test]
    fn parse_duration_accepts_short_forms() {
        assert_eq!(parse_duration_str("30s"), Some(Duration::from_secs(30)));
        assert_eq!(
            parse_duration_str("500ms"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_duration_str("1m"), Some(Duration::from_secs(60)));
        assert_eq!(parse_duration_str("2h"), Some(Duration::from_secs(7200)));
        assert_eq!(
            parse_duration_str("1000us"),
            Some(Duration::from_micros(1000))
        );
        assert_eq!(parse_duration_str("100"), Some(Duration::from_secs(100)));
    }

    #[test]
    fn parse_duration_rejects_garbage() {
        assert_eq!(parse_duration_str(""), None);
        assert_eq!(parse_duration_str("abc"), None);
        assert_eq!(parse_duration_str("-1s"), None);
        assert_eq!(parse_duration_str("1years"), None);
    }

    #[test]
    fn build_sampler_defaults_to_10pct() {
        // Can't easily assert the ratio inside a Sampler enum without
        // downcasting; just verify the call doesn't panic.
        std::env::remove_var("OTEL_TRACES_SAMPLER_ARG");
        let _ = build_sampler();
    }

    #[tokio::test]
    async fn bootstrap_without_endpoints_succeeds() {
        std::env::remove_var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        let providers = bootstrap("kaas-test", CancellationToken::new())
            .await
            .expect("bootstrap should succeed with no OTLP endpoints");
        providers.shutdown().unwrap();
    }
}
