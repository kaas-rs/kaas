#!/usr/bin/env bash
# Shared helpers for the kafka-*.sh test scripts. Source this from each script.
#
#   . "$(dirname "$0")/_common.sh"
#
# Provides:
#   $BOOTSTRAP     bootstrap server (override with env var, default in-cluster Service DNS)
#   $KAFKA_BIN     path to Apache Kafka shell tools (defaults to /opt/kafka/bin)
#   $TOPIC         per-run unique test topic name
#   $TMP           per-run scratch dir, auto-cleaned on exit
#   skip "<reason>"   print reason and exit 77 (autoconf "skipped" exit code)
#   need <bin>     skip if a required tool is not on PATH

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kaas.kaas.svc.cluster.local:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
TOPIC="${TOPIC:-kaas-test-$$-$(date +%s)}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

skip() {
  echo ">> SKIP: $*" >&2
  exit 77
}

need() {
  command -v "$1" >/dev/null 2>&1 || skip "missing required tool: $1"
}

# wait_cluster_ready [timeout_seconds]
#
# Block until the broker StatefulSet is fully Ready again and the
# cluster answers an admin call that needs a coordinator lookup. Any
# script that deliberately disturbs the cluster must call this before it
# returns, so the next script in the suite starts from a settled cluster
# rather than inheriting a half-finished rollout.
#
# Two signals, because they mean different things:
#
#   * Pod readiness. `/readyz` is serving-gated — a Ready broker has
#     finished taking over every partition assignment.json gives it — so
#     "every replica Ready" is the honest cluster-settled signal, not a
#     bind-time latch. Checking the StatefulSet's readyReplicas covers
#     the peers too, not just the pod that was bounced: a bounce moves
#     partitions onto the survivors, and they are taking over while the
#     restarted pod comes back.
#
#   * A consumer-group admin call. Group coordinators move independently
#     of partition leadership (coordinator-of-G is a hash over the broker
#     set), so a client can find every partition healthy and still time
#     out in FIND_COORDINATOR. That is the failure this helper exists to
#     prevent, so probe it directly rather than inferring it.
#
# No-op when kubectl is unavailable or the StatefulSet is absent, so
# scripts that source this outside a cluster are unaffected.
wait_cluster_ready() {
  local timeout="${1:-180}"
  local ns="${NAMESPACE:-kaas}"
  local sts="${STS:-kaas}"
  local i want have

  command -v kubectl >/dev/null 2>&1 || return 0
  kubectl -n "$ns" get sts "$sts" >/dev/null 2>&1 || return 0

  echo ">> waiting for the cluster to settle"
  for ((i = 0; i < timeout; i++)); do
    want="$(kubectl -n "$ns" get sts "$sts" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
    have="$(kubectl -n "$ns" get sts "$sts" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    if [ -n "$want" ] && [ "${have:-0}" = "$want" ]; then
      break
    fi
    sleep 1
  done
  if [ "${have:-0}" != "${want:-}" ]; then
    echo ">> WARN: only ${have:-0}/${want:-?} brokers Ready after ${timeout}s" >&2
  fi

  # Coordinator probe. Bounded and advisory — a persistent failure is
  # the next script's problem to report, not this helper's to mask.
  for ((i = 0; i < 30; i++)); do
    if "$KAFKA_BIN/kafka-consumer-groups.sh" \
        --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo ">> WARN: consumer-group admin calls still failing after the settle wait" >&2
  return 0
}

if [ ! -x "$KAFKA_BIN/kafka-topics.sh" ]; then
  skip "Kafka CLI not found at $KAFKA_BIN; set KAFKA_BIN=/path/to/kafka/bin"
fi

echo ">> bootstrap: $BOOTSTRAP"
echo ">> kafka bin: $KAFKA_BIN"
