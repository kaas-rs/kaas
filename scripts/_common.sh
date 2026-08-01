#!/usr/bin/env bash
# Shared helpers for the kafka-*.sh test scripts. Source this from each script.
#
#   . "$(dirname "$0")/_common.sh"
#
# Provides:
#   $BOOTSTRAP     bootstrap server (override with env var, default in-cluster Service DNS)
#   $KAFKA_BIN     path to Apache Kafka shell tools (defaults to /opt/kafka/bin)
#   $NAMESPACE     Kubernetes namespace holding the cluster (defaults to kaas)
#   $STS           broker StatefulSet name (defaults to kaas)
#   $TOPIC         per-run unique test topic name
#   $TMP           per-run scratch dir, auto-cleaned on exit
#   skip "<reason>"   print reason and exit 77 (autoconf "skipped" exit code)
#   need <bin>     skip if a required tool is not on PATH
#   wait_cluster_ready [timeout]  block until the brokers are serving again

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kaas.kaas.svc.cluster.local:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
NAMESPACE="${NAMESPACE:-kaas}"
STS="${STS:-kaas}"
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
  local deadline=$(( SECONDS + ${1:-180} ))
  local ns="$NAMESPACE"
  local sts="$STS"
  local want have

  command -v kubectl >/dev/null 2>&1 || return 0
  want="$(kubectl -n "$ns" get sts "$sts" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
  [ -n "$want" ] || return 0

  echo ">> waiting for the cluster to settle"
  # Deadline rather than an iteration count: each `kubectl` is a process
  # spawn plus an API round trip, so counting iterations would make the
  # timeout argument mean something much longer than seconds.
  while [ "$SECONDS" -lt "$deadline" ]; do
    have="$(kubectl -n "$ns" get sts "$sts" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    [ "${have:-0}" = "$want" ] && break
    sleep 1
  done
  [ "${have:-0}" = "$want" ] ||
    echo ">> WARN: only ${have:-0}/$want brokers Ready before the deadline" >&2

  # Coordinator probe. Bounded and advisory — a persistent failure is
  # the next script's problem to report, not this helper's to mask.
  while [ "$SECONDS" -lt "$deadline" ]; do
    "$KAFKA_BIN/kafka-consumer-groups.sh" \
      --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1 && return 0
    sleep 2
  done
  echo ">> WARN: consumer-group admin calls still failing after the settle wait" >&2
}

if [ ! -x "$KAFKA_BIN/kafka-topics.sh" ]; then
  skip "Kafka CLI not found at $KAFKA_BIN; set KAFKA_BIN=/path/to/kafka/bin"
fi

echo ">> bootstrap: $BOOTSTRAP"
echo ">> kafka bin: $KAFKA_BIN"
