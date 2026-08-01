#!/usr/bin/env bash
# Smoke-tests the gh #28 transaction.timeout.ms reaper at the shell
# level. The reaper is broker-internal and not directly observable
# from Apache shell tools (kafka-transactions describe-producers is
# gh #114-blocked here — see kafka-txn-coordinator.sh), so we
# exercise it indirectly:
#
#   1. The broker must respond to InitProducerId for a transactional
#      producer (i.e. the txn coordinator route is wired). Without
#      this, the reaper has no slot files to sweep.
#   2. The broker's clusterDir on the shared PVC must contain a
#      txn_state/ subdirectory once a transactional producer has
#      initialised — that's the gh #29 file-shaped __transaction_state.
#
# If we're not in-cluster we can't peek at /data so scenario 2
# self-skips. That's fine — the smoke test still proves the broker
# advertises the wire API.

. "$(dirname "$0")/_common.sh"

echo ">> Scenario 1: broker accepts InitProducerId for a transactional.id"
# kafka-verifiable-producer in 4.x dropped --transactional-id, so we
# don't have a CLI to drive InitProducerId directly. The ApiVersions
# probe in kafka-txn-coordinator.sh covers the wire surface. Here we
# only re-assert that the broker is reachable and lists key 22 in
# its ApiVersions response — the load-bearing precondition for the
# reaper to have anything to sweep.
"$KAFKA_BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" \
  > "$TMP/api.out" 2>&1
if ! grep -qE "^[[:space:]]*(22|InitProducerId\(22\))[[:space:]]*:" "$TMP/api.out"; then
  echo "FAIL: API key 22 (InitProducerId) not advertised — txn reaper can't engage" >&2
  exit 1
fi
echo "   InitProducerId advertised, reaper precondition met"

echo ">> Scenario 2: cluster txn_state/ directory present on shared PVC"
# Only meaningful in-cluster: exec into a broker pod if kubectl is on
# PATH and one exists. Otherwise skip — this scenario is purely a
# deployment-time smoke test, not a wire-protocol assertion.
if ! command -v kubectl >/dev/null 2>&1; then
  echo "   kubectl not present, scenario 2 skipped (run inside the cluster to exercise)"
  echo ">> PASS (wire surface OK; deployment-time slot-dir check skipped)"
  exit 0
fi

# Select on the chart's labels, and pin the component: the operator
# carries the same `name` label, and it does not mount the cluster-state
# volume this scenario inspects.
pod=$(kubectl -n "$NAMESPACE" get pods \
  -l app.kubernetes.io/name=kaas,app.kubernetes.io/component=broker \
  -o name 2>/dev/null | head -1)
if [ -z "$pod" ]; then
  echo "   no kaas broker pod found in namespace $NAMESPACE, scenario 2 skipped"
  echo ">> PASS (wire surface OK; deployment-time slot-dir check skipped)"
  exit 0
fi

# Ask the broker where its cluster state lives rather than assuming.
# gh #221 phase 1 moved it off the data volume onto its own PVC, so
# `KAAS_CLUSTER_DIR` is `/cluster` on a chart deploy and only falls back
# to `<data dir>/__cluster` on the classic single-volume layout. A
# hardcoded path here doesn't fail loudly — it lands in the "not yet
# populated" branch below and reports a healthy cluster as first-boot.
cluster_dir=$(kubectl -n "$NAMESPACE" get "$pod" -o jsonpath='{.spec.containers[?(@.name=="broker")].env[?(@.name=="KAAS_CLUSTER_DIR")].value}' 2>/dev/null)
if [ -z "$cluster_dir" ]; then
  data_dir=$(kubectl -n "$NAMESPACE" get "$pod" -o jsonpath='{.spec.containers[?(@.name=="broker")].env[?(@.name=="KAAS_DATA_DIR")].value}' 2>/dev/null)
  cluster_dir="${data_dir:-/data}/__cluster"
fi
echo "   cluster-state dir: $cluster_dir"

if kubectl -n "$NAMESPACE" exec "$pod" -c broker -- test -d "$cluster_dir/txn_state" 2>/dev/null; then
  slot_count=$(kubectl -n "$NAMESPACE" exec "$pod" -c broker -- \
    sh -c "ls '$cluster_dir/txn_state' 2>/dev/null | wc -l" | tr -d '[:space:]')
  echo "   $cluster_dir/txn_state exists on $pod (slot files: $slot_count)"
else
  # First-boot case: the dir is created lazily on the first
  # GetOrAllocate. Empty cluster = empty dir = OK, not a FAIL.
  echo "   $cluster_dir/txn_state not yet populated (first-boot / no txn producer yet) — OK"
fi

echo ">> PASS (gh #28 reaper preconditions satisfied)"
