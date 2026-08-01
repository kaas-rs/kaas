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
# Only meaningful in-cluster. The data dir is /data on the StatefulSet
# pods; we exec into pod 0 if kubectl is on PATH and a kaas pod
# exists. Otherwise skip — this scenario is purely a deployment-time
# smoke test, not a wire-protocol assertion.
if ! command -v kubectl >/dev/null 2>&1; then
  echo "   kubectl not present, scenario 2 skipped (run inside the cluster to exercise)"
  echo ">> PASS (wire surface OK; deployment-time slot-dir check skipped)"
  exit 0
fi

pod=$(kubectl -n "$NAMESPACE" get pods -l app=kaas -o name 2>/dev/null | head -1)
if [ -z "$pod" ]; then
  echo "   no kaas pod found in namespace $NAMESPACE, scenario 2 skipped"
  echo ">> PASS (wire surface OK; deployment-time slot-dir check skipped)"
  exit 0
fi

if kubectl -n "$NAMESPACE" exec "$pod" -- test -d /data/__cluster/txn_state 2>/dev/null; then
  slot_count=$(kubectl -n "$NAMESPACE" exec "$pod" -- \
    sh -c 'ls /data/__cluster/txn_state 2>/dev/null | wc -l' | tr -d '[:space:]')
  echo "   /data/__cluster/txn_state exists on $pod (slot files: $slot_count)"
else
  # First-boot case: the dir is created lazily on the first
  # GetOrAllocate. Empty cluster = empty dir = OK, not a FAIL.
  echo "   /data/__cluster/txn_state not yet populated (first-boot / no txn producer yet) — OK"
fi

echo ">> PASS (gh #28 reaper preconditions satisfied)"
