#!/usr/bin/env bash
# Test kafka-acls.sh against kaas (gh #107: admin-protocol CreateAcls /
# DeleteAcls / DescribeAcls persist via the KafkaUser CR's inline
# spec.authorization.acls list, gh #135).
#
# Kaas's CR model only stores ACLs on KafkaUser, so the writer rejects
# CreateAcls for a principal that has no corresponding CR. This script
# applies a temporary KafkaUser with the operator's auto-generated SCRAM
# Secret (gh #136 — password Secret is optional), runs the scenarios,
# then deletes the CR + the operator-owned credentials Secret.
#
# Requires auth + an admin principal:
#   ADMIN_PROPS=/path/to/admin-client.properties
# Requires kubectl in scope of the kaas namespace (override via
# NAMESPACE=...).
#
# Scenarios:
#   1. --list (initial state)
#   2. --add an ACL: User:<temp> can READ topic <acl-test>
#   3. --list shows the new ACL
#   4. --remove and verify it's gone

. "$(dirname "$0")/_common.sh"

ADMIN_PROPS="${ADMIN_PROPS:-}"
EXTRA=()
[ -n "$ADMIN_PROPS" ] && EXTRA+=(--command-config "$ADMIN_PROPS")

# ADMIN_PROPS is optional: kaas's chart ships a `plain` listener
# (authentication.type=none) on :9092, which is also the in-cluster
# bootstrap default. AdminClient writes through unauthenticated on
# that listener and the gh #107 ACL writer still persists to the
# KafkaUser CR (authorization is independent of authentication, per
# CLAUDE.md). Set ADMIN_PROPS=/path/to/admin.properties when you want
# to exercise the same flow against a SASL/SCRAM listener.

need kubectl

# Per-run unique user so concurrent invocations don't collide and a
# crashed previous run can't leave the next one staring at a stale CR.
USER_NAME="acl-test-$$-$(date +%s)"
ACL_TOPIC="acl-test-$$"
PRINCIPAL="User:$USER_NAME"

# Combined cleanup: _common.sh sets its own EXIT trap for $TMP; we
# replace it with this superset so both run on exit, including failure.
cleanup() {
  local rc=$?
  set +e
  kubectl -n "$NAMESPACE" delete kafkausers.kaas.rs "$USER_NAME" --ignore-not-found --wait=false >/dev/null 2>&1
  # Operator-owned output Secret (gh #136 auto-generated path).
  kubectl -n "$NAMESPACE" delete secret "$USER_NAME-kafka-credentials" --ignore-not-found --wait=false >/dev/null 2>&1
  rm -rf "$TMP"
  exit "$rc"
}
trap cleanup EXIT

echo ">> Applying temporary KafkaUser $NAMESPACE/$USER_NAME"
kubectl apply -f - <<EOF
apiVersion: kaas.rs/v1alpha1
kind: KafkaUser
metadata:
  name: $USER_NAME
  namespace: $NAMESPACE
spec:
  authentication:
    type: scram-sha-512
EOF

# Wait for the CR to be visible to the operator. The ACL writer only
# needs Get to succeed (not Ready), so apply+brief readback is enough;
# we don't gate on Status.Conditions[Ready].
for _ in $(seq 1 30); do
  if kubectl -n "$NAMESPACE" get kafkausers.kaas.rs "$USER_NAME" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

echo ">> Scenario 1: --list (initial)"
"$KAFKA_BIN/kafka-acls.sh" --bootstrap-server "$BOOTSTRAP" "${EXTRA[@]}" --list

echo ">> Scenario 2: --add allow $PRINCIPAL READ on topic $ACL_TOPIC"
"$KAFKA_BIN/kafka-acls.sh" --bootstrap-server "$BOOTSTRAP" "${EXTRA[@]}" \
  --add --allow-principal "$PRINCIPAL" --operation READ --topic "$ACL_TOPIC"

echo ">> Scenario 3: --list shows it"
"$KAFKA_BIN/kafka-acls.sh" --bootstrap-server "$BOOTSTRAP" "${EXTRA[@]}" \
  --list --topic "$ACL_TOPIC" | grep -q "$PRINCIPAL"

echo ">> Verify the CR was actually patched (gh #107 contract)"
# The wire response could have been a stub-success; the test that
# distinguishes real persistence from a phantom is checking the CR.
kubectl -n "$NAMESPACE" get kafkausers.kaas.rs "$USER_NAME" -o jsonpath='{.spec.authorization.acls}' \
  | grep -q "\"name\":\"$ACL_TOPIC\"" \
  || { echo "FAIL: ACL not present on KafkaUser/$USER_NAME spec" >&2; exit 1; }

echo ">> Scenario 4: --remove and verify"
"$KAFKA_BIN/kafka-acls.sh" --bootstrap-server "$BOOTSTRAP" "${EXTRA[@]}" \
  --remove --allow-principal "$PRINCIPAL" --operation READ --topic "$ACL_TOPIC" --force
out=$("$KAFKA_BIN/kafka-acls.sh" --bootstrap-server "$BOOTSTRAP" "${EXTRA[@]}" --list --topic "$ACL_TOPIC")
echo "$out" | grep -q "$PRINCIPAL" && { echo "FAIL: ACL still present" >&2; exit 1; }

# CR-side parity check for delete: the entry should be gone (or the
# operations list emptied, which the writer normalises to entry-drop).
remaining=$(kubectl -n "$NAMESPACE" get kafkausers.kaas.rs "$USER_NAME" -o jsonpath='{.spec.authorization.acls}')
echo "$remaining" | grep -q "\"name\":\"$ACL_TOPIC\"" \
  && { echo "FAIL: ACL entry still on KafkaUser/$USER_NAME spec after --remove" >&2; exit 1; }

echo ">> PASS"
