#!/usr/bin/env bash
# Test kafka-cluster.sh against kaas.
#
# Scenarios:
#   1. cluster-id returns a non-empty cluster ID

. "$(dirname "$0")/_common.sh"

echo ">> Scenario 1: cluster-id"
# Java tool prints "Cluster ID: <id>" with colon separator, not equals.
# Extract everything after the first colon, trim leading spaces.
id=$("$KAFKA_BIN/kafka-cluster.sh" cluster-id --bootstrap-server "$BOOTSTRAP" \
  | sed -n 's/^Cluster ID:[[:space:]]*//p' | head -1)
echo "cluster-id=$id"
[ -n "$id" ] || { echo "FAIL: empty cluster-id" >&2; exit 1; }

echo ">> Scenario 2: AdminClient.describeCluster() round-trip via kafka-broker-api-versions"
# DescribeCluster (API 60) is flexible from v0 — there is no legacy
# encoding to fall back on, so a wrong header entry in the codec
# registry misparses every request rather than only the new versions.
# kafka-broker-api-versions internally calls DescribeCluster on modern
# clients; a successful run here means both the header parsing and the
# response shape are correct.
"$KAFKA_BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" 2>"$TMP/api.err" >/dev/null
if grep -q 'DescribeCluster\|unexpected EOF\|UNKNOWN_API' "$TMP/api.err"; then
  echo "FAIL: DescribeCluster error surfaced via kafka-broker-api-versions:" >&2
  cat "$TMP/api.err" >&2
  exit 1
fi

echo ">> PASS"
