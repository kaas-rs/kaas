#!/usr/bin/env bash
# Test kafka-broker-api-versions.sh against kaas.
#
# Scenarios:
#   1. ApiVersions response lists at minimum the core APIs the broker advertises

. "$(dirname "$0")/_common.sh"

echo ">> Scenario 1: query advertised API versions"
out=$("$KAFKA_BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP")
echo "$out"

# Sanity-check a handful of must-have APIs by name. Extended for gh #1
# to cover the full set every current client needs at bootstrap.
required=(
  Produce Fetch ListOffsets Metadata
  OffsetCommit OffsetFetch FindCoordinator
  JoinGroup SyncGroup Heartbeat LeaveGroup DescribeGroups ListGroups
  SaslHandshake ApiVersions
  CreateTopics DeleteTopics InitProducerId
  DescribeConfigs DescribeCluster
)
# The name alone proves nothing: for a key the broker does NOT serve,
# the Java tool still prints a line — "DescribeCluster(60): UNSUPPORTED"
# — because it enumerates every API the *client* knows. Matching the
# name and stopping there is what let key 60 sit unregistered through a
# green run of this script. Require a version range on the line.
for api in "${required[@]}"; do
  # `|| true`: under `set -euo pipefail` a non-matching grep would
  # abort the script before the FAIL line below ever prints.
  line=$(echo "$out" | grep -E "^[[:space:]]*$api\(" | head -1 || true)
  [ -n "$line" ] || { echo "FAIL: $api not advertised" >&2; exit 1; }
  case "$line" in
    *UNSUPPORTED*) echo "FAIL: $api advertised as UNSUPPORTED: $line" >&2; exit 1 ;;
  esac
done

# gh #1 cross-client check: usable-version negotiation. Each line in
# the tool's output ends with `[usable: N]` where N is the version
# the local Java client picked. -1 means "no overlap" — i.e. the
# broker advertised a version range that doesn't intersect with the
# client's. Any such line is a regression.
if echo "$out" | grep -qE '\[usable: -1\]'; then
  echo "FAIL: at least one API has no usable version overlap with the Java client:" >&2
  echo "$out" | grep -E '\[usable: -1\]' >&2
  exit 1
fi

echo ">> PASS"
