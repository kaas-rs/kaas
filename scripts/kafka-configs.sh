#!/usr/bin/env bash
# Test kafka-configs.sh against kaas.
#
# Scenarios:
#   1. --describe broker config (read path)
#   2. --describe topic config (read path)
#   3. --alter topic config, then assert --describe reports it as a
#      dynamic (non-default) config — the gh #236 round trip. Also
#      asserts an unknown key is rejected rather than silently accepted.
#   4. --describe --all (broad DescribeConfigs surface).
#   5. --describe specific broker by id.
#   6-9. Client quota CRUD via AlterClientQuotas / DescribeClientQuotas
#      (api keys 49 / 48). Currently a GAP (issue #122), expected to fail
#      until the quota engine lands.

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1

echo ">> Scenario 1: --describe broker config"
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type brokers --entity-default --describe

echo ">> Scenario 2: --describe topic config for '$TOPIC'"
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type topics --entity-name "$TOPIC" --describe

echo ">> Scenario 3: --alter topic config retention.ms, then round-trip via --describe"
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type topics --entity-name "$TOPIC" \
  --alter --add-config retention.ms=60000
# The change is CR-mediated (broker patch → operator reconcile → .config.json),
# so give the operator a moment before asserting the read side.
deadline=$((SECONDS + 30))
until "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
        --entity-type topics --entity-name "$TOPIC" --describe 2>&1 \
      | grep -q 'retention.ms=60000'; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    # gh #236: --describe (non---all) prints only non-default entries, so
    # this fails both when the alter was dropped and when the override is
    # misreported as DEFAULT_CONFIG.
    echo "FAIL: retention.ms=60000 not reported as a dynamic config within 30s (gh #236)" >&2
    exit 1
  fi
  sleep 1
done
echo "override round-tripped as a dynamic topic config"

echo ">> Scenario 3b: --alter with an unknown key must be rejected, not swallowed"
if "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
     --entity-type topics --entity-name "$TOPIC" \
     --alter --add-config max.message.bytes=1048576 2>&1; then
  echo "FAIL: unsupported key max.message.bytes was accepted — silent-drop regression (gh #236)" >&2
  exit 1
else
  echo "(expected) unsupported key rejected with an error"
fi

echo ">> Scenario 4: --describe with --all (every config key, not just overridden)"
# Exercises the broader DescribeConfigs surface — every key
# returned must have its source (DEFAULT_CONFIG, TOPIC_CONFIG, etc).
# Pre-#93 only overridden keys were returned; the broker config
# should now expose its full key set.
out=$("$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type topics --entity-name "$TOPIC" --describe --all 2>&1)
echo "$out" | head -20
echo "$out" | grep -q 'cleanup.policy' || { echo "FAIL: cleanup.policy not in --describe --all output" >&2; exit 1; }

echo ">> Scenario 5: --describe specific broker config"
# Per-broker (id=0) describe vs default. Required for tools like
# kafbat-ui that query individual brokers.
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type brokers --entity-name 0 --describe 2>&1 | head -5

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

# ---------------------------------------------------------------------------
# Client quota scenarios (issue #122 — accept-all-4-keys, enforce-3).
# Every quota path goes via AlterClientQuotas (api_key=49) and
# DescribeClientQuotas (api_key=48). The tool also drives the broker's
# Produce/Fetch throttling — see scenario 9 for the live-throttle probe.
# ---------------------------------------------------------------------------

QUOTA_USER="kaas-quota-test-$$"

# Producer quota target for the scenarios below: 10 MB/s
# (10 * 1024 * 1024 = 10_485_760 B/s). The probe in scenario 10 pushes
# substantially more than the quota's worth of bytes in <1s of wall clock
# so the throttle has to engage; if the broker is uncapped, the perf tool
# reports MB/sec well above the limit.
QUOTA_PRODUCER_BPS=10485760
QUOTA_CONSUMER_BPS=20971520

echo ">> Scenario 6 (XFAIL, gap #122): --alter user quota (producer_byte_rate=10 MB/s)"
# Once #122 lands, the broker must (a) accept the alter, (b) persist it
# (file or in-memory), (c) return it on describe.
if "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
     --entity-type users --entity-name "$QUOTA_USER" \
     --alter --add-config "producer_byte_rate=$QUOTA_PRODUCER_BPS" 2>&1; then
  echo "UNEXPECTED PASS — quota engine may be live; verify scenario 7 + 10 pass too, then close #122."
else
  echo "(expected) alter rejected — quota engine not implemented yet (#122)"
fi

echo ">> Scenario 7 (XFAIL, gap #122): --describe user quota round-trip"
# Even if scenario 6 returned an error, the tool will issue
# DescribeClientQuotas to render the result. Once #122 is in, this should
# echo back what we just set.
out=$("$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type users --entity-name "$QUOTA_USER" --describe 2>&1 || true)
echo "$out" | head -5
if echo "$out" | grep -q "producer_byte_rate=$QUOTA_PRODUCER_BPS"; then
  echo "UNEXPECTED PASS — describe returned the alter we just made; #122 ready to close."
else
  echo "(expected) describe didn't round-trip producer_byte_rate (#122)"
fi

echo ">> Scenario 8 (XFAIL, gap #122): --alter all 4 Apache quota keys"
# Wire-protocol compatibility check: kaas must accept all 4 Apache keys
# (including controller_mutation_rate, which it stores but doesn't enforce —
# accept-but-no-op for round-trip compat with kafka-configs.sh that round-
# trips a full quota config). Tools that re-issue every field on a
# single-field edit shouldn't break.
if "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
     --entity-type users --entity-name "$QUOTA_USER" \
     --alter --add-config "producer_byte_rate=$QUOTA_PRODUCER_BPS,consumer_byte_rate=$QUOTA_CONSUMER_BPS,request_percentage=50,controller_mutation_rate=10" 2>&1; then
  echo "UNEXPECTED PASS — 4-key alter accepted; verify --describe round-trip on all four."
else
  echo "(expected) 4-key alter rejected (#122)"
fi

echo ">> Scenario 9 (XFAIL, gap #122): --describe with --entity-default (user-level default)"
# Default-entity precedence (rule 6 in issue #122's 8-level hierarchy):
# 'users/<default>' applies to every authenticated principal that doesn't
# have a more specific override.
out=$("$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type users --entity-default --describe 2>&1 || true)
echo "$out" | head -5
echo "(observed default-user quota config above; should round-trip an alter)"

echo ">> Scenario 10 (XFAIL, gap #122): live throttle probe at 10 MB/s cap"
# End-to-end check that the throttle is server-enforced (KIP-219 ordering:
# server sends response with throttle_time_ms then mutes). User is pinned
# at producer_byte_rate=10 MB/s (set in scenarios 6 + 8 above); the probe
# tries to push 100 MB (100k * 1KB records) with --throughput -1 (unbounded
# offered rate). Expected outcomes:
#   - Pre-#122: broker uncapped, perf tool reports MB/sec well above 10
#     (single-broker NFS-backed setups peak ~30-60 MB/sec for this shape).
#   - Post-#122: rate plateaus near 10 MB/sec and the run takes ~10s+
#     instead of ~1-3s. Heuristic threshold below picks 15 MB/sec as the
#     pass/fail line (50% margin above the cap).
PROBE_TOPIC="quota-probe-$$"
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$PROBE_TOPIC" --partitions 1 --replication-factor 1 \
  >/dev/null 2>&1 || true
probe_out=$("$KAFKA_BIN/kafka-producer-perf-test.sh" \
  --topic "$PROBE_TOPIC" \
  --num-records 100000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers="$BOOTSTRAP" client.id="$QUOTA_USER" 2>&1 || true)
echo "$probe_out" | tail -3
# Threshold: 15 MB/sec. Below = throttle is plausibly engaged. Above =
# broker is uncapped (pre-#122 expected behavior).
if echo "$probe_out" | grep -Eq '\([0-9]+\.[0-9]+ MB/sec\)' && \
   echo "$probe_out" | awk '/records\/sec/ { for(i=1;i<=NF;i++) if($i=="MB/sec)") { gsub(/\(/,"",$(i-1)); if($(i-1)+0 < 15.0) exit 0; else exit 1; } } END { exit 1 }'; then
  echo "UNEXPECTED PASS — observed rate <15 MB/sec under a 10 MB/sec quota; throttle is firing. Verify request latency reflects throttle_time_ms before closing #122."
else
  echo "(expected) producer ran uncapped at >15 MB/s — quota not enforced (#122)"
fi

"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type users --entity-name "$QUOTA_USER" \
  --alter --delete-config 'producer_byte_rate,consumer_byte_rate,request_percentage,controller_mutation_rate' \
  >/dev/null 2>&1 || true
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --delete --topic "$PROBE_TOPIC" >/dev/null 2>&1 || true

echo ">> PASS (config read/alter round trip + quota XFAILs as expected pending #122)"
