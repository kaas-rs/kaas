# kaas-auth

SCRAM-SHA-512, SASL PLAIN, and SASL/OAUTHBEARER, mTLS principal mapping, ACL evaluation, and debt-carrying client quotas — loaded from operator-written files with hot-reload.

The security crate, deliberately split along the axis the
[architecture chapter](../architecture/listeners-auth.md) explains:
authentication is per-listener, authorization and quotas are cluster-wide.

**Authentication**: `scram.rs` (the SCRAM-SHA-512 server state machine —
SCRAM-SHA-256 is *not* implemented), `plain.rs` (SASL PLAIN, only offered
over TLS), `oauth.rs` (SASL/OAUTHBEARER, KIP-255: RFC 7628 framing + JWT
validation against a hot-swapped JWKS, `alg` allowlisted, fail-closed
before the first key fetch — the fetch loop itself lives in `bins/kaas`),
`mtls.rs` (peer-cert principal extraction) +
`principal_mapping.rs` (Apache's `ssl.principal.mapping.rules` syntax,
KIP-371 — parse errors fail at startup), `engine.rs` + `selector.rs` (the
`AuthEngine` seam and its per-listener selection), `credentials.rs` (the
Strimzi-shape `credentials.json` loader).

**Authorization & quotas**: `acls.rs` (the `acls.json` loader and ACL
engine — deny overrides allow; literal, prefixed, and `*` pattern types per
KIP-290), `authorizer.rs` (`AllowAllAuthorizer` + the super-user
early-allow wrapper), `quota.rs` (token buckets with **debt-carry** — the
gh #125 fix that stops N concurrent clients bursting at N× the configured
rate), `types.rs` (`Principal`, `Resource`, `Operation`).

**Operational contract**: both JSON files are written by the operator and
hot-reloaded by the broker — no restart on user/ACL changes, no Kubernetes
call on the request path. `KAAS_AUTH_DISABLED=true` swaps in the allow-all
engine everywhere.

**Start reading at** `engine.rs` for the seams, then `scram.rs` against an
RFC 5802 refresher, then `quota.rs` (the debt-carry test
`multi_client_contention_carries_debt` is the spec).
