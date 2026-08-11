# kaas-test-harness

Shared test helpers — reserved as the only place in the workspace where a decoded-record representation would be allowed to live.

Still an empty placeholder crate (a doc comment, no code, no dependents),
to be populated as integration tests need it. Its charter is narrow and
deliberate: shared fixtures and record-construction helpers for tests.
Production crates must never grow a decoded-record type — when a test needs
one, it belongs here, where the tripwire counters can't be quietly bypassed
([wire protocol](../compat/wire-protocol.md)).
