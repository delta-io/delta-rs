---
applyTo: "crates/core/**,crates/kernel-executor/**,crates/deltalake/**"
---

# Rust core review guidance

deltalake-core holds the kernel, transaction log, and table operations.

- Public API: new public structs, enums, functions, and non-obvious members
  need `///` rustdoc; new enums that may grow should be `#[non_exhaustive]`;
  log-action serde must tolerate unknown fields. Public API mirroring an
  upstream crate (parquet-rs, object_store, datafusion) should match the
  upstream naming.
- Errors: no `unwrap`/`expect`/panics outside tests. Add variants to the
  crate's thiserror error enums; messages must describe the actual condition
  and include the offending value or path.
- Protocol: changes to the transaction log, protocol/WriterFeatures, or
  stored metadata must match PROTOCOL.md and remain parseable by
  delta-spark. Watch integer casts on version numbers (e.g. a `-1` sentinel
  cast to `u64` becomes `u64::MAX`).
- Transactions: changes under `kernel/transaction/` affect concurrent-writer
  correctness; require tests exercising conflicting commits.
- Async/perf: no blocking I/O or `std::fs` in async code; table storage goes
  through `object_store`. Do not rebuild HTTP clients or connection pools
  per call; avoid speculative `Vec::with_capacity` and large `collect()`s
  that could stream.
- Parallel paths: when a change adds a second implementation alongside an
  existing one (serial vs parallel writer, two scan paths), verify identical
  observable behavior and flag copy-pasted logic that could drift; prefer a
  shared helper.
- Async tests use `#[tokio::test]`; integration tests live in
  `crates/core/tests/`.
