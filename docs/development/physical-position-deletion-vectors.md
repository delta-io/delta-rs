# Physical-position deletion-vector scans

The first change replaces execution-order mask consumption with immutable lookup by
`(compact file identity, absolute physical Parquet row position)`. The complete
selected population and store bindings are captured before asynchronous DV loading.
Missing, duplicate, unexpected, or inconsistent loading results are errors. An
explicitly selected file without a DV is all-live; an unknown identity is an error.
One `Arc<DeletionVectorIndex>` owns the masks across executions and streams. A new
snapshot creates independent visibility even when immutable footer metadata is reused.

There are three separate count proofs:

| Count | Evidence |
| --- | --- |
| Log-declared physical population | Valid selected action `numRecords`; required for a DV, optional otherwise. |
| Footer-verified physical population | Complete footer and consistent row groups/ordinals, compared with applicable log metadata before positional decoding. |
| Exact visible population | Complete selected population and matching validated DV state; metadata-only execution uses the log/DV protocol contract without opening Parquet. |

Sparse masks have an implicit live tail, bounded by the physical population. Reader
coordinates are never reconstructed by counting returned batches. Hidden position
fields use Parquet's nullable Int64 `RowNumber`; their definitions, lineage, and
on-disk collisions are checked. Filtering applies to payload and identity together
before Kernel transformations and strips support fields at the output boundary.

The session's existing metadata cache supplies one retention allowance across stores
and concurrent scans. Private namespaced views tag entries with store-allocation
identity, full object metadata, and footer-validation evidence. Weak store references
prevent allocation identity reuse without retaining unregistered stores. Namespace and
validation storage are charged to the backing cache. Eviction does not release
metadata still held by an active reader. `active_footer_reference_bytes` reports
Parquet's memory estimate summed over live reader references; shared allocations may
be counted more than once and allocator overhead is excluded. It is not RSS.

The correctness change retains DV predicate, file-layout, repartitioning, and
fetch/LIMIT containment. MERGE still uses a one-based surviving-row ordinal. Its
migration is a separate follow-up: indexed visibility alone does not permit moving
predicates or truncation before ordinal assignment. RowTracking restrictions are
independent as well. No public metadata function, durable row identity, decoder-level
DV selection, or physical serialization codec is introduced.

## Qualification and repeatable measurement

The fixture generator's original coordinates and declared deleted-position sets form
the oracle. Reader identity and final logical output are checked separately. Private
reader tests cover the optimizer matrix while production plans remain contained:
page index and row filter on/off, batches 1/7/8192, partitions 1/2/4/8, and file
repartitioning on/off. Fixtures have unequal groups and verified multiple pages.
Additional cases cover malformed counts/ordinals, missing statistics, cache collisions
with equal validation attributes, concurrent snapshots, old-plan reuse, cancellation,
and logical serialization. Native FFI qualification is recorded separately from
Python integration.

Run the scan suite, complete core suite, formatting, strict all-target Clippy, and
incremental diff whitespace checks at the final source head. Python contribution
checks, build, unit tests, and documentation are separate gates. An inherited failure
or unavailable prerequisite remains an unmet gate; it is not a successful check.
Record source/parent SHAs, lock hash, toolchain/target, features/profile, fixture and
harness revisions, commands, and terminal statuses with each result.

`cargo bench --locked -p deltalake-core --features datafusion --bench dv_physical_position`
runs the shared Criterion harness. It covers no/sparse/dense DVs, large and many-small
files, selective predicates, partitions 1/2/4/8, prepared/reset and fresh plans, and
cold/warm metadata, with ten samples per case. Set `DV_BENCH_TELEMETRY` to a JSONL
path for planning/first-read/stream times, I/O requests and bytes, pruning metrics,
cache retention, and sampled active footer references. Capture process peak RSS with
an external process monitor. Use separate processes and identical locks, fixtures,
profiles, and settings for parent and follow-ups. Keep distributions/confidence and
investigate repeatable no-DV regressions above 5%; do not infer an RSS bound from the
cache allowance. Benchmark-only overlays on the parent must be recorded explicitly.
