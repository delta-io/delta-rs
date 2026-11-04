# Copilot code review instructions for delta-rs

delta-rs implements the Delta Lake protocol (PROTOCOL.md) in Rust under
`crates/`, with Python bindings under `python/`. Correctness against the
protocol and parity with delta-spark matter more than style.

## Do not comment on

- Formatting, line wrapping, or import ordering: `cargo fmt` and
  `ruff format` are enforced in CI.
- Lint findings: `cargo clippy` and `ruff check` run in CI; reviewers defer
  to them.
- Spelling: a typos workflow runs in CI.
- PR title format, conventional commits, commit sign-off/DCO, and coverage
  percentages: automation already enforces or reports these.
- Generated, vendored, or test fixture data files (including `dat/`).
- Readability or maintainability opinions without a concrete failure mode.
- Verbose or self-explanatory comments unless they are inaccurate.

## Focus review on

These reflect the feedback maintainers actually give on this repository.

- Tests: new behavior, new public functions, and new branches need tests
  that fail when the new code path is disabled; a test that passes either
  way does not cover it.
- Public API docs: new public structs, enums, functions, and non-obvious
  members need documentation in the crate's style, and names should be
  self-explanatory without abbreviations.
- Error handling: no `unwrap`/`expect`/direct indexing outside tests; flag
  error-to-`None` conversions that change caller semantics and error
  messages that misdescribe the actual condition.
- Performance: flag blocking or synchronous I/O inside async code, HTTP
  clients or connection pools rebuilt per call, speculative
  `Vec::with_capacity` where the size is unknown, and large `collect()`s
  that could stream.
- Performance PRs: expect a reproducible scenario and before/after numbers
  (runtime and memory), not just a plausible argument.
- Semver hazards: renamed or removed public items, removed trait impls
  (e.g. `Debug`), and Cargo feature changes that drop transitive features.
  Deprecated APIs must keep working with a deprecation warning for a few
  minor releases.
- Parallel implementations: when a change adds a second code path alongside
  an existing one, verify identical observable behavior and flag
  copy-pasted logic that could drift.
- Protocol and catalog semantics: protocol-visible changes need parity
  evidence against PROTOCOL.md and delta-spark; catalog integrations must
  match the external system's real semantics; watch integer casts on
  version numbers.
- Silent default changes: in PRs claiming "no behavior change" or a pure
  refactor, verify per-operation defaults are unchanged on every call
  path, and flag new features mixed into a refactor (they belong in a
  follow-up PR).
- Unrelated changes to LICENSE, copyright headers, or README that are not
  the PR's stated purpose.

Report only findings you are confident in: cite the file and line, do not
infer behavior from names, and propose a concrete fix when possible.
