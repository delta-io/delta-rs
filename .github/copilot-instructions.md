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
- PR title format or conventional commits: automation enforces these.
- Commit sign-off/DCO: a status check enforces this.
- Coverage percentages: codecov already reports them.
- Generated, vendored, or test fixture data files (including `dat/`).
- Readability or maintainability opinions without a concrete failure mode.

## Focus review on

- Correctness against PROTOCOL.md and delta-spark behavior; protocol-visible
  changes need parity evidence.
- Missing tests for changed behavior: a test that also passes with the new
  code path disabled does not cover it.
- New panic paths (`unwrap`, `expect`, direct indexing) outside tests, and
  error-to-`None` conversions that change caller semantics.
- Semver hazards: renamed or removed public items, removed trait impls (e.g.
  `Debug`), Cargo feature changes that drop transitive features.
- Silent default changes: in PRs claiming "no behavior change" or a pure
  refactor, verify per-operation defaults are unchanged on every call path,
  and flag new features mixed into a refactor (they belong in a follow-up).
- Performance PRs: expect a reproducible scenario and before/after numbers,
  not just a plausible argument.
- Unrelated changes to LICENSE, copyright headers, or README that are not
  the PR's stated purpose.
- Comments that merely restate the code (see "Using AI-generated code" in
  CONTRIBUTING.md).

Report only findings you are confident in: cite the file and line, do not
infer behavior from names, and propose a concrete fix when possible.
