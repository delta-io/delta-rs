---
applyTo: "python/**"
---

# Python bindings review guidance

`python/src` is pyo3 glue over deltalake-core; `python/deltalake` is the
public package.

- Keep the binding layer thin: table logic belongs in deltalake-core, not
  reimplemented in Python or pyo3 code. Do not duplicate validation the Rust
  layer already performs.
- Public functions and classes need numpydoc-style docstrings with
  `Parameters` sections; they generate the API reference on
  delta-io.github.io.
- Deprecations: do not remove or rename parameters outright. Keep the old
  form working, emit `warnings.warn(..., DeprecationWarning, stacklevel=2)`,
  and drop it only after a few minor releases.
- User-facing API changes should update the matching `docs/usage/` page.
