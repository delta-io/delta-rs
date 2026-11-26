---
applyTo: "docs/**,mkdocs.yml,README.md"
---

# Documentation review guidance

The docs are a mkdocs site; API reference pages render from Python
docstrings via mkdocstrings.

- Report factual errors, broken internal links, and code samples that would
  not run as written. Do not review prose style or wording.
- New user-facing features should come with a docs page or section; flag
  features merged with no documentation.
- Check terminology precision: names must not overstate behavior (e.g. a
  file-pruning predicate is not a full query predicate).
