---
applyTo: "crates/aws/**,crates/azure/**,crates/gcp/**,crates/hdfs/**,crates/lakefs/**,crates/opendal/**,crates/catalog-glue/**,crates/catalog-unity/**,crates/mount/**"
---

# Storage and catalog backend review guidance

These crates are thin integrations over `object_store` and deltalake-core.

- Logic shared across backends belongs in deltalake-core; flag per-backend
  copy-paste of nontrivial logic.
- Verify assumptions against the external system's real semantics (e.g.
  Unity Catalog commit listing order and version sentinels), not the
  obvious-looking interpretation; check integer casts on version numbers.
- New optional dependencies must be feature-gated like the existing ones.
- Flag credential or endpoint handling that could log secrets.

In these crates, report only near-certain and severe issues; skip style and
minor API-shape feedback.
