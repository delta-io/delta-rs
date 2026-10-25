# Stable shredded VARIANT fixture

One synthetic row written by Databricks and rewritten with
`REORG TABLE ... APPLY (SHRED VARIANT)`. The Parquet body has a VARIANT logical
type and metadata/value/typed_value fields.

The Delta log is a minimal synthetic version-0 log requiring variantType and
variantShredding. It declares only body VARIANT; extra physical row-tracking
columns from the original Parquet file are ignored. This fixture does not test
row tracking, deletion vectors, partitioning, or catalog commits.

Expected body: {"id":1,"message":"hello shredded variant","active":true,
"tags":["uc","datafusion"]}.

The engine-independent integration test loads the Delta snapshot and checks its
logical type and protocol features. It does not claim support for decoding the
shredded Parquet payload.

```bash
cargo test -p deltalake-core --no-default-features --features rustls --test it variant_shredding
```

With the DataFusion feature, the reader reconstructs the Variant values before
applying the Delta logical schema. Read coverage is in `crates/core/tests/variant.rs`:

```bash
cargo test -p deltalake-core --features datafusion --test variant
```

The tests cover mixed physical layouts, residual values, nulls, nested values,
column mapping, partitions, and deletion vectors. Stable `variantShredding`
read support is enabled only in builds with the DataFusion reader. Shredded
writes and the older `variantShredding-preview` feature remain unsupported.
