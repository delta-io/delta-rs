Deltalake
=========

[![crates.io](https://img.shields.io/crates/v/deltalake.svg?style=flat-square)](https://crates.io/crates/deltalake)
[![api_doc](https://img.shields.io/badge/doc-api-blue)](https://docs.rs/deltalake)

Native Delta Lake implementation in Rust


Usage
-----

### API

```rust
let table = deltalake::open_table("./tests/data/simple_table").await.unwrap();
println!("{}", table.get_files());
```


### CLI

```bash
❯ cargo run --bin delta-inspect files ./tests/data/delta-0.2.0
part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet
part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet
part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet
❯ cargo run --bin delta-inspect info ./tests/data/delta-0.2.0
DeltaTable(./tests/data/delta-0.2.0)
        version: 3
        metadata: GUID=22ef18ba-191c-4c36-a606-3dad5cdf3830, name=None, description=None, partitionColumns=[], createdTime=1564524294376, configuration={}
        min_version: read=1, write=2
        files count: 3
```

Optional cargo package features
-----------------------

- `s3` - enable the S3 storage backend to work with Delta Tables in AWS S3.
- `s3-rustls` - enable the S3 storage backend but rely on [rustls](https://github.com/ctz/rustls) rather than OpenSSL (`native-tls`).
- `azure` - enable the Azure storage backend to work with Delta Tables in Azure Data Lake Storage Gen2 accounts.
- `datafusion-ext` - enable the `datafusion::datasource::TableProvider` trait implementation for Delta Tables, allowing them to be queried using [DataFusion](https://github.com/apache/arrow/tree/master/rust/datafusion).


Development
-----------

To run s3 integration tests from local machine, we use docker-compose to stand
up AWS local stack. To spin up the test environment run `docker-compose up` in
the root of the `delta-rs` repo.
