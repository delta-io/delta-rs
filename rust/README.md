Deltalake
=========

Native Delta Lake implementation in Rust


Usage
-----

### API

```rust
let table = deltalake::open_table("./tests/data/simple_table").unwrap();
println!(table.get_files());
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
        metadata: GUID=22ef18ba-191c-4c36-a606-3dad5cdf3830, name=None, description=None, partitionColumns=[], configuration={}
        min_version: read=1, write=2
        files count: 3
```
