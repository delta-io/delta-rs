# Changelog

## [rust-v0.9.0](https://github.com/delta-io/delta-rs/tree/rust-v0.9.0) (2023-04-14)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.8.0...rust-v0.9.0)

**Implemented enhancements:**

- hdfs support [\#300](https://github.com/delta-io/delta-rs/issues/300)
- Add decimal primitive type to document [\#1280](https://github.com/delta-io/delta-rs/issues/1280)
- Improve error message when filtering on non-existant partition columns [\#1218](https://github.com/delta-io/delta-rs/issues/1218)

**Fixed bugs:**

- Datafusion table provider: issues with timestamp types [\#441](https://github.com/delta-io/delta-rs/issues/441)
- Not matching column names when creating a RecordBatch from MapArray [\#1257](https://github.com/delta-io/delta-rs/issues/1257)
- All stores created using `DeltaObjectStore::new` have an identical `object_store_url` [\#1188](https://github.com/delta-io/delta-rs/issues/1188)

**Merged pull requests:**

- Upgrade datafusion to 22 which brings arrow upgrades with it [\#1249](https://github.com/delta-io/delta-rs/pull/1249) ([rtyler](https://github.com/rtyler))
- chore: df / arrow changes after update [\#1288](https://github.com/delta-io/delta-rs/pull/1288) ([roeap](https://github.com/roeap))
- feat: read schema from parquet files in datafusion scans [\#1266](https://github.com/delta-io/delta-rs/pull/1266) ([roeap](https://github.com/roeap))
- HDFS storage support via datafusion-objectstore-hdfs [\#1279](https://github.com/delta-io/delta-rs/pull/1279) ([iajoiner](https://github.com/iajoiner))
- Add description of decimal primitive to SchemaDataType [\#1281](https://github.com/delta-io/delta-rs/pull/1281) ([ognis1205](https://github.com/ognis1205))
- Fix names and nullability when creating RecordBatch from MapArray [\#1258](https://github.com/delta-io/delta-rs/pull/1258) ([balbok0](https://github.com/balbok0))
- Simplify the Store Backend Configuration code [\#1265](https://github.com/delta-io/delta-rs/pull/1265) ([mrjoe7](https://github.com/mrjoe7))
- feat: optimistic transaction protocol [\#632](https://github.com/delta-io/delta-rs/pull/632) ([roeap](https://github.com/roeap))
- Write support for additional Arrow datatypes [\#1044](https://github.com/delta-io/delta-rs/pull/1044)([chitralverma](https://github.com/chitralverma))
- Unique delta object store url [\#1212](https://github.com/delta-io/delta-rs/pull/1212) ([gruuya](https://github.com/gruuya))
- improve err msg on use of non-partitioned column [\#1221](https://github.com/delta-io/delta-rs/pull/1221) ([marijncv](https://github.com/marijncv))

## [rust-v0.8.0](https://github.com/delta-io/delta-rs/tree/rust-v0.8.0) (2023-03-10)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.7.0...rust-v0.8.0)

**Implemented enhancements:**

- feat(rust): support additional types for partition values [\#1170](https://github.com/delta-io/delta-rs/issues/1170)

**Fixed bugs:**

- File pruning does not occur on partition columns [\#1175](https://github.com/delta-io/delta-rs/issues/1175)
- Bug: Error loading Delta table locally [\#1157](https://github.com/delta-io/delta-rs/issues/1157)
- Deltalake 0.7.0 with s3 feature compliation error due to rusoto_dynamodb version conflict [\#1191](https://github.com/delta-io/delta-rs/issues/1191)
- Writing from a Delta table scan using WriteBuilder fails due to missing object store [\#1186](https://github.com/delta-io/delta-rs/issues/1186)

**Merged pull requests:**

- build(deps): bump datafusion [\#1217](https://github.com/delta-io/delta-rs/pull/1217) ([roeap](https://github.com/roeap))
- Implement pruning on partition columns [\#1179](https://github.com/delta-io/delta-rs/pull/1179) ([Blajda](https://github.com/Blajda))
- feat: enable passing storage options to Delta table builder via Datafusion's CREATE EXTERNAL TABLE [\#1043](https://github.com/delta-io/delta-rs/pull/1043) ([gruuya](https://github.com/gruuya))
- feat: typed commit info [\#1207](https://github.com/delta-io/delta-rs/pull/1207) ([roeap](https://github.com/roeap))
- add boolean, date, timestamp & binary partition types [\#1180](https://github.com/delta-io/delta-rs/pull/1180) ([marijncv](https://github.com/marijncv))
- feat: extend configuration handling [\#1206](https://github.com/delta-io/delta-rs/pull/1206) ([marijncv](https://github.com/marijncv))
- fix: load command for local tables [\#1205](https://github.com/delta-io/delta-rs/pull/1205) ([roeap](https://github.com/roeap))
- Enable passing Datafusion session state to WriteBuilder [\#1187](https://github.com/delta-io/delta-rs/pull/1187) ([gruuya](https://github.com/gruuya))
- chore: increment dynamodb_lock version [\#1202](https://github.com/delta-io/delta-rs/pull/1202) ([wjones127](https://github.com/wjones127))
- fix: update out-of-date doc about datafusion [\#1183](https://github.com/delta-io/delta-rs/pull/1183) ([xudong963](https://github.com/xudong963))
- feat: move and update Optimize operation [\#1154](https://github.com/delta-io/delta-rs/pull/1154) ([roeap](https://github.com/roeap))
- add test for extract_partition_values [\#1159](https://github.com/delta-io/delta-rs/pull/1159) ([marijncv](https://github.com/marijncv))
- fix typo [\#1166](https://github.com/delta-io/delta-rs/pull/1166) ([spebern](https://github.com/spebern))
- chore: remove star dependencies [\#1139](https://github.com/delta-io/delta-rs/pull/1139) ([wjones127](https://github.com/wjones127))

## [rust-v0.7.0](https://github.com/delta-io/delta-rs/tree/rust-v0.7.0) (2023-02-11)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.6.0...rust-v0.7.0)

**Implemented enhancements:**

- Support FSCK REPAIR TABLE Operation [\#1092](https://github.com/delta-io/delta-rs/issues/1092)
- Expose the Delta Log in a DataFrame that's easy for analysis [\#1031](https://github.com/delta-io/delta-rs/issues/1031)
- Provide case-insensitive storage options in backend [\#999](https://github.com/delta-io/delta-rs/issues/999)
- Support local file path in CreateBuilder::with\_location\(\) [\#998](https://github.com/delta-io/delta-rs/issues/998)
- Save operational params in the same way with delta io [\#1054](https://github.com/delta-io/delta-rs/pull/1054) ([ismoshkov](https://github.com/ismoshkov))

**Fixed bugs:**

- DeltaTable DataFusion TableProvider does not support filter pushdown [\#1064](https://github.com/delta-io/delta-rs/issues/1064)
- DeltaTable DataFusion scan does not prune files properly [\#1063](https://github.com/delta-io/delta-rs/issues/1063)
- deltalake.DeltaTable constructor hangs in Jupyter [\#1093](https://github.com/delta-io/delta-rs/issues/1093)
- Transaction log JSON formatting issue when writing data via Python bindings [\#1017](https://github.com/delta-io/delta-rs/issues/1017)
- crates.io entry is missing link to rustdoc documentation [\#1076](https://github.com/delta-io/delta-rs/issues/1076)
- URL Registered with ObjectStore registry is different from url in DeltaScan  [\#1018](https://github.com/delta-io/delta-rs/issues/1018)
- Not able to connect to Azure Storage with client id/secret [\#977](https://github.com/delta-io/delta-rs/issues/977)
- Deltalake 0.5 crate s3 feature dynamodb version mismatch [\#973](https://github.com/delta-io/delta-rs/issues/973)
- Overwrite mode does not work with Azure [\#939](https://github.com/delta-io/delta-rs/issues/939)
- Use Chrono without default  features [\#914](https://github.com/delta-io/delta-rs/issues/914)
- `cargo test` does not run due to tls conflict [\#985](https://github.com/delta-io/delta-rs/issues/985)
- Azure SAS authorization fails with `<AuthenticationErrorDetail>Signature fields not well formed.` [\#910](https://github.com/delta-io/delta-rs/issues/910)

**Merged pull requests:**

- Make rustls default across all packages [\#1097](https://github.com/delta-io/delta-rs/pull/1097) ([wjones127](https://github.com/wjones127))
- Implement filesystem check [\#1103](https://github.com/delta-io/delta-rs/pull/1103) ([Blajda](https://github.com/Blajda))
- refactor: move vacuum command to operations module [\#1045](https://github.com/delta-io/delta-rs/pull/1045) ([roeap](https://github.com/roeap))
- feat: enable passing storage options to Delta table builder via DataFusion's CREATE EXTERNAL TABLE [\#1043](https://github.com/delta-io/delta-rs/pull/1043) ([gruuya](https://github.com/gruuya))
- feat: improve storage location handling [\#1065](https://github.com/delta-io/delta-rs/pull/1065) ([roeap](https://github.com/roeap))
- Fix to support UTC timezone [\#1022](https://github.com/delta-io/delta-rs/pull/1022) ([andrei-ionescu](https://github.com/andrei-ionescu))
- feat: harmonize and simplify storage configuration [\#1052](https://github.com/delta-io/delta-rs/pull/1052) ([roeap](https://github.com/roeap))
- feat: expose function to get table of add actions [\#1033](https://github.com/delta-io/delta-rs/pull/1033) ([wjones127](https://github.com/wjones127))
- fix: change unexpected field logging level to debug [\#1112](https://github.com/delta-io/delta-rs/pull/1112) ([houqp](https://github.com/houqp))
- fix: datafusion predicate pushdown and dependencies [\#1071](https://github.com/delta-io/delta-rs/pull/1071) ([roeap](https://github.com/roeap))
- fix: azure sas key url encoding [\#1036](https://github.com/delta-io/delta-rs/pull/1036) ([roeap](https://github.com/roeap))
- Add provisional workaround to support CDC \#1039 [\#1042](https://github.com/delta-io/delta-rs/pull/1042) ([Fazzani](https://github.com/Fazzani))
- improve debuggability of json ser/de errors [\#1119](https://github.com/delta-io/delta-rs/pull/1119) ([houqp](https://github.com/houqp))
- Add an example of writing to a delta table with a RecordBatch [\#1085](https://github.com/delta-io/delta-rs/pull/1085) ([rtyler](https://github.com/rtyler))
- minor: optimize partition lookup for vacuum loop [\#1120](https://github.com/delta-io/delta-rs/pull/1120) ([houqp](https://github.com/houqp))
- Add missing documentation metadata to Cargo.toml [\#1077](https://github.com/delta-io/delta-rs/pull/1077) ([johnbatty](https://github.com/johnbatty))
- add test for null\_count\_schema\_for\_fields [\#1135](https://github.com/delta-io/delta-rs/pull/1135) ([marijncv](https://github.com/marijncv))
- add test for min\_max\_schema\_for\_fields [\#1122](https://github.com/delta-io/delta-rs/pull/1122) ([marijncv](https://github.com/marijncv))
- add test for get\_boolean\_from\_metadata [\#1121](https://github.com/delta-io/delta-rs/pull/1121) ([marijncv](https://github.com/marijncv))
- add test for left\_larger\_than\_right [\#1110](https://github.com/delta-io/delta-rs/pull/1110) ([marijncv](https://github.com/marijncv))
- Add test for: to\_scalar\_value [\#1086](https://github.com/delta-io/delta-rs/pull/1086) ([marijncv](https://github.com/marijncv))
- Fix typo in delta-inspect [\#1072](https://github.com/delta-io/delta-rs/pull/1072) ([byteink](https://github.com/byteink))
- chore: update datafusion [\#1114](https://github.com/delta-io/delta-rs/pull/1114) ([roeap](https://github.com/roeap))

## [rust-v0.6.0](https://github.com/delta-io/delta-rs/tree/rust-v0.6.0) (2022-12-16)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.5.0...rust-v0.6.0)

**Implemented enhancements:**

- Support Apache Arrow DataFusion 15 [\#1020](https://github.com/delta-io/delta-rs/issues/1020)
- Python package: Loosen version requirements for maturin [\#1004](https://github.com/delta-io/delta-rs/issues/1004)
- Remove `Cargo.lock` from library crates and add `Cargo.lock` to binary ones [\#1000](https://github.com/delta-io/delta-rs/issues/1000)
- More frequent Rust releases [\#969](https://github.com/delta-io/delta-rs/issues/969)
- Thoughts on adding read\_delta to pandas [\#869](https://github.com/delta-io/delta-rs/issues/869)
- Add the support of the AWS\_PROFILE environment variable for S3 [\#986](https://github.com/delta-io/delta-rs/pull/986) ([fvaleye](https://github.com/fvaleye))

**Fixed bugs:**

- Azure SAS signatures ending in "=" don't work [\#1003](https://github.com/delta-io/delta-rs/issues/1003)
- Fail to compile deltalake crate, need to update dynamodb\_lock in crates.io [\#1002](https://github.com/delta-io/delta-rs/issues/1002)
- error reading delta table to pandas: runtime dropped the dispatch task [\#975](https://github.com/delta-io/delta-rs/issues/975)
- MacOS arm64 wheels are generated incorrectly [\#972](https://github.com/delta-io/delta-rs/issues/972)
- Overwrite creates new file [\#960](https://github.com/delta-io/delta-rs/issues/960)
- The written delta file has corrupted structure [\#956](https://github.com/delta-io/delta-rs/issues/956)
- Write mode doesn't work with Azure storage [\#955](https://github.com/delta-io/delta-rs/issues/955)
- Python: We don't error on reader protocol v2 [\#886](https://github.com/delta-io/delta-rs/issues/886)
- Cannot open a deltatable in S3 using AWS\_PROFILE based credentials from a local machine [\#855](https://github.com/delta-io/delta-rs/issues/855)

**Merged pull requests:**

- Support DataFusion 15 [\#1021](https://github.com/delta-io/delta-rs/pull/1021) ([andrei-ionescu](https://github.com/andrei-ionescu))
- fix truncating signature on SAS [\#1007](https://github.com/delta-io/delta-rs/pull/1007) ([damiondoesthings](https://github.com/damiondoesthings))
- Loosen version requirement for maturin [\#1005](https://github.com/delta-io/delta-rs/pull/1005) ([gyscos](https://github.com/gyscos))
- Update `.gitignore` and add/remove `Cargo.lock` when appropriate [\#1001](https://github.com/delta-io/delta-rs/pull/1001) ([iajoiner](https://github.com/iajoiner))
- fix: get azure client secret from config [\#981](https://github.com/delta-io/delta-rs/pull/981) ([roeap](https://github.com/roeap))
- feat: check invariants in write command [\#980](https://github.com/delta-io/delta-rs/pull/980) ([roeap](https://github.com/roeap))
- Add a new release github action for Python binding: macos with universal2 wheel [\#976](https://github.com/delta-io/delta-rs/pull/976) ([fvaleye](https://github.com/fvaleye))
- Bump version of the Python binding to 0.6.4 [\#970](https://github.com/delta-io/delta-rs/pull/970) ([fvaleye](https://github.com/fvaleye))
- Handle pandas timestamps [\#958](https://github.com/delta-io/delta-rs/pull/958) ([hayesgb](https://github.com/hayesgb))
- test\(python\): add azure integration tests [\#912](https://github.com/delta-io/delta-rs/pull/912) ([wjones127](https://github.com/wjones127))


\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
