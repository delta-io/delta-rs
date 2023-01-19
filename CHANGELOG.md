# Changelog

## [rust-v0.7.0](https://github.com/delta-io/delta-rs/tree/rust-v0.7.0) (2023-01-19)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.6.0...rust-v0.7.0)

**Implemented enhancements:**

- Support Statistics in PyArrow [\#1070](https://github.com/delta-io/delta-rs/issues/1070)
- Use existing labels for PRs if possible [\#1051](https://github.com/delta-io/delta-rs/issues/1051)
- Expose the Delta Log in a DataFrame that's easy for analysis [\#1031](https://github.com/delta-io/delta-rs/issues/1031)
- Automatically label PRs [\#1026](https://github.com/delta-io/delta-rs/issues/1026)
- Provide case-insensitive storage options in backend [\#999](https://github.com/delta-io/delta-rs/issues/999)
- Support local file path in CreateBuilder::with\_location\(\) [\#998](https://github.com/delta-io/delta-rs/issues/998)
- Upgrade actions to not use deprecated features. [\#978](https://github.com/delta-io/delta-rs/issues/978)
- Save operational params in the same way with delta io [\#1054](https://github.com/delta-io/delta-rs/pull/1054) ([ismoshkov](https://github.com/ismoshkov))

**Fixed bugs:**

- crates.io entry is missing link to rustdoc documentation [\#1076](https://github.com/delta-io/delta-rs/issues/1076)
- DeltaTable DataFusion TableProvider does not support filter pushdown [\#1064](https://github.com/delta-io/delta-rs/issues/1064)
- DeltaTable DataFusion scan does not prune files properly [\#1063](https://github.com/delta-io/delta-rs/issues/1063)
- CI constantly fails for `python_build / Python Build (Python 3.10 PyArrow latest)` [\#1025](https://github.com/delta-io/delta-rs/issues/1025)
- URL Registered with ObjectStore registry is different from url in DeltaScan  [\#1018](https://github.com/delta-io/delta-rs/issues/1018)
- Transaction log JSON formatting issue when writing data via Python bindings [\#1017](https://github.com/delta-io/delta-rs/issues/1017)
- DeltaStorageHandler is not serializable [\#1015](https://github.com/delta-io/delta-rs/issues/1015)
- Deltalake 0.5 crate s3 feature dynamodb version mismatch [\#973](https://github.com/delta-io/delta-rs/issues/973)
- Use Chrono without default  features [\#914](https://github.com/delta-io/delta-rs/issues/914)
- Azure SAS authorization fails with `<AuthenticationErrorDetail>Signature fields not well formed.` [\#910](https://github.com/delta-io/delta-rs/issues/910)

**Merged pull requests:**

- ci: Merge several automatically generated labels [\#1081](https://github.com/delta-io/delta-rs/pull/1081) ([iajoiner](https://github.com/iajoiner))
- Add missing documentation metadata to Cargo.toml [\#1077](https://github.com/delta-io/delta-rs/pull/1077) ([johnbatty](https://github.com/johnbatty))
- Fix typo in delta-inspect [\#1072](https://github.com/delta-io/delta-rs/pull/1072) ([byteink](https://github.com/byteink))
- fix: datafusion predicate pushdown and dependencies [\#1071](https://github.com/delta-io/delta-rs/pull/1071) ([roeap](https://github.com/roeap))
- feat: improve storage location handling [\#1065](https://github.com/delta-io/delta-rs/pull/1065) ([roeap](https://github.com/roeap))
- Expose checkpoint creation for current table state in python [\#1058](https://github.com/delta-io/delta-rs/pull/1058) ([ismoshkov](https://github.com/ismoshkov))
- feat: harmonize and simplify storage configuration [\#1052](https://github.com/delta-io/delta-rs/pull/1052) ([roeap](https://github.com/roeap))
- bump version for dynamodb\_lock crate [\#1047](https://github.com/delta-io/delta-rs/pull/1047) ([houqp](https://github.com/houqp))
- chore: update github actions to latest versions [\#1046](https://github.com/delta-io/delta-rs/pull/1046) ([roeap](https://github.com/roeap))
- refactor: move vacuum command to operations module [\#1045](https://github.com/delta-io/delta-rs/pull/1045) ([roeap](https://github.com/roeap))
- feat: enable passing storage options to Delta table builder via DataFusion's CREATE EXTERNAL TABLE [\#1043](https://github.com/delta-io/delta-rs/pull/1043) ([gruuya](https://github.com/gruuya))
- Add provisional workaround to support CDC \#1039 [\#1042](https://github.com/delta-io/delta-rs/pull/1042) ([Fazzani](https://github.com/Fazzani))
- fix: azure sas key url encoding [\#1036](https://github.com/delta-io/delta-rs/pull/1036) ([roeap](https://github.com/roeap))
- chore: fix new lints from new cargo [\#1034](https://github.com/delta-io/delta-rs/pull/1034) ([wjones127](https://github.com/wjones127))
- feat: expose function to get table of add actions [\#1033](https://github.com/delta-io/delta-rs/pull/1033) ([wjones127](https://github.com/wjones127))
- refactor\(api!\): refactor Python APIs for getting file list [\#1032](https://github.com/delta-io/delta-rs/pull/1032) ([wjones127](https://github.com/wjones127))
- Fix to support UTC timezone [\#1022](https://github.com/delta-io/delta-rs/pull/1022) ([andrei-ionescu](https://github.com/andrei-ionescu))
- feat: make `DeltaStorageHandler` pickle serializable [\#1016](https://github.com/delta-io/delta-rs/pull/1016) ([roeap](https://github.com/roeap))
- feat: clean up dependencies and feature flags [\#1014](https://github.com/delta-io/delta-rs/pull/1014) ([roeap](https://github.com/roeap))
- test\(python\): make sure integration tests wait for services to start [\#979](https://github.com/delta-io/delta-rs/pull/979) ([wjones127](https://github.com/wjones127))
- test\(python\): add read / write benchmarks [\#933](https://github.com/delta-io/delta-rs/pull/933) ([wjones127](https://github.com/wjones127))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
