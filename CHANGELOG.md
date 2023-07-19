# Changelog

## [rust-v0.13.1](https://github.com/delta-io/delta-rs/tree/rust-v0.13.1) (2023-07-18)

**Fixed bugs:**

* Revert premature merge of an attempted fix for binary column statistics [\#1544](https://github.com/delta-io/delta-rs/pull/1544)

## [rust-v0.13.0](https://github.com/delta-io/delta-rs/tree/rust-v0.13.0) (2023-07-15)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.12.0...rust-v0.13.0)

**Implemented enhancements:**

- Add nested struct supports [\#1518](https://github.com/delta-io/delta-rs/issues/1518)
- Support FixedLenByteArray UUID statistics as a logical scalar [\#1483](https://github.com/delta-io/delta-rs/issues/1483)
- Exposing create\_add in the API [\#1458](https://github.com/delta-io/delta-rs/issues/1458)
- Update features table on README [\#1404](https://github.com/delta-io/delta-rs/issues/1404)
- docs\(python\): show data catalog options in Python API reference [\#1347](https://github.com/delta-io/delta-rs/issues/1347)
- Add optimization to only list log files starting at a certain name [\#1252](https://github.com/delta-io/delta-rs/issues/1252)
- Support configuring parquet compression [\#1235](https://github.com/delta-io/delta-rs/issues/1235)
- parallel processing in Optimize command [\#1171](https://github.com/delta-io/delta-rs/issues/1171)

**Fixed bugs:**

- get\_add\_actions\(\) MAX is not showing complete value [\#1534](https://github.com/delta-io/delta-rs/issues/1534)
- Can't get stats's minValues in add actions [\#1515](https://github.com/delta-io/delta-rs/issues/1515)
- Pyarrow is\_null filter not working as expected after loading using deltalake [\#1496](https://github.com/delta-io/delta-rs/issues/1496)
- Can't write to table that uses generated columns [\#1495](https://github.com/delta-io/delta-rs/issues/1495)
- Json error: Binary is not supported by JSON when writing checkpoint files [\#1493](https://github.com/delta-io/delta-rs/issues/1493)
- \_last\_checkpoint size field is incorrect [\#1468](https://github.com/delta-io/delta-rs/issues/1468)
- Error when Z Ordering a larger dataset [\#1459](https://github.com/delta-io/delta-rs/issues/1459)
- Timestamp parsing issue [\#1455](https://github.com/delta-io/delta-rs/issues/1455)
- File options are ignored when writing delta [\#1444](https://github.com/delta-io/delta-rs/issues/1444)
- Slack Invite Link No Longer Valid [\#1425](https://github.com/delta-io/delta-rs/issues/1425)
- `cleanup_metadata` doesn't remove `.checkpoint.parquet` files [\#1420](https://github.com/delta-io/delta-rs/issues/1420)
- The test of reading the data from the blob storage located in Azurite container failed [\#1415](https://github.com/delta-io/delta-rs/issues/1415)
- The test of reading the data from the bucket located in Minio container failed [\#1408](https://github.com/delta-io/delta-rs/issues/1408)
- Datafusion: unreachable code reached when parsing statistics with missing columns [\#1374](https://github.com/delta-io/delta-rs/issues/1374)
- vacuum is very slow on Cloudflare R2 [\#1366](https://github.com/delta-io/delta-rs/issues/1366)

**Closed issues:**

- Expose Compression Options or WriterProperties for writing to Delta [\#1469](https://github.com/delta-io/delta-rs/issues/1469)
- Support out-of-core Z-order using DataFusion [\#1460](https://github.com/delta-io/delta-rs/issues/1460)
- Expose Z-order in Python [\#1442](https://github.com/delta-io/delta-rs/issues/1442)

**Merged pull requests:**

- chore: fix the latest clippy warnings with the newer rustc's [\#1536](https://github.com/delta-io/delta-rs/pull/1536) ([rtyler](https://github.com/rtyler))
- docs: show data catalog options in Python API reference [\#1532](https://github.com/delta-io/delta-rs/pull/1532) ([omkar-foss](https://github.com/omkar-foss))
- fix: handle nulls in file-level stats [\#1520](https://github.com/delta-io/delta-rs/pull/1520) ([wjones127](https://github.com/wjones127))
- feat: add nested struct supports [\#1519](https://github.com/delta-io/delta-rs/pull/1519) ([haruband](https://github.com/haruband))
- fix: tiny typo in AggregatedStats [\#1516](https://github.com/delta-io/delta-rs/pull/1516) ([haruband](https://github.com/haruband))
- refactor: unify with\_predicate for delete ops [\#1512](https://github.com/delta-io/delta-rs/pull/1512) ([Blajda](https://github.com/Blajda))
- chore: remove deprecated table functions [\#1511](https://github.com/delta-io/delta-rs/pull/1511) ([roeap](https://github.com/roeap))
- chore: update datafusion and related crates [\#1504](https://github.com/delta-io/delta-rs/pull/1504) ([roeap](https://github.com/roeap))
- feat: implement restore operation [\#1502](https://github.com/delta-io/delta-rs/pull/1502) ([loleek](https://github.com/loleek))
- chore: fix mypy failure [\#1500](https://github.com/delta-io/delta-rs/pull/1500) ([wjones127](https://github.com/wjones127))
- fix: avoid writing statistics for binary columns to fix JSON error [\#1498](https://github.com/delta-io/delta-rs/pull/1498) ([ChewingGlass](https://github.com/ChewingGlass))
- feat\(rust\): expose WriterProperties method on RecordBatchWriter and DeltaWriter [\#1497](https://github.com/delta-io/delta-rs/pull/1497) ([theelderbeever](https://github.com/theelderbeever))
- feat: add UUID statistics handling [\#1484](https://github.com/delta-io/delta-rs/pull/1484) ([atefsaw](https://github.com/atefsaw))
- feat: expose create\_add to the public [\#1482](https://github.com/delta-io/delta-rs/pull/1482) ([atefsaw](https://github.com/atefsaw))
- fix: add `sizeInBytes` to \_last\_checkpoint and change `size` to \# of actions [\#1477](https://github.com/delta-io/delta-rs/pull/1477) ([cmackenzie1](https://github.com/cmackenzie1))
- fix\(python\): match Field signatures [\#1463](https://github.com/delta-io/delta-rs/pull/1463) ([guilhem-dvr](https://github.com/guilhem-dvr))
- feat: handle larger z-order jobs with streaming output and spilling [\#1461](https://github.com/delta-io/delta-rs/pull/1461) ([wjones127](https://github.com/wjones127))
- chore: increment python version [\#1449](https://github.com/delta-io/delta-rs/pull/1449) ([wjones127](https://github.com/wjones127))
- chore: upgrade to arrow 40 and datafusion 26 [\#1448](https://github.com/delta-io/delta-rs/pull/1448) ([rtyler](https://github.com/rtyler))
- feat\(python\): expose z-order in Python [\#1443](https://github.com/delta-io/delta-rs/pull/1443) ([wjones127](https://github.com/wjones127))
- ci: prune CI/CD pipelines [\#1433](https://github.com/delta-io/delta-rs/pull/1433) ([roeap](https://github.com/roeap))
- refactor: remove `LoadCheckpointError` and `ApplyLogError` [\#1432](https://github.com/delta-io/delta-rs/pull/1432) ([roeap](https://github.com/roeap))
- feat: update writers to include compression method in file name [\#1431](https://github.com/delta-io/delta-rs/pull/1431) ([Blajda](https://github.com/Blajda))
- refactor: move checkpoint and errors into separate module [\#1430](https://github.com/delta-io/delta-rs/pull/1430) ([roeap](https://github.com/roeap))
- feat: add z-order optimize [\#1429](https://github.com/delta-io/delta-rs/pull/1429) ([wjones127](https://github.com/wjones127))
- fix: casting when data to be written does not match table schema [\#1427](https://github.com/delta-io/delta-rs/pull/1427) ([Blajda](https://github.com/Blajda))
- docs: update README.adoc to fix expired Slack link [\#1426](https://github.com/delta-io/delta-rs/pull/1426) ([dennyglee](https://github.com/dennyglee))
- chore: remove no-longer-necessary build.rs for Rust bindings [\#1424](https://github.com/delta-io/delta-rs/pull/1424) ([rtyler](https://github.com/rtyler))
- chore: remove the delta-checkpoint lambda which I have moved to a new repo [\#1423](https://github.com/delta-io/delta-rs/pull/1423) ([rtyler](https://github.com/rtyler))
- refactor: rewrite redundant\_async\_block [\#1422](https://github.com/delta-io/delta-rs/pull/1422) ([cmackenzie1](https://github.com/cmackenzie1))
- fix: update cleanup regex to include `checkpoint.parquet` files [\#1421](https://github.com/delta-io/delta-rs/pull/1421) ([cmackenzie1](https://github.com/cmackenzie1))
- docs: update features table in README [\#1414](https://github.com/delta-io/delta-rs/pull/1414) ([ognis1205](https://github.com/ognis1205))
- fix: `get_prune_stats` returns homogenous `ArrayRef` [\#1413](https://github.com/delta-io/delta-rs/pull/1413) ([cmackenzie1](https://github.com/cmackenzie1))
- feat: explicit python exceptions [\#1409](https://github.com/delta-io/delta-rs/pull/1409) ([roeap](https://github.com/roeap))
- feat: implement update operation [\#1390](https://github.com/delta-io/delta-rs/pull/1390) ([Blajda](https://github.com/Blajda))
- feat: allow concurrent file compaction [\#1383](https://github.com/delta-io/delta-rs/pull/1383) ([wjones127](https://github.com/wjones127))

## [rust-v0.12.0](https://github.com/delta-io/delta-rs/tree/rust-v0.12.0) (2023-05-30)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.11.0...rust-v0.12.0)

**Implemented enhancements:**

- Release delta-rs `0.11.0` \(next release after `0.10.0`\)  [\#1362](https://github.com/delta-io/delta-rs/issues/1362)
- Support writing statistics for date columns in Rust [\#1209](https://github.com/delta-io/delta-rs/issues/1209)

**Fixed bugs:**

- Rust writer in operations makes a lot of data copies [\#1394](https://github.com/delta-io/delta-rs/issues/1394)
- Unable to read timestamp fields from column statistics [\#1372](https://github.com/delta-io/delta-rs/issues/1372)
- Unable to write custom metadata via configuration since version 0.9.0 [\#1353](https://github.com/delta-io/delta-rs/issues/1353)
- .get\_add\_actions\(\) returns wrong column statistics when dataSkippingNumIndexedCols property of the table was changed [\#1223](https://github.com/delta-io/delta-rs/issues/1223)
- Ensure decimal statistics are written correctly in Rust [\#1208](https://github.com/delta-io/delta-rs/issues/1208)

**Merged pull requests:**

- feat: add list\_with\_offset to DeltaObjectStore [\#1410](https://github.com/delta-io/delta-rs/pull/1410) ([ognis1205](https://github.com/ognis1205))
- chore: type-check friendlier exports [\#1407](https://github.com/delta-io/delta-rs/pull/1407) ([roeap](https://github.com/roeap))
- chore: remove ancillary crates from the git tree [\#1406](https://github.com/delta-io/delta-rs/pull/1406) ([rtyler](https://github.com/rtyler))
- chore: bump the version for the next release [\#1405](https://github.com/delta-io/delta-rs/pull/1405) ([rtyler](https://github.com/rtyler))
- feat: more efficient parquet writer and more statistics [\#1397](https://github.com/delta-io/delta-rs/pull/1397) ([wjones127](https://github.com/wjones127))
- perf: improve record batch partitioning [\#1396](https://github.com/delta-io/delta-rs/pull/1396) ([roeap](https://github.com/roeap))
- chore: bump datafusion to 25 [\#1389](https://github.com/delta-io/delta-rs/pull/1389) ([roeap](https://github.com/roeap))
- refactor!: remove `DeltaDataType` aliases [\#1388](https://github.com/delta-io/delta-rs/pull/1388) ([cmackenzie1](https://github.com/cmackenzie1))
- feat: vacuum with concurrent requests [\#1382](https://github.com/delta-io/delta-rs/pull/1382) ([wjones127](https://github.com/wjones127))
- feat: add datafusion storage catalog [\#1381](https://github.com/delta-io/delta-rs/pull/1381) ([roeap](https://github.com/roeap))
- docs: updated schema.rs to use the right signature for decimal data type in documentation [\#1377](https://github.com/delta-io/delta-rs/pull/1377) ([rahulj51](https://github.com/rahulj51))
- fix: delete operation when partition and non partition columns are used [\#1375](https://github.com/delta-io/delta-rs/pull/1375) ([Blajda](https://github.com/Blajda))
- fix: add conversion for string for `Field::TimestampMicros` \(\#1372\) [\#1373](https://github.com/delta-io/delta-rs/pull/1373) ([cmackenzie1](https://github.com/cmackenzie1))
- fix: allow user defined config keys [\#1365](https://github.com/delta-io/delta-rs/pull/1365) ([roeap](https://github.com/roeap))
- ci: disable full debug symbol generation [\#1364](https://github.com/delta-io/delta-rs/pull/1364) ([roeap](https://github.com/roeap))
- fix: include stats for all columns \(\#1223\) [\#1342](https://github.com/delta-io/delta-rs/pull/1342) ([mrjoe7](https://github.com/mrjoe7))

## [rust-v0.11.0](https://github.com/delta-io/delta-rs/tree/rust-v0.11.0) (2023-05-12)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.10.0...rust-v0.11.0)

**Implemented enhancements:**

- Implement simple delete case [\#832](https://github.com/delta-io/delta-rs/issues/832)

**Merged pull requests:**

- chore: update Rust package version [\#1346](https://github.com/delta-io/delta-rs/pull/1346) ([rtyler](https://github.com/rtyler))
- fix: replace deprecated arrow::json::reader::Decoder [\#1226](https://github.com/delta-io/delta-rs/pull/1226) ([rtyler](https://github.com/rtyler))
- feat: delete operation [\#1176](https://github.com/delta-io/delta-rs/pull/1176) ([Blajda](https://github.com/Blajda))
- feat: add `wasbs` to known schemes [\#1345](https://github.com/delta-io/delta-rs/pull/1345) ([iajoiner](https://github.com/iajoiner))
- test: add some missing unit and doc tests for DeltaTablePartition [\#1341](https://github.com/delta-io/delta-rs/pull/1341) ([rtyler](https://github.com/rtyler))
- feat: write command improvements [\#1267](https://github.com/delta-io/delta-rs/pull/1267) ([roeap](https://github.com/roeap))
- feat: added support for Databricks Unity Catalog [\#1331](https://github.com/delta-io/delta-rs/pull/1331) ([nohajc](https://github.com/nohajc))
- fix: double url encode of partition key [\#1324](https://github.com/delta-io/delta-rs/pull/1324) ([mrjoe7](https://github.com/mrjoe7))

## [rust-v0.10.0](https://github.com/delta-io/delta-rs/tree/rust-v0.10.0) (2023-05-02)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.9.0...rust-v0.10.0)

**Implemented enhancements:**

- Support Optimize on non-append-only tables [\#1125](https://github.com/delta-io/delta-rs/issues/1125)

**Fixed bugs:**

- DataFusion integration incorrectly handles partition columns defined "first" in schema [\#1168](https://github.com/delta-io/delta-rs/issues/1168)
- Datafusion: SQL projection returns wrong column for partitioned data [\#1292](https://github.com/delta-io/delta-rs/issues/1292)
- Unable to query partitioned tables [\#1291](https://github.com/delta-io/delta-rs/issues/1291)

**Merged pull requests:**

- chore: add deprecation notices for commit logic on `DeltaTable` [\#1323](https://github.com/delta-io/delta-rs/pull/1323) ([roeap](https://github.com/roeap))
- fix: handle local paths on windows [\#1322](https://github.com/delta-io/delta-rs/pull/1322) ([roeap](https://github.com/roeap))
- fix: scan partitioned tables with datafusion [\#1303](https://github.com/delta-io/delta-rs/pull/1303) ([roeap](https://github.com/roeap))
- fix: allow special characters in storage prefix [\#1311](https://github.com/delta-io/delta-rs/pull/1311) ([wjones127](https://github.com/wjones127))
- feat: upgrade to Arrow 37 and Datafusion 23 [\#1314](https://github.com/delta-io/delta-rs/pull/1314) ([rtyler](https://github.com/rtyler))
- Hide the parquet/json feature behind our own JSON feature [\#1307](https://github.com/delta-io/delta-rs/pull/1307) ([rtyler](https://github.com/rtyler))
- Enable the json feature for the parquet crate [\#1300](https://github.com/delta-io/delta-rs/pull/1300) ([rtyler](https://github.com/rtyler))

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
