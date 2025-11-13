# Changelog

## [rust-v0.28.0](https://github.com/delta-io/delta-rs/tree/rust-v0.28.0) (2025-08-27)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.27.0...rust-v0.28.0)


:warning: There is a known performance regression when opening very wide tables (50+ columns) that have hundreds of thousands of transactions. The fix is pending a new [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs) release.

**Implemented enhancements:**

- Python: Automatically convert Pandas null types to valid Delta Lake types in write\_deltalake\(\) [\#3691](https://github.com/delta-io/delta-rs/issues/3691)
- Update HDFS object store to 0.15 [\#3680](https://github.com/delta-io/delta-rs/issues/3680)
- create a v2 uuid checkpoint regression test [\#3666](https://github.com/delta-io/delta-rs/issues/3666)
- Feature: update python table vacuum to add keep\_versions parameter [\#3634](https://github.com/delta-io/delta-rs/issues/3634)
- TypeError in `DeltaTable.to_pyarrow_dataset` when using non-string partition filter values \(e.g., int\) [\#3597](https://github.com/delta-io/delta-rs/issues/3597)
- Make "cloud" feature optional [\#3589](https://github.com/delta-io/delta-rs/issues/3589)
- `convert_to_deltalake` cannot convert parquet dataset if it has millisecond-precision timestamps [\#3535](https://github.com/delta-io/delta-rs/issues/3535)
- Musl wheels  [\#3399](https://github.com/delta-io/delta-rs/issues/3399)

**Fixed bugs:**

- Automatically register the AWS, Azure, GCS, HDFS, LakeFS, and Unity storage handlers when the corresponding feature is enabled so `DeltaOps::try_from_uri` no longer errors with unknown schemes such as `gs://`.
- Significant performance regression when opening S3 table on next branch [\#3667](https://github.com/delta-io/delta-rs/issues/3667)
- Concurrent overwrite doesn't fail conflict checking [\#3622](https://github.com/delta-io/delta-rs/issues/3622)
- source distributions missing in v1.1.1 [\#3621](https://github.com/delta-io/delta-rs/issues/3621)
- Missing linux distro for v1.1.1 [\#3620](https://github.com/delta-io/delta-rs/issues/3620)
- azurite tets failing in main [\#3612](https://github.com/delta-io/delta-rs/issues/3612)
- Generic S3 Error on \_last\_checkpoint on ARM64 AWS Lambda with `write_deltalake` [\#3602](https://github.com/delta-io/delta-rs/issues/3602)
- write\_deltalake merge with list and large\_list [\#3595](https://github.com/delta-io/delta-rs/issues/3595)
- Python DeltaTable does not support writes in multiple threads \(again?\) [\#3594](https://github.com/delta-io/delta-rs/issues/3594)
- Checkpoint creation fails on Azure in \>=1.0.0 with "Azure does not support suffix range requests" [\#3593](https://github.com/delta-io/delta-rs/issues/3593)
- Partition value strings containing reserved ASCII and non-ASCII are double-encoded. [\#3577](https://github.com/delta-io/delta-rs/issues/3577)
- Deltalake version 1.0.2 errors with Azure Storage after appending many times [\#3567](https://github.com/delta-io/delta-rs/issues/3567)
- Python deltalake 1.0.2 is not compatible with polars.Array [\#3566](https://github.com/delta-io/delta-rs/issues/3566)
- Checkpoint schema breaking change between 0.25.5 and 1.0.2 [\#3527](https://github.com/delta-io/delta-rs/issues/3527)

**Closed issues:**

- Array/list not encoded with partition filters [\#3648](https://github.com/delta-io/delta-rs/issues/3648)

**Merged pull requests:**

- fix: reintroduce the 100 commit checkpoint interval [\#3708](https://github.com/delta-io/delta-rs/pull/3708) ([rtyler](https://github.com/rtyler))
- fix: enabling correctly pulling partition values out of column mapped tables [\#3706](https://github.com/delta-io/delta-rs/pull/3706) ([rtyler](https://github.com/rtyler))
- fix\(format\): fix formatting in Python for conversion file [\#3705](https://github.com/delta-io/delta-rs/pull/3705) ([fvaleye](https://github.com/fvaleye))
- chore: remove unused dependencies [\#3698](https://github.com/delta-io/delta-rs/pull/3698) ([rtyler](https://github.com/rtyler))
- fix\(pandas\): implement-automatic-conversion-for-pandas-null-types [\#3695](https://github.com/delta-io/delta-rs/pull/3695) ([fvaleye](https://github.com/fvaleye))
- chore: update hdfs object store to 0.15 [\#3681](https://github.com/delta-io/delta-rs/pull/3681) ([Kimahriman](https://github.com/Kimahriman))
- feat!: use kernel predicates on file streams [\#3669](https://github.com/delta-io/delta-rs/pull/3669) ([roeap](https://github.com/roeap))
- chore: bump python [\#3664](https://github.com/delta-io/delta-rs/pull/3664) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: use RFC3896 percent encoding with delta protocol correctness [\#3661](https://github.com/delta-io/delta-rs/pull/3661) ([ion-elgreco](https://github.com/ion-elgreco))
- feat!: kernel log replay [\#3660](https://github.com/delta-io/delta-rs/pull/3660) ([roeap](https://github.com/roeap))
- ci: run integration tests against next branches [\#3658](https://github.com/delta-io/delta-rs/pull/3658) ([roeap](https://github.com/roeap))
- fix: handle checking partition filters in array/list when converting … [\#3657](https://github.com/delta-io/delta-rs/pull/3657) ([smeyerre](https://github.com/smeyerre))
- fix: aws special paths encoding [\#3656](https://github.com/delta-io/delta-rs/pull/3656) ([roeap](https://github.com/roeap))
- feat: support converting parquet with non-microsecond timestamps to d… [\#3654](https://github.com/delta-io/delta-rs/pull/3654) ([smeyerre](https://github.com/smeyerre))
- chore: use pytest-xdist for speeding up python tests [\#3642](https://github.com/delta-io/delta-rs/pull/3642) ([rtyler](https://github.com/rtyler))
- chore: remove deprecated use of kernel's Table [\#3639](https://github.com/delta-io/delta-rs/pull/3639) ([rtyler](https://github.com/rtyler))
- feat: add keep\_versions parameter to vacuum command for python [\#3635](https://github.com/delta-io/delta-rs/pull/3635) ([corwinjoy](https://github.com/corwinjoy))
- chore: bump version for release [\#3633](https://github.com/delta-io/delta-rs/pull/3633) ([rtyler](https://github.com/rtyler))
- fix: avoid parsing generationExpressions as JSON [\#3632](https://github.com/delta-io/delta-rs/pull/3632) ([rtyler](https://github.com/rtyler))
- feat: build musl wheels upon release [\#3631](https://github.com/delta-io/delta-rs/pull/3631) ([rtyler](https://github.com/rtyler))
- fix: make the docs link checking more useful/less faily [\#3630](https://github.com/delta-io/delta-rs/pull/3630) ([rtyler](https://github.com/rtyler))
- fix: coerce polars.Array into a suitable Arrow list type [\#3623](https://github.com/delta-io/delta-rs/pull/3623) ([rtyler](https://github.com/rtyler))
- fix: ensure openssl-sys doesn't creep into the dependency via the kernel default engine [\#3619](https://github.com/delta-io/delta-rs/pull/3619) ([rtyler](https://github.com/rtyler))
- fix: allow writing to DeltaTable objects across Python threads [\#3618](https://github.com/delta-io/delta-rs/pull/3618) ([rtyler](https://github.com/rtyler))
- docs: fix broken daft links \(how daft\) [\#3617](https://github.com/delta-io/delta-rs/pull/3617) ([rtyler](https://github.com/rtyler))
- fix: ensure new checkpoints can be written after old checkpoints [\#3616](https://github.com/delta-io/delta-rs/pull/3616) ([rtyler](https://github.com/rtyler))
- fix: switch the url schemes for Azure integration tests [\#3614](https://github.com/delta-io/delta-rs/pull/3614) ([rtyler](https://github.com/rtyler))
- fix: allow non-string primitive types for partition filters when converting to pyarrow dataset [\#3613](https://github.com/delta-io/delta-rs/pull/3613) ([smeyerre](https://github.com/smeyerre))
- refactor: make match\_partitions and new\_metadata public [\#3605](https://github.com/delta-io/delta-rs/pull/3605) ([zeevm](https://github.com/zeevm))
- fix: fix typo to fix CI typo check [\#3604](https://github.com/delta-io/delta-rs/pull/3604) ([alamb](https://github.com/alamb))
- chore: update to DataFusion `49.0.0` [\#3603](https://github.com/delta-io/delta-rs/pull/3603) ([alamb](https://github.com/alamb))
- chore: minor API changes after integration testing [\#3598](https://github.com/delta-io/delta-rs/pull/3598) ([rtyler](https://github.com/rtyler))
- fix: scan time was always 0 for merge metrics [\#3596](https://github.com/delta-io/delta-rs/pull/3596) ([rtyler](https://github.com/rtyler))
- refactor: make "cloud" feature in object\_store optional [\#3590](https://github.com/delta-io/delta-rs/pull/3590) ([zeevm](https://github.com/zeevm))
- chore: generate a more recentish updated changelog [\#3588](https://github.com/delta-io/delta-rs/pull/3588) ([rtyler](https://github.com/rtyler))
- fix: creating new DeltaTable with invalid table name path no longer creates empty directory [\#3504](https://github.com/delta-io/delta-rs/pull/3504) ([smeyerre](https://github.com/smeyerre))


## [rust-v0.27.0](https://github.com/delta-io/delta-rs/tree/rust-v0.27.0) (2025-07-12)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.26.2...rust-v0.27.0)

**Implemented enhancements:**

- Feature: Vacuum with version retention [\#3530](https://github.com/delta-io/delta-rs/issues/3530)
- Any way to prune the delta\_log or support shallow clones [\#3565](https://github.com/delta-io/delta-rs/issues/3565)
- Upgrade Arrow version to 55.1.0 [\#3540](https://github.com/delta-io/delta-rs/issues/3540)
- Add config option to suppress `deltalake_core::writer::stats` warnings about bytes columns [\#3519](https://github.com/delta-io/delta-rs/issues/3519)
- Remove pyarrow dependency \(make opt-in\), replace with arro3 for core components [\#3455](https://github.com/delta-io/delta-rs/issues/3455)
- Don't retry lakefs commit or merge on `412` response \(precondition failed\) [\#3429](https://github.com/delta-io/delta-rs/issues/3429)
- Use `object_store` spawnService [\#3427](https://github.com/delta-io/delta-rs/issues/3427)
- Alter table description [\#3401](https://github.com/delta-io/delta-rs/issues/3401)
- Remove put if absent options injection  [\#3310](https://github.com/delta-io/delta-rs/issues/3310)
- v1.0 Release tracking issue [\#3250](https://github.com/delta-io/delta-rs/issues/3250)
- feat: add a table description and name to the Delta Table from Python [\#3464](https://github.com/delta-io/delta-rs/pull/3464) ([fvaleye](https://github.com/fvaleye))


**Fixed bugs:**

- Python building broken on main due to maturin issue [\#3559](https://github.com/delta-io/delta-rs/issues/3559)
- TypeError: write\_deltalake\(\) got an unexpected keyword argument 'schema' \(deltalake/polars\) [\#3546](https://github.com/delta-io/delta-rs/issues/3546)
- SchemaMismatchError on empty ArrayType field while contains\_null=True [\#3544](https://github.com/delta-io/delta-rs/issues/3544)
- Can't open a delta-table: Unsupported reader features required: DeletionVectors [\#3543](https://github.com/delta-io/delta-rs/issues/3543)
- Attempting to write a transaction 3 but the underlying table has been updated to 3 [\#3534](https://github.com/delta-io/delta-rs/issues/3534)
- DeltaOps not recognizing abfss scheme for Azure [\#3523](https://github.com/delta-io/delta-rs/issues/3523)
- Query execution time difference between `QueryBuilder` and using DataFusion directly. [\#3517](https://github.com/delta-io/delta-rs/issues/3517)
- bug: timezone not preserved & raise exc on merge operation [\#3507](https://github.com/delta-io/delta-rs/issues/3507)
- allow\_unsafe\_rename option stopped working in version 1 [\#3493](https://github.com/delta-io/delta-rs/issues/3493)
- predicate appears to ignore partition and stats in pruning [\#3491](https://github.com/delta-io/delta-rs/issues/3491)
- `max_rows_per_file` ignored when writing with rust engine [\#3490](https://github.com/delta-io/delta-rs/issues/3490)
- delta-rs includes pending versions written by spark [\#3422](https://github.com/delta-io/delta-rs/issues/3422)

**Merged pull requests:**

- chore: bump minor version for rust crate [\#3586](https://github.com/delta-io/delta-rs/pull/3586) ([rtyler](https://github.com/rtyler))
- refactor!: use delta-kernel Protocol and Metadata actions [\#3581](https://github.com/delta-io/delta-rs/pull/3581) ([roeap](https://github.com/roeap))
- feat: vacuum with version retention [\#3537](https://github.com/delta-io/delta-rs/pull/3537) ([corwinjoy](https://github.com/corwinjoy))
- chore: bump patch versions for another relaese [\#3585](https://github.com/delta-io/delta-rs/pull/3585) ([rtyler](https://github.com/rtyler))
- feat: write `engineInfo` with delta-rs version [\#3584](https://github.com/delta-io/delta-rs/pull/3584) ([zachschuermann](https://github.com/zachschuermann))
- chore: remove the deltalake-sql crate [\#3582](https://github.com/delta-io/delta-rs/pull/3582) ([rtyler](https://github.com/rtyler))
- chore: latest clippy [\#3571](https://github.com/delta-io/delta-rs/pull/3571) ([roeap](https://github.com/roeap))
- feat: convert partition filters to kernel predicates [\#3570](https://github.com/delta-io/delta-rs/pull/3570) ([roeap](https://github.com/roeap))
- refactor: move schema code to kernel module [\#3569](https://github.com/delta-io/delta-rs/pull/3569) ([roeap](https://github.com/roeap))
- chore: remove redundant words in comment [\#3568](https://github.com/delta-io/delta-rs/pull/3568) ([shangchenglumetro](https://github.com/shangchenglumetro))
- docs: ensure create\_checkpoint\(\) is visible in the Python API docs [\#3564](https://github.com/delta-io/delta-rs/pull/3564) ([itamarst](https://github.com/itamarst))
- chore: upgrade to delta\_kernel 0.12.x [\#3561](https://github.com/delta-io/delta-rs/pull/3561) ([rtyler](https://github.com/rtyler))
- chore: clean up licenses in python project which are causing build issues [\#3560](https://github.com/delta-io/delta-rs/pull/3560) ([rtyler](https://github.com/rtyler))
- chore: update arrow/parquet to 55.2.0 [\#3558](https://github.com/delta-io/delta-rs/pull/3558) ([alamb](https://github.com/alamb))
- fix: use proper DeltaTableState for vacuum commits [\#3550](https://github.com/delta-io/delta-rs/pull/3550) ([jeromegn](https://github.com/jeromegn))
- fix: version binary search [\#3549](https://github.com/delta-io/delta-rs/pull/3549) ([aditanase](https://github.com/aditanase))
- chore: update the minor version to reflect a behavior change [\#3542](https://github.com/delta-io/delta-rs/pull/3542) ([rtyler](https://github.com/rtyler))
- chore: pin aws crates [\#3532](https://github.com/delta-io/delta-rs/pull/3532) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: set java version to 21 for pyspark 4.0 [\#3524](https://github.com/delta-io/delta-rs/pull/3524) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: using state provided in args in merge op [\#3522](https://github.com/delta-io/delta-rs/pull/3522) ([gtrawinski](https://github.com/gtrawinski))
- refactor: remove unecessary uses of datafusion subcrates [\#3521](https://github.com/delta-io/delta-rs/pull/3521) ([alamb](https://github.com/alamb))
- chore: update to DataFusion `48.0.0` / arrow to 55.2.0 [\#3520](https://github.com/delta-io/delta-rs/pull/3520) ([alamb](https://github.com/alamb))
- feat: make TableConfig accessible [\#3518](https://github.com/delta-io/delta-rs/pull/3518) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: remove forced table update from python writer [\#3515](https://github.com/delta-io/delta-rs/pull/3515) ([ohanf](https://github.com/ohanf))
- refactor: compute stats schema with kernel types [\#3514](https://github.com/delta-io/delta-rs/pull/3514) ([roeap](https://github.com/roeap))
- feat: add convenience extension for kernel engine types [\#3510](https://github.com/delta-io/delta-rs/pull/3510) ([roeap](https://github.com/roeap))
- refactor: move LazyTableProvider into python crate [\#3509](https://github.com/delta-io/delta-rs/pull/3509) ([roeap](https://github.com/roeap))
- fix: setting wrong schema in table provider for `merge` [\#3508](https://github.com/delta-io/delta-rs/pull/3508) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: constraint parsing, roundtripping [\#3503](https://github.com/delta-io/delta-rs/pull/3503) ([ion-elgreco](https://github.com/ion-elgreco))
- refactor!: have DeltaTable::version return an Option [\#3500](https://github.com/delta-io/delta-rs/pull/3500) ([roeap](https://github.com/roeap))
- chore!: remove get\_earliest\_version [\#3499](https://github.com/delta-io/delta-rs/pull/3499) ([roeap](https://github.com/roeap))
- chore: prepare for the next python release [\#3498](https://github.com/delta-io/delta-rs/pull/3498) ([rtyler](https://github.com/rtyler))
- ci: improve coverage collection [\#3497](https://github.com/delta-io/delta-rs/pull/3497) ([roeap](https://github.com/roeap))
- chore: update runner [\#3494](https://github.com/delta-io/delta-rs/pull/3494) ([ion-elgreco](https://github.com/ion-elgreco))
- docs: update link to df [\#3489](https://github.com/delta-io/delta-rs/pull/3489) ([rluvaton](https://github.com/rluvaton))
- refactor!: remove and deprecate some python methods [\#3488](https://github.com/delta-io/delta-rs/pull/3488) ([roeap](https://github.com/roeap))
- fix: ensure projecting only columns that exist in new files afte sche… [\#3487](https://github.com/delta-io/delta-rs/pull/3487) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- chore: exclude Invariants from the default writer v2 feature set [\#3486](https://github.com/delta-io/delta-rs/pull/3486) ([rtyler](https://github.com/rtyler))
- test: improve storage config testing [\#3485](https://github.com/delta-io/delta-rs/pull/3485) ([roeap](https://github.com/roeap))
- refactor!: get transaction versions for specific applications [\#3484](https://github.com/delta-io/delta-rs/pull/3484) ([roeap](https://github.com/roeap))
- docs: fix bullet list formatting in dagster docs [\#3483](https://github.com/delta-io/delta-rs/pull/3483) ([avriiil](https://github.com/avriiil))
- fix: set casting safe param to False [\#3481](https://github.com/delta-io/delta-rs/pull/3481) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: update kernel to 0.11 [\#3480](https://github.com/delta-io/delta-rs/pull/3480) ([roeap](https://github.com/roeap))
- chore: update migration docs [\#3479](https://github.com/delta-io/delta-rs/pull/3479) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: remove unused stats\_parsed field [\#3475](https://github.com/delta-io/delta-rs/pull/3475) ([roeap](https://github.com/roeap))
- refactor: remove protocol error [\#3473](https://github.com/delta-io/delta-rs/pull/3473) ([roeap](https://github.com/roeap))
- chore: more typos [\#3471](https://github.com/delta-io/delta-rs/pull/3471) ([roeap](https://github.com/roeap))
- chore: remove unused time\_utils [\#3470](https://github.com/delta-io/delta-rs/pull/3470) ([roeap](https://github.com/roeap))
- chore: set correct markers [\#3469](https://github.com/delta-io/delta-rs/pull/3469) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: schema conversion, add conversion test cases [\#3468](https://github.com/delta-io/delta-rs/pull/3468) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: write checkpoints with kernel [\#3466](https://github.com/delta-io/delta-rs/pull/3466) ([roeap](https://github.com/roeap))
- fix: correct spelling errors found by CI spell checker [\#3465](https://github.com/delta-io/delta-rs/pull/3465) ([fvaleye](https://github.com/fvaleye))
- chore: update kernel [\#3462](https://github.com/delta-io/delta-rs/pull/3462) ([roeap](https://github.com/roeap))
- fix: use more accurate log path parsing [\#3461](https://github.com/delta-io/delta-rs/pull/3461) ([roeap](https://github.com/roeap))
- refactor: remove pyarrow dependency [\#3459](https://github.com/delta-io/delta-rs/pull/3459) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: mark more tests which require datafusion [\#3458](https://github.com/delta-io/delta-rs/pull/3458) ([rtyler](https://github.com/rtyler))
- ci: add spellchecker to pr tests [\#3457](https://github.com/delta-io/delta-rs/pull/3457) ([roeap](https://github.com/roeap))
- refactor: use full paths in log processing [\#3456](https://github.com/delta-io/delta-rs/pull/3456) ([roeap](https://github.com/roeap))
- chore: ensuring default builds work without datafusion [\#3453](https://github.com/delta-io/delta-rs/pull/3453) ([rtyler](https://github.com/rtyler))
- refactor: use LogStore in Snapshot / LogSegment APIs [\#3452](https://github.com/delta-io/delta-rs/pull/3452) ([roeap](https://github.com/roeap))
- test: avoid circular dependency with core/test crates [\#3450](https://github.com/delta-io/delta-rs/pull/3450) ([roeap](https://github.com/roeap))
- feat: expose kernel Engine on LogStore [\#3446](https://github.com/delta-io/delta-rs/pull/3446) ([roeap](https://github.com/roeap))
- refactor: more specific factory parameter names [\#3445](https://github.com/delta-io/delta-rs/pull/3445) ([roeap](https://github.com/roeap))
- docs: add 1.0.0 migration guide [\#3443](https://github.com/delta-io/delta-rs/pull/3443) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: minor table module refactors [\#3442](https://github.com/delta-io/delta-rs/pull/3442) ([rtyler](https://github.com/rtyler))
- chore: remove unused code and deps [\#3441](https://github.com/delta-io/delta-rs/pull/3441) ([roeap](https://github.com/roeap))
- chore: experiment with using sccache in GitHub Actions [\#3437](https://github.com/delta-io/delta-rs/pull/3437) ([rtyler](https://github.com/rtyler))
- feat: optimize datafusion predicate pushdown and partition pruning [\#3436](https://github.com/delta-io/delta-rs/pull/3436) ([rtyler](https://github.com/rtyler))
- chore: prepare py-1.0 release [\#3435](https://github.com/delta-io/delta-rs/pull/3435) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: make codecov more vigorously enforced to help ensure quality [\#3434](https://github.com/delta-io/delta-rs/pull/3434) ([rtyler](https://github.com/rtyler))
- chore: rely on the testing during coverage generation to speed up tests [\#3431](https://github.com/delta-io/delta-rs/pull/3431) ([rtyler](https://github.com/rtyler))
- chore: bump crate versions which are due for release [\#3430](https://github.com/delta-io/delta-rs/pull/3430) ([rtyler](https://github.com/rtyler))
- chore\(deps\): bump foyer to v0.17.2 to prevent from wrong result [\#3428](https://github.com/delta-io/delta-rs/pull/3428) ([MrCroxx](https://github.com/MrCroxx))
- feat: spawn io with spawn service [\#3426](https://github.com/delta-io/delta-rs/pull/3426) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: ignore temp log entries [\#3423](https://github.com/delta-io/delta-rs/pull/3423) ([corwinjoy](https://github.com/corwinjoy))
- fix: build Unity Catalog crate without DataFusion [\#3420](https://github.com/delta-io/delta-rs/pull/3420) ([linhr](https://github.com/linhr))
- feat: added a check for gc code to run [\#3419](https://github.com/delta-io/delta-rs/pull/3419) ([JustinRush80](https://github.com/JustinRush80))
- chore: include license file in deltalake-derive crate [\#3417](https://github.com/delta-io/delta-rs/pull/3417) ([ankane](https://github.com/ankane))
- fix: drop column update [\#3416](https://github.com/delta-io/delta-rs/pull/3416) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: missed a version bump for core [\#3415](https://github.com/delta-io/delta-rs/pull/3415) ([rtyler](https://github.com/rtyler))
- chore: bringing dat integration testing in ahead of kernel replay [\#3411](https://github.com/delta-io/delta-rs/pull/3411) ([rtyler](https://github.com/rtyler))
- chore: reduce scope of feature flags and compilation requirements for subcrates [\#3409](https://github.com/delta-io/delta-rs/pull/3409) ([rtyler](https://github.com/rtyler))
- chore: commit the contents of the 0.26.0 release [\#3408](https://github.com/delta-io/delta-rs/pull/3408) ([rtyler](https://github.com/rtyler))
- chore: bump versions of rust crates for another release party [\#3406](https://github.com/delta-io/delta-rs/pull/3406) ([rtyler](https://github.com/rtyler))
- fix: the default target size should be 100MB [\#3404](https://github.com/delta-io/delta-rs/pull/3404) ([HiromuHota](https://github.com/HiromuHota))
- chore: update delta\_kernel to 0.10.0 [\#3403](https://github.com/delta-io/delta-rs/pull/3403) ([zachschuermann](https://github.com/zachschuermann))
- refactor: make "cloud" feature in object\_store optional [\#3398](https://github.com/delta-io/delta-rs/pull/3398) ([zeevm](https://github.com/zeevm))
- chore: put a couple symbols behind the right feature gate [\#3393](https://github.com/delta-io/delta-rs/pull/3393) ([rtyler](https://github.com/rtyler))
- fix: clippy warnings [\#3390](https://github.com/delta-io/delta-rs/pull/3390) ([alamb](https://github.com/alamb))
- feat: derive macro for config implementations [\#3389](https://github.com/delta-io/delta-rs/pull/3389) ([roeap](https://github.com/roeap))
- feat!: update storage configuration system [\#3383](https://github.com/delta-io/delta-rs/pull/3383) ([roeap](https://github.com/roeap))
- refactor!: move storage module into logstore [\#3382](https://github.com/delta-io/delta-rs/pull/3382) ([roeap](https://github.com/roeap))
- chore: move proofs into dedicated folder [\#3381](https://github.com/delta-io/delta-rs/pull/3381) ([roeap](https://github.com/roeap))
- refactor: move transaction module to kernel [\#3380](https://github.com/delta-io/delta-rs/pull/3380) ([roeap](https://github.com/roeap))
- chore: clippy [\#3379](https://github.com/delta-io/delta-rs/pull/3379) ([roeap](https://github.com/roeap))
- feat: upgrade to DataFusion 47.0.0 [\#3378](https://github.com/delta-io/delta-rs/pull/3378) ([alamb](https://github.com/alamb))
- fix: if field contains space in constraint expression, checks will fail [\#3374](https://github.com/delta-io/delta-rs/pull/3374) ([Nordalf](https://github.com/Nordalf))
- fix: parse unconventional logs [\#3373](https://github.com/delta-io/delta-rs/pull/3373) ([roeap](https://github.com/roeap))
- feat: introduce VacuumMode::Full for cleaning up orphaned files [\#3368](https://github.com/delta-io/delta-rs/pull/3368) ([rtyler](https://github.com/rtyler))
- chore: fix some minor build warnings [\#3366](https://github.com/delta-io/delta-rs/pull/3366) ([rtyler](https://github.com/rtyler))
- chore: remove cdf feature [\#3365](https://github.com/delta-io/delta-rs/pull/3365) ([ion-elgreco](https://github.com/ion-elgreco))
- docs: add example how to authenticate using Azure CLI for Azure ADSL integration [\#3357](https://github.com/delta-io/delta-rs/pull/3357) ([DanielBertocci](https://github.com/DanielBertocci))
- fix: parse snapshot [\#3355](https://github.com/delta-io/delta-rs/pull/3355) ([ion-elgreco](https://github.com/ion-elgreco))
- docs: update merge-tables.md with "Optimizing Merge Performance" section [\#3351](https://github.com/delta-io/delta-rs/pull/3351) ([ldacey](https://github.com/ldacey))
- fix: use field physical name when resolving partition columns [\#3349](https://github.com/delta-io/delta-rs/pull/3349) ([zeevm](https://github.com/zeevm))
- feat: during LakeFS file operations, skip merge when 0 changes [\#3346](https://github.com/delta-io/delta-rs/pull/3346) ([smeyerre](https://github.com/smeyerre))
- refactor\(python\): improve typing, linting [\#3344](https://github.com/delta-io/delta-rs/pull/3344) ([ion-elgreco](https://github.com/ion-elgreco))
- docs: update dataFusion integration example [\#3343](https://github.com/delta-io/delta-rs/pull/3343) ([riziles](https://github.com/riziles))
- perf: use lazy sync reader [\#3338](https://github.com/delta-io/delta-rs/pull/3338) ([ion-elgreco](https://github.com/ion-elgreco))
- feat\(api\): add rustls and native-tls features [\#3335](https://github.com/delta-io/delta-rs/pull/3335) ([zeevm](https://github.com/zeevm))
- refactor: add 'cloud' feature to 'core' to enable 'cloud' on 'object\_store' only when needed [\#3332](https://github.com/delta-io/delta-rs/pull/3332) ([zeevm](https://github.com/zeevm))
- chore: improve io error msg [\#3328](https://github.com/delta-io/delta-rs/pull/3328) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: remove pyarrow upper [\#3325](https://github.com/delta-io/delta-rs/pull/3325) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: block\_in\_place to allow nested tasks [\#3324](https://github.com/delta-io/delta-rs/pull/3324) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: check for all known valid delta files in is\_deltatable [\#3318](https://github.com/delta-io/delta-rs/pull/3318) ([umartin](https://github.com/umartin))
- fix: added restored metadata as action to the next committed version [\#3303](https://github.com/delta-io/delta-rs/pull/3303) ([Nordalf](https://github.com/Nordalf))
- fix: correct Python docs for incremental compaction on OPTIMIZE [\#3301](https://github.com/delta-io/delta-rs/pull/3301) ([roykim98](https://github.com/roykim98))

## [rust-v0.26.2](https://github.com/delta-io/delta-rs/tree/rust-v0.26.2) (2025-05-15)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.26.1...rust-v0.26.2)

**Fixed bugs:**

- Unable to use deltalake with MinIO [\#3418](https://github.com/delta-io/delta-rs/issues/3418)
- Column \_\_delta\_rs\_update\_predicate when update a table [\#3414](https://github.com/delta-io/delta-rs/issues/3414)
- Onelake incompatible with rustls [\#3243](https://github.com/delta-io/delta-rs/issues/3243)

## [rust-v0.26.1](https://github.com/delta-io/delta-rs/tree/rust-v0.26.1) (2025-05-05)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.26.0...rust-v0.26.1)

## [rust-v0.26.0](https://github.com/delta-io/delta-rs/tree/rust-v0.26.0) (2025-05-03)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.25.0...rust-v0.26.0)

**Implemented enhancements:**

- Make "cloud" feature optional [\#3397](https://github.com/delta-io/delta-rs/issues/3397)
- Delta Column mapping feature [\#3358](https://github.com/delta-io/delta-rs/issues/3358)
- Allow choosing kernel built-in engine [\#3334](https://github.com/delta-io/delta-rs/issues/3334)
- Don't enable "cloud" feature on object\_store by default [\#3331](https://github.com/delta-io/delta-rs/issues/3331)
- Cannot use latest delta-rs with pyarrow 19.0.1 [\#3323](https://github.com/delta-io/delta-rs/issues/3323)
- Support for Parquet Bloom Filters at RowGroup Level [\#3322](https://github.com/delta-io/delta-rs/issues/3322)

**Fixed bugs:**

- Timezone information in schema dropped and converted to UTC when writing PyArrow table [\#3402](https://github.com/delta-io/delta-rs/issues/3402)
- Got "Generic DeltaTable error: type\_coercion" when updating the deltatable [\#3400](https://github.com/delta-io/delta-rs/issues/3400)
- Removing pinned chrono version [\#3391](https://github.com/delta-io/delta-rs/issues/3391)
- CDF changes fully loaded into memory [\#3388](https://github.com/delta-io/delta-rs/issues/3388)
- Unable to use `write_deltalake` on nullable columns with mode = "overwrite" [\#3387](https://github.com/delta-io/delta-rs/issues/3387)
- `uv add deltalake` fails without pinning to 0.25.4 [\#3364](https://github.com/delta-io/delta-rs/issues/3364)
- Unable to use Arrow UUID type with DeltaLake and schema\(\).to\_pyarrow\(\), Python Exception raised. [\#3363](https://github.com/delta-io/delta-rs/issues/3363)
- Writer Incompatibility Issue Between Delta Lake Protocol Version and Rust Writer [\#3356](https://github.com/delta-io/delta-rs/issues/3356)
- Restore not readable in Synapse/Databricks [\#3354](https://github.com/delta-io/delta-rs/issues/3354)
- Restore is not rolling back schema changes [\#3352](https://github.com/delta-io/delta-rs/issues/3352)
- Schema Mode merge with subfields 'struct fields don't match' [\#3350](https://github.com/delta-io/delta-rs/issues/3350)
- Error resolving renamed partition columns [\#3348](https://github.com/delta-io/delta-rs/issues/3348)
- `DeltaError` on attempting merge operation involving map field [\#3340](https://github.com/delta-io/delta-rs/issues/3340)
- MaxCommitAttempts error due to stale snapshot used during OPTIMIZE [\#3337](https://github.com/delta-io/delta-rs/issues/3337)
- \_internal.DeltaError: Failed to parse parquet: Parquet error: Z-order failed while scanning data [\#3327](https://github.com/delta-io/delta-rs/issues/3327)
- Error writing delta table with Null columns [\#3316](https://github.com/delta-io/delta-rs/issues/3316)
- Error in writing panda data frame with pyarrow engine to DeltaTable [\#3315](https://github.com/delta-io/delta-rs/issues/3315)
- DeltaTable.is\_deltatable sometimes return false for valid deltatable [\#3314](https://github.com/delta-io/delta-rs/issues/3314)
- Runtime panic in new streaming writer: Cannot start a runtime from within a runtime [\#3271](https://github.com/delta-io/delta-rs/issues/3271)
- Build failure due to conflicting strum versions in deltalake-core dependencies [\#3267](https://github.com/delta-io/delta-rs/issues/3267)

**Closed issues:**

- How to read delta table in `azure databricks unity catalog` [\#3276](https://github.com/delta-io/delta-rs/issues/3276)
- Feature request: provide capability for pass in timestamp value when dump delta log [\#3258](https://github.com/delta-io/delta-rs/issues/3258)

## [rust-v0.25.0](https://github.com/delta-io/delta-rs/tree/rust-v0.25.0) (2025-03-09)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.24.0...rust-v0.25.0)

**Implemented enhancements:**

- Configurable column encoding for parquet checkpoint files to address Fabric limitation [\#3212](https://github.com/delta-io/delta-rs/issues/3212)

**Fixed bugs:**

- Writing from Windows host to Ubuntu WSL virtual machine doesn't succeed [\#3307](https://github.com/delta-io/delta-rs/issues/3307)
- DeltaTable\(path\) errors out for unknown data types [\#3305](https://github.com/delta-io/delta-rs/issues/3305)
- Commit Properties / Custom Metadata not loaded [\#3304](https://github.com/delta-io/delta-rs/issues/3304)
- peek\_next\_commit\(\) panics on invalid json [\#3297](https://github.com/delta-io/delta-rs/issues/3297)
- cargo build fail on macOS 15.3.1 [\#3295](https://github.com/delta-io/delta-rs/issues/3295)
- Error when writing polars dataframe with enum or categorical columns [\#3284](https://github.com/delta-io/delta-rs/issues/3284)
- Schema evolution causing table ID to be regenerated, breaks Spark streaming jobs [\#3274](https://github.com/delta-io/delta-rs/issues/3274)
- is\_deltatable creates the path if it doesn't exist [\#3259](https://github.com/delta-io/delta-rs/issues/3259)
- `is_deltatable` throwing S3 error in 0.25.1 on linux aarch64 build [\#3241](https://github.com/delta-io/delta-rs/issues/3241)
- Datafusion error: External error: Failed to get a credential from UnityCatalog client configuration. [\#3236](https://github.com/delta-io/delta-rs/issues/3236)
- deltalake 0.24.0 cannot compile with E0308 error [\#3235](https://github.com/delta-io/delta-rs/issues/3235)
- 0.25.0 can't be pip installed on linux [\#3234](https://github.com/delta-io/delta-rs/issues/3234)
- Azure blob - trying to fetch \_delta\_log for large table exception: Failed to parse parquet: External: Generic MicrosoftAzure error: error decoding response body [\#3232](https://github.com/delta-io/delta-rs/issues/3232)
- Trying to open a `DeltaTable` at a non-existent path creates the path [\#3228](https://github.com/delta-io/delta-rs/issues/3228)
- don't write deletion vector entry in the log [\#3211](https://github.com/delta-io/delta-rs/issues/3211)

**Closed issues:**

- Memory leak on 0.25.x [\#3306](https://github.com/delta-io/delta-rs/issues/3306)

**Merged pull requests:**

- fix: serialize empty deletionVector in add actions as absent [\#3309](https://github.com/delta-io/delta-rs/pull/3309) ([rtyler](https://github.com/rtyler))
- fix: prevent panics when peek\_next\_commit\(\) encounters invalid data [\#3308](https://github.com/delta-io/delta-rs/pull/3308) ([rtyler](https://github.com/rtyler))
- fix\(pandas\): retain pyarrow decimal datatype in to\_pandas\(\) by adding types\_mapper to prevent precision loss [\#3296](https://github.com/delta-io/delta-rs/pull/3296) ([Abhishek1005](https://github.com/Abhishek1005))
- chore: bump python version for release [\#3291](https://github.com/delta-io/delta-rs/pull/3291) ([rtyler](https://github.com/rtyler))
- feat: remove optimize operations when building without Apache Datafusion [\#3290](https://github.com/delta-io/delta-rs/pull/3290) ([rtyler](https://github.com/rtyler))
- chore: upgrade the kernel version and bump our majorish versions too [\#3289](https://github.com/delta-io/delta-rs/pull/3289) ([rtyler](https://github.com/rtyler))
- fix: timestamp truncation in stats parsed [\#3288](https://github.com/delta-io/delta-rs/pull/3288) ([ion-elgreco](https://github.com/ion-elgreco))
- refactor: drop pyarrow support, restructure python modules [\#3285](https://github.com/delta-io/delta-rs/pull/3285) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: compare before after schema before creating action [\#3282](https://github.com/delta-io/delta-rs/pull/3282) ([ion-elgreco](https://github.com/ion-elgreco))
- refactor: simplify expressions [\#3281](https://github.com/delta-io/delta-rs/pull/3281) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: remove unused dev dependency on `home` [\#3277](https://github.com/delta-io/delta-rs/pull/3277) ([alamb](https://github.com/alamb))
- fix\(rust\): on write, have a schema evolution maintain metadata table id [\#3275](https://github.com/delta-io/delta-rs/pull/3275) ([liamphmurphy](https://github.com/liamphmurphy))
- fix: chrono 0.4.40 causes disambiguation syntax when building arrow deps [\#3272](https://github.com/delta-io/delta-rs/pull/3272) ([Nordalf](https://github.com/Nordalf))
- refactor: replaced asterisk with constraint name in get\_constraints [\#3270](https://github.com/delta-io/delta-rs/pull/3270) ([Nordalf](https://github.com/Nordalf))
- fix: commitlint config [\#3268](https://github.com/delta-io/delta-rs/pull/3268) ([roeap](https://github.com/roeap))
- chore: use builder API to create `FileScanConfig` [\#3266](https://github.com/delta-io/delta-rs/pull/3266) ([alamb](https://github.com/alamb))
- chore: update tests to use `Column::new` and other expr\_fn functions [\#3265](https://github.com/delta-io/delta-rs/pull/3265) ([alamb](https://github.com/alamb))
- chore: fix clippy warnings on main [\#3264](https://github.com/delta-io/delta-rs/pull/3264) ([alamb](https://github.com/alamb))
- fix: aarch64 ssl handshakes [\#3263](https://github.com/delta-io/delta-rs/pull/3263) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: fix some compiler warnings [\#3262](https://github.com/delta-io/delta-rs/pull/3262) ([alamb](https://github.com/alamb))
- chore: upgrade to DataFusion 46.0.0 [\#3261](https://github.com/delta-io/delta-rs/pull/3261) ([alamb](https://github.com/alamb))
- fix: enable missing cloud features for uc crate [\#3260](https://github.com/delta-io/delta-rs/pull/3260) ([omkar-foss](https://github.com/omkar-foss))
- docs: update CONTRIBUTING.md and Makefile to account for switch to uv [\#3257](https://github.com/delta-io/delta-rs/pull/3257) ([adamreeve](https://github.com/adamreeve))
- refactor: async writer + multi-part [\#3255](https://github.com/delta-io/delta-rs/pull/3255) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: refresh snapshot after vacuuming logs [\#3252](https://github.com/delta-io/delta-rs/pull/3252) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: more manual inline format args [\#3251](https://github.com/delta-io/delta-rs/pull/3251) ([nyurik](https://github.com/nyurik))
- chore: update readme for Generated columns [\#3247](https://github.com/delta-io/delta-rs/pull/3247) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: set clientVersion for python binding [\#3242](https://github.com/delta-io/delta-rs/pull/3242) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: bump the patch for a new release of python [\#3240](https://github.com/delta-io/delta-rs/pull/3240) ([rtyler](https://github.com/rtyler))
- chore: use rustls [\#3239](https://github.com/delta-io/delta-rs/pull/3239) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: use tableprovider scan in z-order [\#3238](https://github.com/delta-io/delta-rs/pull/3238) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: bump python 0.25 [\#3233](https://github.com/delta-io/delta-rs/pull/3233) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: switch to cache@v4 [\#3230](https://github.com/delta-io/delta-rs/pull/3230) ([rtyler](https://github.com/rtyler))
- feat: streamed execution, writer refactor, simplified generated columns and schema evolution [\#3229](https://github.com/delta-io/delta-rs/pull/3229) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(typo\): s/implemtnation/implementation/ [\#3227](https://github.com/delta-io/delta-rs/pull/3227) ([akesling](https://github.com/akesling))
- feat: cdf tableprovider with predicate pushdown support [\#3220](https://github.com/delta-io/delta-rs/pull/3220) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(rust, python\): treat FSCK files\_removed as strings [\#3219](https://github.com/delta-io/delta-rs/pull/3219) ([liamphmurphy](https://github.com/liamphmurphy))
- fix: load cdf latest version [\#3218](https://github.com/delta-io/delta-rs/pull/3218) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: make Add:get\_stats public [\#3216](https://github.com/delta-io/delta-rs/pull/3216) ([jkylling](https://github.com/jkylling))
- feat: configurable column encoding for parquet checkpoint files [\#3214](https://github.com/delta-io/delta-rs/pull/3214) ([dmunch](https://github.com/dmunch))
- chore: use flags for apple arm64 [\#3213](https://github.com/delta-io/delta-rs/pull/3213) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: only use stats for required cols [\#3210](https://github.com/delta-io/delta-rs/pull/3210) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: adhere write stats configuration [\#3209](https://github.com/delta-io/delta-rs/pull/3209) ([ion-elgreco](https://github.com/ion-elgreco))

## [rust-v0.20.1](https://github.com/delta-io/delta-rs/tree/rust-v0.20.1) (2024-09-27)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.20.0...rust-v0.20.1)

**Implemented enhancements:**

- Allow to specify Azurite hostname and service port as backend [\#2900](https://github.com/delta-io/delta-rs/issues/2900)
- docs section usage/Managing a table is out of date w.r.t. optimizing tables [\#2891](https://github.com/delta-io/delta-rs/issues/2891)
- generate more sensible row group size [\#2545](https://github.com/delta-io/delta-rs/issues/2545)

**Fixed bugs:**

- Cannot write to Minio with deltalake.write\_deltalake or Polars [\#2894](https://github.com/delta-io/delta-rs/issues/2894)
- Schema Mismatch Error When appending Parquet Files with Metadata using Rust Engine [\#2888](https://github.com/delta-io/delta-rs/issues/2888)
- Assume role support has been broken since 2022 :rofl:  [\#2879](https://github.com/delta-io/delta-rs/issues/2879)
- z-order fails on table that is partitioned by value with space [\#2834](https://github.com/delta-io/delta-rs/issues/2834)
- "builder error for url" when creating an instance of a DeltaTable which is located in an azurite blob storage [\#2815](https://github.com/delta-io/delta-rs/issues/2815)

**Closed issues:**

- delta-rs can't write to a table if datafusion is not enabled [\#2910](https://github.com/delta-io/delta-rs/issues/2910)

## [rust-v0.20.0](https://github.com/delta-io/delta-rs/tree/rust-v0.20.0) (2024-09-18)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.19.1...rust-v0.20.0)

**Fixed bugs:**

- DeltaTableBuilder flags ignored [\#2808](https://github.com/delta-io/delta-rs/issues/2808)
- Require files in config is not anymore used to skip reading add actions [\#2796](https://github.com/delta-io/delta-rs/issues/2796)

**Merged pull requests:**

- feat: improve AWS credential loading between S3 and DynamoDb code paths [\#2887](https://github.com/delta-io/delta-rs/pull/2887) ([rtyler](https://github.com/rtyler))
- feat: add support for `pyarrow.ExtensionType` [\#2885](https://github.com/delta-io/delta-rs/pull/2885) ([fecet](https://github.com/fecet))
- fix: conditionally disable enable\_io non-unix based systems [\#2884](https://github.com/delta-io/delta-rs/pull/2884) ([hntd187](https://github.com/hntd187))
- docs: fix typo in delta-lake-dagster [\#2883](https://github.com/delta-io/delta-rs/pull/2883) ([jessy1092](https://github.com/jessy1092))
- fix: pin broken dependencies and changes in 0.19.1 [\#2878](https://github.com/delta-io/delta-rs/pull/2878) ([rtyler](https://github.com/rtyler))
- chore: cleanup codecov defaults [\#2876](https://github.com/delta-io/delta-rs/pull/2876) ([rtyler](https://github.com/rtyler))
- fix: prepare the next :crab: release with fixed version ranges [\#2875](https://github.com/delta-io/delta-rs/pull/2875) ([rtyler](https://github.com/rtyler))
- chore: exclude parquet from dependabot as well [\#2874](https://github.com/delta-io/delta-rs/pull/2874) ([rtyler](https://github.com/rtyler))
- chore: attempt to ignore all dependabot checks for arrow and datafusion [\#2870](https://github.com/delta-io/delta-rs/pull/2870) ([rtyler](https://github.com/rtyler))
- fix\(rust\): scan schema fix for predicate [\#2869](https://github.com/delta-io/delta-rs/pull/2869) ([sherlockbeard](https://github.com/sherlockbeard))
- chore: rearrange github actions a bit [\#2868](https://github.com/delta-io/delta-rs/pull/2868) ([rtyler](https://github.com/rtyler))
- fix: set put mode to overwrite in mount backend [\#2861](https://github.com/delta-io/delta-rs/pull/2861) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: pin the build-dependencies for Python to a slightly older vendored openssl [\#2856](https://github.com/delta-io/delta-rs/pull/2856) ([rtyler](https://github.com/rtyler))
- fix: escaped columns in dataskippingstatscolumns [\#2855](https://github.com/delta-io/delta-rs/pull/2855) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: re-enable optional old casting behavior in merge [\#2853](https://github.com/delta-io/delta-rs/pull/2853) ([ion-elgreco](https://github.com/ion-elgreco))
- docs: concurrent writes permission missing [\#2846](https://github.com/delta-io/delta-rs/pull/2846) ([poguez](https://github.com/poguez))
- chore: update python [\#2845](https://github.com/delta-io/delta-rs/pull/2845) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: pin the Rust baseline version to 1.80 [\#2842](https://github.com/delta-io/delta-rs/pull/2842) ([rtyler](https://github.com/rtyler))
- fix: stats is optional in add action [\#2841](https://github.com/delta-io/delta-rs/pull/2841) ([jkylling](https://github.com/jkylling))
- chore\(aws\): use backon to replace backoff [\#2840](https://github.com/delta-io/delta-rs/pull/2840) ([Xuanwo](https://github.com/Xuanwo))
- feat\(rust\): add operationMetrics to WRITE [\#2838](https://github.com/delta-io/delta-rs/pull/2838) ([gavinmead](https://github.com/gavinmead))
- chore: enable codecov reporting [\#2836](https://github.com/delta-io/delta-rs/pull/2836) ([rtyler](https://github.com/rtyler))
- docs: fix documentation about max\_spill\_size [\#2835](https://github.com/delta-io/delta-rs/pull/2835) ([junhl](https://github.com/junhl))
- chore: set max\_retries in CommitProperties [\#2826](https://github.com/delta-io/delta-rs/pull/2826) ([helanto](https://github.com/helanto))
- refactor\(python\): post\_commit\_hook\_properties derive [\#2824](https://github.com/delta-io/delta-rs/pull/2824) ([ion-elgreco](https://github.com/ion-elgreco))
- refactor\(python\): add pymergebuilder [\#2823](https://github.com/delta-io/delta-rs/pull/2823) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: make `Add::get_json_stats` public [\#2822](https://github.com/delta-io/delta-rs/pull/2822) ([gruuya](https://github.com/gruuya))
- docs: fix docstring of set\_table\_properties [\#2820](https://github.com/delta-io/delta-rs/pull/2820) ([astrojuanlu](https://github.com/astrojuanlu))
- fix\(rust\): set token provider explicitly [\#2817](https://github.com/delta-io/delta-rs/pull/2817) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: public method to get partitions for DeltaTable \(\#2671\) [\#2816](https://github.com/delta-io/delta-rs/pull/2816) ([omkar-foss](https://github.com/omkar-foss))
- perf: conditional put for default log store \(e.g. azure, gcs, minio, cloudflare\) [\#2813](https://github.com/delta-io/delta-rs/pull/2813) ([ion-elgreco](https://github.com/ion-elgreco))
- test\(python\): fix optimize call in benchmark [\#2812](https://github.com/delta-io/delta-rs/pull/2812) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: use table config target file size, expose target\_file\_size in python [\#2811](https://github.com/delta-io/delta-rs/pull/2811) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(python, rust\): use require files [\#2809](https://github.com/delta-io/delta-rs/pull/2809) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(python, rust\): allow `in` pushdowns in early\_filter [\#2807](https://github.com/delta-io/delta-rs/pull/2807) ([ion-elgreco](https://github.com/ion-elgreco))
- docs: added WriterProperties documentation [\#2804](https://github.com/delta-io/delta-rs/pull/2804) ([sherlockbeard](https://github.com/sherlockbeard))
- fix: enable feature flags to which deltalake-core build tokio with enable\_io [\#2803](https://github.com/delta-io/delta-rs/pull/2803) ([rtyler](https://github.com/rtyler))
- chore\(python\): remove deprecated or duplicated functions [\#2801](https://github.com/delta-io/delta-rs/pull/2801) ([ion-elgreco](https://github.com/ion-elgreco))
- chore\(python\): raise not implemented in from\_data\_catalog [\#2799](https://github.com/delta-io/delta-rs/pull/2799) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(rust\): `max_spill_size` default value [\#2795](https://github.com/delta-io/delta-rs/pull/2795) ([mrjsj](https://github.com/mrjsj))
- feat\(python, rust\): add ColumnProperties And rework in python WriterProperties [\#2793](https://github.com/delta-io/delta-rs/pull/2793) ([sherlockbeard](https://github.com/sherlockbeard))
- feat: configurable IO runtime [\#2789](https://github.com/delta-io/delta-rs/pull/2789) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: remove some `file_actions` call sites [\#2787](https://github.com/delta-io/delta-rs/pull/2787) ([roeap](https://github.com/roeap))
- style: more consistent imports [\#2786](https://github.com/delta-io/delta-rs/pull/2786) ([roeap](https://github.com/roeap))
- feat\(python, rust\): added statistics\_truncate\_length in WriterProperties [\#2784](https://github.com/delta-io/delta-rs/pull/2784) ([sherlockbeard](https://github.com/sherlockbeard))
- fix: pin maturin version [\#2778](https://github.com/delta-io/delta-rs/pull/2778) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: trim trailing slash in url storage options \(\#2656\) [\#2775](https://github.com/delta-io/delta-rs/pull/2775) ([omkar-foss](https://github.com/omkar-foss))
- chore: update the changelog with the 0.19.0 release [\#2774](https://github.com/delta-io/delta-rs/pull/2774) ([rtyler](https://github.com/rtyler))
- feat\(python, rust\): `add feature` operation [\#2712](https://github.com/delta-io/delta-rs/pull/2712) ([ion-elgreco](https://github.com/ion-elgreco))

## [rust-v0.19.1](https://github.com/delta-io/delta-rs/tree/rust-v0.19.1) (2024-09-11)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.19.0...rust-v0.19.1)

**Implemented enhancements:**

- question: deletionVectors support [\#2829](https://github.com/delta-io/delta-rs/issues/2829)
- \[Minor\] Make `Add::get_json_stats` public [\#2821](https://github.com/delta-io/delta-rs/issues/2821)
- expose target\_file\_size in python side for WriterProperties [\#2810](https://github.com/delta-io/delta-rs/issues/2810)
- expose default\_column\_properties, column\_properties of parquet WriterProperties in python [\#2785](https://github.com/delta-io/delta-rs/issues/2785)
- CDC support in deltalog when writing delta table [\#2720](https://github.com/delta-io/delta-rs/issues/2720)
- Function behaving similarly to `SHOW PARTITIONS` in the Python API [\#2671](https://github.com/delta-io/delta-rs/issues/2671)
- Expose set\_statistics\_truncate\_length via Python WriterProperties [\#2630](https://github.com/delta-io/delta-rs/issues/2630)

**Fixed bugs:**

- `write_deltalake` with predicate throw index out of bounds [\#2867](https://github.com/delta-io/delta-rs/issues/2867)
- writing to blobfuse has stopped working in 0.19.2 [\#2860](https://github.com/delta-io/delta-rs/issues/2860)
- cannot read from public GCS bucket if non logged in [\#2859](https://github.com/delta-io/delta-rs/issues/2859)
- Stats missing for `dataSkippingStatsColumns` when escaping column name [\#2849](https://github.com/delta-io/delta-rs/issues/2849)
- 0.19.2 install error when using poetry, pdm on Ubuntu [\#2848](https://github.com/delta-io/delta-rs/issues/2848)
- `deltalake-*` crates use different version than specified in `Cargo.toml`, leading to unexpected behavior [\#2847](https://github.com/delta-io/delta-rs/issues/2847)
- Databricks fails integrity check after compacting with delta-rs [\#2839](https://github.com/delta-io/delta-rs/issues/2839)
- "failed to load region from IMDS" back in 0.19 despite `AWS_EC2_METADATA_DISABLED=true` [\#2819](https://github.com/delta-io/delta-rs/issues/2819)
- min/max\_row\_groups not respected [\#2814](https://github.com/delta-io/delta-rs/issues/2814)
- Large Memory Spike on Merge [\#2802](https://github.com/delta-io/delta-rs/issues/2802)
- Deleting large number of records fails with no error message [\#2798](https://github.com/delta-io/delta-rs/issues/2798)
- `max_spill_size` incorrect default value [\#2794](https://github.com/delta-io/delta-rs/issues/2794)
- Delta-RS Saved Delta Table not properly ingested into Databricks [\#2779](https://github.com/delta-io/delta-rs/issues/2779)
- Missing Linux binary releases and source tarball for Python release v0.19.0 [\#2777](https://github.com/delta-io/delta-rs/issues/2777)
- Transaction log parsing performance regression [\#2760](https://github.com/delta-io/delta-rs/issues/2760)
- `RecordBatchWriter` only creates stats for the first 32 columns; this prevents calling `create_checkpoint`. [\#2745](https://github.com/delta-io/delta-rs/issues/2745)
- `DeltaScanBuilder` does not respect datafusion context's `datafusion.execution.parquet.pushdown_filters` [\#2739](https://github.com/delta-io/delta-rs/issues/2739)
- `IN (...)` clauses appear to be ignored in merge commands with S3 - extra partitions scanned [\#2726](https://github.com/delta-io/delta-rs/issues/2726)
- Trailing slash on AWS\_ENDPOINT raises S3 Error [\#2656](https://github.com/delta-io/delta-rs/issues/2656)
- AsyncChunkReader::get\_bytes error: Generic MicrosoftAzure error: error decoding response body [\#2592](https://github.com/delta-io/delta-rs/issues/2592)

## [rust-v0.19.0](https://github.com/delta-io/delta-rs/tree/rust-v0.19.0) (2024-08-14)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.18.2...rust-v0.19.0)

**Implemented enhancements:**

- Only allow squash merge [\#2542](https://github.com/delta-io/delta-rs/issues/2542)

**Fixed bugs:**

- Write also insert change types in writer CDC [\#2750](https://github.com/delta-io/delta-rs/issues/2750)
- Regression in Python multiprocessing support [\#2744](https://github.com/delta-io/delta-rs/issues/2744)
- SchemaError occurs during table optimisation after upgrade to v0.18.1 [\#2731](https://github.com/delta-io/delta-rs/issues/2731)
- AWS WebIdentityToken exposure in log files [\#2719](https://github.com/delta-io/delta-rs/issues/2719)
- Write performance degrades with multiple writers [\#2683](https://github.com/delta-io/delta-rs/issues/2683)
- Write monotonic sequence, but read is non monotonic [\#2659](https://github.com/delta-io/delta-rs/issues/2659)
- Python `write_deltalake` with `schema_mode="merge"` casts types [\#2642](https://github.com/delta-io/delta-rs/issues/2642)
- Newest docs \(potentially\) not released [\#2587](https://github.com/delta-io/delta-rs/issues/2587)
- CDC is not generated for Structs and Lists [\#2568](https://github.com/delta-io/delta-rs/issues/2568)

**Closed issues:**

- delete\_dir bug [\#2713](https://github.com/delta-io/delta-rs/issues/2713)

**Merged pull requests:**

- chore: fix a bunch of clippy lints and re-enable tests [\#2773](https://github.com/delta-io/delta-rs/pull/2773) ([rtyler](https://github.com/rtyler))
- feat: more economic data skipping with datafusion [\#2772](https://github.com/delta-io/delta-rs/pull/2772) ([roeap](https://github.com/roeap))
- chore: prepare the next notable release of 0.19.0 [\#2768](https://github.com/delta-io/delta-rs/pull/2768) ([rtyler](https://github.com/rtyler))
- feat: restore the TryFrom for DeltaTablePartition [\#2767](https://github.com/delta-io/delta-rs/pull/2767) ([rtyler](https://github.com/rtyler))
- feat: fail fast on forked process [\#2765](https://github.com/delta-io/delta-rs/pull/2765) ([Tom-Newton](https://github.com/Tom-Newton))
- perf: early stop if all values in arr are null [\#2764](https://github.com/delta-io/delta-rs/pull/2764) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(python, rust\): don't flatten fields during cdf read [\#2763](https://github.com/delta-io/delta-rs/pull/2763) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: upgrade to datafusion 41 [\#2761](https://github.com/delta-io/delta-rs/pull/2761) ([rtyler](https://github.com/rtyler))
- fix\(python, rust\): cdc in writer not creating inserts [\#2751](https://github.com/delta-io/delta-rs/pull/2751) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: improved test fixtures [\#2749](https://github.com/delta-io/delta-rs/pull/2749) ([roeap](https://github.com/roeap))
- feat: introduce CDC generation for merge operations [\#2747](https://github.com/delta-io/delta-rs/pull/2747) ([rtyler](https://github.com/rtyler))
- docs: fix broken link in docs [\#2746](https://github.com/delta-io/delta-rs/pull/2746) ([astrojuanlu](https://github.com/astrojuanlu))
- chore: update delta\_kernel to 0.3.0 [\#2742](https://github.com/delta-io/delta-rs/pull/2742) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- chore: add to code\_owner crates [\#2741](https://github.com/delta-io/delta-rs/pull/2741) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: update changelog and versions for next release [\#2740](https://github.com/delta-io/delta-rs/pull/2740) ([rtyler](https://github.com/rtyler))
- feat\(python, rust\): arrow large/view types passthrough, rust default engine [\#2738](https://github.com/delta-io/delta-rs/pull/2738) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: column parsing to include nested columns and enclosing char [\#2737](https://github.com/delta-io/delta-rs/pull/2737) ([gtrawinski](https://github.com/gtrawinski))

## [rust-v0.18.2](https://github.com/delta-io/delta-rs/tree/rust-v0.18.2) (2024-08-07)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.18.1...rust-v0.18.2)

**Implemented enhancements:**

- Choose which columns to store min/max values for [\#2709](https://github.com/delta-io/delta-rs/issues/2709)
- Projection pushdown for load\_cdf [\#2681](https://github.com/delta-io/delta-rs/issues/2681)
- Way to check if Delta table exists at specified path [\#2662](https://github.com/delta-io/delta-rs/issues/2662)
- Support HDFS via hdfs-native package [\#2611](https://github.com/delta-io/delta-rs/issues/2611)
- Deletion `_change_type` does not appear in change data feed [\#2579](https://github.com/delta-io/delta-rs/issues/2579)

**Fixed bugs:**

- Slow add\_actions.to\_pydict for tables with large number of columns, impacting read performance [\#2733](https://github.com/delta-io/delta-rs/issues/2733)
- append is deleting records [\#2716](https://github.com/delta-io/delta-rs/issues/2716)
- segmentation fault - Python 3.10 on Mac M3  [\#2706](https://github.com/delta-io/delta-rs/issues/2706)
- Failure to delete dir and files [\#2703](https://github.com/delta-io/delta-rs/issues/2703)
- DeltaTable.from\_data\_catalog not working [\#2699](https://github.com/delta-io/delta-rs/issues/2699)
- Project should use the same version of `ruff` in the `lint` stage of `python_build.yml` as in `pyproject.toml` [\#2678](https://github.com/delta-io/delta-rs/issues/2678)
- un-tracked columns are giving json error when pyarrow schema have field with nullable=False and create\_checkpoint is triggered  [\#2675](https://github.com/delta-io/delta-rs/issues/2675)
- \[BUG\]write\_delta\({'custom\_metadata':str}\) cannot be converted. str to pyDict error \(0.18.2\_DeltaPython/Windows10\) [\#2697](https://github.com/delta-io/delta-rs/issues/2697)
- Pyarrow engine not supporting schema overwrite with Append mode [\#2654](https://github.com/delta-io/delta-rs/issues/2654)
- `deltalake-core` version re-exported by `deltalake` different than versions used by `deltalake-azure` and `deltalake-gcp` [\#2647](https://github.com/delta-io/delta-rs/issues/2647)
- i32 limit in JSON stats [\#2646](https://github.com/delta-io/delta-rs/issues/2646)
- Rust writer not encoding correct URL for partitions in delta table [\#2634](https://github.com/delta-io/delta-rs/issues/2634)
- Large Types breaks merge predicate pruning [\#2632](https://github.com/delta-io/delta-rs/issues/2632)
- Getting error when converting a partitioned parquet table to delta table [\#2626](https://github.com/delta-io/delta-rs/issues/2626)
- Arrow: Parquet does not support writing empty structs when creating checkpoint [\#2622](https://github.com/delta-io/delta-rs/issues/2622)
- InvalidTableLocation\("Unknown scheme: gs"\) on 0.18.0 [\#2610](https://github.com/delta-io/delta-rs/issues/2610)
- Unable to read delta table created using Uniform [\#2578](https://github.com/delta-io/delta-rs/issues/2578)
- schema merging doesn't work when overwriting with a predicate [\#2567](https://github.com/delta-io/delta-rs/issues/2567)

**Closed issues:**

- Unable to write new partitions with type timestamp on tables created with delta-rs 0.10.0 [\#2631](https://github.com/delta-io/delta-rs/issues/2631)

**Merged pull requests:**

- fix: schema adapter doesn't map partial batches correctly [\#2735](https://github.com/delta-io/delta-rs/pull/2735) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- perf: grab file size in rust [\#2734](https://github.com/delta-io/delta-rs/pull/2734) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: use logical plan in update, refactor/simplify CDCTracker [\#2727](https://github.com/delta-io/delta-rs/pull/2727) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: use logical plan in delete, delta planner refactoring [\#2725](https://github.com/delta-io/delta-rs/pull/2725) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: try an alternative docke compose invocation syntax [\#2724](https://github.com/delta-io/delta-rs/pull/2724) ([rtyler](https://github.com/rtyler))
- fix\(python, rust\): use input schema to get correct schema in cdf reads [\#2723](https://github.com/delta-io/delta-rs/pull/2723) ([ion-elgreco](https://github.com/ion-elgreco))
- feat\(python, rust\): cdc write-support for `overwrite` and `replacewhere` writes [\#2722](https://github.com/delta-io/delta-rs/pull/2722) ([ion-elgreco](https://github.com/ion-elgreco))
- feat\(python, rust\): cdc write-support for `delete` operation [\#2721](https://github.com/delta-io/delta-rs/pull/2721) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: enabling actions for merge groups [\#2718](https://github.com/delta-io/delta-rs/pull/2718) ([rtyler](https://github.com/rtyler))
- perf: apply projection when reading checkpoint parquet [\#2717](https://github.com/delta-io/delta-rs/pull/2717) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- feat\(python\): add DeltaTable.is\_deltatable static method \(\#2662\) [\#2715](https://github.com/delta-io/delta-rs/pull/2715) ([omkar-foss](https://github.com/omkar-foss))
- chore: prepare python release 0.18.3 [\#2707](https://github.com/delta-io/delta-rs/pull/2707) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(python, rust\): use url encoder when encoding partition values [\#2705](https://github.com/delta-io/delta-rs/pull/2705) ([ion-elgreco](https://github.com/ion-elgreco))
- feat\(python, rust\): add projection in CDF reads [\#2704](https://github.com/delta-io/delta-rs/pull/2704) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: ensure DataFusion SessionState Parquet options are applied to DeltaScan [\#2702](https://github.com/delta-io/delta-rs/pull/2702) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- chore: refactor `write_deltalake` in `writer.py` [\#2695](https://github.com/delta-io/delta-rs/pull/2695) ([fpgmaas](https://github.com/fpgmaas))
- fix\(python\): empty dataset fix for "pyarrow" engine [\#2689](https://github.com/delta-io/delta-rs/pull/2689) ([sherlockbeard](https://github.com/sherlockbeard))
- chore: add test coverage command to `Makefile` [\#2688](https://github.com/delta-io/delta-rs/pull/2688) ([fpgmaas](https://github.com/fpgmaas))
- chore: create separate action to setup python and rust in the cicd pipeline [\#2687](https://github.com/delta-io/delta-rs/pull/2687) ([fpgmaas](https://github.com/fpgmaas))
- fix: update delta kernel version [\#2685](https://github.com/delta-io/delta-rs/pull/2685) ([jeppe742](https://github.com/jeppe742))
- chore: update README.md [\#2684](https://github.com/delta-io/delta-rs/pull/2684) ([veronewra](https://github.com/veronewra))
- fix\(rust,python\): checkpoint with column nullable false [\#2680](https://github.com/delta-io/delta-rs/pull/2680) ([sherlockbeard](https://github.com/sherlockbeard))
- chore: pin `ruff` and `mypy` versions in the `lint` stage in the CI pipeline [\#2679](https://github.com/delta-io/delta-rs/pull/2679) ([fpgmaas](https://github.com/fpgmaas))
- chore: enable `RUF` ruleset for `ruff` [\#2677](https://github.com/delta-io/delta-rs/pull/2677) ([fpgmaas](https://github.com/fpgmaas))
- chore: remove stale code for conditional import of `Literal` [\#2676](https://github.com/delta-io/delta-rs/pull/2676) ([fpgmaas](https://github.com/fpgmaas))
- chore: remove references to black from the project [\#2674](https://github.com/delta-io/delta-rs/pull/2674) ([fpgmaas](https://github.com/fpgmaas))
- chore: bump ruff to 0.5.2 [\#2673](https://github.com/delta-io/delta-rs/pull/2673) ([fpgmaas](https://github.com/fpgmaas))
- chore: improve contributing.md [\#2672](https://github.com/delta-io/delta-rs/pull/2672) ([fpgmaas](https://github.com/fpgmaas))
- feat: support userMetadata in CommitInfo [\#2670](https://github.com/delta-io/delta-rs/pull/2670) ([jkylling](https://github.com/jkylling))
- chore: upgrade to datafusion 40 [\#2661](https://github.com/delta-io/delta-rs/pull/2661) ([rtyler](https://github.com/rtyler))
- docs: improve navigation fixes [\#2660](https://github.com/delta-io/delta-rs/pull/2660) ([avriiil](https://github.com/avriiil))
- docs: add integration docs for s3 backend [\#2658](https://github.com/delta-io/delta-rs/pull/2658) ([avriiil](https://github.com/avriiil))
- docs: fix bullets on hdfs docs [\#2653](https://github.com/delta-io/delta-rs/pull/2653) ([Kimahriman](https://github.com/Kimahriman))
- ci: update CODEOWNERS [\#2650](https://github.com/delta-io/delta-rs/pull/2650) ([hntd187](https://github.com/hntd187))
- feat\(rust\): fix size\_in\_bytes in last\_checkpoint\_ to i64 [\#2649](https://github.com/delta-io/delta-rs/pull/2649) ([sherlockbeard](https://github.com/sherlockbeard))
- chore: increase subcrate versions [\#2648](https://github.com/delta-io/delta-rs/pull/2648) ([rtyler](https://github.com/rtyler))
- chore: missed one macos runner reference in actions [\#2645](https://github.com/delta-io/delta-rs/pull/2645) ([rtyler](https://github.com/rtyler))
- chore: add a reproduction case for merge failures with struct\<string\> [\#2644](https://github.com/delta-io/delta-rs/pull/2644) ([rtyler](https://github.com/rtyler))
- chore: remove macos builders from pull request flow [\#2638](https://github.com/delta-io/delta-rs/pull/2638) ([rtyler](https://github.com/rtyler))
- fix: enable parquet pushdown for DeltaScan via TableProvider impl for DeltaTable  \(rebase\) [\#2637](https://github.com/delta-io/delta-rs/pull/2637) ([rtyler](https://github.com/rtyler))
- chore: fix documentation generation with a pin of griffe [\#2636](https://github.com/delta-io/delta-rs/pull/2636) ([rtyler](https://github.com/rtyler))
- fix\(python\): fixed large\_dtype to schema convert [\#2635](https://github.com/delta-io/delta-rs/pull/2635) ([sherlockbeard](https://github.com/sherlockbeard))
- fix\(rust, python\): fix writing empty structs when creating checkpoint [\#2627](https://github.com/delta-io/delta-rs/pull/2627) ([sherlockbeard](https://github.com/sherlockbeard))
- fix\(rust, python\): fix merge schema with overwrite [\#2623](https://github.com/delta-io/delta-rs/pull/2623) ([sherlockbeard](https://github.com/sherlockbeard))
- chore: bump python 0.18.2 [\#2621](https://github.com/delta-io/delta-rs/pull/2621) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: report DataFusion metrics for DeltaScan [\#2617](https://github.com/delta-io/delta-rs/pull/2617) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- feat\(rust,python\): cast each parquet file to delta schema [\#2615](https://github.com/delta-io/delta-rs/pull/2615) ([HawaiianSpork](https://github.com/HawaiianSpork))
- fix\(rust\): inconsistent order of partitioning columns \(\#2494\) [\#2614](https://github.com/delta-io/delta-rs/pull/2614) ([aditanase](https://github.com/aditanase))
- docs: add Daft writer [\#2594](https://github.com/delta-io/delta-rs/pull/2594) ([avriiil](https://github.com/avriiil))
- feat\(python, rust\): `add column` operation [\#2562](https://github.com/delta-io/delta-rs/pull/2562) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: change arrow map root name to follow with parquet root name [\#2538](https://github.com/delta-io/delta-rs/pull/2538) ([sclmn](https://github.com/sclmn))
- feat\(python\): handle PyCapsule interface objects in write\_deltalake [\#2534](https://github.com/delta-io/delta-rs/pull/2534) ([kylebarron](https://github.com/kylebarron))

## [rust-v0.19.0](https://github.com/delta-io/delta-rs/tree/rust-v0.19.0) (2024-08-14)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.18.2...rust-v0.19.0)

**Implemented enhancements:**

- Only allow squash merge [\#2542](https://github.com/delta-io/delta-rs/issues/2542)

**Fixed bugs:**

- Write also insert change types in writer CDC [\#2750](https://github.com/delta-io/delta-rs/issues/2750)
- Regression in Python multiprocessing support [\#2744](https://github.com/delta-io/delta-rs/issues/2744)
- SchemaError occurs during table optimisation after upgrade to v0.18.1 [\#2731](https://github.com/delta-io/delta-rs/issues/2731)
- AWS WebIdentityToken exposure in log files [\#2719](https://github.com/delta-io/delta-rs/issues/2719)
- Write performance degrades with multiple writers [\#2683](https://github.com/delta-io/delta-rs/issues/2683)
- Write monotonic sequence, but read is non monotonic [\#2659](https://github.com/delta-io/delta-rs/issues/2659)
- Python `write_deltalake` with `schema_mode="merge"` casts types [\#2642](https://github.com/delta-io/delta-rs/issues/2642)
- Newest docs \(potentially\) not released [\#2587](https://github.com/delta-io/delta-rs/issues/2587)
- CDC is not generated for Structs and Lists [\#2568](https://github.com/delta-io/delta-rs/issues/2568)

**Closed issues:**

- delete\_dir bug [\#2713](https://github.com/delta-io/delta-rs/issues/2713)

**Merged pull requests:**

- chore: fix a bunch of clippy lints and re-enable tests [\#2773](https://github.com/delta-io/delta-rs/pull/2773) ([rtyler](https://github.com/rtyler))
- feat: more economic data skipping with datafusion [\#2772](https://github.com/delta-io/delta-rs/pull/2772) ([roeap](https://github.com/roeap))
- chore: prepare the next notable release of 0.19.0 [\#2768](https://github.com/delta-io/delta-rs/pull/2768) ([rtyler](https://github.com/rtyler))
- feat: restore the TryFrom for DeltaTablePartition [\#2767](https://github.com/delta-io/delta-rs/pull/2767) ([rtyler](https://github.com/rtyler))
- feat: fail fast on forked process [\#2765](https://github.com/delta-io/delta-rs/pull/2765) ([Tom-Newton](https://github.com/Tom-Newton))
- perf: early stop if all values in arr are null [\#2764](https://github.com/delta-io/delta-rs/pull/2764) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(python, rust\): don't flatten fields during cdf read [\#2763](https://github.com/delta-io/delta-rs/pull/2763) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: upgrade to datafusion 41 [\#2761](https://github.com/delta-io/delta-rs/pull/2761) ([rtyler](https://github.com/rtyler))
- fix\(python, rust\): cdc in writer not creating inserts [\#2751](https://github.com/delta-io/delta-rs/pull/2751) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: improved test fixtures [\#2749](https://github.com/delta-io/delta-rs/pull/2749) ([roeap](https://github.com/roeap))
- feat: introduce CDC generation for merge operations [\#2747](https://github.com/delta-io/delta-rs/pull/2747) ([rtyler](https://github.com/rtyler))
- docs: fix broken link in docs [\#2746](https://github.com/delta-io/delta-rs/pull/2746) ([astrojuanlu](https://github.com/astrojuanlu))
- chore: update delta\_kernel to 0.3.0 [\#2742](https://github.com/delta-io/delta-rs/pull/2742) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- chore: add to code\_owner crates [\#2741](https://github.com/delta-io/delta-rs/pull/2741) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: update changelog and versions for next release [\#2740](https://github.com/delta-io/delta-rs/pull/2740) ([rtyler](https://github.com/rtyler))
- feat\(python, rust\): arrow large/view types passthrough, rust default engine [\#2738](https://github.com/delta-io/delta-rs/pull/2738) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: column parsing to include nested columns and enclosing char [\#2737](https://github.com/delta-io/delta-rs/pull/2737) ([gtrawinski](https://github.com/gtrawinski))

## [rust-v0.18.2](https://github.com/delta-io/delta-rs/tree/rust-v0.18.2) (2024-08-07)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.18.1...rust-v0.18.2)

**Implemented enhancements:**

- Choose which columns to store min/max values for [\#2709](https://github.com/delta-io/delta-rs/issues/2709)
- Projection pushdown for load\_cdf [\#2681](https://github.com/delta-io/delta-rs/issues/2681)
- Way to check if Delta table exists at specified path [\#2662](https://github.com/delta-io/delta-rs/issues/2662)
- Support HDFS via hdfs-native package [\#2611](https://github.com/delta-io/delta-rs/issues/2611)
- Deletion `_change_type` does not appear in change data feed [\#2579](https://github.com/delta-io/delta-rs/issues/2579)
- Could you please explain in the README what "Deltalake" is for the uninitiated? [\#2523](https://github.com/delta-io/delta-rs/issues/2523)
- Discuss: Allow protocol change during write actions  [\#2444](https://github.com/delta-io/delta-rs/issues/2444)
- Support for Arrow PyCapsule interface [\#2376](https://github.com/delta-io/delta-rs/issues/2376)

**Fixed bugs:**

- Slow add\_actions.to\_pydict for tables with large number of columns, impacting read performance [\#2733](https://github.com/delta-io/delta-rs/issues/2733)
- append is deleting records [\#2716](https://github.com/delta-io/delta-rs/issues/2716)
- segmentation fault - Python 3.10 on Mac M3  [\#2706](https://github.com/delta-io/delta-rs/issues/2706)
- Failure to delete dir and files [\#2703](https://github.com/delta-io/delta-rs/issues/2703)
- DeltaTable.from\_data\_catalog not working [\#2699](https://github.com/delta-io/delta-rs/issues/2699)
- Project should use the same version of `ruff` in the `lint` stage of `python_build.yml` as in `pyproject.toml` [\#2678](https://github.com/delta-io/delta-rs/issues/2678)
- un-tracked columns are giving json error when pyarrow schema have field with nullable=False and create\_checkpoint is triggered  [\#2675](https://github.com/delta-io/delta-rs/issues/2675)
- \[BUG\]write\_delta\({'custom\_metadata':str}\) cannot be converted. str to pyDict error \(0.18.2\_DeltaPython/Windows10\) [\#2697](https://github.com/delta-io/delta-rs/issues/2697)
- Pyarrow engine not supporting schema overwrite with Append mode [\#2654](https://github.com/delta-io/delta-rs/issues/2654)
- `deltalake-core` version re-exported by `deltalake` different than versions used by `deltalake-azure` and `deltalake-gcp` [\#2647](https://github.com/delta-io/delta-rs/issues/2647)
- i32 limit in JSON stats [\#2646](https://github.com/delta-io/delta-rs/issues/2646)
- Rust writer not encoding correct URL for partitions in delta table [\#2634](https://github.com/delta-io/delta-rs/issues/2634)
- Large Types breaks merge predicate pruning [\#2632](https://github.com/delta-io/delta-rs/issues/2632)
- Getting error when converting a partitioned parquet table to delta table [\#2626](https://github.com/delta-io/delta-rs/issues/2626)
- Arrow: Parquet does not support writing empty structs when creating checkpoint [\#2622](https://github.com/delta-io/delta-rs/issues/2622)
- InvalidTableLocation\("Unknown scheme: gs"\) on 0.18.0 [\#2610](https://github.com/delta-io/delta-rs/issues/2610)
- Unable to read delta table created using Uniform [\#2578](https://github.com/delta-io/delta-rs/issues/2578)
- schema merging doesn't work when overwriting with a predicate [\#2567](https://github.com/delta-io/delta-rs/issues/2567)
- Not working in AWS Lambda \(0.16.2 - 0.17.4\) OSError: Generic S3 error [\#2511](https://github.com/delta-io/delta-rs/issues/2511)
- DataFusion filter on partition column doesn't work. \(when the physical schema ordering is different to logical one\) [\#2494](https://github.com/delta-io/delta-rs/issues/2494)
- Creating checkpoints for tables with missing column stats results in Err [\#2493](https://github.com/delta-io/delta-rs/issues/2493)
- Cannot merge to a table with a timestamp column after upgrading delta-rs [\#2478](https://github.com/delta-io/delta-rs/issues/2478)
- Azure AD Auth fails on ARM64 [\#2475](https://github.com/delta-io/delta-rs/issues/2475)
- Generic S3 error: Error after 0 retries ... Broken pipe \(os error 32\) [\#2403](https://github.com/delta-io/delta-rs/issues/2403)
- write\_deltalake identifies large\_string as datatype even though string is set in schema [\#2374](https://github.com/delta-io/delta-rs/issues/2374)
- Inconsistent arrow timestamp type breaks datafusion query [\#2341](https://github.com/delta-io/delta-rs/issues/2341)

**Closed issues:**

- Unable to write new partitions with type timestamp on tables created with delta-rs 0.10.0 [\#2631](https://github.com/delta-io/delta-rs/issues/2631)

**Merged pull requests:**

- fix: schema adapter doesn't map partial batches correctly [\#2735](https://github.com/delta-io/delta-rs/pull/2735) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- perf: grab file size in rust [\#2734](https://github.com/delta-io/delta-rs/pull/2734) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: use logical plan in update, refactor/simplify CDCTracker [\#2727](https://github.com/delta-io/delta-rs/pull/2727) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: use logical plan in delete, delta planner refactoring [\#2725](https://github.com/delta-io/delta-rs/pull/2725) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: try an alternative docke compose invocation syntax [\#2724](https://github.com/delta-io/delta-rs/pull/2724) ([rtyler](https://github.com/rtyler))
- fix\(python, rust\): use input schema to get correct schema in cdf reads [\#2723](https://github.com/delta-io/delta-rs/pull/2723) ([ion-elgreco](https://github.com/ion-elgreco))
- feat\(python, rust\): cdc write-support for `overwrite` and `replacewhere` writes [\#2722](https://github.com/delta-io/delta-rs/pull/2722) ([ion-elgreco](https://github.com/ion-elgreco))
- feat\(python, rust\): cdc write-support for `delete` operation [\#2721](https://github.com/delta-io/delta-rs/pull/2721) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: enabling actions for merge groups [\#2718](https://github.com/delta-io/delta-rs/pull/2718) ([rtyler](https://github.com/rtyler))
- perf: apply projection when reading checkpoint parquet [\#2717](https://github.com/delta-io/delta-rs/pull/2717) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- feat\(python\): add DeltaTable.is\_deltatable static method \(\#2662\) [\#2715](https://github.com/delta-io/delta-rs/pull/2715) ([omkar-foss](https://github.com/omkar-foss))
- chore: prepare python release 0.18.3 [\#2707](https://github.com/delta-io/delta-rs/pull/2707) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(python, rust\): use url encoder when encoding partition values [\#2705](https://github.com/delta-io/delta-rs/pull/2705) ([ion-elgreco](https://github.com/ion-elgreco))
- feat\(python, rust\): add projection in CDF reads [\#2704](https://github.com/delta-io/delta-rs/pull/2704) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: ensure DataFusion SessionState Parquet options are applied to DeltaScan [\#2702](https://github.com/delta-io/delta-rs/pull/2702) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- chore: refactor `write_deltalake` in `writer.py` [\#2695](https://github.com/delta-io/delta-rs/pull/2695) ([fpgmaas](https://github.com/fpgmaas))
- fix\(python\): empty dataset fix for "pyarrow" engine [\#2689](https://github.com/delta-io/delta-rs/pull/2689) ([sherlockbeard](https://github.com/sherlockbeard))
- chore: add test coverage command to `Makefile` [\#2688](https://github.com/delta-io/delta-rs/pull/2688) ([fpgmaas](https://github.com/fpgmaas))
- chore: create separate action to setup python and rust in the cicd pipeline [\#2687](https://github.com/delta-io/delta-rs/pull/2687) ([fpgmaas](https://github.com/fpgmaas))
- fix: update delta kernel version [\#2685](https://github.com/delta-io/delta-rs/pull/2685) ([jeppe742](https://github.com/jeppe742))
- chore: update README.md [\#2684](https://github.com/delta-io/delta-rs/pull/2684) ([veronewra](https://github.com/veronewra))
- fix\(rust,python\): checkpoint with column nullable false [\#2680](https://github.com/delta-io/delta-rs/pull/2680) ([sherlockbeard](https://github.com/sherlockbeard))
- chore: pin `ruff` and `mypy` versions in the `lint` stage in the CI pipeline [\#2679](https://github.com/delta-io/delta-rs/pull/2679) ([fpgmaas](https://github.com/fpgmaas))
- chore: enable `RUF` ruleset for `ruff` [\#2677](https://github.com/delta-io/delta-rs/pull/2677) ([fpgmaas](https://github.com/fpgmaas))
- chore: remove stale code for conditional import of `Literal` [\#2676](https://github.com/delta-io/delta-rs/pull/2676) ([fpgmaas](https://github.com/fpgmaas))
- chore: remove references to black from the project [\#2674](https://github.com/delta-io/delta-rs/pull/2674) ([fpgmaas](https://github.com/fpgmaas))
- chore: bump ruff to 0.5.2 [\#2673](https://github.com/delta-io/delta-rs/pull/2673) ([fpgmaas](https://github.com/fpgmaas))
- chore: improve contributing.md [\#2672](https://github.com/delta-io/delta-rs/pull/2672) ([fpgmaas](https://github.com/fpgmaas))
- feat: support userMetadata in CommitInfo [\#2670](https://github.com/delta-io/delta-rs/pull/2670) ([jkylling](https://github.com/jkylling))
- chore: upgrade to datafusion 40 [\#2661](https://github.com/delta-io/delta-rs/pull/2661) ([rtyler](https://github.com/rtyler))
- docs: improve navigation fixes [\#2660](https://github.com/delta-io/delta-rs/pull/2660) ([avriiil](https://github.com/avriiil))
- docs: add integration docs for s3 backend [\#2658](https://github.com/delta-io/delta-rs/pull/2658) ([avriiil](https://github.com/avriiil))
- docs: fix bullets on hdfs docs [\#2653](https://github.com/delta-io/delta-rs/pull/2653) ([Kimahriman](https://github.com/Kimahriman))
- ci: update CODEOWNERS [\#2650](https://github.com/delta-io/delta-rs/pull/2650) ([hntd187](https://github.com/hntd187))
- feat\(rust\): fix size\_in\_bytes in last\_checkpoint\_ to i64 [\#2649](https://github.com/delta-io/delta-rs/pull/2649) ([sherlockbeard](https://github.com/sherlockbeard))
- chore: increase subcrate versions [\#2648](https://github.com/delta-io/delta-rs/pull/2648) ([rtyler](https://github.com/rtyler))
- chore: missed one macos runner reference in actions [\#2645](https://github.com/delta-io/delta-rs/pull/2645) ([rtyler](https://github.com/rtyler))
- chore: add a reproduction case for merge failures with struct\<string\> [\#2644](https://github.com/delta-io/delta-rs/pull/2644) ([rtyler](https://github.com/rtyler))
- chore: remove macos builders from pull request flow [\#2638](https://github.com/delta-io/delta-rs/pull/2638) ([rtyler](https://github.com/rtyler))
- fix: enable parquet pushdown for DeltaScan via TableProvider impl for DeltaTable  \(rebase\) [\#2637](https://github.com/delta-io/delta-rs/pull/2637) ([rtyler](https://github.com/rtyler))
- chore: fix documentation generation with a pin of griffe [\#2636](https://github.com/delta-io/delta-rs/pull/2636) ([rtyler](https://github.com/rtyler))
- fix\(python\): fixed large\_dtype to schema convert [\#2635](https://github.com/delta-io/delta-rs/pull/2635) ([sherlockbeard](https://github.com/sherlockbeard))
- fix\(rust, python\): fix writing empty structs when creating checkpoint [\#2627](https://github.com/delta-io/delta-rs/pull/2627) ([sherlockbeard](https://github.com/sherlockbeard))
- fix\(rust, python\): fix merge schema with overwrite [\#2623](https://github.com/delta-io/delta-rs/pull/2623) ([sherlockbeard](https://github.com/sherlockbeard))
- chore: bump python 0.18.2 [\#2621](https://github.com/delta-io/delta-rs/pull/2621) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: report DataFusion metrics for DeltaScan [\#2617](https://github.com/delta-io/delta-rs/pull/2617) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- feat\(rust,python\): cast each parquet file to delta schema [\#2615](https://github.com/delta-io/delta-rs/pull/2615) ([HawaiianSpork](https://github.com/HawaiianSpork))
- fix\(rust\): inconsistent order of partitioning columns \(\#2494\) [\#2614](https://github.com/delta-io/delta-rs/pull/2614) ([aditanase](https://github.com/aditanase))
- docs: add Daft writer [\#2594](https://github.com/delta-io/delta-rs/pull/2594) ([avriiil](https://github.com/avriiil))
- feat\(python, rust\): `add column` operation [\#2562](https://github.com/delta-io/delta-rs/pull/2562) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: change arrow map root name to follow with parquet root name [\#2538](https://github.com/delta-io/delta-rs/pull/2538) ([sclmn](https://github.com/sclmn))
- feat\(python\): handle PyCapsule interface objects in write\_deltalake [\#2534](https://github.com/delta-io/delta-rs/pull/2534) ([kylebarron](https://github.com/kylebarron))
- feat: improve merge performance by using predicate non-partition columns min/max for prefiltering [\#2513](https://github.com/delta-io/delta-rs/pull/2513) ([JonasDev1](https://github.com/JonasDev1))
- feat\(python, rust\): cleanup expired logs post-commit hook [\#2459](https://github.com/delta-io/delta-rs/pull/2459) ([ion-elgreco](https://github.com/ion-elgreco))

## [rust-v0.18.0](https://github.com/delta-io/delta-rs/tree/rust-v0.18.0) (2024-06-12)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.17.3...rust-v0.18.0)

**Implemented enhancements:**

- documentation: concurrent writes for non-S3 backends [\#2556](https://github.com/delta-io/delta-rs/issues/2556)
- pyarrow options for `write_delta` [\#2515](https://github.com/delta-io/delta-rs/issues/2515)
- \[deltalake\_aws\] Allow configuring separate endpoints for S3 and DynamoDB clients. [\#2498](https://github.com/delta-io/delta-rs/issues/2498)
- Include file stats when converting a parquet directory to a Delta table [\#2490](https://github.com/delta-io/delta-rs/issues/2490)
- Adopt the delta kernel types [\#2489](https://github.com/delta-io/delta-rs/issues/2489)

**Fixed bugs:**

- `raise_if_not_exists` for properties not configurable on CreateBuilder [\#2564](https://github.com/delta-io/delta-rs/issues/2564)
- write\_deltalake with rust engine fails when mode is append and overwrite schema is enabled [\#2553](https://github.com/delta-io/delta-rs/issues/2553)
- Running the basic\_operations examples fails with `Error: Transaction { source: WriterFeaturesRequired(TimestampWithoutTimezone) `} [\#2552](https://github.com/delta-io/delta-rs/issues/2552)
-  invalid peer certificate: BadSignature when connecting to s3 from  arm64/aarch64 [\#2551](https://github.com/delta-io/delta-rs/issues/2551)
- load\_cdf\(\) issue : Generic S3 error: request or response body error: operation timed out [\#2549](https://github.com/delta-io/delta-rs/issues/2549)
- write\_deltalake fails on Databricks volume [\#2540](https://github.com/delta-io/delta-rs/issues/2540)
- Getting "Microsoft Azure Error: Operation timed out" when trying to retrieve big files [\#2537](https://github.com/delta-io/delta-rs/issues/2537)
- Impossible to append to a DeltaTable with float data type on RHEL [\#2520](https://github.com/delta-io/delta-rs/issues/2520)
- Creating DeltaTable object slow [\#2518](https://github.com/delta-io/delta-rs/issues/2518)
- `write_deltalake` throws parser error when using `rust` engine and big decimals [\#2510](https://github.com/delta-io/delta-rs/issues/2510)
- TypeError: Object of type int64 is not JSON serializable when writing using a Pandas dataframe [\#2501](https://github.com/delta-io/delta-rs/issues/2501)
- unable to read delta table when table contains both null and non-null add stats [\#2477](https://github.com/delta-io/delta-rs/issues/2477)
- Commits on WriteMode::MergeSchema cause table metadata corruption [\#2468](https://github.com/delta-io/delta-rs/issues/2468)
- S3 object store always returns IMDS warnings [\#2460](https://github.com/delta-io/delta-rs/issues/2460)
- File skipping according to documentation [\#2427](https://github.com/delta-io/delta-rs/issues/2427)
- LockClientError [\#2379](https://github.com/delta-io/delta-rs/issues/2379)
- get\_app\_transaction\_version\(\) returns wrong result [\#2340](https://github.com/delta-io/delta-rs/issues/2340)
- Property setting in `create` is not handled correctly [\#2247](https://github.com/delta-io/delta-rs/issues/2247)
- Handling of decimals in scientific notation  [\#2221](https://github.com/delta-io/delta-rs/issues/2221)
- Unable to append to delta table without datafusion feature [\#2204](https://github.com/delta-io/delta-rs/issues/2204)
- Decimal Column with Value 0 Causes Failure in Python Binding [\#2193](https://github.com/delta-io/delta-rs/issues/2193)

**Merged pull requests:**

- docs: improve S3 access docs [\#2589](https://github.com/delta-io/delta-rs/pull/2589) ([avriiil](https://github.com/avriiil))
- chore: bump macOS runners, maybe resolve import error [\#2588](https://github.com/delta-io/delta-rs/pull/2588) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: bump to datafusion 39, arrow 52, pyo3 0.21 [\#2581](https://github.com/delta-io/delta-rs/pull/2581) ([abhiaagarwal](https://github.com/abhiaagarwal))
- feat: add custom dynamodb endpoint configuration [\#2575](https://github.com/delta-io/delta-rs/pull/2575) ([hnaoto](https://github.com/hnaoto))
- fix: consistently use raise\_if\_key\_not\_exists in CreateBuilder [\#2569](https://github.com/delta-io/delta-rs/pull/2569) ([vegarsti](https://github.com/vegarsti))
- fix: add raise\_if\_key\_not\_exists to CreateBuilder [\#2565](https://github.com/delta-io/delta-rs/pull/2565) ([vegarsti](https://github.com/vegarsti))
- docs: dt.delete add context + api docs link [\#2560](https://github.com/delta-io/delta-rs/pull/2560) ([avriiil](https://github.com/avriiil))
- fix: update deltalake crate examples for crate layout and TimestampNtz [\#2559](https://github.com/delta-io/delta-rs/pull/2559) ([jhoekx](https://github.com/jhoekx))
- docs: clarify locking mechanism requirement for S3 [\#2558](https://github.com/delta-io/delta-rs/pull/2558) ([inigohidalgo](https://github.com/inigohidalgo))
- fix: remove deprecated overwrite\_schema configuration which has incorrect behavior [\#2554](https://github.com/delta-io/delta-rs/pull/2554) ([rtyler](https://github.com/rtyler))
- fix: clippy warnings [\#2548](https://github.com/delta-io/delta-rs/pull/2548) ([imor](https://github.com/imor))
- docs: dask write syntax fix [\#2543](https://github.com/delta-io/delta-rs/pull/2543) ([avriiil](https://github.com/avriiil))
- fix: cast support fields nested in lists and maps [\#2541](https://github.com/delta-io/delta-rs/pull/2541) ([HawaiianSpork](https://github.com/HawaiianSpork))
- feat: implement transaction identifiers - continued [\#2539](https://github.com/delta-io/delta-rs/pull/2539) ([roeap](https://github.com/roeap))
- docs: pull delta from conda not pip [\#2535](https://github.com/delta-io/delta-rs/pull/2535) ([avriiil](https://github.com/avriiil))
- chore: expose `files_by_partition` to public api [\#2533](https://github.com/delta-io/delta-rs/pull/2533) ([edmondop](https://github.com/edmondop))
- chore: bump python 0.17.5 [\#2531](https://github.com/delta-io/delta-rs/pull/2531) ([ion-elgreco](https://github.com/ion-elgreco))
- feat\(rust\): make PartitionWriter public [\#2525](https://github.com/delta-io/delta-rs/pull/2525) ([adriangb](https://github.com/adriangb))
- fix: msrv in workspace [\#2524](https://github.com/delta-io/delta-rs/pull/2524) ([roeap](https://github.com/roeap))
- chore: fixing some clips [\#2521](https://github.com/delta-io/delta-rs/pull/2521) ([rtyler](https://github.com/rtyler))
- fix: enable field\_with\_name to support nested fields with '.' delimiter [\#2519](https://github.com/delta-io/delta-rs/pull/2519) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- chore: tidying up builds without datafusion feature and clippy [\#2516](https://github.com/delta-io/delta-rs/pull/2516) ([rtyler](https://github.com/rtyler))
- fix\(python\): release GIL on most operations [\#2512](https://github.com/delta-io/delta-rs/pull/2512) ([adriangb](https://github.com/adriangb))
- docs: fix typo [\#2508](https://github.com/delta-io/delta-rs/pull/2508) ([avriiil](https://github.com/avriiil))
- fix\(rust, python\): fixed differences in storage options between log and object stores [\#2500](https://github.com/delta-io/delta-rs/pull/2500) ([mightyshazam](https://github.com/mightyshazam))
- docs: improve daft integration docs [\#2496](https://github.com/delta-io/delta-rs/pull/2496) ([avriiil](https://github.com/avriiil))
- feat: adopt kernel schema types [\#2495](https://github.com/delta-io/delta-rs/pull/2495) ([roeap](https://github.com/roeap))
- feat: add stats to convert-to-delta operation [\#2491](https://github.com/delta-io/delta-rs/pull/2491) ([gruuya](https://github.com/gruuya))
- fix\(python, rust\): region lookup wasn't working correctly for dynamo [\#2488](https://github.com/delta-io/delta-rs/pull/2488) ([mightyshazam](https://github.com/mightyshazam))
- feat: introduce CDC write-side support for the Update operations [\#2486](https://github.com/delta-io/delta-rs/pull/2486) ([rtyler](https://github.com/rtyler))
- fix\(python\): reuse state in `to_pyarrow_dataset` [\#2485](https://github.com/delta-io/delta-rs/pull/2485) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: check to see if the file exists before attempting to rename [\#2482](https://github.com/delta-io/delta-rs/pull/2482) ([rtyler](https://github.com/rtyler))
- fix\(python, rust\): use new schema for stats parsing instead of old [\#2480](https://github.com/delta-io/delta-rs/pull/2480) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(rust\): unable to read delta table when table contains both null and non-null add stats [\#2476](https://github.com/delta-io/delta-rs/pull/2476) ([yjshen](https://github.com/yjshen))
- chore: update the changelog to include rust-v0.17.3 [\#2473](https://github.com/delta-io/delta-rs/pull/2473) ([rtyler](https://github.com/rtyler))
- chore: a bunch of tweaks to get releases out the door [\#2472](https://github.com/delta-io/delta-rs/pull/2472) ([rtyler](https://github.com/rtyler))
- chore: bump the core crate for its next release [\#2470](https://github.com/delta-io/delta-rs/pull/2470) ([rtyler](https://github.com/rtyler))
- fix: return unsupported error for merging schemas in the presence of partition columns [\#2469](https://github.com/delta-io/delta-rs/pull/2469) ([emcake](https://github.com/emcake))
- feat\(python\): add  parameter to DeltaTable.to\_pyarrow\_dataset\(\) [\#2465](https://github.com/delta-io/delta-rs/pull/2465) ([adriangb](https://github.com/adriangb))
- feat\(python, rust\): add OBJECT\_STORE\_CONCURRENCY\_LIMIT setting for ObjectStoreFactory [\#2458](https://github.com/delta-io/delta-rs/pull/2458) ([vigimite](https://github.com/vigimite))
- fix\(rust\): handle 429 from GCS [\#2454](https://github.com/delta-io/delta-rs/pull/2454) ([adriangb](https://github.com/adriangb))
- fix\(python\): reuse table state in write engine [\#2453](https://github.com/delta-io/delta-rs/pull/2453) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(rust\): implement abort commit for S3DynamoDBLogStore [\#2452](https://github.com/delta-io/delta-rs/pull/2452) ([PeterKeDer](https://github.com/PeterKeDer))
- fix\(python, rust\): check timestamp\_ntz in nested fields, add check\_can\_write in pyarrow writer [\#2443](https://github.com/delta-io/delta-rs/pull/2443) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(python, rust\): remove imds calls from profile auth and region [\#2442](https://github.com/delta-io/delta-rs/pull/2442) ([mightyshazam](https://github.com/mightyshazam))
- fix\(python, rust\): use from\_name during column projection creation [\#2441](https://github.com/delta-io/delta-rs/pull/2441) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: bump python for 0.17 release [\#2439](https://github.com/delta-io/delta-rs/pull/2439) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(python,rust\): missing remove actions during `create_or_replace` [\#2437](https://github.com/delta-io/delta-rs/pull/2437) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: introduce the Operation trait to enforce consistency between operations [\#2435](https://github.com/delta-io/delta-rs/pull/2435) ([rtyler](https://github.com/rtyler))
- fix\(python\): load\_as\_version with datetime object with no timezone specified [\#2429](https://github.com/delta-io/delta-rs/pull/2429) ([t1g0rz](https://github.com/t1g0rz))
- feat\(python, rust\): respect column stats collection configurations [\#2428](https://github.com/delta-io/delta-rs/pull/2428) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: lazy static runtime in python [\#2424](https://github.com/delta-io/delta-rs/pull/2424) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: implement repartitioned for DeltaScan [\#2421](https://github.com/delta-io/delta-rs/pull/2421) ([jkylling](https://github.com/jkylling))
- fix: return error when checkpoints and metadata get out of sync [\#2406](https://github.com/delta-io/delta-rs/pull/2406) ([esarili](https://github.com/esarili))
- fix\(rust\): stats\_parsed has different number of records with stats [\#2405](https://github.com/delta-io/delta-rs/pull/2405) ([yjshen](https://github.com/yjshen))
- docs: add Daft integration [\#2402](https://github.com/delta-io/delta-rs/pull/2402) ([avriiil](https://github.com/avriiil))
- feat\(rust\): advance state in post commit [\#2396](https://github.com/delta-io/delta-rs/pull/2396) ([ion-elgreco](https://github.com/ion-elgreco))
- chore\(rust\): bump arrow v51 and datafusion v37.1 [\#2395](https://github.com/delta-io/delta-rs/pull/2395) ([lasantosr](https://github.com/lasantosr))
- docs: document required aws permissions [\#2393](https://github.com/delta-io/delta-rs/pull/2393) ([ale-rinaldi](https://github.com/ale-rinaldi))
- feat\(rust\): post commit hook \(v2\), create checkpoint hook [\#2391](https://github.com/delta-io/delta-rs/pull/2391) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: time travel when checkpointed and logs removed [\#2389](https://github.com/delta-io/delta-rs/pull/2389) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(rust\): remove flush after writing every batch [\#2387](https://github.com/delta-io/delta-rs/pull/2387) ([PeterKeDer](https://github.com/PeterKeDer))
- feat: added configuration variables to handle EC2 metadata service [\#2385](https://github.com/delta-io/delta-rs/pull/2385) ([mightyshazam](https://github.com/mightyshazam))
- fix\(rust\): timestamp deserialization format, missing type [\#2383](https://github.com/delta-io/delta-rs/pull/2383) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: bump chrono [\#2372](https://github.com/delta-io/delta-rs/pull/2372) ([universalmind303](https://github.com/universalmind303))
- chore: bump python 0.16.4 [\#2371](https://github.com/delta-io/delta-rs/pull/2371) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: add snappy compression on checkpoint files [\#2365](https://github.com/delta-io/delta-rs/pull/2365) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: add config for parquet pushdown on delta scan [\#2364](https://github.com/delta-io/delta-rs/pull/2364) ([Blajda](https://github.com/Blajda))
- fix\(python,rust\): optimize compact on schema evolved table [\#2358](https://github.com/delta-io/delta-rs/pull/2358) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(python, rust\): expr parsing date/timestamp [\#2357](https://github.com/delta-io/delta-rs/pull/2357) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: remove tmp files in cleanup\_metadata [\#2356](https://github.com/delta-io/delta-rs/pull/2356) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: make struct fields nullable in stats schema [\#2346](https://github.com/delta-io/delta-rs/pull/2346) ([qinix](https://github.com/qinix))
- fix\(rust\): adhere to protocol for Decimal [\#2332](https://github.com/delta-io/delta-rs/pull/2332) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(rust\): raise schema mismatch when decimal is not subset [\#2330](https://github.com/delta-io/delta-rs/pull/2330) ([ion-elgreco](https://github.com/ion-elgreco))
- feat\(rust\): derive Copy on some public enums [\#2329](https://github.com/delta-io/delta-rs/pull/2329) ([lasantosr](https://github.com/lasantosr))
- fix: merge pushdown handling [\#2326](https://github.com/delta-io/delta-rs/pull/2326) ([Blajda](https://github.com/Blajda))
- fix: merge concurrency control [\#2324](https://github.com/delta-io/delta-rs/pull/2324) ([ion-elgreco](https://github.com/ion-elgreco))
- Revert 2291 merge predicate fix [\#2323](https://github.com/delta-io/delta-rs/pull/2323) ([Blajda](https://github.com/Blajda))
- fix: try to fix timeouts [\#2318](https://github.com/delta-io/delta-rs/pull/2318) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(rust\): serialize MetricDetails from compaction runs to a string [\#2317](https://github.com/delta-io/delta-rs/pull/2317) ([liamphmurphy](https://github.com/liamphmurphy))
- docs: add example in to\_pyarrow\_dataset [\#2315](https://github.com/delta-io/delta-rs/pull/2315) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(python\): wrong batch size [\#2314](https://github.com/delta-io/delta-rs/pull/2314) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: object store 0.9.1 [\#2311](https://github.com/delta-io/delta-rs/pull/2311) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: checkpoint features format below v3,7 [\#2307](https://github.com/delta-io/delta-rs/pull/2307) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: schema evolution not coercing with large arrow types [\#2305](https://github.com/delta-io/delta-rs/pull/2305) ([aersam](https://github.com/aersam))
- fix: clean up some non-datafusion builds [\#2303](https://github.com/delta-io/delta-rs/pull/2303) ([rtyler](https://github.com/rtyler))
- docs: fix typo [\#2300](https://github.com/delta-io/delta-rs/pull/2300) ([LauH1987](https://github.com/LauH1987))
- docs: make replaceWhere example compile [\#2299](https://github.com/delta-io/delta-rs/pull/2299) ([LauH1987](https://github.com/LauH1987))
- fix\(rust\): add missing chrono-tz feature [\#2295](https://github.com/delta-io/delta-rs/pull/2295) ([ion-elgreco](https://github.com/ion-elgreco))
- chore\(python\): bump to v0.16.1 [\#2294](https://github.com/delta-io/delta-rs/pull/2294) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(rust\): features not maintained in protocol after checkpoint [\#2293](https://github.com/delta-io/delta-rs/pull/2293) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: merge predicate for concurrent writes [\#2291](https://github.com/delta-io/delta-rs/pull/2291) ([JonasDev1](https://github.com/JonasDev1))
- fix: replace assert and AssertionError with appropriate exceptions [\#2286](https://github.com/delta-io/delta-rs/pull/2286) ([joe-sharman](https://github.com/joe-sharman))
- docs: fix typo in delta-lake-polars.md [\#2285](https://github.com/delta-io/delta-rs/pull/2285) ([vladdoster](https://github.com/vladdoster))
- fix\(python, rust\): prevent table scan returning large arrow dtypes [\#2274](https://github.com/delta-io/delta-rs/pull/2274) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(python\): always encapsulate column names in backticks in \_all functions [\#2271](https://github.com/delta-io/delta-rs/pull/2271) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(rust\): read only checkpoints that match \_last\_checkpoint version [\#2270](https://github.com/delta-io/delta-rs/pull/2270) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: add .venv to .gitignore [\#2268](https://github.com/delta-io/delta-rs/pull/2268) ([gacharya](https://github.com/gacharya))
- feat\(python, rust\): add `set table properties` operation [\#2264](https://github.com/delta-io/delta-rs/pull/2264) ([ion-elgreco](https://github.com/ion-elgreco))
- docs: use dagster deltalake polars library [\#2263](https://github.com/delta-io/delta-rs/pull/2263) ([avriiil](https://github.com/avriiil))
- docs: update comment about r2 requiring locks [\#2261](https://github.com/delta-io/delta-rs/pull/2261) ([cmackenzie1](https://github.com/cmackenzie1))
- fix\(\#2256\): use consistent units of time [\#2260](https://github.com/delta-io/delta-rs/pull/2260) ([cmackenzie1](https://github.com/cmackenzie1))
- chore: update the changelog for rust-v0.17.1 [\#2259](https://github.com/delta-io/delta-rs/pull/2259) ([rtyler](https://github.com/rtyler))
- feat\(python\): release GIL in the write\_deltalake function [\#2257](https://github.com/delta-io/delta-rs/pull/2257) ([franz101](https://github.com/franz101))
- chore\(rust\): bump datafusion to 36 [\#2249](https://github.com/delta-io/delta-rs/pull/2249) ([universalmind303](https://github.com/universalmind303))
- chore!: replace rusoto with AWS SDK [\#2243](https://github.com/delta-io/delta-rs/pull/2243) ([mightyshazam](https://github.com/mightyshazam))
- fix: handle conflict checking in optimize correctly [\#2208](https://github.com/delta-io/delta-rs/pull/2208) ([emcake](https://github.com/emcake))
- feat: logical Node for find files [\#2194](https://github.com/delta-io/delta-rs/pull/2194) ([hntd187](https://github.com/hntd187))

## [rust-v0.17.3](https://github.com/delta-io/delta-rs/tree/rust-v0.17.3) (2024-05-01)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.17.1...rust-v0.17.3)

**Implemented enhancements:**

- Limit concurrent ObjectStore access to avoid resource limitations in constrained environments [\#2457](https://github.com/delta-io/delta-rs/issues/2457)
- How to get a DataFrame in Rust? [\#2404](https://github.com/delta-io/delta-rs/issues/2404)
- Allow checkpoint creation when partition column is "timestampNtz " [\#2381](https://github.com/delta-io/delta-rs/issues/2381)
- is there a way to make writing timestamp\_ntz optional [\#2339](https://github.com/delta-io/delta-rs/issues/2339)
- Update arrow dependency [\#2328](https://github.com/delta-io/delta-rs/issues/2328)
- Release GIL in deltalake.write\_deltalake [\#2234](https://github.com/delta-io/delta-rs/issues/2234)
- Unable to retrieve custom metadata from tables in rust [\#2153](https://github.com/delta-io/delta-rs/issues/2153)
- Refactor commit interface to be a Builder [\#2131](https://github.com/delta-io/delta-rs/issues/2131)

**Fixed bugs:**

- Handle rate limiting during write contention [\#2451](https://github.com/delta-io/delta-rs/issues/2451)
- regression : delta.logRetentionDuration don't seems to be respected  [\#2447](https://github.com/delta-io/delta-rs/issues/2447)
- Issue writing to mounted storage in AKS using delta-rs library [\#2445](https://github.com/delta-io/delta-rs/issues/2445)
- TableMerger - when\_matched\_delete\(\) fails when Column names contain special characters [\#2438](https://github.com/delta-io/delta-rs/issues/2438)
-  Generic DeltaTable error: External error: Arrow error: Invalid argument error: arguments need to have the same data type - while merge data in to delta table [\#2423](https://github.com/delta-io/delta-rs/issues/2423)
- Merge on predicate throw error on date column: Unable to convert expression to string [\#2420](https://github.com/delta-io/delta-rs/issues/2420)
- Writing Tables with Append mode errors if the schema metadata is different [\#2419](https://github.com/delta-io/delta-rs/issues/2419)
- Logstore issues on AWS Lambda [\#2410](https://github.com/delta-io/delta-rs/issues/2410)
- Datafusion timestamp type doesn't respect delta lake schema [\#2408](https://github.com/delta-io/delta-rs/issues/2408)
- Compacting produces smaller row groups than expected [\#2386](https://github.com/delta-io/delta-rs/issues/2386)
- ValueError: Partition value cannot be parsed from string. [\#2380](https://github.com/delta-io/delta-rs/issues/2380)
- Very slow s3 connection after 0.16.1 [\#2377](https://github.com/delta-io/delta-rs/issues/2377)
- Merge update+insert truncates a delta table if the table is big enough [\#2362](https://github.com/delta-io/delta-rs/issues/2362)
- Do not add readerFeatures or writerFeatures keys under checkpoint files if minReaderVersion or minWriterVersion do not satisfy the requirements [\#2360](https://github.com/delta-io/delta-rs/issues/2360)
- Create empty table failed on rust engine [\#2354](https://github.com/delta-io/delta-rs/issues/2354)
- Getting error message when running in lambda: message: "Too many open files" [\#2353](https://github.com/delta-io/delta-rs/issues/2353)
- Temporary files filling up \_delta\_log folder - increasing table load time [\#2351](https://github.com/delta-io/delta-rs/issues/2351)
- compact fails with merged schemas [\#2347](https://github.com/delta-io/delta-rs/issues/2347)
- Cannot merge into table partitioned by date type column on 0.16.3 [\#2344](https://github.com/delta-io/delta-rs/issues/2344)
- Merge breaks using logical datatype decimal128 [\#2343](https://github.com/delta-io/delta-rs/issues/2343)
- Decimal types are not checked against max precision/scale at table creation [\#2331](https://github.com/delta-io/delta-rs/issues/2331)
- Merge update+insert truncates a delta table [\#2320](https://github.com/delta-io/delta-rs/issues/2320)
- Extract `add.stats_parsed` with wrong type [\#2312](https://github.com/delta-io/delta-rs/issues/2312)
- Process fails without error message when executing merge [\#2310](https://github.com/delta-io/delta-rs/issues/2310)
- delta\_rs don't seems to respect the row group size [\#2309](https://github.com/delta-io/delta-rs/issues/2309)
- Auth error when running inside VS Code  [\#2306](https://github.com/delta-io/delta-rs/issues/2306)
- Unable to read deltatables with binary columns: Binary is not supported by JSON [\#2302](https://github.com/delta-io/delta-rs/issues/2302)
- Schema evolution not coercing with Large arrow types [\#2298](https://github.com/delta-io/delta-rs/issues/2298)
- Panic in `deltalake_core::kernel::snapshot::log_segment::list_log_files_with_checkpoint::{{closure}}` [\#2290](https://github.com/delta-io/delta-rs/issues/2290)
- Checkpoint does not preserve reader and writer features for the table protocol. [\#2288](https://github.com/delta-io/delta-rs/issues/2288)
- Z-Order with larger dataset resulting in memory error [\#2284](https://github.com/delta-io/delta-rs/issues/2284)
- Successful writes return error when using concurrent writers [\#2279](https://github.com/delta-io/delta-rs/issues/2279)
- Rust writer should raise when decimal types are incompatible \(currently writers and puts table in invalid state\) [\#2275](https://github.com/delta-io/delta-rs/issues/2275)
- Generic DeltaTable error: Version mismatch with new schema merge functionality in AWS S3 [\#2262](https://github.com/delta-io/delta-rs/issues/2262)
- DeltaTable is not resilient to corrupted checkpoint state [\#2258](https://github.com/delta-io/delta-rs/issues/2258)
- Inconsistent units of time [\#2256](https://github.com/delta-io/delta-rs/issues/2256)
- Partition column comparison is an assertion rather than if block with raise exception [\#2242](https://github.com/delta-io/delta-rs/issues/2242)
- Unable to merge column names starting from numbers [\#2230](https://github.com/delta-io/delta-rs/issues/2230)
- Merging to a table with multiple distinct partitions in parallel fails [\#2227](https://github.com/delta-io/delta-rs/issues/2227)
- cleanup\_metadata not respecting custom `logRetentionDuration` [\#2180](https://github.com/delta-io/delta-rs/issues/2180)
- Merge predicate fails with a field with a space [\#2167](https://github.com/delta-io/delta-rs/issues/2167)
- When\_matched\_update causes records to be lost with  explicit predicate [\#2158](https://github.com/delta-io/delta-rs/issues/2158)
- Merge execution time grows exponetially with the number of column [\#2107](https://github.com/delta-io/delta-rs/issues/2107)
- \_internal.DeltaError when merging [\#2084](https://github.com/delta-io/delta-rs/issues/2084)

## [rust-v0.17.1](https://github.com/delta-io/delta-rs/tree/rust-v0.17.1) (2024-03-06)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.17.0...rust-v0.17.1)

**Implemented enhancements:**

- Get statistics metadata [\#2233](https://github.com/delta-io/delta-rs/issues/2233)
- add option to append only a subsets of columns [\#2212](https://github.com/delta-io/delta-rs/issues/2212)
- add documentation how to configure delta.logRetentionDuration [\#2072](https://github.com/delta-io/delta-rs/issues/2072)
- Add `drop constraint` [\#2070](https://github.com/delta-io/delta-rs/issues/2070)
- Add 0.16 deprecation warnings for DynamoDB lock [\#2049](https://github.com/delta-io/delta-rs/issues/2049)

**Fixed bugs:**

- cleanup\_metadata not respecting custom `logRetentionDuration` [\#2180](https://github.com/delta-io/delta-rs/issues/2180)
- Rust writer panics on empty record batches [\#2253](https://github.com/delta-io/delta-rs/issues/2253)
- DeltaLake executed Rust: write method not found in `DeltaOps`  [\#2244](https://github.com/delta-io/delta-rs/issues/2244)
- DELTA\_FILE\_PATTERN regex is incorrectly matching tmp commit files [\#2201](https://github.com/delta-io/delta-rs/issues/2201)
- Failed to create checkpoint with "Parquet does not support writing empty structs" [\#2189](https://github.com/delta-io/delta-rs/issues/2189)
- Error when parsing delete expressions [\#2187](https://github.com/delta-io/delta-rs/issues/2187)
- terminate called without an active exception [\#2184](https://github.com/delta-io/delta-rs/issues/2184)
- Now conda-installable on M1 [\#2178](https://github.com/delta-io/delta-rs/issues/2178)
- Add error message for partition\_by check [\#2177](https://github.com/delta-io/delta-rs/issues/2177)
- deltalake 0.15.2 prints partitions\_values and paths which is not desired [\#2176](https://github.com/delta-io/delta-rs/issues/2176)
- cleanup\_metadata can potentially delete most recent checkpoint, corrupting table [\#2174](https://github.com/delta-io/delta-rs/issues/2174)
- Broken filter for newly created delta table [\#2169](https://github.com/delta-io/delta-rs/issues/2169)
- Hash for StructField should consider more than the name [\#2045](https://github.com/delta-io/delta-rs/issues/2045)
- Schema comparison in writer [\#1853](https://github.com/delta-io/delta-rs/issues/1853)
- fix\(python\): sort before schema comparison [\#2209](https://github.com/delta-io/delta-rs/pull/2209) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: prevent writing checkpoints with a version that does not exist in table state [\#1863](https://github.com/delta-io/delta-rs/pull/1863) ([rtyler](https://github.com/rtyler))

**Closed issues:**

- Bug/Question:  arrow's`FixedSizeList` is not roundtrippable [\#2162](https://github.com/delta-io/delta-rs/issues/2162)

**Merged pull requests:**

- fix: fixes panic on empty write [\#2254](https://github.com/delta-io/delta-rs/pull/2254) ([aersam](https://github.com/aersam))
- fix\(rust\): typo deletionvectors [\#2251](https://github.com/delta-io/delta-rs/pull/2251) ([ion-elgreco](https://github.com/ion-elgreco))
- fix\(rust\): make interval parsing compatible with plural form [\#2250](https://github.com/delta-io/delta-rs/pull/2250) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: bump to 0.16 [\#2248](https://github.com/delta-io/delta-rs/pull/2248) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: merge schema support for the write operation and Python [\#2246](https://github.com/delta-io/delta-rs/pull/2246) ([rtyler](https://github.com/rtyler))
- fix: object\_store 0.9.0 since 0.9.1 causes CI failure [\#2245](https://github.com/delta-io/delta-rs/pull/2245) ([aersam](https://github.com/aersam))
- chore\(python\): bump version [\#2241](https://github.com/delta-io/delta-rs/pull/2241) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: fix ruff and mypy version and do formatting [\#2240](https://github.com/delta-io/delta-rs/pull/2240) ([aersam](https://github.com/aersam))
- feat\(python, rust\): timestampNtz support [\#2236](https://github.com/delta-io/delta-rs/pull/2236) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: clean up some compilation failures and un-ignore some tests [\#2231](https://github.com/delta-io/delta-rs/pull/2231) ([rtyler](https://github.com/rtyler))
- docs: fixing example in CONTRIBUTING.md [\#2224](https://github.com/delta-io/delta-rs/pull/2224) ([gacharya](https://github.com/gacharya))
- perf: directly create projection instead of using DataFrame::with\_column [\#2222](https://github.com/delta-io/delta-rs/pull/2222) ([emcake](https://github.com/emcake))
- chore: remove caches from github actions [\#2215](https://github.com/delta-io/delta-rs/pull/2215) ([rtyler](https://github.com/rtyler))
- fix: `is_commit_file` should only catch commit jsons [\#2213](https://github.com/delta-io/delta-rs/pull/2213) ([emcake](https://github.com/emcake))
- chore: fix the Cargo.tomls to publish information properly on docs.rs [\#2211](https://github.com/delta-io/delta-rs/pull/2211) ([rtyler](https://github.com/rtyler))
- fix\(writer\): retry storage.put on temporary network errors [\#2207](https://github.com/delta-io/delta-rs/pull/2207) ([qinix](https://github.com/qinix))
- fix: canonicalize config keys [\#2206](https://github.com/delta-io/delta-rs/pull/2206) ([emcake](https://github.com/emcake))
- docs: update README code samples for newer versions [\#2202](https://github.com/delta-io/delta-rs/pull/2202) ([jhoekx](https://github.com/jhoekx))
- docs: dask integration fix formatting typo [\#2196](https://github.com/delta-io/delta-rs/pull/2196) ([avriiil](https://github.com/avriiil))
- fix: add data\_type and nullable to StructField hash \(\#2045\) [\#2190](https://github.com/delta-io/delta-rs/pull/2190) ([sonhmai](https://github.com/sonhmai))
- fix: removed panic in  method [\#2185](https://github.com/delta-io/delta-rs/pull/2185) ([mightyshazam](https://github.com/mightyshazam))
- feat: implement string representation for PartitionFilter [\#2183](https://github.com/delta-io/delta-rs/pull/2183) ([sonhmai](https://github.com/sonhmai))
- fix: correct map field names [\#2182](https://github.com/delta-io/delta-rs/pull/2182) ([emcake](https://github.com/emcake))
- feat: add comment to explain why assert has failed and show state [\#2179](https://github.com/delta-io/delta-rs/pull/2179) ([braaannigan](https://github.com/braaannigan))
- docs: include the 0.17.0 changelog [\#2173](https://github.com/delta-io/delta-rs/pull/2173) ([rtyler](https://github.com/rtyler))
- fix\(python\): skip empty row groups during stats gathering [\#2172](https://github.com/delta-io/delta-rs/pull/2172) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: 0.17.0 publish changes [\#2171](https://github.com/delta-io/delta-rs/pull/2171) ([rtyler](https://github.com/rtyler))
- chore\(python\): bump version [\#2170](https://github.com/delta-io/delta-rs/pull/2170) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: update all the package metadata for publication to crates.io [\#2168](https://github.com/delta-io/delta-rs/pull/2168) ([rtyler](https://github.com/rtyler))
- fix: rm println in python lib [\#2166](https://github.com/delta-io/delta-rs/pull/2166) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: cleanup minor clippies and other warns [\#2161](https://github.com/delta-io/delta-rs/pull/2161) ([rtyler](https://github.com/rtyler))
- feat: implement clone for DeltaTable struct [\#2160](https://github.com/delta-io/delta-rs/pull/2160) ([mightyshazam](https://github.com/mightyshazam))
- fix: allow loading of tables with identity columns [\#2155](https://github.com/delta-io/delta-rs/pull/2155) ([rtyler](https://github.com/rtyler))
- fix: replace BTreeMap with IndexMap to preserve insertion order [\#2150](https://github.com/delta-io/delta-rs/pull/2150) ([roeap](https://github.com/roeap))
- fix: made generalize\_filter less permissive, also added more cases [\#2149](https://github.com/delta-io/delta-rs/pull/2149) ([emcake](https://github.com/emcake))
- docs: add delta lake best practices [\#2147](https://github.com/delta-io/delta-rs/pull/2147) ([MrPowers](https://github.com/MrPowers))
- chore: shorten up the crate folder names in the tree [\#2145](https://github.com/delta-io/delta-rs/pull/2145) ([rtyler](https://github.com/rtyler))
- fix\(\#2143\): keep specific error type when writing fails [\#2144](https://github.com/delta-io/delta-rs/pull/2144) ([abaerptc](https://github.com/abaerptc))
- refactor\(python\): drop custom filesystem in write\_deltalake [\#2137](https://github.com/delta-io/delta-rs/pull/2137) ([ion-elgreco](https://github.com/ion-elgreco))
- docs: use transparent logo in README [\#2132](https://github.com/delta-io/delta-rs/pull/2132) ([roeap](https://github.com/roeap))
- fix: order logical schema to match physical schema [\#2129](https://github.com/delta-io/delta-rs/pull/2129) ([Blajda](https://github.com/Blajda))
- feat: expose stats schema on Snapshot [\#2128](https://github.com/delta-io/delta-rs/pull/2128) ([roeap](https://github.com/roeap))
- feat: update table config to contain new config keys [\#2127](https://github.com/delta-io/delta-rs/pull/2127) ([roeap](https://github.com/roeap))
- fix: clean-up paths created during tests [\#2126](https://github.com/delta-io/delta-rs/pull/2126) ([roeap](https://github.com/roeap))
- fix: prevent empty stats struct during parquet write [\#2125](https://github.com/delta-io/delta-rs/pull/2125) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- fix: temporarily skip s3 roundtrip test [\#2124](https://github.com/delta-io/delta-rs/pull/2124) ([roeap](https://github.com/roeap))
- fix: do not write empty parquet file/add on writer close; accurately … [\#2123](https://github.com/delta-io/delta-rs/pull/2123) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- docs: add dask page to integration docs [\#2122](https://github.com/delta-io/delta-rs/pull/2122) ([avriiil](https://github.com/avriiil))
- chore: upgrade to DataFusion 35.0 [\#2121](https://github.com/delta-io/delta-rs/pull/2121) ([philippemnoel](https://github.com/philippemnoel))
- fix\(s3\): restore working test for DynamoDb log store repair log on read [\#2120](https://github.com/delta-io/delta-rs/pull/2120) ([dispanser](https://github.com/dispanser))
- fix: set partition values for added files when building compaction plan [\#2119](https://github.com/delta-io/delta-rs/pull/2119) ([alexwilcoxson-rel](https://github.com/alexwilcoxson-rel))
- fix: add missing pandas import [\#2116](https://github.com/delta-io/delta-rs/pull/2116) ([Tim-Haarman](https://github.com/Tim-Haarman))
- chore: temporarily ignore the repair on update test [\#2114](https://github.com/delta-io/delta-rs/pull/2114) ([rtyler](https://github.com/rtyler))
- docs: delta lake is great for small data [\#2113](https://github.com/delta-io/delta-rs/pull/2113) ([MrPowers](https://github.com/MrPowers))
- chore: removed unnecessary print statement from update method [\#2111](https://github.com/delta-io/delta-rs/pull/2111) ([LilMonk](https://github.com/LilMonk))
- fix: schema issue within writebuilder [\#2106](https://github.com/delta-io/delta-rs/pull/2106) ([universalmind303](https://github.com/universalmind303))
- docs: fix arg indent [\#2103](https://github.com/delta-io/delta-rs/pull/2103) ([wchatx](https://github.com/wchatx))
- docs: delta lake file skipping [\#2096](https://github.com/delta-io/delta-rs/pull/2096) ([MrPowers](https://github.com/MrPowers))
- docs: move dynamo docs into new docs page [\#2093](https://github.com/delta-io/delta-rs/pull/2093) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: bump python [\#2092](https://github.com/delta-io/delta-rs/pull/2092) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: allow merge\_execute to release the GIL [\#2091](https://github.com/delta-io/delta-rs/pull/2091) ([emcake](https://github.com/emcake))
- docs: how delta lake transactions work [\#2089](https://github.com/delta-io/delta-rs/pull/2089) ([MrPowers](https://github.com/MrPowers))
- fix: reinstate copy-if-not-exists passthrough [\#2083](https://github.com/delta-io/delta-rs/pull/2083) ([emcake](https://github.com/emcake))
- docs: make an overview tab visible in docs [\#2080](https://github.com/delta-io/delta-rs/pull/2080) ([r3stl355](https://github.com/r3stl355))
- docs: add usage guide for check constraints [\#2079](https://github.com/delta-io/delta-rs/pull/2079) ([hntd187](https://github.com/hntd187))
- docs: update docs for rust print statement [\#2077](https://github.com/delta-io/delta-rs/pull/2077) ([skariyania](https://github.com/skariyania))
- docs: add page on why to use delta lake [\#2076](https://github.com/delta-io/delta-rs/pull/2076) ([MrPowers](https://github.com/MrPowers))
- feat\(rust, python\): add `drop constraint` operation [\#2071](https://github.com/delta-io/delta-rs/pull/2071) ([ion-elgreco](https://github.com/ion-elgreco))
- refactor: add deltalake-gcp crate [\#2061](https://github.com/delta-io/delta-rs/pull/2061) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: allow checkpoints to contain metadata actions without a createdTime value [\#2059](https://github.com/delta-io/delta-rs/pull/2059) ([rtyler](https://github.com/rtyler))
- chore: bump version python [\#2047](https://github.com/delta-io/delta-rs/pull/2047) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: ensure metadata cleanup do not corrupt tables without checkpoints [\#2044](https://github.com/delta-io/delta-rs/pull/2044) ([Blajda](https://github.com/Blajda))
- docs: update docs for merge [\#2042](https://github.com/delta-io/delta-rs/pull/2042) ([Blajda](https://github.com/Blajda))
- chore: update documentation for S3 / DynamoDb log store configuration [\#2041](https://github.com/delta-io/delta-rs/pull/2041) ([dispanser](https://github.com/dispanser))
- feat: arrow backed log replay and table state [\#2037](https://github.com/delta-io/delta-rs/pull/2037) ([roeap](https://github.com/roeap))
- fix: properly deserialize percent-encoded file paths of Remove actions, to make sure tombstone and file paths match [\#2035](https://github.com/delta-io/delta-rs/pull/2035) ([sigorbor](https://github.com/sigorbor))
- fix: remove casts of structs to record batch [\#2033](https://github.com/delta-io/delta-rs/pull/2033) ([Blajda](https://github.com/Blajda))
- feat\(python, rust\): expose custom\_metadata for all operations [\#2032](https://github.com/delta-io/delta-rs/pull/2032) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: refactor WriterProperties class [\#2030](https://github.com/delta-io/delta-rs/pull/2030) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: update datafusion [\#2029](https://github.com/delta-io/delta-rs/pull/2029) ([roeap](https://github.com/roeap))
- refactor: increase metadata action usage [\#2027](https://github.com/delta-io/delta-rs/pull/2027) ([roeap](https://github.com/roeap))
- fix: github actions for releasing docs [\#2026](https://github.com/delta-io/delta-rs/pull/2026) ([r3stl355](https://github.com/r3stl355))
- feat: introduce schema evolution on RecordBatchWriter [\#2024](https://github.com/delta-io/delta-rs/pull/2024) ([rtyler](https://github.com/rtyler))
- refactor: move azure integration to dedicated crate [\#2023](https://github.com/delta-io/delta-rs/pull/2023) ([roeap](https://github.com/roeap))
- fix: use temporary table names during the constraint checks [\#2017](https://github.com/delta-io/delta-rs/pull/2017) ([r3stl355](https://github.com/r3stl355))
- docs: add alterer [\#2014](https://github.com/delta-io/delta-rs/pull/2014) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: version bump python release [\#2011](https://github.com/delta-io/delta-rs/pull/2011) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: fix the test\_restore\_by\_datetime test [\#2010](https://github.com/delta-io/delta-rs/pull/2010) ([r3stl355](https://github.com/r3stl355))
- feat\(rust\): add more commit info to most operations [\#2009](https://github.com/delta-io/delta-rs/pull/2009) ([ion-elgreco](https://github.com/ion-elgreco))
- feat\(python\): add schema conversion of FixedSizeBinaryArray and FixedSizeListType [\#2005](https://github.com/delta-io/delta-rs/pull/2005) ([balbok0](https://github.com/balbok0))
- feat\(python\): expose large\_dtype param in `merge` [\#2003](https://github.com/delta-io/delta-rs/pull/2003) ([ion-elgreco](https://github.com/ion-elgreco))
- docs: add writer properties to docs [\#2002](https://github.com/delta-io/delta-rs/pull/2002) ([ion-elgreco](https://github.com/ion-elgreco))
- chore: fix CI breaking lint issues [\#1999](https://github.com/delta-io/delta-rs/pull/1999) ([r3stl355](https://github.com/r3stl355))
- feat: implementation for replaceWhere [\#1996](https://github.com/delta-io/delta-rs/pull/1996) ([r3stl355](https://github.com/r3stl355))
- chore: refactoring AWS code out of the core crate [\#1995](https://github.com/delta-io/delta-rs/pull/1995) ([rtyler](https://github.com/rtyler))
- feat\(python\): expose custom metadata to writers [\#1994](https://github.com/delta-io/delta-rs/pull/1994) ([ion-elgreco](https://github.com/ion-elgreco))
- docs: datafusion integration [\#1993](https://github.com/delta-io/delta-rs/pull/1993) ([MrPowers](https://github.com/MrPowers))
- fix: flakey gcs test [\#1987](https://github.com/delta-io/delta-rs/pull/1987) ([roeap](https://github.com/roeap))
- fix: implement consistent formatting for constraint expressions [\#1985](https://github.com/delta-io/delta-rs/pull/1985) ([Blajda](https://github.com/Blajda))
- fix: case sensitivity for z-order [\#1982](https://github.com/delta-io/delta-rs/pull/1982) ([Blajda](https://github.com/Blajda))
- feat\(python\): add writer\_properties to all operations [\#1980](https://github.com/delta-io/delta-rs/pull/1980) ([ion-elgreco](https://github.com/ion-elgreco))
- refactor: trigger metadata retrieval only during `DeltaTable.metadata` [\#1979](https://github.com/delta-io/delta-rs/pull/1979) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: retry with exponential backoff for DynamoDb interaction [\#1975](https://github.com/delta-io/delta-rs/pull/1975) ([dispanser](https://github.com/dispanser))
- feat\(python\): expose `add constraint` operation [\#1973](https://github.com/delta-io/delta-rs/pull/1973) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: properly decode percent-encoded file paths coming from parquet checkpoints [\#1970](https://github.com/delta-io/delta-rs/pull/1970) ([sigorbor](https://github.com/sigorbor))
- feat: omit unmodified files during merge write [\#1969](https://github.com/delta-io/delta-rs/pull/1969) ([Blajda](https://github.com/Blajda))
- feat\(python\): combine load\_version/load\_with\_datetime into `load_as_version` [\#1968](https://github.com/delta-io/delta-rs/pull/1968) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: enable S3 integration tests to be configured via environment vars [\#1966](https://github.com/delta-io/delta-rs/pull/1966) ([dispanser](https://github.com/dispanser))
- fix: handle empty table response in unity api [\#1963](https://github.com/delta-io/delta-rs/pull/1963) ([JonasDev1](https://github.com/JonasDev1))
- docs: add auto-release when docs are merged to main [\#1962](https://github.com/delta-io/delta-rs/pull/1962) ([r3stl355](https://github.com/r3stl355))
- feat: cast list items to default before write with different item names [\#1959](https://github.com/delta-io/delta-rs/pull/1959) ([JonasDev1](https://github.com/JonasDev1))
- feat: merge using partition filters [\#1958](https://github.com/delta-io/delta-rs/pull/1958) ([emcake](https://github.com/emcake))
- chore: relocate cast\_record\_batch into its own module to shed the datafusion dependency [\#1955](https://github.com/delta-io/delta-rs/pull/1955) ([rtyler](https://github.com/rtyler))
- fix: respect case sensitivity on operations [\#1954](https://github.com/delta-io/delta-rs/pull/1954) ([Blajda](https://github.com/Blajda))
- docs: add better installation instructions [\#1951](https://github.com/delta-io/delta-rs/pull/1951) ([MrPowers](https://github.com/MrPowers))
- docs: add polars integration [\#1949](https://github.com/delta-io/delta-rs/pull/1949) ([MrPowers](https://github.com/MrPowers))
- fix: add arrow page back [\#1944](https://github.com/delta-io/delta-rs/pull/1944) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: remove the get\_data\_catalog\(\) function [\#1941](https://github.com/delta-io/delta-rs/pull/1941) ([rtyler](https://github.com/rtyler))
- chore: update runs-on value in python\_release.yml [\#1940](https://github.com/delta-io/delta-rs/pull/1940) ([wjones127](https://github.com/wjones127))
- docs: start how delta lake works [\#1938](https://github.com/delta-io/delta-rs/pull/1938) ([MrPowers](https://github.com/MrPowers))
- docs: add logo, dark mode, boost search [\#1936](https://github.com/delta-io/delta-rs/pull/1936) ([ion-elgreco](https://github.com/ion-elgreco))
- refactor: prefer usage of metadata and protocol fields [\#1935](https://github.com/delta-io/delta-rs/pull/1935) ([roeap](https://github.com/roeap))
- chore: update python version [\#1934](https://github.com/delta-io/delta-rs/pull/1934) ([wjones127](https://github.com/wjones127))
- feat\(python\): expose create to DeltaTable class [\#1932](https://github.com/delta-io/delta-rs/pull/1932) ([ion-elgreco](https://github.com/ion-elgreco))
- docs: fix all examples and change overall structure [\#1931](https://github.com/delta-io/delta-rs/pull/1931) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: update to include pyarrow-hotfix [\#1930](https://github.com/delta-io/delta-rs/pull/1930) ([dennyglee](https://github.com/dennyglee))
- fix: get rid of panic in during table [\#1928](https://github.com/delta-io/delta-rs/pull/1928) ([dimonchik-suvorov](https://github.com/dimonchik-suvorov))
- fix\(rust/python\): `optimize.compact` not working with tables with mixed large/normal arrow [\#1926](https://github.com/delta-io/delta-rs/pull/1926) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: extend write\_deltalake to accept Deltalake schema [\#1922](https://github.com/delta-io/delta-rs/pull/1922) ([r3stl355](https://github.com/r3stl355))
- fix: fail fast for opening non-existent path [\#1917](https://github.com/delta-io/delta-rs/pull/1917) ([dimonchik-suvorov](https://github.com/dimonchik-suvorov))
- feat: check constraints [\#1915](https://github.com/delta-io/delta-rs/pull/1915) ([hntd187](https://github.com/hntd187))
- docs: delta lake arrow integration page [\#1914](https://github.com/delta-io/delta-rs/pull/1914) ([MrPowers](https://github.com/MrPowers))
- feat: add more info for contributors [\#1913](https://github.com/delta-io/delta-rs/pull/1913) ([r3stl355](https://github.com/r3stl355))
- fix: add buffer flushing to filesystem writes [\#1911](https://github.com/delta-io/delta-rs/pull/1911) ([r3stl355](https://github.com/r3stl355))
- docs: update docs home page and add pandas integration [\#1905](https://github.com/delta-io/delta-rs/pull/1905) ([MrPowers](https://github.com/MrPowers))
- feat: implement S3 log store with transactions backed by DynamoDb [\#1904](https://github.com/delta-io/delta-rs/pull/1904) ([dispanser](https://github.com/dispanser))
- fix: prune each merge bin with only 1 file [\#1902](https://github.com/delta-io/delta-rs/pull/1902) ([haruband](https://github.com/haruband))
- docs: update python docs link in readme.md [\#1899](https://github.com/delta-io/delta-rs/pull/1899) ([thomasfrederikhoeck](https://github.com/thomasfrederikhoeck))
- docs: on append, overwrite, delete and z-ordering [\#1897](https://github.com/delta-io/delta-rs/pull/1897) ([MrPowers](https://github.com/MrPowers))
- feat: compare timestamp partition values as timestamps instead of strings [\#1895](https://github.com/delta-io/delta-rs/pull/1895) ([sigorbor](https://github.com/sigorbor))
- feat\(python\): expose rust writer as additional engine v2 [\#1891](https://github.com/delta-io/delta-rs/pull/1891) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: add high-level checking for append-only tables [\#1887](https://github.com/delta-io/delta-rs/pull/1887) ([junjunjd](https://github.com/junjunjd))
- test: loading version 0 Delta table [\#1885](https://github.com/delta-io/delta-rs/pull/1885) ([dimonchik-suvorov](https://github.com/dimonchik-suvorov))
- fix: improve catalog failure error message, add missing Glue native-tls feature dependency [\#1883](https://github.com/delta-io/delta-rs/pull/1883) ([r3stl355](https://github.com/r3stl355))
- refactor: simplify `DeltaTableState` [\#1877](https://github.com/delta-io/delta-rs/pull/1877) ([roeap](https://github.com/roeap))
- refactor: express log schema in delta types [\#1876](https://github.com/delta-io/delta-rs/pull/1876) ([roeap](https://github.com/roeap))
- docs: add Rust installation instructions [\#1875](https://github.com/delta-io/delta-rs/pull/1875) ([MrPowers](https://github.com/MrPowers))
- chore: clippy [\#1871](https://github.com/delta-io/delta-rs/pull/1871) ([roeap](https://github.com/roeap))
- fix: docs deployment action [\#1869](https://github.com/delta-io/delta-rs/pull/1869) ([r3stl355](https://github.com/r3stl355))
- docs: tell how to claim an issue [\#1866](https://github.com/delta-io/delta-rs/pull/1866) ([wjones127](https://github.com/wjones127))
- feat: drop python 3.7 and adopt 3.12 [\#1859](https://github.com/delta-io/delta-rs/pull/1859) ([roeap](https://github.com/roeap))
- feat: create benchmarks for merge [\#1857](https://github.com/delta-io/delta-rs/pull/1857) ([Blajda](https://github.com/Blajda))
- chore: add @ion-elgreco to python/ [\#1855](https://github.com/delta-io/delta-rs/pull/1855) ([rtyler](https://github.com/rtyler))
- fix: compile error with lifetime issues on optimize \(\#1843\) [\#1852](https://github.com/delta-io/delta-rs/pull/1852) ([dispanser](https://github.com/dispanser))
- feat: implement issue auto-assign on `take` comment [\#1851](https://github.com/delta-io/delta-rs/pull/1851) ([r3stl355](https://github.com/r3stl355))
- docs: add docs on small file compaction with optimize [\#1850](https://github.com/delta-io/delta-rs/pull/1850) ([MrPowers](https://github.com/MrPowers))
- fix: checkpoint error with Azure Synapse [\#1848](https://github.com/delta-io/delta-rs/pull/1848) ([PierreDubrulle](https://github.com/PierreDubrulle))
- feat\(python\): expose `convert_to_deltalake` [\#1842](https://github.com/delta-io/delta-rs/pull/1842) ([ion-elgreco](https://github.com/ion-elgreco))
- ci: adopt `ruff format` for formatting [\#1841](https://github.com/delta-io/delta-rs/pull/1841) ([roeap](https://github.com/roeap))

## [rust-v0.17.0](https://github.com/delta-io/delta-rs/tree/rust-v0.17.0) (2024-02-06)

:warning: The release of 0.17.0 **removes** the legacy dynamodb lock functionality, AWS users must read these release notes! :warning:

### File handlers

The 0.17.0 release moves storage implementations into their own crates, such as
`deltalake-aws`. A consequence of that refactoring is that custom storage and
file scheme handlers must be registered/initialized at runtime. Storage
subcrates conventionally define a `register_handlers` function which performs
that task. Users may see errors such as:
```
thread 'main' panicked at /home/ubuntu/.cargo/registry/src/index.crates.io-6f17d22bba15001f/deltalake-core-0.17.0/src/table/builder.rs:189:48:
The specified table_uri is not valid: InvalidTableLocation("Unknown scheme: s3")
```

* Users of the meta-crate (`deltalake`) can call the storage crate via: `deltalake::aws::register_handlers(None);` at the entrypoint for their code.
* Users who adopt `core` and storage crates independently (e.g. `deltalake-aws`) can register via `deltalake_aws::register_handlers(None);`.

The AWS, Azure, and GCP crates must all have their custom file schemes registered in this fashion.


### dynamodblock to S3DynamoDbLogStore

The locking mechanism is fundamentally different between `deltalake` v0.16.x and v0.17.0, starting with this release the `deltalake` and `deltalake-aws` crates this library now relies on the same [protocol for concurrent writes on AWS](https://docs.delta.io/latest/delta-storage.html#setup-configuration-s3-multi-cluster) as the Delta Lake/Spark implementation.

Fundamentally the DynamoDB table structure changes, [which is documented here](https://docs.delta.io/latest/delta-storage.html#setup-configuration-s3-multi-cluster). The configuration of a Rust process should continue to use the `AWS_S3_LOCKING_PROVIDER` environment value of `dynamodb`.  The new table must be specified with the `DELTA_DYNAMO_TABLE_NAME` environment or configuration variable, and that should name the _new_ `S3DynamoDbLogStore` compatible DynamoDB table.

Because locking is required to ensure safe cconsistent writes, **there is no iterative migration**, 0.16 and 0.17 writers **cannot** safely coexist. The following steps should be taken when upgrading:

1. Stop all 0.16.x writers
2. Ensure writes are completed, and lock table is empty.
3. Deploy 0.17.0 writers



[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.16.5...rust-v0.17.0)

**Implemented enhancements:**

- Expose the ability to compile DataFusion with SIMD [\#2118](https://github.com/delta-io/delta-rs/issues/2118)
- Updating Table log retention configuration with `write_deltalake` silently changes nothing [\#2108](https://github.com/delta-io/delta-rs/issues/2108)
- ALTER table, ALTER Column, Add/Modify Comment, Add/remove/rename partitions, Set Tags, Set location, Set TBLProperties [\#2088](https://github.com/delta-io/delta-rs/issues/2088)
- Docs: Update docs for check constraints [\#2063](https://github.com/delta-io/delta-rs/issues/2063)
- Don't `ensure_table_uri` when creating a table `with_log_store` [\#2036](https://github.com/delta-io/delta-rs/issues/2036)
- Exposing custom\_metadata in merge operation [\#2031](https://github.com/delta-io/delta-rs/issues/2031)
- Support custom table properties via TableAlterer and write/merge [\#2022](https://github.com/delta-io/delta-rs/issues/2022)
- Remove parquet2 crate support [\#2004](https://github.com/delta-io/delta-rs/issues/2004)
- Merge operation that only touches necessary partitions [\#1991](https://github.com/delta-io/delta-rs/issues/1991)
- store userMetadata on write operations [\#1990](https://github.com/delta-io/delta-rs/issues/1990)
- Create Dask integration page [\#1956](https://github.com/delta-io/delta-rs/issues/1956)
- Merge: Filtering on partitions [\#1918](https://github.com/delta-io/delta-rs/issues/1918)
- Rethink the load\_version and load\_with\_datetime interfaces [\#1910](https://github.com/delta-io/delta-rs/issues/1910)
- docs: Delta Lake + Arrow Integration [\#1908](https://github.com/delta-io/delta-rs/issues/1908)
- docs: Delta Lake + Polars integration [\#1906](https://github.com/delta-io/delta-rs/issues/1906)
- Rethink decision to expose the public interface in namespaces [\#1900](https://github.com/delta-io/delta-rs/issues/1900)
- Add documentation on how to build and run documentation locally [\#1893](https://github.com/delta-io/delta-rs/issues/1893)
- Add API to create an empty Delta Lake table [\#1892](https://github.com/delta-io/delta-rs/issues/1892)
- Implementing CHECK constraints  [\#1881](https://github.com/delta-io/delta-rs/issues/1881)
- Check Invariants are respecting table features for write paths  [\#1880](https://github.com/delta-io/delta-rs/issues/1880)
- Organize docs with single lefthand sidebar [\#1873](https://github.com/delta-io/delta-rs/issues/1873)
- Make sure invariants are handled properly throughout the codebase [\#1870](https://github.com/delta-io/delta-rs/issues/1870)
- Unable to use deltalake `Schema` in `write_deltalake` [\#1862](https://github.com/delta-io/delta-rs/issues/1862)
- Add a Rust-backed engine for write\_deltalake [\#1861](https://github.com/delta-io/delta-rs/issues/1861)
- Run doctest in CI for Python API examples [\#1783](https://github.com/delta-io/delta-rs/issues/1783)
- \[RFC\] Use arrow for checkpoint reading and state handling [\#1776](https://github.com/delta-io/delta-rs/issues/1776)
- Expose Python exceptions in public module [\#1771](https://github.com/delta-io/delta-rs/issues/1771)
- Expose cleanup\_metadata or create\_checkpoint\_from\_table\_uri\_and\_cleanup to the Python API [\#1768](https://github.com/delta-io/delta-rs/issues/1768)
- Expose convert\_to\_delta to Python API [\#1767](https://github.com/delta-io/delta-rs/issues/1767)
- Add high-level checking for append-only tables [\#1759](https://github.com/delta-io/delta-rs/issues/1759)

**Fixed bugs:**

- Row order no longer preserved after merge operation [\#2165](https://github.com/delta-io/delta-rs/issues/2165)
- Error when reading delta table with IDENTITY column [\#2152](https://github.com/delta-io/delta-rs/issues/2152)
- Merge on IS NULL condition doesn't work for empty table [\#2148](https://github.com/delta-io/delta-rs/issues/2148)
- JsonWriter converts structured parsing error into plain string [\#2143](https://github.com/delta-io/delta-rs/issues/2143)
- Pandas import error when merging tables  [\#2112](https://github.com/delta-io/delta-rs/issues/2112)
-   test\_repair\_on\_update broken in main [\#2109](https://github.com/delta-io/delta-rs/issues/2109)
- `WriteBuilder::with_input_execution_plan` does not apply the schema to the log's metadata fields [\#2105](https://github.com/delta-io/delta-rs/issues/2105)
- MERGE logical plan vs execution plan schema mismatch [\#2104](https://github.com/delta-io/delta-rs/issues/2104)
- Partitions not pushed down [\#2090](https://github.com/delta-io/delta-rs/issues/2090)
- Cant create empty table with write\_deltalake [\#2086](https://github.com/delta-io/delta-rs/issues/2086)
- Unexpected high costs on Google Cloud Storage [\#2085](https://github.com/delta-io/delta-rs/issues/2085)
- Unable to read s3 table: `Unknown scheme: s3` [\#2065](https://github.com/delta-io/delta-rs/issues/2065)
- write\_deltalake not respecting writer\_properties [\#2064](https://github.com/delta-io/delta-rs/issues/2064)
- Unable to read/write tables with the "gs" schema in the table\_uri in 0.15.1 [\#2060](https://github.com/delta-io/delta-rs/issues/2060)
- LockClient required error for S3 backend in 0.15.1 python [\#2057](https://github.com/delta-io/delta-rs/issues/2057)
- Error while writing Pandas DataFrame to Delta Lake \(S3\) [\#2051](https://github.com/delta-io/delta-rs/issues/2051)
- Error with dynamo locking provider on 0.15 [\#2034](https://github.com/delta-io/delta-rs/issues/2034)
- Conda version 0.15.0 is missing files [\#2021](https://github.com/delta-io/delta-rs/issues/2021)
- Rust panicking through Python library when a delete predicate uses a nullable field [\#2019](https://github.com/delta-io/delta-rs/issues/2019)
- No snapshot or version 0 found, perhaps /Users/watsy0007/resources/test\_table/ is an empty dir? [\#2016](https://github.com/delta-io/delta-rs/issues/2016)
- Generic DeltaTable error: type\_coercion in Struct column in merge operation [\#1998](https://github.com/delta-io/delta-rs/issues/1998)
- Constraint expr not formatted during commit action [\#1971](https://github.com/delta-io/delta-rs/issues/1971)
- .load\_with\_datetime\(\) is incorrectly rounding to nearest second [\#1967](https://github.com/delta-io/delta-rs/issues/1967)
- vacuuming log files [\#1965](https://github.com/delta-io/delta-rs/issues/1965)
- Unable to merge uppercase column names [\#1960](https://github.com/delta-io/delta-rs/issues/1960)
- Schema error: Invalid data type for Delta Lake: Null [\#1946](https://github.com/delta-io/delta-rs/issues/1946)
- Python v0.14 wheel files not up to date [\#1945](https://github.com/delta-io/delta-rs/issues/1945)
- python Release 0.14 is missing Windows wheels [\#1942](https://github.com/delta-io/delta-rs/issues/1942)
- CI integration test fails randomly:  test\_restore\_by\_datetime [\#1925](https://github.com/delta-io/delta-rs/issues/1925)
- Merge data freezes indefenetely [\#1920](https://github.com/delta-io/delta-rs/issues/1920)
- Load DeltaTable from non-existing folder causing empty folder creation [\#1916](https://github.com/delta-io/delta-rs/issues/1916)
- Reoptimizes merge bins with only 1 file, even though they have no effect. [\#1901](https://github.com/delta-io/delta-rs/issues/1901)
- The Python Docs link in README.MD points to old docs [\#1898](https://github.com/delta-io/delta-rs/issues/1898)
- optimize.compact\(\) fails with bad schema after updating to pyarrow 8.0 [\#1889](https://github.com/delta-io/delta-rs/issues/1889)
- Python build is broken on main [\#1856](https://github.com/delta-io/delta-rs/issues/1856)
- Checkpoint error with Azure Synapse [\#1847](https://github.com/delta-io/delta-rs/issues/1847)
- merge very slow compared to delete + append on larger dataset [\#1846](https://github.com/delta-io/delta-rs/issues/1846)
- get\_add\_actions fails with deltalake 0.13 [\#1835](https://github.com/delta-io/delta-rs/issues/1835)
- Handle PyArrow CVE-2023-47248 [\#1834](https://github.com/delta-io/delta-rs/issues/1834)
- Delta-rs writer hangs with to many file handles open \(Azure\) [\#1832](https://github.com/delta-io/delta-rs/issues/1832)
- Encountering NotATable\("No snapshot or version 0 found, perhaps xxx is an empty dir?"\) [\#1831](https://github.com/delta-io/delta-rs/issues/1831)
- write\_deltalake is not creating checkpoints [\#1815](https://github.com/delta-io/delta-rs/issues/1815)
- Problem writing tables in directory named with char `~` [\#1806](https://github.com/delta-io/delta-rs/issues/1806)
- DeltaTable Merge throws in merging if there are uppercase in Schema. [\#1797](https://github.com/delta-io/delta-rs/issues/1797)
- rust merge error - datafusion panics [\#1790](https://github.com/delta-io/delta-rs/issues/1790)
- expose use\_dictionary=False when writing Delta Table and running optimize [\#1772](https://github.com/delta-io/delta-rs/issues/1772)

**Closed issues:**

- Is this print necessary? Can we remove this. [\#2110](https://github.com/delta-io/delta-rs/issues/2110)
- Azure concurrent writes [\#2069](https://github.com/delta-io/delta-rs/issues/2069)
- Fix docs deployment [\#1867](https://github.com/delta-io/delta-rs/issues/1867)
- Add a header in old docs and direct users to new docs [\#1865](https://github.com/delta-io/delta-rs/issues/1865)

## [rust-v0.16.5](https://github.com/delta-io/delta-rs/tree/rust-v0.16.5) (2023-11-15)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.16.4...rust-v0.16.5)

**Implemented enhancements:**

- When will upgrade object\_store to 0.8? [\#1858](https://github.com/delta-io/delta-rs/issues/1858)
- No Official Help [\#1849](https://github.com/delta-io/delta-rs/issues/1849)
- Auto assign GitHub issues with a "take" message [\#1791](https://github.com/delta-io/delta-rs/issues/1791)

**Fixed bugs:**

- cargo clippy fails on core in main [\#1843](https://github.com/delta-io/delta-rs/issues/1843)

## [rust-v0.16.4](https://github.com/delta-io/delta-rs/tree/rust-v0.16.4) (2023-11-12)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.16.3...rust-v0.16.4)

**Implemented enhancements:**

- Unable to add deltalake git dependency to cargo.toml [\#1821](https://github.com/delta-io/delta-rs/issues/1821)

## [rust-v0.16.3](https://github.com/delta-io/delta-rs/tree/rust-v0.16.3) (2023-11-08)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.16.2...rust-v0.16.3)

**Implemented enhancements:**

- Docs: add release GitHub action [\#1799](https://github.com/delta-io/delta-rs/issues/1799)
- Use bulk deletes where possible [\#1761](https://github.com/delta-io/delta-rs/issues/1761)

**Fixed bugs:**

- Code Owners no longer valid [\#1794](https://github.com/delta-io/delta-rs/issues/1794)
- `MERGE` works incorrectly with partitioned table if the data column order is not same as table column order [\#1787](https://github.com/delta-io/delta-rs/issues/1787)
- errors when using pyarrow dataset as a source [\#1779](https://github.com/delta-io/delta-rs/issues/1779)
- Write to Microsoft OneLake failed. [\#1764](https://github.com/delta-io/delta-rs/issues/1764)

## [rust-v0.16.2](https://github.com/delta-io/delta-rs/tree/rust-v0.16.2) (2023-10-21)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.16.1...rust-v0.16.2)

## [rust-v0.16.1](https://github.com/delta-io/delta-rs/tree/rust-v0.16.1) (2023-10-21)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.16.0...rust-v0.16.1)

## [rust-v0.16.0](https://github.com/delta-io/delta-rs/tree/rust-v0.16.0) (2023-09-27)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.15.0...rust-v0.16.0)

**Implemented enhancements:**

- Expose Optimize option min\_commit\_interval in Python [\#1640](https://github.com/delta-io/delta-rs/issues/1640)
- Expose create\_checkpoint\_for [\#1513](https://github.com/delta-io/delta-rs/issues/1513)
- integration tests regularly fail for HDFS [\#1428](https://github.com/delta-io/delta-rs/issues/1428)
- Add Support for Microsoft OneLake [\#1418](https://github.com/delta-io/delta-rs/issues/1418)
- add support for atomic rename in R2 [\#1356](https://github.com/delta-io/delta-rs/issues/1356)

**Fixed bugs:**

- Writing with large arrow types \(e.g. large\_utf8\), writes wrong partition encoding [\#1669](https://github.com/delta-io/delta-rs/issues/1669)
- \[python\] Different stringification of partition values in reader and writer [\#1653](https://github.com/delta-io/delta-rs/issues/1653)
- Unable to interface with data written from Spark Databricks [\#1651](https://github.com/delta-io/delta-rs/issues/1651)
- `get_last_checkpoint` does some unnecessary listing [\#1643](https://github.com/delta-io/delta-rs/issues/1643)
- `PartitionWriter`'s `buffer_len` doesn't include incomplete row groups [\#1637](https://github.com/delta-io/delta-rs/issues/1637)
- Slack community invite link has expired [\#1636](https://github.com/delta-io/delta-rs/issues/1636)
- delta-rs does not appear to support tables with liquid clustering [\#1626](https://github.com/delta-io/delta-rs/issues/1626)
- Internal Parquet panic when using a Map type.  [\#1619](https://github.com/delta-io/delta-rs/issues/1619)
- partition\_by with "$" on local filesystem [\#1591](https://github.com/delta-io/delta-rs/issues/1591)
- ProtocolChanged error when performing append write [\#1585](https://github.com/delta-io/delta-rs/issues/1585)
- Unable to `cargo update` using git tag or rev on Rust 1.70 [\#1580](https://github.com/delta-io/delta-rs/issues/1580)
- NoMetadata error when reading detlatable [\#1562](https://github.com/delta-io/delta-rs/issues/1562)
- Cannot read delta table: `Delta protocol violation` [\#1557](https://github.com/delta-io/delta-rs/issues/1557)
- Update the CODEOWNERS to capture the current reviewers and contributors [\#1553](https://github.com/delta-io/delta-rs/issues/1553)
- \[Python\] Incorrect file URIs when partition values contain escape character [\#1533](https://github.com/delta-io/delta-rs/issues/1533)
- add documentation how to Query Delta natively from datafusion [\#1485](https://github.com/delta-io/delta-rs/issues/1485)
- Python: write\_deltalake to ADLS Gen2 issue [\#1456](https://github.com/delta-io/delta-rs/issues/1456)
- Partition values that have been url encoded cannot be read when using deltalake [\#1446](https://github.com/delta-io/delta-rs/issues/1446)
- Error optimizing large table [\#1419](https://github.com/delta-io/delta-rs/issues/1419)
- Cannot read partitions with special characters \(including space\) with pyarrow \>= 11 [\#1393](https://github.com/delta-io/delta-rs/issues/1393)
- ImportError: deltalake/\_internal.abi3.so: cannot allocate memory in static TLS block [\#1380](https://github.com/delta-io/delta-rs/issues/1380)
- Invalid JSON in log record missing field `schemaString` for DLT tables [\#1302](https://github.com/delta-io/delta-rs/issues/1302)
- Special characters in partition path not handled locally  [\#1299](https://github.com/delta-io/delta-rs/issues/1299)

**Merged pull requests:**

- chore: bump rust crate version [\#1675](https://github.com/delta-io/delta-rs/pull/1675) ([rtyler](https://github.com/rtyler))
- fix: change partitioning schema from large to normal string for pyarrow\<12 [\#1671](https://github.com/delta-io/delta-rs/pull/1671) ([ion-elgreco](https://github.com/ion-elgreco))
- feat: allow to set large dtypes for the schema check in `write_deltalake` [\#1668](https://github.com/delta-io/delta-rs/pull/1668) ([ion-elgreco](https://github.com/ion-elgreco))
- docs: small consistency update in guide and readme [\#1666](https://github.com/delta-io/delta-rs/pull/1666) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: exception string in writer.py [\#1665](https://github.com/delta-io/delta-rs/pull/1665) ([sebdiem](https://github.com/sebdiem))
- chore: increment python library version [\#1664](https://github.com/delta-io/delta-rs/pull/1664) ([wjones127](https://github.com/wjones127))
- docs: fix some typos [\#1662](https://github.com/delta-io/delta-rs/pull/1662) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: more consistent handling of partition values and file paths [\#1661](https://github.com/delta-io/delta-rs/pull/1661) ([roeap](https://github.com/roeap))
- docs: add docstring to protocol method [\#1660](https://github.com/delta-io/delta-rs/pull/1660) ([MrPowers](https://github.com/MrPowers))
- docs: make docs.rs build docs with all features enabled [\#1658](https://github.com/delta-io/delta-rs/pull/1658) ([simonvandel](https://github.com/simonvandel))
- fix: enable offset listing for s3 [\#1654](https://github.com/delta-io/delta-rs/pull/1654) ([eeroel](https://github.com/eeroel))
- chore: fix the incorrect Slack link in our readme [\#1649](https://github.com/delta-io/delta-rs/pull/1649) ([rtyler](https://github.com/rtyler))
- fix: compensate for invalid log files created by Delta Live Tables [\#1647](https://github.com/delta-io/delta-rs/pull/1647) ([rtyler](https://github.com/rtyler))
- chore: proposed updated CODEOWNERS to allow better review notifications [\#1646](https://github.com/delta-io/delta-rs/pull/1646) ([rtyler](https://github.com/rtyler))
- feat: expose min\_commit\_interval to `optimize.compact` and `optimize.z_order` [\#1645](https://github.com/delta-io/delta-rs/pull/1645) ([ion-elgreco](https://github.com/ion-elgreco))
- fix: avoid excess listing of log files [\#1644](https://github.com/delta-io/delta-rs/pull/1644) ([eeroel](https://github.com/eeroel))
- fix: introduce support for Microsoft OneLake [\#1642](https://github.com/delta-io/delta-rs/pull/1642) ([rtyler](https://github.com/rtyler))
- fix: explicitly require chrono 0.4.31 or greater [\#1641](https://github.com/delta-io/delta-rs/pull/1641) ([rtyler](https://github.com/rtyler))
- fix: include in-progress row group when calculating in-memory buffer length [\#1638](https://github.com/delta-io/delta-rs/pull/1638) ([BnMcG](https://github.com/BnMcG))
- chore: relax chrono pin to 0.4 [\#1635](https://github.com/delta-io/delta-rs/pull/1635) ([houqp](https://github.com/houqp))
- chore: update datafusion to 31, arrow to 46 and object\_store to 0.7 [\#1634](https://github.com/delta-io/delta-rs/pull/1634) ([houqp](https://github.com/houqp))
- docs: update Readme [\#1633](https://github.com/delta-io/delta-rs/pull/1633) ([dennyglee](https://github.com/dennyglee))
- chore: pin the chrono dependency [\#1631](https://github.com/delta-io/delta-rs/pull/1631) ([rtyler](https://github.com/rtyler))
- feat: pass known file sizes to filesystem in Python [\#1630](https://github.com/delta-io/delta-rs/pull/1630) ([eeroel](https://github.com/eeroel))
- feat: implement parsing for the new `domainMetadata` actions in the commit log [\#1629](https://github.com/delta-io/delta-rs/pull/1629) ([rtyler](https://github.com/rtyler))
- ci: fix python release [\#1624](https://github.com/delta-io/delta-rs/pull/1624) ([wjones127](https://github.com/wjones127))
- ci: extend azure timeout [\#1622](https://github.com/delta-io/delta-rs/pull/1622) ([wjones127](https://github.com/wjones127))
- feat: allow multiple incremental commits in optimize [\#1621](https://github.com/delta-io/delta-rs/pull/1621) ([kvap](https://github.com/kvap))
- fix: change map nullable value to false [\#1620](https://github.com/delta-io/delta-rs/pull/1620) ([cmackenzie1](https://github.com/cmackenzie1))
- Introduce the changelog for the last couple releases [\#1617](https://github.com/delta-io/delta-rs/pull/1617) ([rtyler](https://github.com/rtyler))
- chore: bump python version to 0.10.2 [\#1616](https://github.com/delta-io/delta-rs/pull/1616) ([wjones127](https://github.com/wjones127))
- perf: avoid holding GIL in DeltaFileSystemHandler [\#1615](https://github.com/delta-io/delta-rs/pull/1615) ([wjones127](https://github.com/wjones127))
- fix: don't re-encode paths [\#1613](https://github.com/delta-io/delta-rs/pull/1613) ([wjones127](https://github.com/wjones127))
- feat: use url parsing from object store [\#1592](https://github.com/delta-io/delta-rs/pull/1592) ([roeap](https://github.com/roeap))
- feat: buffered reading of transaction logs [\#1549](https://github.com/delta-io/delta-rs/pull/1549) ([eeroel](https://github.com/eeroel))
- feat: merge operation [\#1522](https://github.com/delta-io/delta-rs/pull/1522) ([Blajda](https://github.com/Blajda))
- feat: expose create\_checkpoint\_for to the public [\#1514](https://github.com/delta-io/delta-rs/pull/1514) ([haruband](https://github.com/haruband))
- docs: update Readme [\#1440](https://github.com/delta-io/delta-rs/pull/1440) ([roeap](https://github.com/roeap))
- refactor: re-organize top level modules [\#1434](https://github.com/delta-io/delta-rs/pull/1434) ([roeap](https://github.com/roeap))
- feat: integrate unity catalog with datafusion [\#1338](https://github.com/delta-io/delta-rs/pull/1338) ([roeap](https://github.com/roeap))

## [rust-v0.15.0](https://github.com/delta-io/delta-rs/tree/rust-v0.15.0) (2023-09-06)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.14.0...rust-v0.15.0)

**Implemented enhancements:**

- Configurable number of retries for transaction commit loop [\#1595](https://github.com/delta-io/delta-rs/issues/1595)

**Fixed bugs:**

- Unable to read table using VM Managed Identity on Azure [\#1462](https://github.com/delta-io/delta-rs/issues/1462)
- Unable to query by partition column  [\#1445](https://github.com/delta-io/delta-rs/issues/1445)

**Merged pull requests:**

- fix: update python test [\#1608](https://github.com/delta-io/delta-rs/pull/1608) ([wjones127](https://github.com/wjones127))
- chore: update datafusion to 30, arrow to 45 [\#1606](https://github.com/delta-io/delta-rs/pull/1606) ([scsmithr](https://github.com/scsmithr))
- fix: just make pyarrow 12 the max [\#1603](https://github.com/delta-io/delta-rs/pull/1603) ([wjones127](https://github.com/wjones127))
- fix: support partial statistics in JSON [\#1599](https://github.com/delta-io/delta-rs/pull/1599) ([CurtHagenlocher](https://github.com/CurtHagenlocher))
- feat: allow configurable number of `commit` attempts [\#1596](https://github.com/delta-io/delta-rs/pull/1596) ([cmackenzie1](https://github.com/cmackenzie1))
- fix: querying on date partitions \(fixes \#1445\) [\#1594](https://github.com/delta-io/delta-rs/pull/1594) ([watfordkcf](https://github.com/watfordkcf))
- refactor: clean up arrow schema defs [\#1590](https://github.com/delta-io/delta-rs/pull/1590) ([polynomialherder](https://github.com/polynomialherder))
- feat: add metadata for operations::write::WriteBuilder [\#1584](https://github.com/delta-io/delta-rs/pull/1584) ([abhimanyusinghgaur](https://github.com/abhimanyusinghgaur))
- feat: add metadata for deletion vectors [\#1583](https://github.com/delta-io/delta-rs/pull/1583) ([aersam](https://github.com/aersam))
- fix: remove alpha classifier [\#1578](https://github.com/delta-io/delta-rs/pull/1578) ([marcelotrevisani](https://github.com/marcelotrevisani))
- refactor: use pa.table.cast in delta\_arrow\_schema\_from\_pandas [\#1573](https://github.com/delta-io/delta-rs/pull/1573) ([ion-elgreco](https://github.com/ion-elgreco))

## [rust-v0.14.0](https://github.com/delta-io/delta-rs/tree/rust-v0.14.0) (2023-08-01)

[Full Changelog](https://github.com/delta-io/delta-rs/compare/rust-v0.13.0...rust-v0.14.0)

**Implemented enhancements:**

- Define common dependencies in Cargo Workspace [\#1572](https://github.com/delta-io/delta-rs/issues/1572)
- Make `delta_datafusion::find_files` public [\#1559](https://github.com/delta-io/delta-rs/issues/1559)

**Fixed bugs:**

- Excessive integration test sizes causing builds to fail [\#1550](https://github.com/delta-io/delta-rs/issues/1550)
- Slack invite link is not working [\#1530](https://github.com/delta-io/delta-rs/issues/1530)

**Merged pull requests:**

- fix: correct whitespace in delta protocol reader minimum version error message [\#1576](https://github.com/delta-io/delta-rs/pull/1576) ([polynomialherder](https://github.com/polynomialherder))
- chore: move deps to `[workspace.dependencies]` [\#1575](https://github.com/delta-io/delta-rs/pull/1575) ([cmackenzie1](https://github.com/cmackenzie1))
- chore: update `datafusion` to `28` and arrow to `43` [\#1571](https://github.com/delta-io/delta-rs/pull/1571) ([cmackenzie1](https://github.com/cmackenzie1))
- ci: don't run benchmark in debug mode [\#1566](https://github.com/delta-io/delta-rs/pull/1566) ([wjones127](https://github.com/wjones127))
- ci: install newer rust for macos python release [\#1565](https://github.com/delta-io/delta-rs/pull/1565) ([wjones127](https://github.com/wjones127))
- feat: make find\_files public [\#1560](https://github.com/delta-io/delta-rs/pull/1560) ([yjshen](https://github.com/yjshen))
- feat!: bulk delete for vacuum [\#1556](https://github.com/delta-io/delta-rs/pull/1556) ([Blajda](https://github.com/Blajda))
- chore: address some integration test bloat of disk usage for development [\#1552](https://github.com/delta-io/delta-rs/pull/1552) ([rtyler](https://github.com/rtyler))
- docs: port docs to mkdocs [\#1548](https://github.com/delta-io/delta-rs/pull/1548) ([MrPowers](https://github.com/MrPowers))
- chore: disable incremental builds in CI for saving space [\#1545](https://github.com/delta-io/delta-rs/pull/1545) ([rtyler](https://github.com/rtyler))
- fix: revert premature merge of an attempted fix for binary column statistics [\#1544](https://github.com/delta-io/delta-rs/pull/1544) ([rtyler](https://github.com/rtyler))
- chore: increment python version [\#1542](https://github.com/delta-io/delta-rs/pull/1542) ([wjones127](https://github.com/wjones127))
- feat: add restore command in python binding [\#1529](https://github.com/delta-io/delta-rs/pull/1529) ([loleek](https://github.com/loleek))

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
- Improve error message when filtering on non-existent partition columns [\#1218](https://github.com/delta-io/delta-rs/issues/1218)

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
- Deltalake 0.7.0 with s3 feature compilation error due to rusoto_dynamodb version conflict [\#1191](https://github.com/delta-io/delta-rs/issues/1191)
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
