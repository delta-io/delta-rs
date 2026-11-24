import json
import os
import pathlib
import time
from datetime import datetime, timedelta
from urllib.parse import quote, unquote

import pytest
from arro3.core import Table

from deltalake import CommitProperties, DeltaTable, write_deltalake


def test_vacuum_dry_run_simple_table():
    table_path = "../crates/test/tests/data/delta-0.2.0"
    dt = DeltaTable(table_path)
    retention_periods = 169
    tombstones = dt.vacuum(retention_periods)
    tombstones.sort()
    assert tombstones == [
        "part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet",
        "part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet",
        "part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet",
        "part-00001-4327c977-2734-4477-9507-7ccf67924649-c000.snappy.parquet",
    ]

    retention_periods = -1
    with pytest.raises(Exception) as exception:
        dt.vacuum(retention_periods)
    assert str(exception.value) == "The retention periods should be positive."

    retention_periods = 167
    with pytest.raises(Exception) as exception:
        dt.vacuum(retention_periods)
    assert (
        str(exception.value)
        == "Generic error: Invalid retention period, minimum retention for vacuum is"
        " configured to be greater than 168 hours, got 167 hours"
    )


@pytest.mark.parametrize("use_relative", [True, False])
def test_vacuum_zero_duration(
    tmp_path: pathlib.Path,
    sample_table: Table,
    monkeypatch,
    use_relative: bool,
):
    if use_relative:
        monkeypatch.chdir(tmp_path)  # Make tmp_path the working directory
        (tmp_path / "path/to/table").mkdir(parents=True)
        table_path = "./path/to/table"
    else:
        table_path = str(tmp_path)

    write_deltalake(table_path, sample_table, mode="overwrite")
    dt = DeltaTable(table_path)
    original_files = set(dt.file_uris())
    write_deltalake(table_path, sample_table, mode="overwrite")
    dt.update_incremental()
    new_files = set(dt.file_uris())
    assert new_files.isdisjoint(original_files)

    file_paths = {pathlib.Path(f).name for f in original_files}

    tombstones = set(dt.vacuum(retention_hours=0, enforce_retention_duration=False))
    assert tombstones == file_paths

    tombstones = set(
        dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
    )
    assert tombstones == file_paths

    parquet_files = {f for f in os.listdir(table_path) if f.endswith("parquet")}
    assert parquet_files == {pathlib.Path(f).name for f in new_files}


@pytest.mark.parametrize("predelete", [False, True], ids=["present", "one-absent"])
def test_vacuum_transaction_log(
    tmp_path: pathlib.Path, sample_table: Table, predelete: bool
):
    for i in range(5):
        write_deltalake(tmp_path, sample_table, mode="overwrite")

    dt = DeltaTable(tmp_path)
    active = set(map(pathlib.Path, dt.file_uris()))
    stale = set(tmp_path.glob("*.parquet")) - active
    if predelete:
        stale.pop().unlink()

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    deleted = dt.vacuum(
        retention_hours=0,
        dry_run=False,
        enforce_retention_duration=False,
        commit_properties=commit_properties,
    )

    dt = DeltaTable(tmp_path)

    history = list(dt.history(2))

    expected_start_parameters = {
        "retentionCheckEnabled": "false",
        "specifiedRetentionMillis": "0",
        "defaultRetentionMillis": "604800000",
    }

    assert history[0]["operation"] == "VACUUM END"
    assert history[1]["operation"] == "VACUUM START"

    assert history[0]["userName"] == "John Doe"
    assert history[1]["userName"] == "John Doe"

    assert history[0]["operationParameters"]["status"] == "COMPLETED"
    assert history[1]["operationParameters"] == expected_start_parameters

    assert set(deleted) == {path.name for path in stale}
    assert set(tmp_path.glob("*.parquet")) == active
    assert history[0]["operationMetrics"]["numDeletedFiles"] == len(stale)
    assert history[0]["operationMetrics"]["numAlreadyAbsentFiles"] == int(predelete)
    assert history[1]["operationMetrics"]["numFilesToDelete"] == 4
    assert history[1]["operationMetrics"]["sizeOfDataToDelete"] > 0


@pytest.mark.pyarrow
@pytest.mark.parametrize("full", [False, True], ids=["lite", "full"])
@pytest.mark.parametrize(
    "value",
    [
        "plain",
        "a b",
        "a:b",
        "a/b",
        "a%b",
        "a~b",
        "München",
        "日本語",
        "2024-11-03 00:00:00.000000",
        datetime(2024, 11, 3),
    ],
)
def test_vacuum_partition_path_lifecycle(tmp_path: pathlib.Path, value, full: bool):
    import pyarrow as pa
    import pyarrow.fs as fs

    from deltalake.fs import DeltaStorageHandler

    def commit_actions(version, kind):
        commit = tmp_path / "_delta_log" / f"{version:020d}.json"
        actions = map(json.loads, commit.read_text().splitlines())
        return [action[kind] for action in actions if kind in action]

    data = pa.table({"id": [1, 2], "part": [value, value]})
    write_deltalake(tmp_path, data, partition_by=["part"])
    (physical,) = tmp_path.glob("part=*/*.parquet")
    old_key = physical.relative_to(tmp_path).as_posix()
    (add,) = commit_actions(0, "add")
    assert unquote(add["path"]) == old_key
    partition = add["partitionValues"]["part"]
    if isinstance(value, str):
        assert partition == value
    else:
        assert datetime.fromisoformat(partition) == value
    assert physical.parent.name == f"part={quote(partition, safe='')}"
    store = fs.PyFileSystem(DeltaStorageHandler(str(tmp_path)))
    listed = store.get_file_info(fs.FileSelector("", recursive=True))
    assert {entry.path for entry in listed if entry.path.endswith(".parquet")} == {
        old_key
    }
    dt = DeltaTable(tmp_path)
    assert [pathlib.Path(uri) for uri in dt.file_uris()] == [physical]
    assert dt.get_add_actions().column("path").to_pylist() == [add["path"]]
    assert dt.to_pyarrow_table(engine="datafusion")["part"].to_pylist() == [
        value,
        value,
    ]

    write_deltalake(tmp_path, data, partition_by=["part"], mode="overwrite")
    (remove,) = commit_actions(1, "remove")
    assert unquote(remove["path"]) == old_key
    dt.update_incremental()
    (active,) = map(pathlib.Path, dt.file_uris())
    assert active != physical
    orphan = physical.with_name("orphan.parquet")
    orphan.write_bytes(physical.read_bytes())
    os.utime(orphan, (0, 0))
    expected = {old_key}
    if full:
        expected.add(orphan.relative_to(tmp_path).as_posix())

    # Vacuum expires tombstones strictly older than its millisecond cutoff.
    eligible_at = (remove["deletionTimestamp"] + 1) / 1000
    time.sleep(max(0, eligible_at - time.time()))
    options = dict(retention_hours=0, enforce_retention_duration=False, full=full)
    assert set(dt.vacuum(dry_run=True, **options)) == expected
    assert set(dt.vacuum(dry_run=False, **options)) == expected
    assert active.is_file()
    assert not physical.exists()
    assert orphan.is_file() == (not full)
    assert next(dt.history(1))["operationMetrics"]["numDeletedFiles"] == len(expected)

    # The same tombstone is still eligible on the second run.
    assert dt.vacuum(dry_run=False, **options) == []
    metrics = next(dt.history(1))["operationMetrics"]
    assert metrics["numDeletedFiles"] == 0
    assert metrics["numAlreadyAbsentFiles"] == 1
    assert active.is_file()


def test_vacuum_propagates_delete_failure(tmp_path: pathlib.Path, sample_table: Table):
    write_deltalake(tmp_path, sample_table)
    (victim,) = tmp_path.glob("*.parquet")
    write_deltalake(tmp_path, sample_table, mode="overwrite")
    victim.unlink()
    victim.mkdir()  # A non-file at the tombstoned key makes local file deletion fail.
    dt = DeltaTable(tmp_path)
    with pytest.raises(OSError):
        dt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
    reopened = DeltaTable(tmp_path)
    assert all(pathlib.Path(uri).is_file() for uri in reopened.file_uris())
    assert not any(
        info["operation"] == "VACUUM END"
        and info["operationParameters"]["status"] == "COMPLETED"
        for info in reopened.history()
    )


def test_vacuum_keep_versions():
    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)
    keep_versions = [2, 3]
    tombstones_no_kept_versions = dt.vacuum(
        retention_hours=0,
        dry_run=True,
        enforce_retention_duration=False,
        full=True,
        keep_versions=None,
    )

    # Our simple_table has 32 data files in it which could be vacuumed.
    assert len(tombstones_no_kept_versions) == 32

    tombstones_kept_versions = dt.vacuum(
        retention_hours=0,
        dry_run=True,
        enforce_retention_duration=False,
        full=True,
        keep_versions=keep_versions,
    )

    # with_keep_versions should have fewer files deleted than a full vacuum
    assert len(tombstones_kept_versions) < len(tombstones_no_kept_versions)

    no_kept_versions = set(tombstones_no_kept_versions)
    kept_versions = set(tombstones_kept_versions)

    tombstone_difference = no_kept_versions.difference(kept_versions)

    assert tombstone_difference == {
        # Adds from v3
        "part-00000-f17fcbf5-e0dc-40ba-adae-ce66d1fcaef6-c000.snappy.parquet",
        "part-00001-bb70d2ba-c196-4df2-9c85-f34969ad3aa9-c000.snappy.parquet",
        # Removes from v3, these were add in v2
        "part-00003-53f42606-6cda-4f13-8d07-599a21197296-c000.snappy.parquet",
        "part-00006-46f2ff20-eb5d-4dda-8498-7bfb2940713b-c000.snappy.parquet",
    }


def test_vacuum_keep_versions_from_history_order():
    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)
    keep_versions = [commit["version"] for commit in dt.history()]

    tombstones_sorted = set(
        dt.vacuum(
            retention_hours=0,
            dry_run=True,
            enforce_retention_duration=False,
            full=True,
            keep_versions=sorted(keep_versions),
        )
    )

    tombstones_from_history = set(
        dt.vacuum(
            retention_hours=0,
            dry_run=True,
            enforce_retention_duration=False,
            full=True,
            keep_versions=keep_versions,
        )
    )

    assert tombstones_from_history == tombstones_sorted


def test_vacuum_lite_mode_no_list_operation(
    tmp_path: pathlib.Path, sample_table: Table
):
    """Test that vacuum in lite mode (default) only removes files referenced as
    expired tombstones in the transaction log and does NOT perform a storage list
    operation.  A stray parquet file that exists on disk but is never referenced by
    the delta log is used as a sentinel: Lite mode must not return it, while Full
    mode must find it via the list operation."""

    # Write initial data then overwrite so the first batch of files becomes
    # expired tombstones in the transaction log.
    write_deltalake(tmp_path, sample_table, mode="overwrite")
    dt = DeltaTable(tmp_path)
    original_files = set(dt.file_uris())

    write_deltalake(tmp_path, sample_table, mode="overwrite")
    dt.update_incremental()
    new_files = set(dt.file_uris())
    assert new_files.isdisjoint(original_files)

    # Plant a stray file in the table directory that is NOT referenced anywhere
    # in the transaction log.  Only a storage list operation would discover it.
    stray_filename = "stray_file_not_in_log.parquet"
    (tmp_path / stray_filename).write_bytes(b"not a real parquet file")

    # Lite mode: Should return only the expired tombstones derived from the log.
    lite_tombstones = set(
        dt.vacuum(
            retention_hours=0,
            dry_run=True,
            enforce_retention_duration=False,
            full=False,
        )
    )

    expected_tombstones = {pathlib.Path(f).name for f in original_files}
    assert lite_tombstones == expected_tombstones, (
        "Lite mode should return exactly the log-referenced expired tombstones"
    )

    # The stray file must NOT appear: if it did, a list operation was performed.
    assert stray_filename not in lite_tombstones, (
        "Lite mode must not discover files via a storage list operation"
    )

    # Full mode performs a list and must include the stray file.
    full_tombstones = set(
        dt.vacuum(
            retention_hours=0,
            dry_run=True,
            enforce_retention_duration=False,
            full=True,
        )
    )

    assert stray_filename in full_tombstones, (
        "Full mode should discover the stray file via a storage list operation"
    )


def test_vacuum_lite_mode_no_list_operation_partitioned(
    tmp_path: pathlib.Path, sample_table: Table
):
    """Same as test_vacuum_lite_mode_no_list_operation but for a partitioned table.
    Verifies that lite mode does not perform a storage list operation even when
    the table is partitioned  a stray parquet file planted inside a partition
    directory must not be returned by lite mode but must be found by full mode."""

    # Write partitioned data, then overwrite to create expired tombstones.
    write_deltalake(tmp_path, sample_table, partition_by=["deleted"], mode="overwrite")
    dt = DeltaTable(tmp_path)
    original_files = set(dt.file_uris())

    write_deltalake(tmp_path, sample_table, partition_by=["deleted"], mode="overwrite")
    dt.update_incremental()
    new_files = set(dt.file_uris())
    assert new_files.isdisjoint(original_files)

    # Plant a stray parquet file inside one of the existing partition directories.
    # Because it is never mentioned in the transaction log, only a storage list
    # operation would discover it.
    partition_dir = tmp_path / "deleted=false"
    partition_dir.mkdir(parents=True, exist_ok=True)
    stray_filename = "stray_file_not_in_log.parquet"
    (partition_dir / stray_filename).write_bytes(b"not a real parquet file")
    stray_relative = f"deleted=false/{stray_filename}"

    # Lite mode: Should return only the expired tombstones derived from the log.
    lite_tombstones = set(
        dt.vacuum(
            retention_hours=0,
            dry_run=True,
            enforce_retention_duration=False,
            full=False,
        )
    )

    expected_tombstones = {
        pathlib.Path(f).relative_to(tmp_path).as_posix() for f in original_files
    }
    assert lite_tombstones == expected_tombstones, (
        "Lite mode should return exactly the log-referenced expired tombstones"
    )

    # The stray file must NOT appear: if it did, a list operation was performed.
    assert stray_relative not in lite_tombstones, (
        "Lite mode must not discover files via a storage list operation"
    )

    # Full mode performs a list and must include the stray file.
    full_tombstones = set(
        dt.vacuum(
            retention_hours=0,
            dry_run=True,
            enforce_retention_duration=False,
            full=True,
        )
    )

    assert stray_relative in full_tombstones, (
        "Full mode should discover the stray file via a storage list operation"
    )


@pytest.mark.pyarrow
def test_vacuum_full_keeps_recent_tombstones_readable_for_time_travel(
    tmp_path: pathlib.Path, sample_table: Table
):
    write_deltalake(tmp_path, sample_table, partition_by=["deleted"], mode="append")
    dt = DeltaTable(tmp_path)
    original_file_uris = dt.file_uris()
    assert original_file_uris

    old_ts = (datetime.now() - timedelta(days=10)).timestamp()
    backdated_path = pathlib.Path(original_file_uris[0])
    assert backdated_path.is_absolute()
    os.utime(backdated_path, (old_ts, old_ts))

    write_deltalake(tmp_path, sample_table, partition_by=["deleted"], mode="append")
    write_deltalake(tmp_path, sample_table, partition_by=["deleted"], mode="append")

    dt = DeltaTable(tmp_path)
    version_before_optimize = dt.version()

    optimize_metrics = dt.optimize.compact(min_commit_interval=timedelta(minutes=10))
    assert optimize_metrics["numFilesRemoved"] > 0
    assert dt.version() > version_before_optimize
    dt.vacuum(dry_run=False, full=True)

    time_travel = DeltaTable(tmp_path, version=version_before_optimize)
    assert time_travel.to_pyarrow_table().num_rows == sample_table.num_rows * 3


# https://github.com/delta-io/delta-rs/issues/3745
@pytest.mark.pyarrow
def test_issue_3745(tmp_path: pathlib.Path):
    import pyarrow as pa

    data = pa.Table.from_pydict(
        {
            "x": pa.array(list(range(100)), type=pa.int32()),
        }
    )
    write_deltalake(table_or_uri=tmp_path, data=data, mode="append")

    table = DeltaTable(tmp_path)
    table.create_checkpoint()

    write_deltalake(table_or_uri=tmp_path, data=data, mode="append")

    table = DeltaTable(tmp_path)
    table.vacuum()


def test_positional_commit_args_raise_deprecation_warning(
    tmp_path: pathlib.Path, sample_table: Table
):
    write_deltalake(tmp_path, sample_table)
    dt = DeltaTable(tmp_path)

    commit = CommitProperties(custom_metadata={"userName": "John Doe"})
    with pytest.warns(DeprecationWarning, match="positionally"):
        dt.vacuum(None, True, True, None, commit, True, [0])


def test_vacuum_partial_legacy_positional_args_preserve_defaults():
    captured: dict[str, tuple] = {}

    class StubTable:
        def vacuum(self, *args):
            captured["args"] = args
            return []

    dt = object.__new__(DeltaTable)
    dt._table = StubTable()

    commit = CommitProperties(custom_metadata={"userName": "John Doe"})
    with pytest.warns(DeprecationWarning, match="positionally"):
        dt.vacuum(None, True, True, None, commit)

    assert captured["args"] == (True, None, True, commit, None, False, None)


def test_vacuum_positional_and_keyword_commit_conflict_raises():
    dt = object.__new__(DeltaTable)
    commit = CommitProperties(custom_metadata={"userName": "John Doe"})
    with pytest.raises(TypeError, match="multiple values for 'commit_properties'"):
        dt.vacuum(None, True, True, None, commit, commit_properties=commit)


def test_vacuum_positional_and_keyword_full_conflict_raises():
    class StubTable:
        def vacuum(self, *args):
            return []

    dt = object.__new__(DeltaTable)
    dt._table = StubTable()
    commit = CommitProperties(custom_metadata={"userName": "John Doe"})
    with pytest.raises(TypeError, match="multiple values for 'full'"):
        dt.vacuum(None, True, True, None, commit, False, full=True)


def test_vacuum_positional_and_keyword_keep_versions_conflict_raises():
    class StubTable:
        def vacuum(self, *args):
            return []

    dt = object.__new__(DeltaTable)
    dt._table = StubTable()
    commit = CommitProperties(custom_metadata={"userName": "John Doe"})
    with pytest.raises(TypeError, match="multiple values for 'keep_versions'"):
        dt.vacuum(None, True, True, None, commit, False, [1], keep_versions=[2])


@pytest.mark.polars
def test_vacuum_null_partition(tmp_path: pathlib.Path):
    """
    <https://github.com/delta-io/delta-rs/issues/3896>
    """
    import datetime

    import polars as pl

    # Schema with a nullable partition column
    schema = {
        "date": pl.Date,
        "player_id": pl.Int64,
        "year": pl.Int64,
    }

    # Data to append, including a null in the partition column
    data = pl.DataFrame(
        {
            "date": [datetime.date(2023, 1, 3), datetime.date(2023, 1, 4)],
            "player_id": [3, 4],
            "year": [2023, None],  # Null partition
        },
        schema=schema,
    )

    # Append new data
    print("Appending data with null partition...")
    write_deltalake(
        tmp_path,
        data,
        partition_by=["year"],
        mode="append",
    )
    dt = DeltaTable(tmp_path)
    files_before_optimize = dt.file_uris()

    # # Optimize the table to make old files obsolete
    dt.optimize.z_order(["date", "player_id"])
    files_after_optimize = dt.file_uris()
    assert files_before_optimize != files_after_optimize

    # Vacuum the table
    dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
