import os
import pathlib

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

    file_paths = {f.split(os.path.sep)[-1] for f in original_files}

    tombstones = set(dt.vacuum(retention_hours=0, enforce_retention_duration=False))
    assert tombstones == file_paths

    tombstones = set(
        dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
    )
    assert tombstones == file_paths

    parquet_files = {f for f in os.listdir(table_path) if f.endswith("parquet")}
    assert parquet_files == {f.split(os.path.sep)[-1] for f in new_files}


def test_vacuum_transaction_log(tmp_path: pathlib.Path, sample_table: Table):
    for i in range(5):
        write_deltalake(tmp_path, sample_table, mode="overwrite")

    dt = DeltaTable(tmp_path)

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.vacuum(
        retention_hours=0,
        dry_run=False,
        enforce_retention_duration=False,
        commit_properties=commit_properties,
    )

    dt = DeltaTable(tmp_path)

    history = dt.history(2)

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

    assert history[0]["operationMetrics"]["numDeletedFiles"] == 4
    assert history[1]["operationMetrics"]["numFilesToDelete"] == 4
    assert history[1]["operationMetrics"]["sizeOfDataToDelete"] > 0


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

    expected_tombstones = {f.split(os.path.sep)[-1] for f in original_files}
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
        os.path.join(*f.split(os.path.sep)[-2:]) for f in original_files
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
