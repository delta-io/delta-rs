import datetime
import os
import pathlib
import time

import pytest
from arro3.core import Table

from deltalake import (
    CommitProperties,
    DeltaTable,
    PostCommitHookProperties,
    write_deltalake,
)


@pytest.mark.parametrize("use_relative", [True, False])
def test_restore_with_version(
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

    write_deltalake(table_path, sample_table, mode="append")
    write_deltalake(table_path, sample_table, mode="append")
    write_deltalake(table_path, sample_table, mode="append")

    dt = DeltaTable(table_path)
    old_version = dt.version()
    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.restore(1, commit_properties=commit_properties)
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "RESTORE"
    assert last_action["userName"] == "John Doe"
    assert dt.version() == old_version + 1


@pytest.mark.parametrize("use_relative", [True, False])
def test_restore_with_datetime_str(
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

    write_deltalake(table_path, sample_table, mode="append")
    write_deltalake(table_path, sample_table, mode="append")
    write_deltalake(table_path, sample_table, mode="append")

    dt = DeltaTable(table_path)
    old_version = dt.version()
    dt.restore("2020-05-01T00:47:31-07:00")
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "RESTORE"
    assert dt.version() == old_version + 1


@pytest.mark.parametrize("use_relative", [True, False])
def test_restore_with_datetime(
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

    write_deltalake(table_path, sample_table, mode="append")
    write_deltalake(table_path, sample_table, mode="append")
    write_deltalake(table_path, sample_table, mode="append")

    dt = DeltaTable(table_path)
    old_version = dt.version()
    date = datetime.datetime.strptime(
        "2023-04-26T21:23:32+08:00", "%Y-%m-%dT%H:%M:%S%z"
    )
    dt.restore(date)
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "RESTORE"
    assert dt.version() == old_version + 1


def test_restore_with_post_commithook_properties(
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    write_deltalake(str(tmp_path), sample_table, mode="append")
    write_deltalake(str(tmp_path), sample_table, mode="append")
    write_deltalake(str(tmp_path), sample_table, mode="append")

    dt = DeltaTable(str(tmp_path))
    old_version = dt.version()
    dt.restore(
        1,
        post_commithook_properties=PostCommitHookProperties(
            create_checkpoint=False,
            cleanup_expired_logs=False,
        ),
    )
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "RESTORE"
    assert dt.version() == old_version + 1

    log_dir = tmp_path / "_delta_log"
    checkpoint_files = list(log_dir.glob("*.checkpoint.parquet"))
    assert len(checkpoint_files) == 0


@pytest.mark.parametrize("tz", ["UTC", "Asia/Seoul", "America/Los_Angeles"])
def test_restore_with_naive_datetime(
    tmp_path: pathlib.Path,
    sample_table: Table,
    monkeypatch: pytest.MonkeyPatch,
    tz: str,
):
    if not hasattr(time, "tzset"):
        pytest.skip("time.tzset is not available on this platform")

    for _ in range(3):
        write_deltalake(str(tmp_path), sample_table, mode="append")

    log_path = tmp_path / "_delta_log"
    log_mtime_pairs = [
        (
            "00000000000000000000.json",
            datetime.datetime(2020, 1, 1, 0, tzinfo=datetime.timezone.utc),
        ),
        (
            "00000000000000000001.json",
            datetime.datetime(2020, 1, 1, 8, tzinfo=datetime.timezone.utc),
        ),
        (
            "00000000000000000002.json",
            datetime.datetime(2020, 1, 1, 18, tzinfo=datetime.timezone.utc),
        ),
    ]
    for file_name, moment in log_mtime_pairs:
        ts = moment.timestamp()
        os.utime(log_path / file_name, (ts, ts))

    monkeypatch.setenv("TZ", tz)
    time.tzset()

    dt = DeltaTable(str(tmp_path))
    old_version = dt.version()
    dt.restore(datetime.datetime(2020, 1, 1, 12))

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "RESTORE"
    assert dt.version() == old_version + 1
    # 12:00 read as UTC lands on version 1, which holds two appends.
    assert len(dt.file_uris()) == 2
