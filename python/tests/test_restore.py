import pathlib

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake


@pytest.mark.parametrize("use_relative", [True, False])
def test_restore_with_version(
    tmp_path: pathlib.Path, sample_data: pa.Table, monkeypatch, use_relative: bool
):
    if use_relative:
        monkeypatch.chdir(tmp_path)  # Make tmp_path the working directory
        (tmp_path / "path/to/table").mkdir(parents=True)
        table_path = "./path/to/table"
    else:
        table_path = str(tmp_path)

    write_deltalake(table_path, sample_data, mode="append")
    write_deltalake(table_path, sample_data, mode="append")
    write_deltalake(table_path, sample_data, mode="append")

    dt = DeltaTable(table_path)
    old_version = dt.version()
    dt.restore(version_to_restore=1)
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "RESTORE"
    assert dt.version() == old_version + 1


@pytest.mark.parametrize("use_relative", [True, False])
def test_restore_with_datetime(
    tmp_path: pathlib.Path, sample_data: pa.Table, monkeypatch, use_relative: bool
):
    if use_relative:
        monkeypatch.chdir(tmp_path)  # Make tmp_path the working directory
        (tmp_path / "path/to/table").mkdir(parents=True)
        table_path = "./path/to/table"
    else:
        table_path = str(tmp_path)

    write_deltalake(table_path, sample_data, mode="append")
    write_deltalake(table_path, sample_data, mode="append")
    write_deltalake(table_path, sample_data, mode="append")

    dt = DeltaTable(table_path)
    old_version = dt.version()
    dt.restore(datetime_to_restore="2020-05-01T00:47:31-07:00")
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "RESTORE"
    assert dt.version() == old_version + 1
