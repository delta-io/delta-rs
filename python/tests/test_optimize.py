import pathlib
from datetime import timedelta

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
@pytest.mark.parametrize("use_relative", [True, False])
@pytest.mark.parametrize("large_dtypes", [True, False])
def test_optimize_run_table(
    tmp_path: pathlib.Path,
    sample_data: pa.Table,
    monkeypatch,
    use_relative: bool,
    large_dtypes: bool,
    engine,
):
    if use_relative:
        monkeypatch.chdir(tmp_path)  # Make tmp_path the working directory
        (tmp_path / "path/to/table").mkdir(parents=True)
        table_path = "./path/to/table"
    else:
        table_path = str(tmp_path)

    write_deltalake(
        table_path, sample_data, mode="append", engine=engine, large_dtypes=large_dtypes
    )
    write_deltalake(
        table_path, sample_data, mode="append", engine=engine, large_dtypes=large_dtypes
    )
    write_deltalake(
        table_path, sample_data, mode="append", engine=engine, large_dtypes=large_dtypes
    )

    dt = DeltaTable(table_path)
    old_data = dt.to_pyarrow_table()
    old_version = dt.version()

    dt.optimize.compact()

    new_data = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    assert dt.version() == old_version + 1
    assert old_data == new_data


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
# @pytest.mark.parametrize("large_dtypes", [True, False])
def test_z_order_optimize(
    tmp_path: pathlib.Path,
    sample_data: pa.Table,
    # large_dtypes: bool,
    engine,
):
    write_deltalake(
        tmp_path, sample_data, mode="append", large_dtypes=False, engine=engine
    )
    write_deltalake(
        tmp_path, sample_data, mode="append", large_dtypes=False, engine=engine
    )
    write_deltalake(
        tmp_path, sample_data, mode="append", large_dtypes=False, engine=engine
    )

    dt = DeltaTable(tmp_path)
    old_version = dt.version()

    dt.optimize.z_order(["date32", "timestamp"])
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    assert dt.version() == old_version + 1
    assert len(dt.file_uris()) == 1


def test_optimize_min_commit_interval(
    tmp_path: pathlib.Path,
    sample_data: pa.Table,
):
    write_deltalake(tmp_path, sample_data, partition_by="utf8", mode="append")
    write_deltalake(tmp_path, sample_data, partition_by="utf8", mode="append")
    write_deltalake(tmp_path, sample_data, partition_by="utf8", mode="append")

    dt = DeltaTable(tmp_path)
    old_version = dt.version()

    dt.optimize.z_order(["date32", "timestamp"], min_commit_interval=timedelta(0))

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    # The table has 5 distinct partitions, each of which are Z-ordered
    # independently. So with min_commit_interval=0, each will get its
    # own commit.
    assert dt.version() == old_version + 5
