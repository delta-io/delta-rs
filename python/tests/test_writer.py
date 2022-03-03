import os
import pathlib
from datetime import date, datetime, timedelta
from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from deltalake import DeltaTable, write_deltalake


@pytest.fixture()
def sample_data():
    nrows = 5
    return pa.table(
        {
            "utf8": pa.array([str(x) for x in range(nrows)]),
            "int64": pa.array(list(range(nrows)), pa.int64()),
            "int32": pa.array(list(range(nrows)), pa.int32()),
            "int16": pa.array(list(range(nrows)), pa.int16()),
            "int8": pa.array(list(range(nrows)), pa.int8()),
            "float32": pa.array([float(x) for x in range(nrows)], pa.float32()),
            "float64": pa.array([float(x) for x in range(nrows)], pa.float64()),
            "bool": pa.array([x % 2 == 0 for x in range(nrows)]),
            "binary": pa.array([str(x).encode() for x in range(nrows)]),
            "decimal": pa.array([Decimal("10.000") + x for x in range(nrows)]),
            "date32": pa.array(
                [date(2022, 1, 1) + timedelta(days=x) for x in range(nrows)]
            ),
            "timestamp": pa.array(
                [datetime(2022, 1, 1) + timedelta(hours=x) for x in range(nrows)]
            ),
            "struct": pa.array([{"x": x, "y": str(x)} for x in range(nrows)]),
            "list": pa.array([list(range(x)) for x in range(nrows)]),
            # NOTE: https://github.com/apache/arrow-rs/issues/477
            #'map': pa.array([[(str(y), y) for y in range(x)] for x in range(nrows)], pa.map_(pa.string(), pa.int64())),
        }
    )


@pytest.fixture()
def existing_table(tmp_path: pathlib.Path, sample_data: pa.Table):
    path = str(tmp_path)
    write_deltalake(path, sample_data)
    return DeltaTable(path)


@pytest.mark.skip(reason="Waiting on #570")
def test_handle_existing(tmp_path: pathlib.Path, sample_data: pa.Table):
    # if uri points to a non-empty directory that isn't a delta table, error
    tmp_path
    p = tmp_path / "hello.txt"
    p.write_text("hello")

    with pytest.raises(OSError) as exception:
        write_deltalake(str(tmp_path), sample_data, mode="overwrite")

    assert "directory is not empty" in str(exception)


def test_roundtrip_basic(tmp_path: pathlib.Path, sample_data: pa.Table):
    write_deltalake(str(tmp_path), sample_data)

    assert ("0" * 20 + ".json") in os.listdir(tmp_path / "_delta_log")

    delta_table = DeltaTable(str(tmp_path))
    assert delta_table.pyarrow_schema() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    assert table == sample_data


@pytest.mark.parametrize(
    "column",
    [
        "utf8",
        "int64",
        "int32",
        "int16",
        "int8",
        "float32",
        "float64",
        "bool",
        "binary",
        # TODO: Add decimal and date32 once #565 is merged
        # "decimal",
        # "date32",
    ],
)
def test_roundtrip_partitioned(
    tmp_path: pathlib.Path, sample_data: pa.Table, column: str
):
    write_deltalake(str(tmp_path), sample_data, partition_by=[column])

    delta_table = DeltaTable(str(tmp_path))
    assert delta_table.pyarrow_schema() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    table = table.take(pc.sort_indices(table["int64"]))
    assert table == sample_data


def test_roundtrip_multi_partitioned(tmp_path: pathlib.Path, sample_data: pa.Table):
    write_deltalake(str(tmp_path), sample_data, partition_by=["int32", "bool"])

    delta_table = DeltaTable(str(tmp_path))
    assert delta_table.pyarrow_schema() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    table = table.take(pc.sort_indices(table["int64"]))
    assert table == sample_data


def test_write_modes(tmp_path: pathlib.Path, sample_data: pa.Table):
    path = str(tmp_path)

    write_deltalake(path, sample_data)
    assert DeltaTable(path).to_pyarrow_table() == sample_data

    with pytest.raises(AssertionError):
        write_deltalake(path, sample_data, mode="error")

    write_deltalake(path, sample_data, mode="ignore")
    assert ("0" * 19 + "1.json") not in os.listdir(tmp_path / "_delta_log")

    write_deltalake(path, sample_data, mode="append")
    expected = pa.concat_tables([sample_data, sample_data])
    assert DeltaTable(path).to_pyarrow_table() == expected

    write_deltalake(path, sample_data, mode="overwrite")
    assert DeltaTable(path).to_pyarrow_table() == sample_data


def test_writer_with_table(existing_table: DeltaTable, sample_data: pa.Table):
    write_deltalake(existing_table, sample_data, mode="overwrite")
    existing_table.update_incremental()
    assert existing_table.to_pyarrow_table() == sample_data


def test_fails_wrong_partitioning(existing_table: DeltaTable, sample_data: pa.Table):
    with pytest.raises(AssertionError):
        write_deltalake(existing_table, sample_data, mode="append", partition_by="int32")


def test_write_iterator(tmp_path: pathlib.Path, existing_table: DeltaTable, sample_data: pa.Table):
    batches = existing_table.to_pyarrow_dataset().to_batches()
    write_deltalake(str(tmp_path), batches)
    assert DeltaTable(str(tmp_path)).to_pyarrow_table() == sample_data
