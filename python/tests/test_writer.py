import itertools
import json
import os
import pathlib
import random
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, Iterable, List
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyarrow._dataset_parquet import ParquetReadOptions
from pyarrow.dataset import ParquetFileFormat
from pyarrow.lib import RecordBatchReader

from deltalake import DeltaTable, write_deltalake
from deltalake.table import ProtocolVersions
from deltalake.writer import DeltaTableProtocolError

try:
    from pandas.testing import assert_frame_equal
except ModuleNotFoundError:
    _has_pandas = False
else:
    _has_pandas = True


def _is_old_glibc_version():
    if "CS_GNU_LIBC_VERSION" in os.confstr_names:
        version = os.confstr("CS_GNU_LIBC_VERSION").split(" ")[1]
        return version < "2.28"
    else:
        return False


if sys.platform == "win32":
    pytest.skip("Writer isn't yet supported on Windows", allow_module_level=True)

if _is_old_glibc_version():
    pytest.skip(
        "Writer isn't yet supported on Linux with glibc < 2.28", allow_module_level=True
    )


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
            "list": pa.array([list(range(x + 1)) for x in range(nrows)]),
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

    for add_path in get_add_paths(delta_table):
        # Paths should be relative, and with no partitioning have no directories
        assert "/" not in add_path


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_enforce_schema(existing_table: DeltaTable, mode: str):
    bad_data = pa.table({"x": pa.array([1, 2, 3])})

    with pytest.raises(ValueError):
        write_deltalake(existing_table, bad_data, mode=mode)

    table_uri = existing_table._table.table_uri()
    with pytest.raises(ValueError):
        write_deltalake(table_uri, bad_data, mode=mode)


def test_local_path(tmp_path: pathlib.Path, sample_data: pa.Table, monkeypatch):
    monkeypatch.chdir(tmp_path)  # Make tmp_path the working directory

    local_path = "./path/to/table"
    write_deltalake(local_path, sample_data)
    delta_table = DeltaTable(local_path)
    assert delta_table.pyarrow_schema() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    assert table == sample_data


def test_roundtrip_metadata(tmp_path: pathlib.Path, sample_data: pa.Table):
    write_deltalake(
        str(tmp_path),
        sample_data,
        name="test_name",
        description="test_desc",
        configuration={"configTest": "foobar"},
    )

    delta_table = DeltaTable(str(tmp_path))

    metadata = delta_table.metadata()

    assert metadata.name == "test_name"
    assert metadata.description == "test_desc"
    assert metadata.configuration == {"configTest": "foobar"}


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
        "date32",
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

    for add_path in get_add_paths(delta_table):
        # Paths should be relative
        assert add_path.count("/") == 1


def test_roundtrip_multi_partitioned(tmp_path: pathlib.Path, sample_data: pa.Table):
    write_deltalake(str(tmp_path), sample_data, partition_by=["int32", "bool"])

    delta_table = DeltaTable(str(tmp_path))
    assert delta_table.pyarrow_schema() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    table = table.take(pc.sort_indices(table["int64"]))
    assert table == sample_data

    for add_path in get_add_paths(delta_table):
        # Paths should be relative
        assert add_path.count("/") == 2


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


def test_append_only_should_append_only_with_the_overwrite_mode(
    tmp_path: pathlib.Path, sample_data: pa.Table
):
    path = str(tmp_path)

    config = {"delta.appendOnly": "true"}

    write_deltalake(path, sample_data, mode="append", configuration=config)

    table = DeltaTable(path)
    write_deltalake(table, sample_data, mode="append")

    data_store_types = [path, table]
    fail_modes = ["overwrite", "ignore", "error"]

    for data_store_type, mode in itertools.product(data_store_types, fail_modes):
        with pytest.raises(
            ValueError,
            match=f"If configuration has delta.appendOnly = 'true', mode must be 'append'. Mode is currently {mode}",
        ):
            write_deltalake(data_store_type, sample_data, mode=mode)

    expected = pa.concat_tables([sample_data, sample_data])
    assert table.to_pyarrow_table() == expected
    assert table.version() == 1


def test_writer_with_table(existing_table: DeltaTable, sample_data: pa.Table):
    write_deltalake(existing_table, sample_data, mode="overwrite")
    existing_table.update_incremental()
    assert existing_table.to_pyarrow_table() == sample_data


def test_fails_wrong_partitioning(existing_table: DeltaTable, sample_data: pa.Table):
    with pytest.raises(AssertionError):
        write_deltalake(
            existing_table, sample_data, mode="append", partition_by="int32"
        )


@pytest.mark.pandas
def test_write_pandas(tmp_path: pathlib.Path, sample_data: pa.Table):
    # When timestamp is converted to Pandas, it gets casted to ns resolution,
    # but Delta Lake schemas only support us resolution.
    sample_pandas = sample_data.to_pandas().drop(["timestamp"], axis=1)
    write_deltalake(str(tmp_path), sample_pandas)

    delta_table = DeltaTable(str(tmp_path))
    df = delta_table.to_pandas()
    assert_frame_equal(df, sample_pandas)


def test_write_iterator(
    tmp_path: pathlib.Path, existing_table: DeltaTable, sample_data: pa.Table
):
    batches = existing_table.to_pyarrow_dataset().to_batches()
    with pytest.raises(ValueError):
        write_deltalake(str(tmp_path), batches, mode="overwrite")

    write_deltalake(str(tmp_path), batches, schema=sample_data.schema, mode="overwrite")
    assert DeltaTable(str(tmp_path)).to_pyarrow_table() == sample_data


def test_write_recordbatchreader(
    tmp_path: pathlib.Path, existing_table: DeltaTable, sample_data: pa.Table
):
    batches = existing_table.to_pyarrow_dataset().to_batches()
    reader = RecordBatchReader.from_batches(sample_data.schema, batches)

    write_deltalake(str(tmp_path), reader, mode="overwrite")
    assert DeltaTable(str(tmp_path)).to_pyarrow_table() == sample_data


def test_writer_partitioning(tmp_path: pathlib.Path):
    test_strings = ["a=b", "hello world", "hello%20world"]
    data = pa.table(
        {"p": pa.array(test_strings), "x": pa.array(range(len(test_strings)))}
    )

    write_deltalake(str(tmp_path), data)

    assert DeltaTable(str(tmp_path)).to_pyarrow_table() == data


def get_log_path(table: DeltaTable) -> str:
    """
    Returns _delta_log path for this delta table.
    """
    return table._table.table_uri() + "/_delta_log/" + ("0" * 20 + ".json")


def get_stats(table: DeltaTable):
    log_path = get_log_path(table)

    # Should only have single add entry
    for line in open(log_path, "r").readlines():
        log_entry = json.loads(line)

        if "add" in log_entry:
            return json.loads(log_entry["add"]["stats"])
    else:
        raise AssertionError("No add action found!")


def get_add_paths(table: DeltaTable) -> List[str]:
    log_path = get_log_path(table)

    paths = []

    for line in open(log_path, "r").readlines():
        log_entry = json.loads(line)

        if "add" in log_entry:
            paths.append(log_entry["add"]["path"])

    return paths


def test_writer_stats(existing_table: DeltaTable, sample_data: pa.Table):
    stats = get_stats(existing_table)

    assert stats["numRecords"] == sample_data.num_rows

    assert all(null_count == 0 for null_count in stats["nullCount"].values())

    expected_mins = {
        "utf8": "0",
        "int64": 0,
        "int32": 0,
        "int16": 0,
        "int8": 0,
        "float32": 0.0,
        "float64": 0.0,
        "bool": False,
        "binary": "0",
        # TODO: Writer needs special decoding for decimal and date32.
        #'decimal': '10.000',
        # "date32": '2022-01-01',
        "timestamp": "2022-01-01T00:00:00",
        "struct.x": 0,
        "struct.y": "0",
        "list.list.item": 0,
    }
    assert stats["minValues"] == expected_mins

    expected_maxs = {
        "utf8": "4",
        "int64": 4,
        "int32": 4,
        "int16": 4,
        "int8": 4,
        "float32": 4.0,
        "float64": 4.0,
        "bool": True,
        "binary": "4",
        #'decimal': '40.000',
        # "date32": '2022-01-04',
        "timestamp": "2022-01-01T04:00:00",
        "struct.x": 4,
        "struct.y": "4",
        "list.list.item": 4,
    }
    assert stats["maxValues"] == expected_maxs


def test_writer_null_stats(tmp_path: pathlib.Path):
    data = pa.table(
        {
            "int32": pa.array([1, None, 2, None], pa.int32()),
            "float64": pa.array([1.0, None, None, None], pa.float64()),
            "str": pa.array([None] * 4, pa.string()),
        }
    )
    path = str(tmp_path)
    write_deltalake(path, data)

    table = DeltaTable(path)
    stats = get_stats(table)

    expected_nulls = {"int32": 2, "float64": 3, "str": 4}
    assert stats["nullCount"] == expected_nulls


def test_writer_fails_on_protocol(existing_table: DeltaTable, sample_data: pa.Table):
    existing_table.protocol = Mock(return_value=ProtocolVersions(1, 2))
    with pytest.raises(DeltaTableProtocolError):
        write_deltalake(existing_table, sample_data, mode="overwrite")


@pytest.mark.parametrize(
    "row_count,rows_per_file,expected_files",
    [
        (1000, 100, 10),  # even distribution
        (100, 1, 100),  # single row per file
        (1000, 3, 334),  # uneven distribution, num files = rows/rows_per_file + 1
    ],
)
def test_writer_with_max_rows(
    tmp_path: pathlib.Path, row_count: int, rows_per_file: int, expected_files: int
):
    def get_multifile_stats(table: DeltaTable) -> Iterable[Dict]:
        log_path = get_log_path(table)

        # Should only have single add entry
        for line in open(log_path, "r").readlines():
            log_entry = json.loads(line)

            if "add" in log_entry:
                yield json.loads(log_entry["add"]["stats"])

    data = pa.table(
        {
            "colA": pa.array(range(0, row_count), pa.int32()),
            "colB": pa.array(
                [i * random.random() for i in range(0, row_count)], pa.float64()
            ),
        }
    )
    path = str(tmp_path)
    write_deltalake(
        path,
        data,
        file_options=ParquetFileFormat().make_write_options(),
        max_rows_per_file=rows_per_file,
        max_rows_per_group=rows_per_file,
    )

    table = DeltaTable(path)
    stats = get_multifile_stats(table)
    files_written = [f for f in os.listdir(path) if f != "_delta_log"]

    assert sum([stat_entry["numRecords"] for stat_entry in stats]) == row_count
    assert len(files_written) == expected_files


def test_writer_with_options(tmp_path: pathlib.Path):
    column_values = [datetime(year_, 1, 1, 0, 0, 0) for year_ in range(9000, 9010)]
    data = pa.table({"colA": pa.array(column_values, pa.timestamp("us"))})
    path = str(tmp_path)
    opts = (
        ParquetFileFormat()
        .make_write_options()
        .update(compression="GZIP", coerce_timestamps="us")
    )
    write_deltalake(path, data, file_options=opts)

    table = (
        DeltaTable(path)
        .to_pyarrow_dataset(
            parquet_read_options=ParquetReadOptions(coerce_int96_timestamp_unit="us")
        )
        .to_table()
    )

    assert table == data
