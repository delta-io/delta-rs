import itertools
import json
import os
import pathlib
import random
import threading
from datetime import date, datetime
from decimal import Decimal
from math import inf
from typing import Any, Dict, Iterable, List, Literal
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyarrow.dataset import ParquetFileFormat, ParquetReadOptions, dataset
from pyarrow.lib import RecordBatchReader

from deltalake import DeltaTable, Schema, write_deltalake
from deltalake.exceptions import (
    CommitFailedError,
    DeltaError,
    DeltaProtocolError,
    SchemaMismatchError,
)
from deltalake.table import ProtocolVersions
from deltalake.writer import try_get_table_and_table_uri

try:
    from pandas.testing import assert_frame_equal
except ModuleNotFoundError:
    _has_pandas = False
else:
    _has_pandas = True


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
@pytest.mark.skip(reason="Waiting on #570")
def test_handle_existing(
    tmp_path: pathlib.Path, sample_data: pa.Table, engine: Literal["pyarrow", "rust"]
):
    # if uri points to a non-empty directory that isn't a delta table, error
    tmp_path
    p = tmp_path / "hello.txt"
    p.write_text("hello")

    with pytest.raises(OSError) as exception:
        write_deltalake(tmp_path, sample_data, mode="overwrite", engine=engine)

    assert "directory is not empty" in str(exception)


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_roundtrip_basic(
    tmp_path: pathlib.Path, sample_data: pa.Table, engine: Literal["pyarrow", "rust"]
):
    # Check we can create the subdirectory
    tmp_path = tmp_path / "path" / "to" / "table"
    start_time = datetime.now().timestamp()
    write_deltalake(tmp_path, sample_data, engine=engine)
    end_time = datetime.now().timestamp()

    assert ("0" * 20 + ".json") in os.listdir(tmp_path / "_delta_log")

    delta_table = DeltaTable(tmp_path)
    assert delta_table.schema().to_pyarrow() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    assert table == sample_data

    for add_path in get_add_paths(delta_table):
        # Paths should be relative, and with no partitioning have no directories
        assert "/" not in add_path

    for action in get_add_actions(delta_table):
        path = os.path.join(tmp_path, action["path"])
        actual_size = os.path.getsize(path)
        assert actual_size == action["size"]

        modification_time = action["modificationTime"] / 1000  # convert back to seconds
        assert start_time < modification_time
        assert modification_time < end_time


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_roundtrip_nulls(tmp_path: pathlib.Path, engine: Literal["pyarrow", "rust"]):
    data = pa.table({"x": pa.array([None, None, 1, 2], type=pa.int64())})
    # One row group will have values, one will be all nulls.
    # The first will have None in min and max stats, so we need to handle that.
    write_deltalake(tmp_path, data, min_rows_per_group=2, max_rows_per_group=2)

    delta_table = DeltaTable(tmp_path)
    assert delta_table.schema().to_pyarrow() == data.schema

    table = delta_table.to_pyarrow_table()
    assert table == data

    data = pa.table({"x": pa.array([None, None, None, None], type=pa.int64())})
    # Will be all null, with two row groups
    write_deltalake(
        tmp_path,
        data,
        min_rows_per_group=2,
        max_rows_per_group=2,
        mode="overwrite",
        engine=engine,
    )

    delta_table = DeltaTable(tmp_path)
    assert delta_table.schema().to_pyarrow() == data.schema

    table = delta_table.to_pyarrow_table()
    assert table == data


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_enforce_schema(existing_table: DeltaTable, mode: str):
    bad_data = pa.table({"x": pa.array([1, 2, 3])})

    with pytest.raises(ValueError):
        write_deltalake(existing_table, bad_data, mode=mode, engine="pyarrow")

    table_uri = existing_table._table.table_uri()
    with pytest.raises(ValueError):
        write_deltalake(table_uri, bad_data, mode=mode, engine="pyarrow")


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_enforce_schema_rust_writer(existing_table: DeltaTable, mode: str):
    bad_data = pa.table({"x": pa.array([1, 2, 3])})

    with pytest.raises(
        SchemaMismatchError,
        match=".*Cannot cast schema, number of fields does not match.*",
    ):
        write_deltalake(existing_table, bad_data, mode=mode, engine="rust")

    table_uri = existing_table._table.table_uri()
    with pytest.raises(
        SchemaMismatchError,
        match=".*Cannot cast schema, number of fields does not match.*",
    ):
        write_deltalake(table_uri, bad_data, mode=mode, engine="rust")


def test_update_schema(existing_table: DeltaTable):
    new_data = pa.table({"x": pa.array([1, 2, 3])})

    with pytest.raises(DeltaError):
        write_deltalake(
            existing_table, new_data, mode="append", schema_mode="overwrite"
        )

    write_deltalake(existing_table, new_data, mode="overwrite", schema_mode="overwrite")

    read_data = existing_table.to_pyarrow_table()
    assert new_data == read_data
    assert existing_table.schema().to_pyarrow() == new_data.schema


def test_merge_schema(existing_table: DeltaTable):
    print(existing_table._table.table_uri())
    old_table_data = existing_table.to_pyarrow_table()
    new_data = pa.table(
        {
            "new_x": pa.array([1, 2, 3], pa.int32()),
            "new_y": pa.array([1, 2, 3], pa.int32()),
        }
    )

    write_deltalake(
        existing_table, new_data, mode="append", schema_mode="merge", engine="rust"
    )
    # adjust schema of old_table_data and new_data to match each other

    for i in range(old_table_data.num_columns):
        col = old_table_data.schema.field(i)
        new_data = new_data.add_column(i, col, pa.nulls(new_data.num_rows, col.type))

    old_table_data = old_table_data.append_column(
        pa.field("new_x", pa.int32()), pa.nulls(old_table_data.num_rows, pa.int32())
    )
    old_table_data = old_table_data.append_column(
        pa.field("new_y", pa.int32()), pa.nulls(old_table_data.num_rows, pa.int32())
    )

    # define sort order
    read_data = existing_table.to_pyarrow_table().sort_by(
        [("utf8", "ascending"), ("new_x", "ascending")]
    )
    print(repr(read_data.to_pylist()))
    concated = pa.concat_tables([old_table_data, new_data])
    print(repr(concated.to_pylist()))
    assert read_data == concated

    write_deltalake(existing_table, new_data, mode="overwrite", schema_mode="overwrite")

    assert existing_table.schema().to_pyarrow() == new_data.schema


def test_overwrite_schema(existing_table: DeltaTable):
    new_data_invalid = pa.table(
        {
            "utf8": pa.array([1235, 546, 5645]),
            "new_x": pa.array([1, 2, 3], pa.int32()),
            "new_y": pa.array([1, 2, 3], pa.int32()),
        }
    )

    with pytest.raises(DeltaError):
        write_deltalake(
            existing_table,
            new_data_invalid,
            mode="append",
            schema_mode="overwrite",
            engine="rust",
        )

    new_data = pa.table(
        {
            "utf8": pa.array(["bla", "bli", "blubb"]),
            "new_x": pa.array([1, 2, 3], pa.int32()),
            "new_y": pa.array([1, 2, 3], pa.int32()),
        }
    )
    with pytest.raises(DeltaError):
        write_deltalake(
            existing_table,
            new_data,
            mode="append",
            schema_mode="overwrite",
            engine="rust",
        )

    write_deltalake(existing_table, new_data, mode="overwrite", schema_mode="overwrite")

    assert existing_table.schema().to_pyarrow() == new_data.schema


def test_update_schema_rust_writer_append(existing_table: DeltaTable):
    with pytest.raises(SchemaMismatchError):
        # It's illegal to do schema drift without correct schema_mode
        write_deltalake(
            existing_table,
            pa.table({"x4": pa.array([1, 2, 3])}),
            mode="append",
            schema_mode=None,
            engine="rust",
        )
    with pytest.raises(DeltaError):
        write_deltalake(  # schema_mode overwrite is illegal with append
            existing_table,
            pa.table({"x1": pa.array([1, 2, 3])}),
            mode="append",
            schema_mode="overwrite",
            engine="rust",
        )
    write_deltalake(
        existing_table,
        pa.table({"x2": pa.array([1, 2, 3])}),
        mode="append",
        schema_mode="merge",
        engine="rust",
    )


def test_write_type_castable_types(existing_table: DeltaTable):
    write_deltalake(
        existing_table,
        pa.table({"utf8": pa.array([1, 2, 3])}),
        mode="append",
        schema_mode="merge",
        engine="rust",
    )
    with pytest.raises(
        Exception, match="Cast error: Cannot cast string 'hello' to value of Int8 type"
    ):
        write_deltalake(
            existing_table,
            pa.table({"int8": pa.array(["hello", "2", "3"])}),
            mode="append",
            schema_mode="merge",
            engine="rust",
        )

    with pytest.raises(
        Exception, match="Cast error: Can't cast value 1000 to type Int8"
    ):
        write_deltalake(
            existing_table,
            pa.table({"int8": pa.array([1000, 100, 10])}),
            mode="append",
            schema_mode="merge",
            engine="rust",
        )


def test_update_schema_rust_writer_invalid(existing_table: DeltaTable):
    new_data = pa.table({"x5": pa.array([1, 2, 3])})
    with pytest.raises(
        SchemaMismatchError, match="Cannot cast schema, number of fields does not match"
    ):
        write_deltalake(
            existing_table,
            new_data,
            mode="overwrite",
            schema_mode=None,
            engine="rust",
        )

    write_deltalake(
        existing_table,
        new_data,
        mode="overwrite",
        schema_mode="overwrite",
        engine="rust",
    )

    read_data = existing_table.to_pyarrow_table()
    assert new_data == read_data
    assert existing_table.schema().to_pyarrow() == new_data.schema


def test_merge_schema_rust_writer_with_overwrite(tmp_path: pathlib.Path):
    data = pa.table(
        {
            "a": pa.array([1, 2, 3, 4]),
            "b": pa.array([1, 1, 2, 2]),
            "c": pa.array([10, 11, 12, 13]),
        }
    )
    write_deltalake(
        tmp_path,
        data,
        engine="rust",
    )

    new_data = pa.table({"a": pa.array([100, 200, 300]), "b": pa.array([1, 1, 1])})

    write_deltalake(
        tmp_path,
        new_data,
        mode="overwrite",
        schema_mode="merge",
        engine="rust",
    )
    assert set(DeltaTable(tmp_path).to_pyarrow_table().column_names) == set(
        ["a", "b", "c"]
    )


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_local_path(
    tmp_path: pathlib.Path,
    sample_data: pa.Table,
    monkeypatch,
    engine: Literal["pyarrow", "rust"],
):
    monkeypatch.chdir(tmp_path)  # Make tmp_path the working directory
    (tmp_path / "path/to/table").mkdir(parents=True)

    local_path = "./path/to/table"
    write_deltalake(local_path, sample_data, engine=engine)
    delta_table = DeltaTable(local_path)
    assert delta_table.schema().to_pyarrow() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    assert table == sample_data


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_local_path_with_unsafe_rename(
    tmp_path: pathlib.Path,
    sample_data: pa.Table,
    monkeypatch,
    engine: Literal["pyarrow", "rust"],
):
    monkeypatch.chdir(tmp_path)  # Make tmp_path the working directory
    (tmp_path / "path/to/table").mkdir(parents=True)

    local_path = "./path/to/table"
    storage_opts = {
        "allow_unsafe_rename": "true",
    }
    write_deltalake(
        local_path, sample_data, storage_options=storage_opts, engine=engine
    )
    delta_table = DeltaTable(local_path, storage_options=storage_opts)
    assert delta_table.schema().to_pyarrow() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    assert table == sample_data


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_roundtrip_metadata(tmp_path: pathlib.Path, sample_data: pa.Table, engine):
    write_deltalake(
        tmp_path,
        sample_data,
        name="test_name",
        description="test_desc",
        configuration={"delta.appendOnly": "false"},
        engine=engine,
    )

    delta_table = DeltaTable(tmp_path)

    metadata = delta_table.metadata()

    assert metadata.name == "test_name"
    assert metadata.description == "test_desc"
    assert metadata.configuration == {"delta.appendOnly": "false"}


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
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
        # "binary",
        "date32",
        "timestamp",
    ],
)
def test_roundtrip_partitioned(
    tmp_path: pathlib.Path, sample_data: pa.Table, column: str, engine
):
    write_deltalake(tmp_path, sample_data, partition_by=column, engine=engine)

    delta_table = DeltaTable(tmp_path)
    assert delta_table.schema().to_pyarrow() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    table = table.take(pc.sort_indices(table["int64"]))
    assert table == sample_data

    for add_path in get_add_paths(delta_table):
        # Paths should be relative
        assert add_path.count("/") == 1


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_roundtrip_null_partition(
    tmp_path: pathlib.Path, sample_data: pa.Table, engine
):
    sample_data = sample_data.add_column(
        0, "utf8_with_nulls", pa.array(["a"] * 4 + [None])
    )
    write_deltalake(
        tmp_path, sample_data, partition_by=["utf8_with_nulls"], engine=engine
    )

    delta_table = DeltaTable(tmp_path)
    assert delta_table.schema().to_pyarrow() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    table = table.take(pc.sort_indices(table["int64"]))
    assert table == sample_data


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_roundtrip_multi_partitioned(
    tmp_path: pathlib.Path, sample_data: pa.Table, engine
):
    write_deltalake(
        tmp_path, sample_data, partition_by=["int32", "bool"], engine=engine
    )

    delta_table = DeltaTable(tmp_path)
    assert delta_table.schema().to_pyarrow() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    table = table.take(pc.sort_indices(table["int64"]))
    assert table == sample_data

    for add_path in get_add_paths(delta_table):
        # Paths should be relative
        assert add_path.count("/") == 2


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_write_modes(tmp_path: pathlib.Path, sample_data: pa.Table, engine):
    write_deltalake(tmp_path, sample_data, engine=engine)
    assert DeltaTable(tmp_path).to_pyarrow_table() == sample_data

    if engine == "pyarrow":
        with pytest.raises(FileExistsError):
            write_deltalake(tmp_path, sample_data, mode="error", engine=engine)
    elif engine == "rust":
        with pytest.raises(DeltaError):
            write_deltalake(tmp_path, sample_data, mode="error", engine=engine)

    write_deltalake(tmp_path, sample_data, mode="ignore", engine="rust")
    assert ("0" * 19 + "1.json") not in os.listdir(tmp_path / "_delta_log")

    write_deltalake(tmp_path, sample_data, mode="append", engine="rust")
    expected = pa.concat_tables([sample_data, sample_data])
    assert DeltaTable(tmp_path).to_pyarrow_table() == expected

    write_deltalake(tmp_path, sample_data, mode="overwrite", engine="rust")
    assert DeltaTable(tmp_path).to_pyarrow_table() == sample_data


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_append_only_should_append_only_with_the_overwrite_mode(  # Create rust equivalent rust
    tmp_path: pathlib.Path, sample_data: pa.Table, engine
):
    config = {"delta.appendOnly": "true"}

    write_deltalake(
        tmp_path, sample_data, mode="append", configuration=config, engine=engine
    )

    table = DeltaTable(tmp_path)
    write_deltalake(table, sample_data, mode="append", engine=engine)

    data_store_types = [tmp_path, table]
    fail_modes = ["overwrite", "ignore", "error"]

    for data_store_type, mode in itertools.product(data_store_types, fail_modes):
        with pytest.raises(
            ValueError,
            match=(
                "If configuration has delta.appendOnly = 'true', mode must be"
                f" 'append'. Mode is currently {mode}"
            ),
        ):
            write_deltalake(data_store_type, sample_data, mode=mode, engine=engine)

    expected = pa.concat_tables([sample_data, sample_data])

    assert table.to_pyarrow_table() == expected
    assert table.version() == 1


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_writer_with_table(existing_table: DeltaTable, sample_data: pa.Table, engine):
    write_deltalake(existing_table, sample_data, mode="overwrite", engine=engine)
    assert existing_table.to_pyarrow_table() == sample_data


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_fails_wrong_partitioning(
    existing_table: DeltaTable, sample_data: pa.Table, engine
):
    if engine == "pyarrow":
        with pytest.raises(ValueError):
            write_deltalake(
                existing_table,
                sample_data,
                mode="append",
                partition_by="int32",
                engine=engine,
            )
    elif engine == "rust":
        with pytest.raises(
            DeltaError,
            match='Generic error: Specified table partitioning does not match table partitioning: expected: [], got: ["int32"]',
        ):
            write_deltalake(
                existing_table,
                sample_data,
                mode="append",
                partition_by="int32",
                engine=engine,
            )


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
@pytest.mark.pandas
@pytest.mark.parametrize("schema_provided", [True, False])
def test_write_pandas(
    tmp_path: pathlib.Path, sample_data: pa.Table, schema_provided, engine
):
    # When timestamp is converted to Pandas, it gets casted to ns resolution,
    # but Delta Lake schemas only support us resolution.
    sample_pandas = sample_data.to_pandas()
    if schema_provided is True:
        schema = sample_data.schema
    else:
        schema = None
    write_deltalake(tmp_path, sample_pandas, schema=schema, engine=engine)
    delta_table = DeltaTable(tmp_path)
    df = delta_table.to_pandas()
    assert_frame_equal(df, sample_pandas)


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_write_iterator(
    tmp_path: pathlib.Path, existing_table: DeltaTable, sample_data: pa.Table, engine
):
    batches = existing_table.to_pyarrow_dataset().to_batches()
    with pytest.raises(ValueError):
        write_deltalake(tmp_path, batches, mode="overwrite", engine=engine)

    write_deltalake(
        tmp_path, batches, schema=sample_data.schema, mode="overwrite", engine=engine
    )
    assert DeltaTable(tmp_path).to_pyarrow_table() == sample_data


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
@pytest.mark.parametrize("large_dtypes", [True, False])
@pytest.mark.parametrize(
    "constructor",
    [
        lambda table: table.to_pyarrow_dataset(),
        lambda table: table.to_pyarrow_table(),
        lambda table: table.to_pyarrow_table().to_batches()[0],
    ],
)
def test_write_dataset_table_recordbatch(
    tmp_path: pathlib.Path,
    existing_table: DeltaTable,
    sample_data: pa.Table,
    engine: str,
    large_dtypes: bool,
    constructor,
):
    dataset = constructor(existing_table)

    write_deltalake(
        tmp_path, dataset, mode="overwrite", large_dtypes=large_dtypes, engine=engine
    )
    assert DeltaTable(tmp_path).to_pyarrow_table() == sample_data


@pytest.mark.parametrize("large_dtypes", [True, False])
@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_write_recordbatchreader(
    tmp_path: pathlib.Path,
    existing_table: DeltaTable,
    sample_data: pa.Table,
    large_dtypes: bool,
    engine: Literal["pyarrow", "rust"],
):
    batches = existing_table.to_pyarrow_dataset().to_batches()
    reader = RecordBatchReader.from_batches(
        existing_table.to_pyarrow_dataset().schema, batches
    )

    print("writing second time")
    write_deltalake(
        tmp_path, reader, mode="overwrite", large_dtypes=large_dtypes, engine=engine
    )
    assert DeltaTable(tmp_path).to_pyarrow_table() == sample_data


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_writer_partitioning(
    tmp_path: pathlib.Path, engine: Literal["pyarrow", "rust"]
):
    test_strings = ["a=b", "hello world", "hello%20world"]
    data = pa.table(
        {"p": pa.array(test_strings), "x": pa.array(range(len(test_strings)))}
    )

    write_deltalake(tmp_path, data, engine=engine)

    assert DeltaTable(tmp_path).to_pyarrow_table() == data


def get_log_path(table: DeltaTable) -> str:
    """Returns _delta_log path for this delta table."""
    return table._table.table_uri() + "/_delta_log/" + ("0" * 20 + ".json")


def get_add_actions(table: DeltaTable) -> List[str]:
    log_path = get_log_path(table)

    actions = []

    for line in open(log_path, "r").readlines():
        log_entry = json.loads(line)

        if "add" in log_entry:
            actions.append(log_entry["add"])

    return actions


def get_stats(table: DeltaTable):
    actions = get_add_actions(table)
    # Should only have single add entry
    if len(actions) == 1:
        return json.loads(actions[0]["stats"])
    else:
        raise AssertionError("No add action found!")


def get_add_paths(table: DeltaTable) -> List[str]:
    return [action["path"] for action in get_add_actions(table)]


def test_writer_stats(existing_table: DeltaTable, sample_data: pa.Table):
    stats = get_stats(existing_table)

    assert stats["numRecords"] == sample_data.num_rows

    null_values = []
    for null_count in stats["nullCount"].values():
        if isinstance(null_count, dict):
            null_values.extend(list(null_count.values()))
        else:
            null_values.append(null_count)
    assert all(i == 0 for i in null_values)

    expected_mins = {
        "utf8": "0",
        "int64": 0,
        "int32": 0,
        "int16": 0,
        "int8": 0,
        "float32": -0.0,
        "float64": -0.0,
        "bool": False,
        "binary": "0",
        "timestamp": "2022-01-01T00:00:00Z",
        "struct": {
            "x": 0,
            "y": "0",
        },
    }
    # PyArrow added support for decimal and date32 in 8.0.0
    expected_mins["decimal"] = 10.0
    expected_mins["date32"] = "2022-01-01"

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
        "timestamp": "2022-01-01T04:00:00Z",
        "struct": {"x": 4, "y": "4"},
    }
    # PyArrow added support for decimal and date32 in 8.0.0
    expected_maxs["decimal"] = 14.0
    expected_maxs["date32"] = "2022-01-05"

    assert stats["maxValues"] == expected_maxs


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_writer_null_stats(tmp_path: pathlib.Path, engine: Literal["pyarrow", "rust"]):
    data = pa.table(
        {
            "int32": pa.array([1, None, 2, None], pa.int32()),
            "float64": pa.array([1.0, None, None, None], pa.float64()),
            "str": pa.array([None] * 4, pa.string()),
        }
    )
    write_deltalake(tmp_path, data, engine=engine)

    table = DeltaTable(tmp_path)
    stats = get_stats(table)

    expected_nulls = {"int32": 2, "float64": 3, "str": 4}
    assert stats["nullCount"] == expected_nulls


@pytest.mark.parametrize("engine", ["pyarrow"])
def test_writer_fails_on_protocol(
    existing_table: DeltaTable,
    sample_data: pa.Table,
    engine: Literal["pyarrow", "rust"],
):
    existing_table.protocol = Mock(return_value=ProtocolVersions(1, 3, None, None))
    with pytest.raises(DeltaProtocolError):
        write_deltalake(existing_table, sample_data, mode="overwrite", engine=engine)


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
    write_deltalake(
        tmp_path,
        data,
        engine="pyarrow",
        file_options=ParquetFileFormat().make_write_options(),
        max_rows_per_file=rows_per_file,
        min_rows_per_group=rows_per_file,
        max_rows_per_group=rows_per_file,
    )

    table = DeltaTable(tmp_path)
    stats = get_multifile_stats(table)
    files_written = [f for f in os.listdir(tmp_path) if f != "_delta_log"]

    assert sum([stat_entry["numRecords"] for stat_entry in stats]) == row_count
    assert len(files_written) == expected_files


def test_writer_with_options(tmp_path: pathlib.Path):
    column_values = [datetime(year_, 1, 1, 0, 0, 0) for year_ in range(9000, 9010)]
    data = pa.table({"colA": pa.array(column_values, pa.timestamp("us"))})

    opts = (
        ParquetFileFormat()
        .make_write_options()
        .update(compression="GZIP", coerce_timestamps="us")
    )
    write_deltalake(tmp_path, data, file_options=opts)

    table = (
        DeltaTable(tmp_path)
        .to_pyarrow_dataset(
            parquet_read_options=ParquetReadOptions(coerce_int96_timestamp_unit="us")
        )
        .to_table()
    )

    assert table == data


def test_try_get_table_and_table_uri(tmp_path: pathlib.Path):
    def _normalize_path(t):  # who does not love Windows? ;)
        return t[0], t[1].replace("\\", "/") if t[1] else t[1]

    data = pa.table({"vals": pa.array(["1", "2", "3"])})
    table_or_uri = tmp_path / "delta_table"
    write_deltalake(table_or_uri, data)
    delta_table = DeltaTable(table_or_uri)

    # table_or_uri as DeltaTable
    assert _normalize_path(
        try_get_table_and_table_uri(delta_table, None)
    ) == _normalize_path(
        (
            delta_table,
            str(tmp_path / "delta_table") + "/",
        )
    )

    # table_or_uri as str
    assert _normalize_path(
        try_get_table_and_table_uri(str(tmp_path / "delta_table"), None)
    ) == _normalize_path(
        (
            delta_table,
            str(tmp_path / "delta_table"),
        )
    )
    assert _normalize_path(
        try_get_table_and_table_uri(str(tmp_path / "str"), None)
    ) == _normalize_path(
        (
            None,
            str(tmp_path / "str"),
        )
    )

    # table_or_uri as Path
    assert _normalize_path(
        try_get_table_and_table_uri(tmp_path / "delta_table", None)
    ) == _normalize_path(
        (
            delta_table,
            str(tmp_path / "delta_table"),
        )
    )
    assert _normalize_path(
        try_get_table_and_table_uri(tmp_path / "Path", None)
    ) == _normalize_path(
        (
            None,
            str(tmp_path / "Path"),
        )
    )

    # table_or_uri with invalid parameter type
    with pytest.raises(ValueError):
        try_get_table_and_table_uri(None, None)


@pytest.mark.parametrize(
    "value_1,value_2,value_type,filter_string",
    [
        (1, 2, pa.int64(), "1"),
        (False, True, pa.bool_(), "false"),
        (date(2022, 1, 1), date(2022, 1, 2), pa.date32(), "'2022-01-01'"),
    ],
)
def test_partition_overwrite(
    tmp_path: pathlib.Path,
    value_1: Any,
    value_2: Any,
    value_type: pa.DataType,
    filter_string: str,
):
    sample_data = pa.table(
        {
            "p1": pa.array(["1", "1", "2", "2"], pa.string()),
            "p2": pa.array([value_1, value_2, value_1, value_2], value_type),
            "val": pa.array([1, 1, 1, 1], pa.int64()),
        }
    )
    write_deltalake(tmp_path, sample_data, mode="overwrite", partition_by=["p1", "p2"])

    delta_table = DeltaTable(tmp_path)
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == sample_data
    )

    sample_data = pa.table(
        {
            "p1": pa.array(["1", "1"], pa.string()),
            "p2": pa.array([value_2, value_1], value_type),
            "val": pa.array([2, 2], pa.int64()),
        }
    )
    expected_data = pa.table(
        {
            "p1": pa.array(["1", "1", "2", "2"], pa.string()),
            "p2": pa.array([value_1, value_2, value_1, value_2], value_type),
            "val": pa.array([2, 2, 1, 1], pa.int64()),
        }
    )
    write_deltalake(
        tmp_path,
        sample_data,
        mode="overwrite",
        predicate="p1 = 1",
    )

    delta_table.update_incremental()
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == expected_data
    )

    sample_data = pa.table(
        {
            "p1": pa.array(["1", "2"], pa.string()),
            "p2": pa.array([value_2, value_2], value_type),
            "val": pa.array([3, 3], pa.int64()),
        }
    )
    expected_data = pa.table(
        {
            "p1": pa.array(["1", "1", "2", "2"], pa.string()),
            "p2": pa.array([value_1, value_2, value_1, value_2], value_type),
            "val": pa.array([2, 3, 1, 3], pa.int64()),
        }
    )

    write_deltalake(
        tmp_path,
        sample_data,
        mode="overwrite",
        predicate=f"p2 > {filter_string}",
    )
    delta_table.update_incremental()
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == expected_data
    )

    # Overwrite a single partition
    sample_data = pa.table(
        {
            "p1": pa.array(["1"], pa.string()),
            "p2": pa.array([value_1], value_type),
            "val": pa.array([5], pa.int64()),
        }
    )
    expected_data = pa.table(
        {
            "p1": pa.array(["1", "1", "2", "2"], pa.string()),
            "p2": pa.array([value_1, value_2, value_1, value_2], value_type),
            "val": pa.array([5, 3, 1, 3], pa.int64()),
        }
    )
    write_deltalake(
        tmp_path,
        sample_data,
        mode="overwrite",
        predicate=f"p1 = 1 AND p2 = {filter_string}",
    )
    delta_table.update_incremental()
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == expected_data
    )
    with pytest.raises(DeltaProtocolError, match="Invariant violations"):
        write_deltalake(
            tmp_path, sample_data, mode="overwrite", predicate=f"p2 < {filter_string}"
        )


@pytest.fixture()
def sample_data_for_partitioning() -> pa.Table:
    return pa.table(
        {
            "p1": pa.array(["1", "1", "2", "2"], pa.string()),
            "p2": pa.array([1, 2, 1, 2], pa.int64()),
            "val": pa.array([1, 1, 1, 1], pa.int64()),
        }
    )


def test_partition_overwrite_unfiltered_data_fails(
    tmp_path: pathlib.Path, sample_data_for_partitioning: pa.Table
):
    write_deltalake(
        tmp_path,
        sample_data_for_partitioning,
        mode="overwrite",
        partition_by=["p1", "p2"],
    )
    with pytest.raises(ValueError):
        write_deltalake(
            tmp_path,
            sample_data_for_partitioning,
            mode="overwrite",
            partition_filters=[("p2", "=", "1")],
        )


@pytest.mark.parametrize("large_dtypes", [True, False])
@pytest.mark.parametrize(
    "value_1,value_2,value_type,filter_string",
    [
        (1, 2, pa.int64(), "1"),
        (False, True, pa.bool_(), "false"),
        (date(2022, 1, 1), date(2022, 1, 2), pa.date32(), "2022-01-01"),
    ],
)
def test_replace_where_overwrite(
    tmp_path: pathlib.Path,
    value_1: Any,
    value_2: Any,
    value_type: pa.DataType,
    filter_string: str,
    large_dtypes: bool,
):
    table_path = tmp_path

    sample_data = pa.table(
        {
            "p1": pa.array(["1", "1", "2", "2"], pa.string()),
            "p2": pa.array([value_1, value_2, value_1, value_2], value_type),
            "val": pa.array([1, 1, 1, 1], pa.int64()),
        }
    )
    write_deltalake(
        table_path, sample_data, mode="overwrite", large_dtypes=large_dtypes
    )

    delta_table = DeltaTable(table_path)
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == sample_data
    )

    sample_data = pa.table(
        {
            "p1": pa.array(["1", "1"], pa.string()),
            "p2": pa.array([value_2, value_1], value_type),
            "val": pa.array([2, 3], pa.int64()),
        }
    )
    expected_data = pa.table(
        {
            "p1": pa.array(["1", "1", "2", "2"], pa.string()),
            "p2": pa.array([value_1, value_2, value_1, value_2], value_type),
            "val": pa.array([3, 2, 1, 1], pa.int64()),
        }
    )

    write_deltalake(
        table_path,
        sample_data,
        mode="overwrite",
        predicate="p1 = '1'",
        engine="rust",
        large_dtypes=large_dtypes,
    )

    delta_table.update_incremental()
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == expected_data
    )


@pytest.mark.parametrize(
    "value_1,value_2,value_type,filter_string",
    [
        (1, 2, pa.int64(), "1"),
        (False, True, pa.bool_(), "false"),
        (date(2022, 1, 1), date(2022, 1, 2), pa.date32(), "2022-01-01"),
    ],
)
def test_replace_where_overwrite_partitioned(
    tmp_path: pathlib.Path,
    value_1: Any,
    value_2: Any,
    value_type: pa.DataType,
    filter_string: str,
):
    table_path = tmp_path

    sample_data = pa.table(
        {
            "p1": pa.array(["1", "1", "2", "2"], pa.string()),
            "p2": pa.array([value_1, value_2, value_1, value_2], value_type),
            "val": pa.array([1, 1, 1, 1], pa.int64()),
        }
    )
    write_deltalake(
        table_path, sample_data, mode="overwrite", partition_by=["p1", "p2"]
    )

    delta_table = DeltaTable(table_path)
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == sample_data
    )

    replace_data = pa.table(
        {
            "p1": pa.array(["1", "1"], pa.string()),
            "p2": pa.array([value_2, value_1], value_type),
            "val": pa.array([2, 3], pa.int64()),
        }
    )
    expected_data = pa.table(
        {
            "p1": pa.array(["1", "1", "2", "2"], pa.string()),
            "p2": pa.array([value_1, value_2, value_1, value_2], value_type),
            "val": pa.array([3, 2, 1, 1], pa.int64()),
        }
    )

    write_deltalake(
        table_path,
        replace_data,
        mode="overwrite",
        partition_by=["p1", "p2"],
        predicate="p1 = '1'",
        engine="rust",
    )

    delta_table.update_incremental()
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == expected_data
    )


def test_partition_overwrite_with_new_partition(
    tmp_path: pathlib.Path, sample_data_for_partitioning: pa.Table
):
    write_deltalake(
        tmp_path,
        sample_data_for_partitioning,
        mode="overwrite",
        partition_by=["p1", "p2"],
    )

    new_sample_data = pa.table(
        {
            "p1": pa.array(["1", "2"], pa.string()),
            "p2": pa.array([2, 2], pa.int64()),
            "val": pa.array([2, 3], pa.int64()),
        }
    )
    expected_data = pa.table(
        {
            "p1": pa.array(["1", "1", "2", "2"], pa.string()),
            "p2": pa.array([1, 2, 1, 2], pa.int64()),
            "val": pa.array([1, 2, 1, 3], pa.int64()),
        }
    )
    write_deltalake(tmp_path, new_sample_data, mode="overwrite", predicate="p2 = 2")
    delta_table = DeltaTable(tmp_path)
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == expected_data
    )


def test_partition_overwrite_with_non_partitioned_data(
    tmp_path: pathlib.Path, sample_data_for_partitioning: pa.Table
):
    write_deltalake(tmp_path, sample_data_for_partitioning, mode="overwrite")
    write_deltalake(
        tmp_path,
        sample_data_for_partitioning.filter(pc.field("p1") == "1"),
        mode="overwrite",
        predicate="p1 = 1",
    )


def test_partition_overwrite_with_wrong_partition(
    tmp_path: pathlib.Path, sample_data_for_partitioning: pa.Table
):
    write_deltalake(
        tmp_path,
        sample_data_for_partitioning,
        mode="overwrite",
        partition_by=["p1", "p2"],
    )
    from deltalake.exceptions import DeltaError

    with pytest.raises(DeltaError, match="No field named p999."):
        write_deltalake(
            tmp_path,
            sample_data_for_partitioning,
            mode="overwrite",
            predicate="p999 = 1",
            # partition_filters=[("p999", "=", "1")],
        )

    new_data = pa.table(
        {
            "p1": pa.array(["1"], pa.string()),
            "p2": pa.array([2], pa.int64()),
            "val": pa.array([1], pa.int64()),
        }
    )

    with pytest.raises(
        DeltaProtocolError,
        match="Invariant violations",
    ):
        write_deltalake(
            tmp_path,
            new_data,
            mode="overwrite",
            predicate="p1 = 1 AND p2 = 1",
        )


def test_handles_binary_data(tmp_path: pathlib.Path):
    value = b"\x00\\"
    table = pa.Table.from_pydict({"field_one": [value]})
    write_deltalake(tmp_path, table)

    dt = DeltaTable(tmp_path)
    out = dt.to_pyarrow_table()
    assert table == out


def test_max_partitions_exceeding_fragment_should_fail(
    tmp_path: pathlib.Path, sample_data_for_partitioning: pa.Table
):
    with pytest.raises(
        ValueError,
        match=r"Fragment would be written into \d+ partitions\. This exceeds the maximum of \d+",
    ):
        write_deltalake(
            tmp_path,
            sample_data_for_partitioning,
            mode="overwrite",
            engine="pyarrow",
            max_partitions=1,
            partition_by=["p1", "p2"],
        )


@pytest.mark.parametrize("engine", ["rust", "pyarrow"])
def test_large_arrow_types(tmp_path: pathlib.Path, engine):
    pylist = [
        {"name": "Joey", "gender": b"M", "arr_type": ["x", "y"], "dict": {"a": b"M"}},
        {"name": "Ivan", "gender": b"F", "arr_type": ["x", "z"]},
    ]
    schema = pa.schema(
        [
            pa.field("name", pa.large_string()),
            pa.field("gender", pa.large_binary()),
            pa.field("arr_type", pa.large_list(pa.large_string())),
            pa.field("map_type", pa.map_(pa.large_string(), pa.large_binary())),
            pa.field("struct", pa.struct([pa.field("sub", pa.large_string())])),
        ]
    )
    table = pa.Table.from_pylist(pylist, schema=schema)

    write_deltalake(tmp_path, table, mode="append", engine=engine, large_dtypes=True)
    write_deltalake(tmp_path, table, mode="append", engine=engine, large_dtypes=True)
    write_deltalake(tmp_path, table, mode="append", engine=engine, large_dtypes=True)

    dt = DeltaTable(tmp_path)
    assert table.schema == dt.schema().to_pyarrow(as_large_types=True)


def test_large_arrow_types_dataset_as_large_types(tmp_path: pathlib.Path):
    pylist = [
        {"name": "Joey", "gender": b"M", "arr_type": ["x", "y"], "dict": {"a": b"M"}},
        {"name": "Ivan", "gender": b"F", "arr_type": ["x", "z"]},
    ]
    schema = pa.schema(
        [
            pa.field("name", pa.large_string()),
            pa.field("gender", pa.large_binary()),
            pa.field("arr_type", pa.large_list(pa.large_string())),
            pa.field("map_type", pa.map_(pa.large_string(), pa.large_binary())),
            pa.field("struct", pa.struct([pa.field("sub", pa.large_string())])),
        ]
    )
    table = pa.Table.from_pylist(pylist, schema=schema)

    write_deltalake(tmp_path, table)

    dt = DeltaTable(tmp_path)

    ds = dt.to_pyarrow_dataset(as_large_types=True)
    union_ds = dataset([ds, dataset(table)])
    assert union_ds.to_table().shape[0] == 4


def test_large_arrow_types_explicit_scan_schema(tmp_path: pathlib.Path):
    pylist = [
        {"name": "Joey", "gender": b"M", "arr_type": ["x", "y"], "dict": {"a": b"M"}},
        {"name": "Ivan", "gender": b"F", "arr_type": ["x", "z"]},
    ]
    schema = pa.schema(
        [
            pa.field("name", pa.large_string()),
            pa.field("gender", pa.large_binary()),
            pa.field("arr_type", pa.large_list(pa.large_string())),
            pa.field("map_type", pa.map_(pa.large_string(), pa.large_binary())),
            pa.field("struct", pa.struct([pa.field("sub", pa.large_string())])),
        ]
    )
    table = pa.Table.from_pylist(pylist, schema=schema)

    write_deltalake(tmp_path, table)

    dt = DeltaTable(tmp_path)

    ds = dt.to_pyarrow_dataset(schema=schema)
    union_ds = dataset([ds, dataset(table)])
    assert union_ds.to_table().shape[0] == 4


def test_partition_large_arrow_types(tmp_path: pathlib.Path):
    table = pa.table(
        {
            "foo": pa.array(["1", "1", "2", "2"], pa.large_string()),
            "bar": pa.array([1, 2, 1, 2], pa.int64()),
            "baz": pa.array([1, 1, 1, 1], pa.int64()),
        }
    )

    write_deltalake(tmp_path, table, partition_by=["foo"])

    dt = DeltaTable(tmp_path)
    files = dt.files()
    expected = ["foo=1", "foo=2"]

    result = sorted([file.split("/")[0] for file in files])
    assert expected == result


def test_uint_arrow_types(tmp_path: pathlib.Path):
    pylist = [
        {"num1": 3, "num2": 3, "num3": 3, "num4": 5},
        {"num1": 1, "num2": 13, "num3": 35, "num4": 13},
    ]
    schema = pa.schema(
        [
            pa.field("num1", pa.uint8()),
            pa.field("num2", pa.uint16()),
            pa.field("num3", pa.uint32()),
            pa.field("num4", pa.uint64()),
        ]
    )
    table = pa.Table.from_pylist(pylist, schema=schema)

    write_deltalake(tmp_path, table)


def test_concurrency(existing_table: DeltaTable, sample_data: pa.Table):
    exception = None

    def comp():
        nonlocal exception
        dt = DeltaTable(existing_table.table_uri)
        for _ in range(5):
            # We should always be able to get a consistent table state
            data = DeltaTable(dt.table_uri).to_pyarrow_table()
            # If two overwrites delete the same file and then add their own
            # concurrently, then this will fail.
            assert data.num_rows == sample_data.num_rows
            try:
                write_deltalake(dt.table_uri, sample_data, mode="overwrite")
            except Exception as e:
                exception = e

    n_threads = 2
    threads = [threading.Thread(target=comp) for _ in range(n_threads)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert isinstance(exception, CommitFailedError)
    assert (
        "a concurrent transaction deleted the same data your transaction deletes"
        in str(exception)
    )


def test_issue_1651_roundtrip_timestamp(tmp_path: pathlib.Path):
    data = pa.table(
        {
            "id": pa.array([425], type=pa.int32()),
            "data": pa.array(["python-module-test-write"]),
            "t": pa.array([datetime(2023, 9, 15)]),
        }
    )

    write_deltalake(table_or_uri=tmp_path, mode="append", data=data, partition_by=["t"])
    dt = DeltaTable(table_uri=tmp_path)
    dataset = dt.to_pyarrow_dataset()

    assert dataset.count_rows() == 1


@pytest.mark.parametrize("engine", ["rust", "pyarrow"])
def test_invalid_decimals(tmp_path: pathlib.Path, engine):
    import re

    data = pa.table(
        {"x": pa.array([Decimal("10000000000000000000000000000000000000.0")])}
    )

    with pytest.raises(
        SchemaMismatchError,
        match=re.escape("Invalid data type for Delta Lake: Decimal256(39, 1)"),
    ):
        write_deltalake(table_or_uri=tmp_path, mode="append", data=data, engine=engine)


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_write_large_decimal(tmp_path: pathlib.Path, engine):
    data = pa.table(
        {
            "decimal_column": pa.array(
                [Decimal(11111111111111111), Decimal(22222), Decimal("333333333333.33")]
            )
        }
    )

    write_deltalake(tmp_path, data, engine=engine)


def test_float_values(tmp_path: pathlib.Path):
    data = pa.table(
        {
            "id": pa.array(range(4)),
            "x1": pa.array([0.0, inf, None, 1.0]),
            "x2": pa.array([0.0, -inf, None, 1.0]),
        }
    )
    write_deltalake(tmp_path, data)
    dt = DeltaTable(tmp_path)
    assert dt.to_pyarrow_table() == data

    actions = dt.get_add_actions()
    # x1 has no max, since inf was the highest value
    assert actions["min"].field("x1")[0].as_py() == -0.0
    assert actions["max"].field("x1")[0].as_py() is None
    assert actions["null_count"].field("x1")[0].as_py() == 1
    # x2 has no min, since -inf was the lowest value
    assert actions["min"].field("x2")[0].as_py() is None
    assert actions["max"].field("x2")[0].as_py() == 1.0
    assert actions["null_count"].field("x2")[0].as_py() == 1


def test_with_deltalake_schema(tmp_path: pathlib.Path, sample_data: pa.Table):
    write_deltalake(
        tmp_path,
        sample_data,
        engine="pyarrow",
        schema=Schema.from_pyarrow(sample_data.schema),
    )
    delta_table = DeltaTable(tmp_path)
    assert delta_table.schema().to_pyarrow() == sample_data.schema


def test_with_deltalake_json_schema(tmp_path: pathlib.Path):
    json_schema = '{"type": "struct","fields": [{"name": "campaign", "type": "string", "nullable": true, "metadata": {}},{"name": "account", "type": "string", "nullable": true, "metadata": {}}]}'
    table_schema = Schema.from_json(json_schema)
    table = pa.table(
        {
            "campaign": pa.array([]),
            "account": pa.array([]),
        }
    )
    write_deltalake(tmp_path, table, engine="pyarrow", schema=table_schema)
    table = pa.table(
        {
            "campaign": pa.array(["deltaLake"]),
            "account": pa.array(["admin"]),
        }
    )

    write_deltalake(
        tmp_path, data=table, engine="pyarrow", schema=table_schema, mode="append"
    )

    delta_table = DeltaTable(tmp_path)
    assert delta_table.schema() == table_schema
    assert delta_table.to_pyarrow_table() == table


def test_write_stats_empty_rowgroups(tmp_path: pathlib.Path):
    # https://github.com/delta-io/delta-rs/issues/2169
    data = pa.table(
        {
            "data": pa.array(["B"] * 1024 * 33),
        }
    )
    write_deltalake(
        tmp_path,
        data,
        max_rows_per_file=1024 * 32,
        max_rows_per_group=1024 * 16,
        min_rows_per_group=8 * 1024,
        mode="overwrite",
    )
    dt = DeltaTable(tmp_path)
    assert (
        dt.to_pyarrow_dataset().to_table(filter=(pc.field("data") == "B")).shape[0]
        == 33792
    )


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_schema_cols_diff_order(tmp_path: pathlib.Path, engine):
    data = pa.table(
        {
            "foo": pa.array(["B"] * 10),
            "bar": pa.array([1] * 10),
            "baz": pa.array([2.0] * 10),
        }
    )
    write_deltalake(tmp_path, data, mode="append", engine=engine)

    data = pa.table(
        {
            "baz": pa.array([2.0] * 10),
            "bar": pa.array([1] * 10),
            "foo": pa.array(["B"] * 10),
        }
    )
    write_deltalake(tmp_path, data, mode="append", engine=engine)
    dt = DeltaTable(tmp_path)
    assert dt.version() == 1

    expected = pa.table(
        {
            "baz": pa.array([2.0] * 20),
            "bar": pa.array([1] * 20),
            "foo": pa.array(["B"] * 20),
        }
    )

    assert dt.to_pyarrow_table(columns=["baz", "bar", "foo"]) == expected


def test_empty(existing_table: DeltaTable):
    schema = existing_table.schema().to_pyarrow()
    empty_table = pa.Table.from_pylist([], schema=schema)
    with pytest.raises(DeltaError, match="No data source supplied to write command"):
        write_deltalake(existing_table, empty_table, mode="append", engine="rust")


def test_rust_decimal_cast(tmp_path: pathlib.Path):
    import re

    data = pa.table({"x": pa.array([Decimal("100.1")])})

    write_deltalake(tmp_path, data, mode="append", engine="rust")

    assert DeltaTable(tmp_path).to_pyarrow_table()["x"][0].as_py() == Decimal("100.1")

    # Write smaller decimal,  works since it's fits in the previous decimal precision, scale
    data = pa.table({"x": pa.array([Decimal("10.1")])})
    write_deltalake(tmp_path, data, mode="append", engine="rust")

    data = pa.table({"x": pa.array([Decimal("1000.1")])})
    # write decimal that is larger than target type in table
    with pytest.raises(
        SchemaMismatchError,
        match=re.escape(
            "Cannot cast field x from Decimal128(5, 1) to Decimal128(4, 1)"
        ),
    ):
        write_deltalake(tmp_path, data, mode="append", engine="rust")

    with pytest.raises(SchemaMismatchError):
        write_deltalake(
            tmp_path, data, mode="append", schema_mode="merge", engine="rust"
        )


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_write_stats_column_idx(tmp_path: pathlib.Path, engine):
    def _check_stats(dt: DeltaTable):
        add_actions_table = dt.get_add_actions(flatten=True)
        stats = add_actions_table.to_pylist()[0]

        assert stats["null_count.foo"] == 2
        assert stats["min.foo"] == "a"
        assert stats["max.foo"] == "b"
        assert stats["null_count.bar"] == 1
        assert stats["min.bar"] == 1
        assert stats["max.bar"] == 3
        assert stats["null_count.baz"] is None
        assert stats["min.baz"] is None
        assert stats["max.baz"] is None

    data = pa.table(
        {
            "foo": pa.array(["a", "b", None, None]),
            "bar": pa.array([1, 2, 3, None]),
            "baz": pa.array([1, 1, None, None]),
        }
    )
    write_deltalake(
        tmp_path,
        data,
        mode="append",
        engine=engine,
        configuration={"delta.dataSkippingNumIndexedCols": "2"},
    )

    dt = DeltaTable(tmp_path)
    _check_stats(dt)

    # Check if it properly takes skippingNumIndexCols from the config in the table
    write_deltalake(tmp_path, data, mode="overwrite", engine=engine)

    dt = DeltaTable(tmp_path)
    assert dt.version() == 1
    _check_stats(dt)


def test_write_stats_columns_stats_provided(tmp_path: pathlib.Path):
    def _check_stats(dt: DeltaTable):
        add_actions_table = dt.get_add_actions(flatten=True)
        stats = add_actions_table.to_pylist()[0]

        assert stats["null_count.foo"] == 2
        assert stats["min.foo"] == "a"
        assert stats["max.foo"] == "b"
        assert stats["null_count.bar"] is None
        assert stats["min.bar"] is None
        assert stats["max.bar"] is None
        assert stats["null_count.baz"] == 2
        assert stats["min.baz"] == 1
        assert stats["max.baz"] == 1

    data = pa.table(
        {
            "foo": pa.array(["a", "b", None, None]),
            "bar": pa.array([1, 2, 3, None]),
            "baz": pa.array([1, 1, None, None]),
        }
    )
    write_deltalake(
        tmp_path,
        data,
        mode="append",
        configuration={"delta.dataSkippingStatsColumns": "foo,`baz`"},
    )

    dt = DeltaTable(tmp_path)
    _check_stats(dt)

    # Check if it properly takes skippingNumIndexCols from the config in the table
    write_deltalake(tmp_path, data, mode="overwrite")

    dt = DeltaTable(tmp_path)
    assert dt.version() == 1
    _check_stats(dt)


@pytest.mark.parametrize(
    "array",
    [
        pa.array([[datetime(2010, 1, 1)]]),
        pa.array([{"foo": datetime(2010, 1, 1)}]),
        pa.array([{"foo": [[datetime(2010, 1, 1)]]}]),
        pa.array([{"foo": [[{"foo": datetime(2010, 1, 1)}]]}]),
    ],
)
def test_write_timestamp_ntz_nested(tmp_path: pathlib.Path, array: pa.array):
    data = pa.table({"x": array})
    write_deltalake(tmp_path, data, mode="append", engine="rust")

    dt = DeltaTable(tmp_path)

    protocol = dt.protocol()
    assert protocol.min_reader_version == 3
    assert protocol.min_writer_version == 7
    assert protocol.reader_features == ["timestampNtz"]
    assert protocol.writer_features == ["timestampNtz"]


def test_write_timestamp_ntz_on_table_with_features_not_enabled(tmp_path: pathlib.Path):
    data = pa.table({"x": pa.array(["foo"])})
    write_deltalake(tmp_path, data, mode="append", engine="pyarrow")

    dt = DeltaTable(tmp_path)

    protocol = dt.protocol()
    assert protocol.min_reader_version == 1
    assert protocol.min_writer_version == 2

    data = pa.table({"x": pa.array([datetime(2010, 1, 1)])})
    with pytest.raises(
        DeltaError,
        match="Generic DeltaTable error: Writer features must be specified for writerversion >= 7, please specify: TimestampWithoutTimezone",
    ):
        write_deltalake(
            tmp_path, data, mode="overwrite", engine="pyarrow", schema_mode="overwrite"
        )


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_parse_stats_with_new_schema(tmp_path, engine):
    sample_data = pa.table(
        {
            "val": pa.array([1, 1], pa.int8()),
        }
    )
    write_deltalake(tmp_path, sample_data)

    sample_data = pa.table(
        {
            "val": pa.array([1000000000000, 1000000000000], pa.int64()),
        }
    )
    write_deltalake(
        tmp_path, sample_data, mode="overwrite", schema_mode="overwrite", engine=engine
    )


def test_roundtrip_cdc_evolution(tmp_path: pathlib.Path):
    """
    This test is used as a CDC integration test from Python to ensure,
    approximately, that CDC files are being written
    """
    raw_commit = r"""{"metaData":{"id":"bb0fdeb2-76dd-4f5e-b1ea-845ecec8fa7e","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1713110303902}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":4,"writerFeatures":["changeDataFeed"]}}
"""
    # timestampNtz looks like it might be an unnecessary requirement to write from Python
    os.mkdir(os.path.join(tmp_path, "_delta_log"))
    # This is a stupid hack to make sure we have a CDC capable table from the jump
    with open(
        os.path.join(tmp_path, "_delta_log", "00000000000000000000.json"), "w+"
    ) as fd:
        fd.write(raw_commit)
    assert ("0" * 20 + ".json") in os.listdir(tmp_path / "_delta_log")

    # Make sure the _change_data doesn't exist
    assert not os.path.isdir(os.path.join(tmp_path, "_change_data"))

    nrows = 5
    sample_data = pa.table(
        {
            "utf8": pa.array([str(x) for x in range(nrows)]),
            "int64": pa.array(list(range(nrows)), pa.int64()),
            # See <https://github.com/delta-io/delta-rs/issues/2568>
            # "struct": pa.array([{"x": x, "y": str(x)} for x in range(nrows)]),
            # "list": pa.array([list(range(x + 1)) for x in range(nrows)]),
        }
    )

    write_deltalake(
        tmp_path, sample_data, mode="append", schema_mode="merge", engine="rust"
    )
    assert ("0" * 19 + "1.json") in os.listdir(tmp_path / "_delta_log")

    delta_table = DeltaTable(tmp_path)
    delta_table.update(predicate="utf8 = '1'", updates={"utf8": "'hello world'"})

    delta_table = DeltaTable(tmp_path)
    print(os.listdir(tmp_path))
    # This is kind of a weak test to verify that CDFs were written
    assert os.path.isdir(os.path.join(tmp_path, "_change_data"))


def test_empty_dataset_write(tmp_path: pathlib.Path, sample_data: pa.Table):
    empty_arrow_table = sample_data.schema.empty_table()
    empty_dataset = dataset(empty_arrow_table)
    with pytest.raises(DeltaError, match="No data source supplied to write command"):
        write_deltalake(tmp_path, empty_dataset, mode="append")


@pytest.mark.pandas
def test_predicate_out_of_bounds(tmp_path: pathlib.Path):
    """See <https://github.com/delta-io/delta-rs/issues/2867>"""
    import pandas as pd

    data = [
        (datetime(2024, 7, 31, 9, 30, 0), "AAPL", "20240731", 100, 11.1),
        (datetime(2024, 7, 31, 9, 30, 0), "GOOG", "20240731", 200, 11.1),
    ]
    columns = ["ts", "ins", "date", "f1", "f2"]
    df = pd.DataFrame(data, columns=columns)

    predicate = "date == 20240731"
    write_deltalake(
        table_or_uri=tmp_path,
        data=df,
        partition_by="date",
        mode="overwrite",
        schema_mode="merge",
        predicate=predicate,
    )

    data = [
        (datetime(2024, 7, 31, 9, 30, 0), "AAPL", "20240731", 666, 666),
        (datetime(2024, 7, 31, 9, 30, 0), "GOOG", "20240731", 777, 777),
    ]
    columns = ["ts", "ins", "date", "fb", "fc"]
    df = pd.DataFrame(data, columns=columns)
    write_deltalake(
        table_or_uri=tmp_path,
        data=df,
        partition_by="date",
        mode="overwrite",
        schema_mode="merge",
        predicate=predicate,
    )
