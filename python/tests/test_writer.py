import contextlib
import json
import os
import pathlib
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING
from urllib.request import urlopen

import pytest
from arro3.core import Array, ChunkedArray, DataType, RecordBatchReader, Table
from arro3.core import Field as ArrowField
from arro3.core import Schema as ArrowSchema

from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake
from deltalake._internal import (
    CommitFailedError,
    Field,
    PrimitiveType,
    Schema,
    StructType,
)
from deltalake.exceptions import (
    DeltaError,
    SchemaMismatchError,
)
from deltalake.query import QueryBuilder
from deltalake.writer._utils import try_get_table_and_table_uri

if TYPE_CHECKING:
    import pyarrow as pa


@pytest.mark.skip(reason="Waiting on #570")
def test_handle_existing(
    tmp_path: pathlib.Path,
    sample_data: Table,
):
    # if uri points to a non-empty directory that isn't a delta table, error
    tmp_path
    p = tmp_path / "hello.txt"
    p.write_text("hello")

    with pytest.raises(OSError) as exception:
        write_deltalake(tmp_path, sample_data, mode="overwrite")

    assert "directory is not empty" in str(exception)


@pytest.mark.pyarrow
def test_roundtrip_basic(
    tmp_path: pathlib.Path,
    sample_data_pyarrow: "pa.Table",
):
    # Check we can create the subdirectory
    import pyarrow as pa

    tmp_path = tmp_path / "path" / "to" / "table"
    start_time = datetime.now().timestamp()
    write_deltalake(tmp_path, sample_data_pyarrow)
    end_time = datetime.now().timestamp()

    assert ("0" * 20 + ".json") in os.listdir(tmp_path / "_delta_log")

    delta_table = DeltaTable(tmp_path)
    assert pa.schema(delta_table.schema()) == sample_data_pyarrow.schema

    table = delta_table.to_pyarrow_table()
    assert table == sample_data_pyarrow

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


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_enforce_schema_rust_writer(existing_sample_table: DeltaTable, mode: str):
    bad_data = Table(
        {
            "x": Array(
                [1, 2, 3],
                ArrowField("x", type=DataType.int64(), nullable=False),
            ),
        }
    )

    with pytest.raises(
        SchemaMismatchError,
        match=".*Cannot cast schema, number of fields does not match.*",
    ):
        write_deltalake(
            table_or_uri=existing_sample_table,
            data=bad_data,
            mode=mode,
        )

    table_uri = existing_sample_table._table.table_uri()
    with pytest.raises(
        SchemaMismatchError,
        match=".*Cannot cast schema, number of fields does not match.*",
    ):
        write_deltalake(
            table_uri,
            bad_data,
            mode=mode,
        )


def test_update_schema(existing_sample_table: DeltaTable):
    new_data = Table(
        {
            "x": Array(
                [1, 2, 3],
                ArrowField("x", type=DataType.int64(), nullable=False),
            ),
        }
    )

    with pytest.raises(DeltaError):
        write_deltalake(
            existing_sample_table, new_data, mode="append", schema_mode="overwrite"
        )

    write_deltalake(
        existing_sample_table, new_data, mode="overwrite", schema_mode="overwrite"
    )

    read_data = (
        QueryBuilder()
        .register("tbl", existing_sample_table)
        .execute("select * from tbl")
        .read_all()
    )
    assert new_data == read_data


@pytest.mark.pyarrow
def test_merge_schema(existing_table: DeltaTable):
    import pyarrow as pa

    old_table_data = existing_table.to_pyarrow_table()
    new_data = pa.table(
        {
            "new_x": pa.array([1, 2, 3], pa.int32()),
            "new_y": pa.array([1, 2, 3], pa.int32()),
        }
    )

    write_deltalake(
        existing_table,
        new_data,
        mode="append",
        schema_mode="merge",
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
    concated = pa.concat_tables([old_table_data, new_data])
    assert read_data == concated

    write_deltalake(existing_table, new_data, mode="overwrite", schema_mode="overwrite")

    assert pa.schema(existing_table.schema()) == new_data.schema


@pytest.mark.xfail(reason="update schema code to allow schemaexportable as input")
def test_overwrite_schema(existing_table: DeltaTable):
    new_data_invalid = Table(
        {
            "utf8": Array(
                [1235, 546, 5645],
                ArrowField("utf8", type=DataType.int64(), nullable=False),
            ),
            "new_x": Array(
                [1, 2, 3],
                ArrowField("new_x", type=DataType.int32(), nullable=False),
            ),
            "new_y": Array(
                [1, 2, 3],
                ArrowField("new_y", type=DataType.int32(), nullable=False),
            ),
        }
    )

    with pytest.raises(DeltaError):
        write_deltalake(
            existing_table,
            new_data_invalid,
            mode="append",
            schema_mode="overwrite",
        )

    new_data = Table(
        {
            "utf8": Array(
                ["bla", "bli", "blubb"],
                ArrowField("utf8", type=DataType.utf8(), nullable=False),
            ),
            "new_x": Array(
                [1, 2, 3],
                ArrowField("new_x", type=DataType.int32(), nullable=False),
            ),
            "new_y": Array(
                [1, 2, 3],
                ArrowField("new_y", type=DataType.int32(), nullable=False),
            ),
        }
    )

    with pytest.raises(DeltaError):
        write_deltalake(
            existing_table,
            new_data,
            mode="append",
            schema_mode="overwrite",
        )

    write_deltalake(existing_table, new_data, mode="overwrite", schema_mode="overwrite")

    assert existing_table.schema().to_arrow() == new_data.schema


def test_update_schema_rust_writer_append(existing_sample_table: DeltaTable):
    with pytest.raises(SchemaMismatchError):
        # It's illegal to do schema drift without correct schema_mode
        write_deltalake(
            existing_sample_table,
            Table(
                {
                    "x4": Array(
                        [1, 2, 3],
                        ArrowField("x4", type=DataType.int64(), nullable=False),
                    ),
                }
            ),
            mode="append",
            schema_mode=None,
        )
    with pytest.raises(DeltaError):
        write_deltalake(  # schema_mode overwrite is illegal with append
            existing_sample_table,
            Table(
                {
                    "x1": Array(
                        [1, 2, 3],
                        ArrowField("x1", type=DataType.int64(), nullable=False),
                    ),
                }
            ),
            mode="append",
            schema_mode="overwrite",
        )
    write_deltalake(
        existing_sample_table,
        Table(
            {
                "x2": Array(
                    [1, 2, 3],
                    ArrowField("x2", type=DataType.int64(), nullable=False),
                ),
            }
        ),
        mode="append",
        schema_mode="merge",
    )


@pytest.mark.pyarrow
def test_write_type_castable_types(existing_table: DeltaTable):
    write_deltalake(
        existing_table,
        Table(
            {
                "utf8": Array(
                    ["1", "2", "3"],
                    ArrowField("utf8", type=DataType.string(), nullable=False),
                ),
            }
        ),
        mode="append",
        schema_mode="merge",
    )
    with pytest.raises(
        Exception,
        match="Cast error: Cannot cast string 'hello' to value of Int8 type",
    ):
        write_deltalake(
            existing_table,
            Table(
                {
                    "int8": Array(
                        ["hello", "2", "3"],
                        ArrowField("int8", type=DataType.string(), nullable=False),
                    ),
                }
            ),
            mode="append",
            schema_mode="merge",
        )

    with pytest.raises(
        Exception,
        match="Cast error: Can't cast value 1000 to type Int8",
    ):
        write_deltalake(
            existing_table,
            Table(
                {
                    "int8": Array(
                        [1000, 100, 10],
                        ArrowField("int8", type=DataType.int32(), nullable=False),
                    ),
                }
            ),
            mode="append",
            schema_mode="merge",
        )


def test_update_schema_rust_writer_invalid(existing_sample_table: DeltaTable):
    new_data = Table(
        {
            "x5": Array(
                [1, 2, 3],
                ArrowField("x5", type=DataType.int64(), nullable=False),
            ),
        }
    )

    with pytest.raises(
        SchemaMismatchError, match="Cannot cast schema, number of fields does not match"
    ):
        write_deltalake(
            existing_sample_table,
            new_data,
            mode="overwrite",
            schema_mode=None,
        )

    write_deltalake(
        existing_sample_table,
        new_data,
        mode="overwrite",
        schema_mode="overwrite",
    )

    read_data = (
        QueryBuilder()
        .register("tbl", existing_sample_table)
        .execute("select * from tbl")
        .read_all()
    )
    assert new_data == read_data


def test_merge_schema_rust_writer_with_overwrite(tmp_path: pathlib.Path):
    data = Table(
        {
            "a": Array(
                [1, 2, 3, 4],
                ArrowField("a", type=DataType.int64(), nullable=True),
            ),
            "b": Array(
                [1, 1, 2, 2],
                ArrowField("b", type=DataType.int64(), nullable=True),
            ),
            "c": Array(
                [10, 11, 12, 13],
                ArrowField("c", type=DataType.int64(), nullable=True),
            ),
        }
    )

    write_deltalake(
        tmp_path,
        data,
    )

    new_data = Table(
        {
            "a": Array(
                [100, 200, 300],
                ArrowField("a", type=DataType.int64(), nullable=True),
            ),
            "b": Array(
                [1, 1, 1],
                ArrowField("b", type=DataType.int64(), nullable=True),
            ),
        }
    )

    write_deltalake(
        tmp_path,
        new_data,
        mode="overwrite",
        schema_mode="merge",
    )
    dt = DeltaTable(tmp_path)
    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl")
        .read_all()
        .column_names
    )
    assert set(result) == set(["a", "b", "c"])


@pytest.mark.pyarrow
def test_local_path(
    tmp_path: pathlib.Path,
    sample_table: Table,
    monkeypatch,
):
    import pyarrow as pa

    monkeypatch.chdir(tmp_path)  # Make tmp_path the working directory
    (tmp_path / "path/to/table").mkdir(parents=True)

    local_path = "./path/to/table"
    write_deltalake(local_path, sample_table)
    delta_table = DeltaTable(local_path)

    table = (
        QueryBuilder()
        .register("tbl", delta_table)
        .execute("select * from tbl")
        .read_all()
    )
    assert pa.table(table).to_pydict() == pa.table(sample_table).to_pydict()


@pytest.mark.pyarrow
def test_local_path_with_unsafe_rename(
    tmp_path: pathlib.Path,
    sample_table: Table,
    monkeypatch,
):
    import pyarrow as pa

    monkeypatch.chdir(tmp_path)  # Make tmp_path the working directory
    (tmp_path / "path/to/table").mkdir(parents=True)

    local_path = "./path/to/table"
    storage_opts = {
        "allow_unsafe_rename": "true",
    }
    write_deltalake(local_path, sample_table, storage_options=storage_opts)
    delta_table = DeltaTable(local_path, storage_options=storage_opts)

    table = (
        QueryBuilder()
        .register("tbl", delta_table)
        .execute("select * from tbl")
        .read_all()
    )
    assert pa.table(table).to_pydict() == pa.table(sample_table).to_pydict()


def test_roundtrip_metadata(tmp_path: pathlib.Path, sample_table: Table):
    write_deltalake(
        tmp_path,
        sample_table,
        name="test_name",
        description="test_desc",
        configuration={"delta.appendOnly": "false"},
    )

    delta_table = DeltaTable(tmp_path)

    metadata = delta_table.metadata()

    assert metadata.name == "test_name"
    assert metadata.description == "test_desc"
    assert metadata.configuration == {"delta.appendOnly": "false"}


@pytest.mark.pyarrow
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
    tmp_path: pathlib.Path, sample_data_pyarrow: "pa.Table", column: str
):
    import pyarrow as pa
    import pyarrow.compute as pc

    write_deltalake(tmp_path, sample_data_pyarrow, partition_by=column)

    delta_table = DeltaTable(tmp_path)
    assert pa.schema(delta_table.schema()) == sample_data_pyarrow.schema

    table = delta_table.to_pyarrow_table()
    table = table.take(pc.sort_indices(table["int64"]))
    assert table == sample_data_pyarrow

    for add_path in get_add_paths(delta_table):
        # Paths should be relative
        assert add_path.count("/") == 1


@pytest.mark.pyarrow
def test_roundtrip_null_partition(
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    import pyarrow as pa

    sample_table = sample_table.add_column(
        4,
        "utf8_with_nulls",
        ChunkedArray(
            Array(
                ["a", "a", "a", "a", None],
                type=ArrowField("utf8_with_nulls", DataType.string(), nullable=True),
            )
        ),
    )

    write_deltalake(
        tmp_path,
        sample_table,
        partition_by=["utf8_with_nulls"],
    )

    delta_table = DeltaTable(tmp_path)

    table = (
        QueryBuilder()
        .register("tbl", delta_table)
        .execute("select * from tbl order by price asc")
        .read_all()
    )
    assert pa.table(table).to_pydict() == pa.table(sample_table).to_pydict()


@pytest.mark.pyarrow
def test_roundtrip_multi_partitioned(
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    import pyarrow as pa

    write_deltalake(tmp_path, sample_table, partition_by=["sold", "price"])

    delta_table = DeltaTable(tmp_path)

    table = (
        QueryBuilder()
        .register("tbl", delta_table)
        .execute("select id, price, sold, deleted from tbl order by id asc")
        .read_all()
    )
    assert pa.table(table).to_pydict() == pa.table(sample_table).to_pydict()

    for add_path in get_add_paths(delta_table):
        # Paths should be relative
        assert add_path.count("/") == 2


@pytest.mark.pyarrow
def test_write_modes(tmp_path: pathlib.Path, sample_table: Table):
    import pyarrow as pa

    write_deltalake(
        tmp_path,
        sample_table,
    )
    data = (
        QueryBuilder()
        .register("tbl", DeltaTable(tmp_path))
        .execute("select * from tbl")
        .read_all()
    )
    assert pa.table(data).to_pydict() == pa.table(sample_table).to_pydict()

    with pytest.raises(DeltaError):
        write_deltalake(tmp_path, sample_table, mode="error")

    write_deltalake(
        tmp_path,
        sample_table,
        mode="ignore",
    )
    assert ("0" * 19 + "1.json") not in os.listdir(tmp_path / "_delta_log")

    write_deltalake(
        tmp_path,
        sample_table,
        mode="append",
    )
    expected = RecordBatchReader.from_batches(
        sample_table.schema, [*sample_table.to_batches(), *sample_table.to_batches()]
    ).read_all()
    data = (
        QueryBuilder()
        .register("tbl", DeltaTable(tmp_path))
        .execute("select * from tbl")
        .read_all()
    )
    assert pa.table(data).to_pydict() == pa.table(expected).to_pydict()

    write_deltalake(
        tmp_path,
        sample_table,
        mode="overwrite",
    )
    data = (
        QueryBuilder()
        .register("tbl", DeltaTable(tmp_path))
        .execute("select * from tbl")
        .read_all()
    )
    assert pa.table(data).to_pydict() == pa.table(sample_table).to_pydict()


@pytest.mark.pyarrow
def test_append_only_should_append_only_with_the_overwrite_mode(  # Create rust equivalent rust
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    import pyarrow as pa

    config = {"delta.appendOnly": "true"}

    write_deltalake(
        tmp_path,
        sample_table,
        mode="append",
        configuration=config,
    )

    table = DeltaTable(tmp_path)
    write_deltalake(
        table,
        sample_table,
        mode="append",
    )
    from deltalake.exceptions import CommitFailedError

    with pytest.raises(
        CommitFailedError,
        match="The transaction includes Remove action with data change but Delta table is append-only",
    ):
        write_deltalake(
            table,
            sample_table,
            mode="overwrite",
        )

    expected = RecordBatchReader.from_batches(
        sample_table.schema, [*sample_table.to_batches(), *sample_table.to_batches()]
    ).read_all()

    data = QueryBuilder().register("tbl", table).execute("select * from tbl").read_all()
    assert pa.table(data).to_pydict() == pa.table(expected).to_pydict()
    assert table.version() == 1


@pytest.mark.pyarrow
def test_writer_with_table(existing_sample_table: DeltaTable, sample_table: Table):
    import pyarrow as pa

    write_deltalake(existing_sample_table, sample_table, mode="overwrite")
    data = (
        QueryBuilder()
        .register("tbl", existing_sample_table)
        .execute("select * from tbl")
        .read_all()
    )
    assert pa.table(data).to_pydict() == pa.table(sample_table).to_pydict()


@pytest.mark.pyarrow
def test_fails_wrong_partitioning(
    existing_table: DeltaTable,
    sample_data_pyarrow: "pa.Table",
):
    with pytest.raises(
        DeltaError,
        match='Generic error: Specified table partitioning does not match table partitioning: expected: [], got: ["int32"]',
    ):
        write_deltalake(
            existing_table,
            sample_data_pyarrow,
            mode="append",
            partition_by="int32",
        )


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_write_pandas(tmp_path: pathlib.Path, sample_data_pyarrow: "pa.Table"):
    from pandas.testing import assert_frame_equal

    # When timestamp is converted to Pandas, it gets casted to ns resolution,
    # but Delta Lake schemas only support us resolution.
    sample_pandas = sample_data_pyarrow.to_pandas()
    write_deltalake(tmp_path, sample_pandas)
    delta_table = DeltaTable(tmp_path)
    df = delta_table.to_pandas()
    assert_frame_equal(df, sample_pandas)


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_to_pandas_with_types_mapper(tmp_path: pathlib.Path):
    """Test that DeltaTable.to_pandas() retains PyArrow Decimal type when using types_mapper."""
    import pandas as pd
    import pyarrow as pa

    schema = pa.schema(
        [
            ("id", pa.int32()),
            ("amount", pa.decimal128(18, 0)),
        ]
    )

    decimal_values = [Decimal("100"), Decimal("200"), Decimal("300")]

    data = pa.table(
        [
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array(decimal_values, type=pa.decimal128(18, 0)),
        ],
        schema=schema,
    )

    delta_path = str(tmp_path / "delta_table")
    write_deltalake(delta_path, data)

    dt = DeltaTable(delta_path)

    def types_mapper(pa_type):
        if pa.types.is_decimal(pa_type):
            return pd.ArrowDtype(pa_type)
        return None

    df = dt.to_pandas(types_mapper=types_mapper)

    assert df.dtypes["amount"].pyarrow_dtype == pa.decimal128(18, 0), (
        "amount column should be Decimal(18, 0)"
    )


@pytest.mark.pyarrow
@pytest.mark.parametrize(
    "constructor",
    [
        lambda table: table.to_pyarrow_table(),
        lambda table: table.to_pyarrow_table().to_batches()[0],
    ],
)
def test_write_dataset_table_recordbatch(
    tmp_path: pathlib.Path,
    existing_table: DeltaTable,
    sample_data_pyarrow: "pa.Table",
    constructor,
):
    dataset = constructor(existing_table)
    write_deltalake(tmp_path, dataset, mode="overwrite")
    assert DeltaTable(tmp_path).to_pyarrow_table() == sample_data_pyarrow


@pytest.mark.pyarrow
def test_write_recordbatchreader(
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    import pyarrow as pa

    reader = RecordBatchReader.from_arrow(sample_table)
    write_deltalake(tmp_path, reader, mode="overwrite")
    table = (
        QueryBuilder()
        .register("tbl", DeltaTable(tmp_path))
        .execute("select * from tbl")
        .read_all()
    )
    assert pa.table(table).to_pydict() == pa.table(sample_table).to_pydict()


def test_writer_partitioning(tmp_path: pathlib.Path):
    table = Table(
        {
            "p": Array(
                ["a=b", "hello world", "hello%20world"],
                ArrowField("p", type=DataType.string_view(), nullable=False),
            ),
            "x": Array(
                [0, 1, 2],
                ArrowField("x", type=DataType.int64(), nullable=False),
            ),
        }
    )

    write_deltalake(tmp_path, table)
    result = (
        QueryBuilder()
        .register("tbl", DeltaTable(tmp_path))
        .execute("select * from tbl")
        .read_all()
    )
    assert result == table


def get_log_path(table: DeltaTable) -> str:
    """Returns _delta_log path for this delta table."""
    return table._table.table_uri() + "/_delta_log/" + ("0" * 20 + ".json")


def get_add_actions(table: DeltaTable) -> list[dict]:
    log_path = get_log_path(table)

    actions = []

    for line in urlopen(log_path).readlines():
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


def get_add_paths(table: DeltaTable) -> list[str]:
    return [action["path"] for action in get_add_actions(table)]


@pytest.mark.pyarrow
def test_writer_stats(existing_table: DeltaTable, sample_data_pyarrow: "pa.Table"):
    stats = get_stats(existing_table)

    assert stats["numRecords"] == sample_data_pyarrow.num_rows

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
        "timestamp": "2022-01-01T04:00:00Z",
        "struct": {"x": 4, "y": "4"},
    }
    # PyArrow added support for decimal and date32 in 8.0.0
    expected_maxs["decimal"] = 14.0
    expected_maxs["date32"] = "2022-01-05"

    assert stats["maxValues"] == expected_maxs


def test_writer_null_stats(tmp_path: pathlib.Path):
    data = Table(
        {
            "int32": Array(
                [1, None, 2, None],
                ArrowField("int32", type=DataType.int32(), nullable=True),
            ),
            "float64": Array(
                [1.0, None, None, None],
                ArrowField("float64", type=DataType.float64(), nullable=True),
            ),
            "str": Array(
                [None, None, None, None],
                ArrowField("str", type=DataType.string(), nullable=True),
            ),
            "bin": Array(
                [b"bindata", b"bindata", b"bindata", b"bindata"],
                ArrowField("bin", type=DataType.binary(), nullable=False),
            ),
        }
    )

    write_deltalake(tmp_path, data)

    table = DeltaTable(tmp_path)
    stats = get_stats(table)

    expected_nulls = {"int32": 2, "float64": 3, "str": 4}
    assert stats["nullCount"] == expected_nulls


def test_try_get_table_and_table_uri(tmp_path: pathlib.Path):
    def _normalize_path(t):  # who does not love Windows? ;)
        return t[0], t[1].replace("\\", "/") if t[1] else t[1]

    data = Table(
        {
            "vals": Array(
                ["1", "2", "3"],
                ArrowField("vals", type=DataType.string(), nullable=False),
            ),
        }
    )

    table_or_uri = tmp_path / "delta_table"
    write_deltalake(table_or_uri, data)
    delta_table = DeltaTable(table_or_uri)

    # table_or_uri as DeltaTable
    assert _normalize_path(
        try_get_table_and_table_uri(delta_table, None)
    ) == _normalize_path(
        (
            delta_table,
            "file://" + str(tmp_path / "delta_table") + "/",
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


def param_sequence():
    import pyarrow as pa

    return (1, 2, pa.int64(), "1")


def param_sequence2():
    import pyarrow as pa

    return (False, True, pa.bool_(), "false")


def param_sequence3():
    import pyarrow as pa

    return (date(2022, 1, 1), date(2022, 1, 2), pa.date32(), "'2022-01-01'")


@pytest.mark.pyarrow
@pytest.mark.parametrize(
    "func",
    [
        param_sequence,
        param_sequence2,
        param_sequence3,
    ],
)
def test_partition_overwrite(
    tmp_path: pathlib.Path,
    func,
):
    import pyarrow as pa

    value_1, value_2, value_type, filter_string = func()
    sample_data_pyarrow = pa.table(
        {
            "p1": pa.array(["1", "1", "2", "2"], pa.string()),
            "p2": pa.array([value_1, value_2, value_1, value_2], value_type),
            "val": pa.array([1, 1, 1, 1], pa.int64()),
        }
    )
    write_deltalake(
        tmp_path, sample_data_pyarrow, mode="overwrite", partition_by=["p1", "p2"]
    )

    delta_table = DeltaTable(tmp_path)
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == sample_data_pyarrow
    )

    sample_data_pyarrow = pa.table(
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
        sample_data_pyarrow,
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

    sample_data_pyarrow = pa.table(
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
        sample_data_pyarrow,
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
    sample_data_pyarrow = pa.table(
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
        sample_data_pyarrow,
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
    with pytest.raises(Exception, match="Invalid data found:"):
        write_deltalake(
            tmp_path,
            sample_data_pyarrow,
            mode="overwrite",
            predicate=f"p2 < {filter_string}",
        )


@pytest.fixture()
def sample_data_for_partitioning() -> Table:
    return Table(
        {
            "p1": Array(
                ["1", "1", "2", "2"],
                ArrowField("p1", type=DataType.string(), nullable=False),
            ),
            "p2": Array(
                [1, 2, 1, 2],
                ArrowField("p2", type=DataType.int64(), nullable=False),
            ),
            "val": Array(
                [1, 1, 1, 1],
                ArrowField("val", type=DataType.int64(), nullable=False),
            ),
        }
    )


@pytest.mark.pyarrow
@pytest.mark.parametrize(
    "func",
    [
        param_sequence,
        param_sequence2,
        param_sequence3,
    ],
)
def test_replace_where_overwrite(
    tmp_path: pathlib.Path,
    func,
):
    value_1, value_2, value_type, filter_string = func()
    import pyarrow as pa

    table_path = tmp_path

    sample_data_pyarrow = pa.table(
        {
            "p1": pa.array(["1", "1", "2", "2"], pa.string()),
            "p2": pa.array([value_1, value_2, value_1, value_2], value_type),
            "val": pa.array([1, 1, 1, 1], pa.int64()),
        }
    )
    write_data = sample_data_pyarrow

    write_deltalake(table_path, write_data, mode="overwrite")

    delta_table = DeltaTable(table_path)
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == sample_data_pyarrow
    )

    sample_data_pyarrow = pa.table(
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
        sample_data_pyarrow,
        mode="overwrite",
        predicate="p1 = '1'",
    )

    delta_table.update_incremental()
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == expected_data
    )


@pytest.mark.pyarrow
@pytest.mark.parametrize(
    "func",
    [
        param_sequence,
        param_sequence2,
        param_sequence3,
    ],
)
def test_replace_where_overwrite_partitioned(
    tmp_path: pathlib.Path,
    func,
):
    import pyarrow as pa

    value_1, value_2, value_type, filter_string = func()
    table_path = tmp_path

    sample_data_pyarrow = pa.table(
        {
            "p1": pa.array(["1", "1", "2", "2"], pa.string()),
            "p2": pa.array([value_1, value_2, value_1, value_2], value_type),
            "val": pa.array([1, 1, 1, 1], pa.int64()),
        }
    )
    write_deltalake(
        table_path, sample_data_pyarrow, mode="overwrite", partition_by=["p1", "p2"]
    )

    delta_table = DeltaTable(table_path)
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == sample_data_pyarrow
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
    )

    delta_table.update_incremental()
    assert (
        delta_table.to_pyarrow_table().sort_by(
            [("p1", "ascending"), ("p2", "ascending")]
        )
        == expected_data
    )


@pytest.mark.pyarrow
def test_partition_overwrite_with_new_partition(
    tmp_path: pathlib.Path, sample_data_for_partitioning: Table
):
    import pyarrow as pa

    write_deltalake(
        tmp_path,
        sample_data_for_partitioning,
        mode="overwrite",
        partition_by=["p1", "p2"],
    )

    new_sample_data = Table(
        {
            "p1": Array(
                ["1", "2"],
                ArrowField("p1", type=DataType.string(), nullable=False),
            ),
            "p2": Array(
                [2, 2],
                ArrowField("p2", type=DataType.int64(), nullable=False),
            ),
            "val": Array(
                [2, 3],
                ArrowField("val", type=DataType.int64(), nullable=False),
            ),
        }
    )

    expected_data = Table(
        {
            "p1": Array(
                ["1", "1", "2", "2"],
                ArrowField("p1", type=DataType.string(), nullable=False),
            ),
            "p2": Array(
                [1, 2, 1, 2],
                ArrowField("p2", type=DataType.int64(), nullable=False),
            ),
            "val": Array(
                [1, 2, 1, 3],
                ArrowField("val", type=DataType.int64(), nullable=False),
            ),
        }
    )

    write_deltalake(tmp_path, new_sample_data, mode="overwrite", predicate="p2 = 2")
    delta_table = DeltaTable(tmp_path)
    result = (
        QueryBuilder()
        .register("tbl", delta_table)
        .execute("select p1,p2,val from tbl order by p1 asc, p2 asc")
        .read_all()
    )
    assert pa.table(result).to_pydict() == pa.table(expected_data).to_pydict()


def test_partition_overwrite_with_non_partitioned_data(
    tmp_path: pathlib.Path, sample_data_for_partitioning: Table
):
    write_deltalake(tmp_path, sample_data_for_partitioning, mode="overwrite")
    write_deltalake(
        tmp_path,
        Table(
            {
                "p1": Array(
                    ["1", "1"],
                    ArrowField("p1", type=DataType.string(), nullable=False),
                ),
                "p2": Array(
                    [
                        1,
                        2,
                    ],
                    ArrowField("p2", type=DataType.int64(), nullable=False),
                ),
                "val": Array(
                    [1, 1],
                    ArrowField("val", type=DataType.int64(), nullable=False),
                ),
            }
        ),
        mode="overwrite",
        predicate="p1 = 1",
    )


def test_partition_overwrite_with_wrong_partition(
    tmp_path: pathlib.Path, sample_data_for_partitioning: Table
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
        )

    new_data = Table(
        {
            "p1": Array(
                ["1"],
                ArrowField("p1", type=DataType.string(), nullable=False),
            ),
            "p2": Array(
                [2],
                ArrowField("p2", type=DataType.int64(), nullable=False),
            ),
            "val": Array(
                [1],
                ArrowField("val", type=DataType.int64(), nullable=False),
            ),
        }
    )

    with pytest.raises(
        Exception,
        match="Invalid data found: 1 rows failed validation check.",
    ):
        write_deltalake(
            tmp_path,
            new_data,
            mode="overwrite",
            predicate="p1 = 1 AND p2 = 1",
        )


def test_handles_binary_data(tmp_path: pathlib.Path):
    table = Table(
        {
            "field_one": Array(
                [b"\x00\\"],
                ArrowField("field_one", type=DataType.binary_view(), nullable=True),
            ),
        }
    )

    write_deltalake(tmp_path, table)

    dt = DeltaTable(tmp_path)
    out = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    assert table == out


@pytest.mark.pyarrow
def test_large_arrow_types(tmp_path: pathlib.Path):
    import pyarrow as pa

    pylist = [
        {"name": "Joey", "gender": b"M", "arr_type": ["x", "y"], "dict": {"a": b"M"}},
        {"name": "Ivan", "gender": b"F", "arr_type": ["x", "z"]},
    ]
    schema = pa.schema(
        [
            pa.field("name", pa.large_string()),
            pa.field("gender", pa.large_binary()),
            pa.field(
                "arr_type",
                pa.large_list(pa.field("element", pa.large_string(), nullable=False)),
            ),
            pa.field("map_type", pa.map_(pa.large_string(), pa.large_binary())),
            pa.field("struct", pa.struct([pa.field("sub", pa.large_string())])),
        ]
    )
    table = pa.Table.from_pylist(pylist, schema=schema)

    write_deltalake(tmp_path, table, mode="append")
    write_deltalake(tmp_path, table, mode="append")
    write_deltalake(tmp_path, table, mode="append")

    dt = DeltaTable(tmp_path)
    assert table.schema == pa.schema(dt.schema().to_arrow(as_large_types=True))


@pytest.mark.pyarrow
def test_large_arrow_types_dataset_as_large_types(tmp_path: pathlib.Path):
    import pyarrow as pa
    from pyarrow.dataset import dataset

    pylist = [
        {"name": "Joey", "gender": b"M", "arr_type": ["x", "y"], "dict": {"a": b"M"}},
        {"name": "Ivan", "gender": b"F", "arr_type": ["x", "z"]},
    ]
    schema = pa.schema(
        [
            pa.field("name", pa.large_string()),
            pa.field("gender", pa.large_binary()),
            pa.field(
                "arr_type",
                pa.large_list(pa.field("element", pa.large_string(), nullable=False)),
            ),
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


@pytest.mark.pyarrow
def test_large_arrow_types_explicit_scan_schema(tmp_path: pathlib.Path):
    import pyarrow as pa
    from pyarrow.dataset import dataset

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
    table = Table(
        {
            "foo": Array(
                ["1", "1", "2", "2"],
                ArrowField("foo", type=DataType.large_utf8(), nullable=True),
            ),
            "bar": Array(
                [1, 2, 1, 2],
                ArrowField("bar", type=DataType.int64(), nullable=True),
            ),
            "baz": Array(
                [1, 1, 1, 1],
                ArrowField("baz", type=DataType.int64(), nullable=True),
            ),
        }
    )

    write_deltalake(tmp_path, table, partition_by=["foo"])

    dt = DeltaTable(tmp_path)
    files = dt.file_uris()
    expected = ["foo=1", "foo=2"]

    result = sorted([abs_path.split(os.path.sep)[-2] for abs_path in files])
    assert expected == result


def test_uint_arrow_types(tmp_path: pathlib.Path):
    table = Table(
        {
            "num1": Array(
                [3, 1],
                ArrowField("num1", type=DataType.uint8(), nullable=True),
            ),
            "num2": Array(
                [3, 13],
                ArrowField("num2", type=DataType.uint16(), nullable=True),
            ),
            "num3": Array(
                [3, 35],
                ArrowField("num3", type=DataType.uint32(), nullable=True),
            ),
            "num4": Array(
                [5, 13],
                ArrowField("num4", type=DataType.uint64(), nullable=True),
            ),
        }
    )

    write_deltalake(tmp_path, table)


@pytest.mark.pyarrow
def test_issue_1651_roundtrip_timestamp(tmp_path: pathlib.Path):
    import pyarrow as pa

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


@pytest.mark.pyarrow
def test_write_large_decimal(tmp_path: pathlib.Path):
    import pyarrow as pa

    data = pa.table(
        {
            "decimal_column": pa.array(
                [Decimal(11111111111111111), Decimal(22222), Decimal("333333333333.33")]
            )
        }
    )

    write_deltalake(tmp_path, data)


def test_float_values(tmp_path: pathlib.Path):
    data = Table(
        {
            "id": Array(
                [0, 1, 2, 3],
                ArrowField("id", type=DataType.int64(), nullable=True),
            ),
            "x1": Array(
                [0.0, float("inf"), None, 1.0],
                ArrowField("x1", type=DataType.float64(), nullable=True),
            ),
            "x2": Array(
                [0.0, float("-inf"), None, 1.0],
                ArrowField("x2", type=DataType.float64(), nullable=True),
            ),
        }
    )

    write_deltalake(tmp_path, data)
    dt = DeltaTable(tmp_path)
    assert (
        QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
        == data
    )

    actions = dt.get_add_actions()

    def get_value(name: str):
        return actions.column(name)[0].as_py()

    # x1 has no max, since inf was the highest value
    assert get_value("min")["x1"] == -0.0
    assert get_value("max")["x1"] is None
    assert get_value("null_count")["x1"] == 1
    # x2 has no min, since -inf was the lowest value
    assert get_value("min")["x2"] is None
    assert get_value("max")["x2"] == 1.0
    assert get_value("null_count")["x2"] == 1


def test_schema_cols_diff_order(tmp_path: pathlib.Path):
    data = Table(
        {
            "foo": Array(
                ["B"] * 10,
                ArrowField("foo", type=DataType.string(), nullable=True),
            ),
            "bar": Array(
                [1] * 10,
                ArrowField("bar", type=DataType.int64(), nullable=True),
            ),
            "baz": Array(
                [2.0] * 10,
                ArrowField("baz", type=DataType.float64(), nullable=True),
            ),
        }
    )

    write_deltalake(tmp_path, data, mode="append")

    data = Table(
        {
            "baz": Array(
                [2.0] * 10,
                ArrowField("baz", type=DataType.float64(), nullable=True),
            ),
            "bar": Array(
                [1] * 10,
                ArrowField("bar", type=DataType.int64(), nullable=True),
            ),
            "foo": Array(
                ["B"] * 10,
                ArrowField("foo", type=DataType.string_view(), nullable=True),
            ),
        }
    )

    write_deltalake(tmp_path, data, mode="append")
    dt = DeltaTable(tmp_path)
    assert dt.version() == 1

    expected = Table(
        {
            "baz": Array(
                [2.0] * 20,
                ArrowField("baz", type=DataType.float64(), nullable=True),
            ),
            "bar": Array(
                [1] * 20,
                ArrowField("bar", type=DataType.int64(), nullable=True),
            ),
            "foo": Array(
                ["B"] * 20,
                ArrowField("foo", type=DataType.string_view(), nullable=True),
            ),
        }
    )

    assert (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select baz, bar, foo from tbl")
        .read_all()
        .rechunk()
    ) == expected


@pytest.mark.pyarrow
def test_empty(existing_table: DeltaTable):
    import pyarrow as pa

    schema = existing_table.schema()
    expected = existing_table.to_pyarrow_table()
    empty_table = pa.Table.from_pylist([], schema=pa.schema(schema))
    write_deltalake(
        existing_table,
        empty_table,
        mode="append",
    )

    existing_table.update_incremental()
    assert existing_table.version() == 1
    assert expected == existing_table.to_pyarrow_table()


@pytest.mark.pyarrow
def test_rust_decimal_cast(tmp_path: pathlib.Path):
    import re

    import pyarrow as pa

    data = pa.table({"x": pa.array([Decimal("100.1")])})

    write_deltalake(
        tmp_path,
        data,
        mode="append",
    )

    assert DeltaTable(tmp_path).to_pyarrow_table()["x"][0].as_py() == Decimal("100.1")

    # Write smaller decimal,  works since it's fits in the previous decimal precision, scale
    data = pa.table({"x": pa.array([Decimal("10.1")])})
    write_deltalake(
        tmp_path,
        data,
        mode="append",
    )

    data = pa.table({"x": pa.array([Decimal("1000.1")])})
    # write decimal that is larger than target type in table
    with pytest.raises(
        SchemaMismatchError,
        match=re.escape(
            "Cannot cast field x from Decimal128(5, 1) to Decimal128(4, 1)"
        ),
    ):
        write_deltalake(
            tmp_path,
            data,
            mode="append",
        )

    with pytest.raises(SchemaMismatchError):
        write_deltalake(
            tmp_path,
            data,
            mode="append",
            schema_mode="merge",
        )


def test_write_stats_column_idx(tmp_path: pathlib.Path):
    def _check_stats(dt: DeltaTable):
        add_actions_table = dt.get_add_actions(flatten=True)

        def get_value(name: str):
            return add_actions_table.column(name)[0].as_py()

        assert get_value("null_count.foo") == 2
        assert get_value("min.foo") == "a"
        assert get_value("max.foo") == "b"
        assert get_value("null_count.bar") == 1
        assert get_value("min.bar") == 1
        assert get_value("max.bar") == 3

        with pytest.raises(Exception):
            get_value("null_count.baz")

    data = Table(
        {
            "foo": Array(
                ["a", "b", None, None],
                ArrowField("foo", type=DataType.string(), nullable=True),
            ),
            "bar": Array(
                [1, 2, 3, None],
                ArrowField("bar", type=DataType.int64(), nullable=True),
            ),
            "baz": Array(
                [1, 1, None, None],
                ArrowField("baz", type=DataType.int64(), nullable=True),
            ),
        }
    )

    write_deltalake(
        tmp_path,
        data,
        mode="append",
        configuration={"delta.dataSkippingNumIndexedCols": "2"},
    )

    dt = DeltaTable(tmp_path)
    _check_stats(dt)

    # Check if it properly takes skippingNumIndexCols from the config in the table
    write_deltalake(tmp_path, data, mode="overwrite")

    dt = DeltaTable(tmp_path)
    assert dt.version() == 1
    _check_stats(dt)


def test_write_stats_columns_stats_provided(tmp_path: pathlib.Path):
    def _check_stats(dt: DeltaTable):
        add_actions_table = dt.get_add_actions(flatten=True)

        def get_value(name: str):
            return add_actions_table.column(name)[0].as_py()

        assert get_value("null_count.foo") == 2
        assert get_value("min.foo") == "a"
        assert get_value("max.foo") == "b"
        assert get_value("null_count.baz") == 2
        assert get_value("min.baz") == 1
        assert get_value("max.baz") == 1

        with pytest.raises(Exception):
            get_value("null_count.bar")

    data = Table(
        {
            "foo": Array(
                ["a", "b", None, None],
                ArrowField("foo", type=DataType.string(), nullable=True),
            ),
            "bar": Array(
                [1, 2, 3, None],
                ArrowField("bar", type=DataType.int64(), nullable=True),
            ),
            "baz": Array(
                [1, 1, None, None],
                ArrowField("baz", type=DataType.int64(), nullable=True),
            ),
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


@pytest.mark.pyarrow
@pytest.mark.parametrize(
    "array",
    [
        lambda: __import__("pyarrow").array([[datetime(2010, 1, 1)]]),
        lambda: __import__("pyarrow").array([{"foo": datetime(2010, 1, 1)}]),
        lambda: __import__("pyarrow").array([{"foo": [[datetime(2010, 1, 1)]]}]),
        lambda: __import__("pyarrow").array(
            [{"foo": [[{"foo": datetime(2010, 1, 1)}]]}]
        ),
    ],
)
def test_write_timestamp_ntz_nested(tmp_path: pathlib.Path, array):
    import pyarrow as pa

    data = pa.table({"x": array()})
    write_deltalake(
        tmp_path,
        data,
        mode="append",
    )

    dt = DeltaTable(tmp_path)

    protocol = dt.protocol()
    assert protocol.min_reader_version == 3
    assert protocol.min_writer_version == 7
    assert protocol.reader_features == ["timestampNtz"]
    assert protocol.writer_features == ["timestampNtz"]


def test_parse_stats_with_new_schema(tmp_path):
    data = Table(
        {
            "val": Array(
                [1, 1],
                ArrowField("val", type=DataType.int8(), nullable=True),
            ),
        }
    )

    write_deltalake(tmp_path, data)

    data = Table(
        {
            "val": Array(
                [1000000000000, 1000000000000],
                ArrowField("val", type=DataType.int64(), nullable=True),
            ),
        }
    )

    write_deltalake(tmp_path, data, mode="overwrite", schema_mode="overwrite")


def test_roundtrip_cdc_evolution(tmp_path: pathlib.Path):
    """
    This test is used as a CDC integration test from Python to ensure,
    approximately, that CDC files are being written
    """
    raw_commit = r"""{"metaData":{"id":"bb0fdeb2-76dd-4f5e-b1ea-845ecec8fa7e","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1713110303902}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":7,"writerFeatures":["changeDataFeed"]}}
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
    data = Table(
        {
            "utf8": Array(
                [str(x) for x in range(nrows)],
                ArrowField("utf8", type=DataType.utf8(), nullable=True),
            ),
            "int64": Array(
                list(range(nrows)),
                ArrowField("int64", type=DataType.int64(), nullable=True),
            ),
        }
    )

    write_deltalake(
        tmp_path,
        data,
        mode="append",
        schema_mode="merge",
    )
    assert ("0" * 19 + "1.json") in os.listdir(tmp_path / "_delta_log")

    delta_table = DeltaTable(tmp_path)
    delta_table.update(predicate="utf8 = '1'", updates={"utf8": "'hello world'"})

    delta_table = DeltaTable(tmp_path)
    # This is kind of a weak test to verify that CDFs were written
    assert os.path.isdir(os.path.join(tmp_path, "_change_data"))


@pytest.mark.pyarrow
def test_empty_data_write(tmp_path: pathlib.Path, sample_data_pyarrow: "pa.Table"):
    empty_arrow_table = sample_data_pyarrow.schema.empty_table()
    write_deltalake(tmp_path, empty_arrow_table, mode="append")
    dt = DeltaTable(tmp_path)

    new_dataset = dt.to_pyarrow_dataset()
    assert new_dataset.count_rows() == 0


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


@pytest.mark.pandas
def test_write_timestampntz(tmp_path: pathlib.Path):
    import pandas as pd

    data = [
        ("AAPL", "20240731", 100, 11.1),
        ("GOOG", "20240731", 200, 11.1),
    ]
    columns = ["ins", "date", "f1", "f2"]
    df = pd.DataFrame(data, columns=columns)

    write_deltalake(
        table_or_uri=tmp_path,
        data=df,
        partition_by="date",
        mode="overwrite",
    )

    dt = DeltaTable(tmp_path)
    protocol = dt.protocol()
    # A fresh table with no special features should have the lowest possible
    # minwriter feature
    assert protocol.min_writer_version == 2

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
        mode="append",
        schema_mode="merge",
    )

    dt = DeltaTable(tmp_path)
    protocol = dt.protocol()
    # Now that a datetime has been passed through the writer version needs to
    # be upgraded to 7 to support timestampNtz
    assert protocol.min_writer_version == 7


@pytest.mark.pandas
def test_write_timestamp(tmp_path: pathlib.Path):
    import pandas as pd

    data = [
        ("AAPL", "20240731", 100, 11.1),
        ("GOOG", "20240731", 200, 11.1),
    ]
    columns = ["ins", "date", "f1", "f2"]
    df = pd.DataFrame(data, columns=columns)

    write_deltalake(
        table_or_uri=tmp_path,
        data=df,
        partition_by="date",
        mode="overwrite",
    )

    dt = DeltaTable(tmp_path)
    protocol = dt.protocol()
    # A fresh table with no special features should have the lowest possible
    # minwriter feature
    assert protocol.min_writer_version == 2

    # Performing schema evolution with a timestamp that *has* a timezone should
    # not result in a writer version upgrade!
    data = [
        (
            datetime(2024, 7, 31, 9, 30, 0, tzinfo=timezone.utc),
            "AAPL",
            "20240731",
            666,
            666,
        ),
        (
            datetime(2024, 7, 31, 9, 30, 0, tzinfo=timezone.utc),
            "GOOG",
            "20240731",
            777,
            777,
        ),
    ]
    columns = ["ts", "ins", "date", "fb", "fc"]
    df = pd.DataFrame(data, columns=columns)
    write_deltalake(
        table_or_uri=tmp_path,
        data=df,
        partition_by="date",
        mode="append",
        schema_mode="merge",
    )

    # Reload the table to make sure we have the latest protocol
    dt = DeltaTable(tmp_path)
    protocol = dt.protocol()
    # Now that a datetime has been passed through the writer version needs to
    # be upgraded to 7 to support timestampNtz
    assert protocol.min_writer_version == 2


def test_write_transactions(tmp_path: pathlib.Path, sample_table: Table):
    expected_transactions = [
        Transaction(app_id="app_1", version=1),
        Transaction(app_id="app_2", version=2, last_updated=123456),
    ]
    commit_properties = CommitProperties(app_transactions=expected_transactions)
    write_deltalake(
        table_or_uri=tmp_path,
        data=sample_table,
        mode="overwrite",
        schema_mode="overwrite",
        commit_properties=commit_properties,
    )

    delta_table = DeltaTable(tmp_path)
    transaction_1 = delta_table.transaction_version("app_1")
    assert transaction_1 == 1

    transaction_2 = delta_table.transaction_version("app_2")
    assert transaction_2 == 2


@pytest.mark.parametrize(
    "writer1_txn, writer2_txn, update_second_writer, should_fail",
    [
        # writers have the same snapshot, conflict checker should fail second writer
        (
            [Transaction(app_id="shared", version=1)],
            [Transaction(app_id="shared", version=1)],
            False,
            True,
        ),
        # writers for different app_ids should not conflict even on the same snapshot
        (
            [Transaction(app_id="app1", version=1)],
            [Transaction(app_id="app2", version=1)],
            False,
            False,
        ),
        # different versions for same app_id still fail
        (
            [Transaction(app_id="shared", version=1)],
            [Transaction(app_id="shared", version=2)],
            False,
            True,
        ),
        # writes to the same app_id succeed if writer2 uses a fresh snapshot
        (
            [Transaction(app_id="shared", version=1)],
            [Transaction(app_id="shared", version=2)],
            True,
            False,
        ),
        # all concurrent writes need transactions to be eligible to conflict
        # put another way: writes without transactions should not fail
        (
            None,
            None,
            False,
            False,
        ),
        (
            [Transaction(app_id="app1", version=1)],
            None,
            False,
            False,
        ),
    ],
)
def test_write_concurrent_blind_appends(
    tmp_path: pathlib.Path,
    sample_table: Table,
    writer1_txn: list[Transaction],
    writer2_txn: list[Transaction],
    update_second_writer: bool,
    should_fail: bool,
):
    # initialize a table
    write_deltalake(table_or_uri=tmp_path, data=sample_table)
    # create multiple writers
    writer1_table = DeltaTable(tmp_path)
    writer2_table = DeltaTable(tmp_path)
    # "concurrently" write
    write_deltalake(
        writer1_table,
        sample_table,
        mode="append",
        commit_properties=CommitProperties(app_transactions=writer1_txn),
    )
    if update_second_writer:
        writer2_table.update_incremental()
    with pytest.raises(CommitFailedError) if should_fail else contextlib.nullcontext():
        write_deltalake(
            writer2_table,
            sample_table,
            mode="append",
            commit_properties=CommitProperties(app_transactions=writer2_txn),
        )


@pytest.mark.polars
def test_write_structs(tmp_path: pathlib.Path):
    import polars as pl

    dt = DeltaTable.create(
        tmp_path,
        schema=Schema(
            fields=[
                Field(name="a", type=PrimitiveType("integer")),
                Field(name="b", type=PrimitiveType("string")),
                Field(
                    name="c",
                    type=StructType(
                        [
                            Field(name="d", type=PrimitiveType("short")),
                            Field(name="e", type=PrimitiveType("short")),
                        ]
                    ),
                ),
            ]
        ),
    )

    df = pl.DataFrame(
        {
            "a": [0, 1],
            "b": ["x", "y"],
            "c": [
                {"d": -55, "e": -32},
                {"d": 0, "e": 0},
            ],
        }
    )

    dt.merge(
        source=df,
        predicate=" AND ".join([f"target.{x} = source.{x}" for x in ["a"]]),
        source_alias="source",
        target_alias="target",
    ).when_not_matched_insert_all().execute()


@pytest.mark.polars
@pytest.mark.xfail(reason="polars needs update")
def test_write_type_coercion_predicate(tmp_path: pathlib.Path):
    import polars as pl

    df = pl.DataFrame({"A": [1, 2], "B": ["hi", "hello"], "C": ["a", "b"]})
    df.write_delta(tmp_path)

    df = pl.DataFrame({"A": [10], "B": ["yeah"], "C": ["a"]})
    df.write_delta(
        tmp_path,
        mode="overwrite",
        delta_write_options=dict(predicate="C = 'a'"),
    )


@pytest.mark.pyarrow
def test_write_schema_evolved_same_metadata_id(tmp_path):
    """https://github.com/delta-io/delta-rs/issues/3274"""
    import pyarrow as pa

    data_first_write = pa.array(
        [
            {"name": "Alice", "age": 30, "details": {"email": "alice@example.com"}},
            {"name": "Bob", "age": 25, "details": {"email": "bob@example.com"}},
        ]
    )

    data_second_write = pa.array(
        [
            {
                "name": "Charlie",
                "age": 35,
                "details": {"address": "123 Main St", "email": "charlie@example.com"},
            },
            {
                "name": "Diana",
                "age": 28,
                "details": {"address": "456 Elm St", "email": "diana@example.com"},
            },
        ]
    )

    schema_first_write = pa.schema(
        [
            ("name", pa.string()),
            ("age", pa.int64()),
            ("details", pa.struct([("email", pa.string())])),
        ]
    )

    schema_second_write = pa.schema(
        [
            ("name", pa.string()),
            ("age", pa.int64()),
            (
                "details",
                pa.struct(
                    [
                        ("address", pa.string()),
                        ("email", pa.string()),
                    ]
                ),
            ),
        ]
    )
    table_first_write = pa.Table.from_pylist(
        data_first_write, schema=schema_first_write
    )
    table_second_write = pa.Table.from_pylist(
        data_second_write, schema=schema_second_write
    )

    write_deltalake(
        tmp_path,
        table_first_write,
        mode="append",
    )

    first_metadata_id = DeltaTable(tmp_path).metadata().id

    write_deltalake(tmp_path, table_second_write, mode="append", schema_mode="merge")

    second_metadata_id = DeltaTable(tmp_path).metadata().id

    assert first_metadata_id == second_metadata_id


# <https://github.com/delta-io/delta-rs/issues/2854>
@pytest.mark.polars
@pytest.mark.xfail(reason="polars needs update")
def test_write_binary_col_without_dssc(tmp_path: pathlib.Path):
    import polars as pl

    data_with_bin_col = {
        "x": [b"67890", b"abcde", b"xyzw", b"12345"],
        "y": [1, 2, 3, 4],
        "z": [101, 102, None, 104],
    }

    df_with_bin_col = pl.DataFrame(data_with_bin_col)
    df_with_bin_col.write_delta(tmp_path)

    assert len(df_with_bin_col.rows()) == 4

    dt = DeltaTable(tmp_path)
    stats = dt.get_add_actions(flatten=True)
    assert stats["null_count.x"].to_pylist() == [None]
    assert stats["min.x"].to_pylist() == [None]
    assert stats["max.x"].to_pylist() == [None]
    assert stats["null_count.y"].to_pylist() == [0]
    assert stats["min.y"].to_pylist() == [1]
    assert stats["max.y"].to_pylist() == [4]
    assert stats["null_count.z"].to_pylist() == [1]
    assert stats["min.z"].to_pylist() == [101]
    assert stats["max.z"].to_pylist() == [104]


# <https://github.com/delta-io/delta-rs/issues/2854>
@pytest.mark.polars
@pytest.mark.xfail(reason="polars needs update")
def test_write_binary_col_with_dssc(tmp_path: pathlib.Path):
    import polars as pl

    data_with_bin_col = {
        "x": [b"67890", b"abcde", b"xyzw", b"12345"],
        "y": [1, 2, 3, 4],
        "z.z": [101, 102, None, 104],
    }

    df_with_bin_col = pl.DataFrame(data_with_bin_col)
    df_with_bin_col.write_delta(
        tmp_path,
        delta_write_options={
            "configuration": {"delta.dataSkippingStatsColumns": "x,y,`z.z`"},
        },
    )

    assert len(df_with_bin_col.rows()) == 4

    dt = DeltaTable(tmp_path)
    stats = dt.get_add_actions(flatten=True)
    assert stats["null_count.x"].to_pylist() == [None]
    assert stats["min.x"].to_pylist() == [None]
    assert stats["max.x"].to_pylist() == [None]
    assert stats["null_count.y"].to_pylist() == [0]
    assert stats["min.y"].to_pylist() == [1]
    assert stats["max.y"].to_pylist() == [4]
    assert stats["null_count.z.z"].to_pylist() == [1]
    assert stats["min.z.z"].to_pylist() == [101]
    assert stats["max.z.z"].to_pylist() == [104]


@pytest.mark.polars
@pytest.mark.pyarrow
def test_polars_write_array(tmp_path: pathlib.Path):
    """
    https://github.com/delta-io/delta-rs/issues/3566
    """
    import polars as pl

    from deltalake import DeltaTable, write_deltalake

    df = pl.DataFrame(
        {"array": [[-5, -4], [0, 0], [5, 9]]}, schema={"array": pl.Array(pl.Int32, 2)}
    )
    DeltaTable.create(
        tmp_path,
        df.to_arrow().schema,
        mode="overwrite",
    )
    write_deltalake(
        tmp_path,
        df,
        mode="overwrite",
    )


@pytest.mark.polars
def test_tilde_path_works_with_writes():
    import os
    import shutil
    import uuid

    import polars as pl
    import polars.testing as pl_testing

    df = pl.DataFrame({"num": [1, 2, 3], "letter": ["a", "b", "c"]})

    unique_id = str(uuid.uuid4())[:8]
    tilde_path = f"~/tmp_delta_test_{unique_id}"

    try:
        df.write_delta(tilde_path)
        df_read = pl.read_delta(tilde_path)
        pl_testing.assert_frame_equal(df, df_read)
    finally:
        expanded_path = os.path.expanduser(tilde_path)
        if os.path.exists(expanded_path):
            shutil.rmtree(expanded_path)


@pytest.mark.pyarrow
def test_dots_in_column_names_2624(tmp_path: pathlib.Path):
    """
    <https://github.com/delta-io/delta-rs/issues/2624>
    """
    import pyarrow as pa

    initial = pa.Table.from_pydict(
        {
            "Product.Id": ["x-0", "x-1", "x-2", "x-3"],
            "Cost": [10, 11, 12, 13],
        }
    )

    write_deltalake(
        table_or_uri=tmp_path,
        data=initial,
        partition_by=["Product.Id"],
    )

    update = pa.Table.from_pydict(
        {
            "Product.Id": ["x-1"],
            "Cost": [101],
        }
    )

    write_deltalake(
        table_or_uri=tmp_path,
        data=update,
        partition_by=["Product.Id"],
        mode="overwrite",
        predicate="\"Product.Id\" = 'x-1'",
    )

    dt = DeltaTable(tmp_path)
    expected = pa.Table.from_pydict(
        {
            "Product.Id": ["x-0", "x-1", "x-2", "x-3"],
            "Cost": [10, 101, 12, 13],
        }
    )
    # Sorting just to make sure the equivalency matches up
    actual = dt.to_pyarrow_table().sort_by("Product.Id")
    assert expected == actual


def test_url_encoding(tmp_path):
    """issue ref: https://github.com/delta-io/delta-rs/issues/3939"""
    batch_1 = Table.from_pydict(
        {
            "id": Array(["1 2"], DataType.string()),
            "price": Array(list(range(1)), DataType.int64()),
        },
        schema=ArrowSchema(
            fields=[
                ArrowField("id", type=DataType.string(), nullable=True),
                ArrowField("price", type=DataType.int64(), nullable=True),
            ]
        ),
    )
    write_deltalake(tmp_path, batch_1, partition_by="id")

    batch_2 = Table.from_pydict(
        {
            "id": Array(["1 2"], DataType.string()),
            "price": Array([10], DataType.int64()),
        },
        schema=ArrowSchema(
            fields=[
                ArrowField("id", type=DataType.string(), nullable=True),
                ArrowField("price", type=DataType.int64(), nullable=True),
            ]
        ),
    )

    write_deltalake(tmp_path, batch_2, mode="overwrite", predicate="id = '1 2'")


@pytest.mark.pyarrow
def test_url_encoding_timestamp(tmp_path):
    import datetime as dt

    import pyarrow as pa

    # (step 1) write initial table
    data = pa.Table.from_pylist(
        [
            {"time": dt.datetime(2026, 1, 1, 12, 5, 7), "value": 10},
        ]
    )

    write_deltalake(
        table_or_uri=tmp_path,
        data=data,
        partition_by=["time"],
    )

    # (step 2) overwrite with predicate that filters
    # on a non-partition column
    write_deltalake(
        table_or_uri=tmp_path, data=data, mode="overwrite", predicate="value >= 10"
    )


def test_write_table_with_deletion_vectors(tmp_path: pathlib.Path):
    """
    Tables with deletion vectors should still be writeable even without writing deletion vectors directly
    """
    schema = Schema(
        fields=[
            Field("id", type=PrimitiveType("string"), nullable=True),
            Field("price", type=PrimitiveType("long"), nullable=True),
        ]
    )
    dt = DeltaTable.create(
        tmp_path,
        schema,
        name="test_name",
        description="test_desc",
        configuration={
            "delta.enableDeletionVectors": "true",
        },
    )
    assert dt.protocol().min_writer_version == 7
    assert dt.version() == 0

    data = Table.from_pydict(
        {
            "id": Array(["1 2"], DataType.string()),
            "price": Array([10], DataType.int64()),
        },
        schema=schema,
    )

    write_deltalake(dt, data, mode="append")

    dt = DeltaTable(tmp_path)
    assert dt.version() == 1, "Expected a write to have occurred!"
