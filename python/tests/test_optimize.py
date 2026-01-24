import pathlib
from datetime import timedelta

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import CommitProperties, DeltaTable, write_deltalake
from deltalake.query import QueryBuilder


@pytest.mark.parametrize("use_relative", [True, False])
def test_optimize_run_table(
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
    old_data = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id")
        .read_all()
    )
    old_version = dt.version()

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.optimize.compact(commit_properties=commit_properties)

    new_data = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by id")
        .read_all()
    )
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    assert last_action["userName"] == "John Doe"
    assert dt.version() == old_version + 1
    assert old_data == new_data


def test_z_order_optimize(
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    write_deltalake(
        tmp_path,
        sample_table,
        mode="append",
    )
    write_deltalake(
        tmp_path,
        sample_table,
        mode="append",
    )
    write_deltalake(
        tmp_path,
        sample_table,
        mode="append",
    )

    dt = DeltaTable(tmp_path)
    old_version = dt.version()

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.optimize.z_order(["sold", "price"], commit_properties=commit_properties)
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    assert last_action["userName"] == "John Doe"
    assert dt.version() == old_version + 1
    assert len(dt.file_uris()) == 1


def test_optimize_min_commit_interval(
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    write_deltalake(tmp_path, sample_table, partition_by="id", mode="append")
    write_deltalake(tmp_path, sample_table, partition_by="id", mode="append")
    write_deltalake(tmp_path, sample_table, partition_by="id", mode="append")

    dt = DeltaTable(tmp_path)
    old_version = dt.version()

    dt.optimize.z_order(["sold", "price"], min_commit_interval=timedelta(0))

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    # The table has 5 distinct partitions, each of which are Z-ordered
    # independently. So with min_commit_interval=0, each will get its
    # own commit.
    assert dt.version() == old_version + 5


def test_optimize_schema_evolved_table(
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    data = Table(
        {
            "foo": Array(
                ["1"],
                ArrowField("foo", type=DataType.string(), nullable=True),
            ),
        }
    )

    write_deltalake(tmp_path, data, mode="append", schema_mode="merge")

    data = Table(
        {
            "bar": Array(
                ["1"],
                ArrowField("bar", type=DataType.string(), nullable=True),
            ),
        }
    )

    write_deltalake(tmp_path, data, mode="append", schema_mode="merge")

    dt = DeltaTable(tmp_path)
    old_version = dt.version()

    dt.optimize.compact()

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    assert dt.version() == old_version + 1

    data = Table(
        {
            "foo": Array(
                ["1", None],
                ArrowField("foo", type=DataType.string_view(), nullable=True),
            ),
            "bar": Array(
                [None, "1"],
                ArrowField("bar", type=DataType.string_view(), nullable=True),
            ),
        }
    )

    assert (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl order by foo asc")
        .read_all()
        == data
    )


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_zorder_with_space_partition(tmp_path: pathlib.Path):
    import pandas as pd

    df = pd.DataFrame(
        {
            "user": ["James", "Anna", "Sara", "Martin"],
            "country": ["United States", "Canada", "Costa Rica", "South Africa"],
            "age": [34, 23, 45, 26],
        }
    )

    write_deltalake(
        table_or_uri=tmp_path,
        data=df,
        mode="overwrite",
        partition_by=["country"],
    )

    test_table = DeltaTable(tmp_path)

    # retrieve by partition works fine
    partitioned_df = test_table.to_pandas(
        partitions=[("country", "=", "United States")],
    )
    _ = partitioned_df

    test_table.optimize.z_order(columns=["user"])


@pytest.mark.pyarrow
def test_optimize_schema_evolved_3185(tmp_path):
    """https://github.com/delta-io/delta-rs/issues/3185"""
    import pyarrow as pa

    # Define the data for the first write
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

    write_deltalake(tmp_path, table_second_write, mode="append", schema_mode="merge")

    dt = DeltaTable(tmp_path)

    dt.optimize.z_order(columns=["name"])

    assert dt.version() == 2
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"


def test_compact_with_spill_parameters(
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    write_deltalake(tmp_path, sample_table, mode="append")
    write_deltalake(tmp_path, sample_table, mode="append")
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)
    old_version = dt.version()
    old_num_files = len(dt.file_uris())

    dt.optimize.compact(
        max_spill_size=100 * 1024 * 1024 * 1024,  # 100 GB
        max_temp_directory_size=500 * 1024 * 1024 * 1024,  # 500 GB
    )

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    assert dt.version() == old_version + 1
    assert len(dt.file_uris()) <= old_num_files


def test_z_order_with_spill_parameters(
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    write_deltalake(tmp_path, sample_table, mode="append")
    write_deltalake(tmp_path, sample_table, mode="append")
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)
    old_version = dt.version()

    dt.optimize.z_order(
        columns=["sold", "price"],
        max_spill_size=100 * 1024 * 1024 * 1024,  # 100 GB
        max_temp_directory_size=500 * 1024 * 1024 * 1024,  # 500 GB
    )

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    assert dt.version() == old_version + 1
    assert len(dt.file_uris()) == 1
