import pathlib
from datetime import timedelta

import pyarrow as pa
import pytest

try:
    import pandas as pd
except ModuleNotFoundError:
    _has_pandas = False
else:
    _has_pandas = True

from deltalake import CommitProperties, DeltaTable, write_deltalake


@pytest.mark.parametrize("use_relative", [True, False])
def test_optimize_run_table(
    tmp_path: pathlib.Path,
    sample_data: pa.Table,
    monkeypatch,
    use_relative: bool,
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
    old_data = dt.to_pyarrow_table()
    old_version = dt.version()

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.optimize.compact(commit_properties=commit_properties)

    new_data = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    assert last_action["userName"] == "John Doe"
    assert dt.version() == old_version + 1
    assert old_data == new_data


def test_z_order_optimize(
    tmp_path: pathlib.Path,
    sample_data: pa.Table,
):
    write_deltalake(
        tmp_path,
        sample_data,
        mode="append",
    )
    write_deltalake(
        tmp_path,
        sample_data,
        mode="append",
    )
    write_deltalake(
        tmp_path,
        sample_data,
        mode="append",
    )

    dt = DeltaTable(tmp_path)
    old_version = dt.version()

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.optimize.z_order(["date32", "timestamp"], commit_properties=commit_properties)
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    assert last_action["userName"] == "John Doe"
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


def test_optimize_schema_evolved_table(
    tmp_path: pathlib.Path,
    sample_data: pa.Table,
):
    data = pa.table({"foo": pa.array(["1"])})

    write_deltalake(tmp_path, data, mode="append", schema_mode="merge")

    data = pa.table({"bar": pa.array(["1"])})
    write_deltalake(tmp_path, data, mode="append", schema_mode="merge")

    dt = DeltaTable(tmp_path)
    old_version = dt.version()

    dt.optimize.compact()

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "OPTIMIZE"
    assert dt.version() == old_version + 1

    data = pa.table(
        {
            "foo": pa.array([None, "1"]),
            "bar": pa.array(["1", None]),
        }
    )

    assert dt.to_pyarrow_table().sort_by([("foo", "ascending")]) == data.sort_by(
        [("foo", "ascending")]
    )


@pytest.mark.pandas
def test_zorder_with_space_partition(tmp_path: pathlib.Path):
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
    print(partitioned_df)

    test_table.optimize.z_order(columns=["user"])


def test_optimize_schema_evolved_3185(tmp_path):
    """https://github.com/delta-io/delta-rs/issues/3185"""

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
