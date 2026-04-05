import pathlib
from datetime import timedelta

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import CommitProperties, DeltaTable, write_deltalake
from deltalake.query import QueryBuilder


def ordered_range_table(start: int, rows: int) -> Table:
    values = list(range(start, start + rows))
    return Table(
        {
            "x": Array(values, type=DataType.int32()),
            "y": Array([value * 10 for value in values], type=DataType.int32()),
        }
    )


def x_file_ranges(dt: DeltaTable) -> list[tuple[int, int, int]]:
    add_actions = dt.get_add_actions(flatten=True)
    mins = add_actions["min.x"].to_pylist()
    maxs = add_actions["max.x"].to_pylist()
    sizes = add_actions["size_bytes"].to_pylist()

    ranges = [
        (min_value, max_value, size_bytes)
        for min_value, max_value, size_bytes in zip(mins, maxs, sizes, strict=True)
        if min_value is not None and max_value is not None
    ]
    return sorted(ranges, key=lambda item: item[0])


def overlap_is_suffix_like(
    ranges: list[tuple[int, int, int]], recent_start: int
) -> bool:
    overlapping = [max_value >= recent_start for _, max_value, _ in ranges]
    if not any(overlapping):
        return True

    first_overlap = overlapping.index(True)
    return all(overlapping[first_overlap:])


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


def test_optimize_metrics_report_planner_strategy(
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    write_deltalake(tmp_path, sample_table, mode="append")
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)
    metrics = dt.optimize.compact()

    assert metrics["plannerStrategy"] == "preserveLocality"
    assert metrics["preservedStableOrder"] is True
    assert metrics["preserveInsertionOrder"] is True
    assert metrics["maxBinSpanFiles"] == 2
    assert "maxInputDisplacement" not in metrics


def test_zorder_metrics_do_not_claim_preserve_insertion_order(
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    write_deltalake(tmp_path, sample_table, mode="append")
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)
    metrics = dt.optimize.z_order(["sold"])

    assert metrics["plannerStrategy"] == "zOrder"
    assert metrics["preservedStableOrder"] is False
    assert metrics["preserveInsertionOrder"] is False
    assert metrics["maxBinSpanFiles"] == 2
    assert "maxInputDisplacement" not in metrics


def test_compact_rejects_target_size_larger_than_i64_max(
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    write_deltalake(tmp_path, sample_table, mode="append")
    dt = DeltaTable(tmp_path)

    with pytest.raises(ValueError, match="target_file_size must be between 1 and"):
        dt.optimize.compact(target_size=2**63)


def test_zorder_rejects_target_size_larger_than_i64_max(
    tmp_path: pathlib.Path,
    sample_table: Table,
):
    write_deltalake(tmp_path, sample_table, mode="append")
    dt = DeltaTable(tmp_path)

    with pytest.raises(ValueError, match="target_file_size must be between 1 and"):
        dt.optimize.z_order(["sold"], target_size=2**63)


def test_compact_preserves_tail_locality_after_small_recent_appends(
    tmp_path: pathlib.Path,
):
    old_rows = 5_000
    recent_rows = 1_250
    base_batches = 6
    append_batches = 2

    for batch_idx in range(base_batches):
        write_deltalake(
            tmp_path,
            ordered_range_table(batch_idx * old_rows, old_rows),
            mode="append",
        )

    dt = DeltaTable(tmp_path)
    base_size = dt.get_add_actions(flatten=True)["size_bytes"].to_pylist()[0]
    target_size = base_size * 2 + (base_size // 2)

    dt.optimize.compact(target_size=target_size)

    recent_start = base_batches * old_rows
    for batch_idx in range(append_batches):
        write_deltalake(
            tmp_path,
            ordered_range_table(
                recent_start + (batch_idx * recent_rows),
                recent_rows,
            ),
            mode="append",
        )

    dt = DeltaTable(tmp_path)
    metrics = dt.optimize.compact(target_size=target_size)
    dt = DeltaTable(tmp_path)

    ranges = x_file_ranges(dt)
    recent_count = (
        QueryBuilder()
        .register("tbl", dt)
        .execute(f"select count(*) as cnt from tbl where x >= {recent_start}")
        .read_all()["cnt"]
        .to_pylist()[0]
    )

    assert recent_count == append_batches * recent_rows
    assert overlap_is_suffix_like(ranges, recent_start), f"{ranges!r}"
    assert metrics["plannerStrategy"] == "preserveLocality"
    assert metrics["preservedStableOrder"] is True
    assert "maxInputDisplacement" not in metrics


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
