import itertools
import json
import os
import pathlib
from datetime import datetime
from typing import List

import pyarrow as pa
import pyarrow.compute as pc
import pytest
import ray

from deltalake import DeltaTable, write_deltalake, write_table


def test_input_invalid_datatype(tmp_path: pathlib.Path, sample_data: pa.Table):
    # Check we can create the subdirectory
    tmp_path = tmp_path / "path" / "to" / "table"
    with pytest.raises(ValueError):
        write_table.write_deltalake_ray(tmp_path, sample_data)


def test_roundtrip_basic(tmp_path: pathlib.Path, sample_data: pa.Table):
    # Check we can create the subdirectory
    tmp_path = tmp_path / "path" / "to" / "table"
    start_time = datetime.now().timestamp()
    ray_ds = ray.data.from_arrow_refs([ray.put(sample_data)])
    write_table.write_deltalake_ray(tmp_path, ray_ds)
    end_time = datetime.now().timestamp()

    assert ("0" * 20 + ".json") in os.listdir(tmp_path / "_delta_log")

    delta_table = DeltaTable(tmp_path)

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


def test_roundtrip_nulls(tmp_path: pathlib.Path):
    data = pa.table({"x": pa.array([None, None, 1, 2], type=pa.int64())})
    # One row group will have values, one will be all nulls.
    # The first will have None in min and max stats, so we need to handle that.
    ray_ds = ray.data.from_arrow_refs([ray.put(data)])
    write_table.write_deltalake_ray(tmp_path, ray_ds)

    delta_table = DeltaTable(tmp_path)

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
    )

    delta_table = DeltaTable(tmp_path)
    assert delta_table.schema().to_pyarrow() == data.schema

    table = delta_table.to_pyarrow_table()
    assert table == data


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_enforce_schema(existing_table: DeltaTable, mode: str):
    bad_data = pa.table({"x": pa.array([1, 2, 3])})
    with pytest.raises(ValueError):
        write_table.write_deltalake_ray(existing_table, bad_data, mode=mode)

    table_uri = existing_table._table.table_uri()
    with pytest.raises(ValueError):
        write_table.write_deltalake_ray(table_uri, bad_data, mode=mode)


def test_update_schema(existing_table: DeltaTable):
    new_data = pa.table({"x": pa.array([1, 2, 3])})
    ray_ds = ray.data.from_arrow_refs([ray.put(new_data)])
    with pytest.raises(ValueError):
        write_table.write_deltalake_ray(
            existing_table, ray_ds, mode="append", overwrite_schema=True
        )

    write_table.write_deltalake_ray(
        existing_table, ray_ds, mode="overwrite", overwrite_schema=True
    )

    read_data = existing_table.to_pyarrow_table()
    assert new_data == read_data
    assert existing_table.schema().to_pyarrow() == new_data.schema


def test_roundtrip_metadata(tmp_path: pathlib.Path, sample_data: pa.Table):
    write_deltalake(
        tmp_path,
        sample_data,
        name="test_name",
        description="test_desc",
        configuration={"delta.appendOnly": "false", "foo": "bar"},
    )

    delta_table = DeltaTable(tmp_path)

    metadata = delta_table.metadata()

    assert metadata.name == "test_name"
    assert metadata.description == "test_desc"
    assert metadata.configuration == {"delta.appendOnly": "false", "foo": "bar"}


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
    write_deltalake(tmp_path, sample_data, partition_by=column)

    delta_table = DeltaTable(tmp_path)
    assert delta_table.schema().to_pyarrow() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    table = table.take(pc.sort_indices(table["int64"]))
    assert table == sample_data

    for add_path in get_add_paths(delta_table):
        # Paths should be relative
        assert add_path.count("/") == 1


def test_roundtrip_null_partition(tmp_path: pathlib.Path, sample_data: pa.Table):
    sample_data = sample_data.add_column(
        0, "utf8_with_nulls", pa.array(["a"] * 4 + [None])
    )
    write_deltalake(tmp_path, sample_data, partition_by=["utf8_with_nulls"])

    delta_table = DeltaTable(tmp_path)
    assert delta_table.schema().to_pyarrow() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    table = table.take(pc.sort_indices(table["int64"]))
    assert table == sample_data


def test_roundtrip_multi_partitioned(tmp_path: pathlib.Path, sample_data: pa.Table):
    write_deltalake(tmp_path, sample_data, partition_by=["int32", "bool"])

    delta_table = DeltaTable(tmp_path)
    assert delta_table.schema().to_pyarrow() == sample_data.schema

    table = delta_table.to_pyarrow_table()
    table = table.take(pc.sort_indices(table["int64"]))
    assert table == sample_data

    for add_path in get_add_paths(delta_table):
        # Paths should be relative
        assert add_path.count("/") == 2


def test_write_modes(tmp_path: pathlib.Path, sample_data: pa.Table):
    ray_ds = ray.data.from_arrow_refs([ray.put(sample_data)])
    write_table.write_deltalake_ray(tmp_path, ray_ds)
    assert DeltaTable(tmp_path).to_pyarrow_table() == sample_data

    with pytest.raises(AssertionError):
        write_table.write_deltalake_ray(tmp_path, ray_ds, mode="error")

    write_table.write_deltalake_ray(tmp_path, ray_ds, mode="ignore")
    assert ("0" * 19 + "1.json") not in os.listdir(tmp_path / "_delta_log")

    write_table.write_deltalake_ray(tmp_path, ray_ds, mode="append")
    expected = pa.concat_tables([sample_data, sample_data])
    assert DeltaTable(tmp_path).to_pyarrow_table() == expected

    write_table.write_deltalake_ray(tmp_path, ray_ds, mode="overwrite")
    assert DeltaTable(tmp_path).to_pyarrow_table() == sample_data


def test_append_only_should_append_only_with_the_overwrite_mode(
    tmp_path: pathlib.Path, sample_data: pa.Table
):
    config = {"delta.appendOnly": "true"}

    ray_ds = ray.data.from_arrow_refs([ray.put(sample_data)])
    write_table.write_deltalake_ray(
        tmp_path, ray_ds, mode="append", configuration=config
    )

    table = DeltaTable(tmp_path)
    write_table.write_deltalake_ray(table, ray_ds, mode="append")

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
            write_table.write_deltalake_ray(data_store_type, ray_ds, mode=mode)

    expected = pa.concat_tables([sample_data, sample_data])

    assert table.to_pyarrow_table() == expected
    assert table.version() == 1


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


def get_add_paths(table: DeltaTable) -> List[str]:
    return [action["path"] for action in get_add_actions(table)]
