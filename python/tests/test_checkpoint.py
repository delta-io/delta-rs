import datetime as dt
import os
import pathlib
import shutil
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import (
    DeltaTable,
    PostCommitHookProperties,
    QueryBuilder,
    write_deltalake,
)

if TYPE_CHECKING:
    import pyarrow as pa


def test_checkpoint(tmp_path: pathlib.Path, sample_table: Table):
    tmp_table_path = tmp_path / "path" / "to" / "table"
    checkpoint_path = tmp_table_path / "_delta_log" / "_last_checkpoint"
    last_checkpoint_path = (
        tmp_table_path / "_delta_log" / "00000000000000000000.checkpoint.parquet"
    )
    write_deltalake(str(tmp_table_path), sample_table)

    assert not checkpoint_path.exists()

    delta_table = DeltaTable(str(tmp_table_path))
    delta_table.create_checkpoint()

    assert last_checkpoint_path.exists()
    assert checkpoint_path.exists()


def setup_cleanup_metadata(tmp_path: pathlib.Path, sample_table: Table):
    tmp_table_path = tmp_path / "path" / "to" / "table"
    first_log_path = tmp_table_path / "_delta_log" / "00000000000000000000.json"
    first_failed_log_path = (
        tmp_table_path / "_delta_log" / "00000000000000000000.json.tmp"
    )
    second_log_path = tmp_table_path / "_delta_log" / "00000000000000000001.json"
    second_failed_log_path = (
        tmp_table_path / "_delta_log" / "00000000000000000002.json.tmp"
    )
    third_log_path = tmp_table_path / "_delta_log" / "00000000000000000002.json"

    # Create few log files
    write_deltalake(str(tmp_table_path), sample_table)
    write_deltalake(str(tmp_table_path), sample_table, mode="overwrite")
    delta_table = DeltaTable(str(tmp_table_path))
    delta_table.delete()

    # Create failed json commit
    shutil.copy(str(first_log_path), str(first_failed_log_path))
    shutil.copy(str(third_log_path), str(second_failed_log_path))

    # Move first failed log entry timestamp back in time for more than 30 days
    old_ts = (dt.datetime.now() - dt.timedelta(days=31)).timestamp()
    os.utime(first_failed_log_path, (old_ts, old_ts))

    # Move first log entry timestamp back in time for more than 30 days
    old_ts = (dt.datetime.now() - dt.timedelta(days=31)).timestamp()
    os.utime(first_log_path, (old_ts, old_ts))

    # Move second log entry timestamp back in time for a minute
    near_ts = (dt.datetime.now() - dt.timedelta(minutes=1)).timestamp()
    os.utime(second_log_path, (near_ts, near_ts))

    assert first_log_path.exists()
    assert first_failed_log_path.exists()
    assert second_log_path.exists()
    assert third_log_path.exists()
    assert second_failed_log_path.exists()
    return delta_table


def test_cleanup_metadata(tmp_path: pathlib.Path, sample_table: Table):
    delta_table = setup_cleanup_metadata(tmp_path, sample_table)
    delta_table.create_checkpoint()
    delta_table.cleanup_metadata()

    tmp_table_path = tmp_path / "path" / "to" / "table"
    first_failed_log_path = (
        tmp_table_path / "_delta_log" / "00000000000000000000.json.tmp"
    )
    first_log_path = tmp_table_path / "_delta_log" / "00000000000000000000.json"
    second_log_path = tmp_table_path / "_delta_log" / "00000000000000000001.json"
    second_failed_log_path = (
        tmp_table_path / "_delta_log" / "00000000000000000002.json.tmp"
    )
    third_log_path = tmp_table_path / "_delta_log" / "00000000000000000002.json"

    assert not first_log_path.exists()
    assert not first_failed_log_path.exists()
    assert second_log_path.exists()
    assert third_log_path.exists()
    assert second_failed_log_path.exists()


def test_cleanup_metadata_log_cleanup_hook(tmp_path: pathlib.Path, sample_table: Table):
    delta_table = setup_cleanup_metadata(tmp_path, sample_table)
    delta_table.create_checkpoint()

    write_deltalake(delta_table, sample_table, mode="append")

    tmp_table_path = tmp_path / "path" / "to" / "table"
    first_failed_log_path = (
        tmp_table_path / "_delta_log" / "00000000000000000000.json.tmp"
    )
    first_log_path = tmp_table_path / "_delta_log" / "00000000000000000000.json"
    second_log_path = tmp_table_path / "_delta_log" / "00000000000000000001.json"
    second_failed_log_path = (
        tmp_table_path / "_delta_log" / "00000000000000000002.json.tmp"
    )
    third_log_path = tmp_table_path / "_delta_log" / "00000000000000000002.json"

    assert not first_log_path.exists()
    assert not first_failed_log_path.exists()
    assert second_log_path.exists()
    assert third_log_path.exists()
    assert second_failed_log_path.exists()


def test_cleanup_metadata_log_cleanup_hook_disabled(
    tmp_path: pathlib.Path, sample_table: Table
):
    delta_table = setup_cleanup_metadata(tmp_path, sample_table)
    delta_table.create_checkpoint()

    write_deltalake(
        delta_table,
        sample_table,
        mode="append",
        post_commithook_properties=PostCommitHookProperties(cleanup_expired_logs=False),
    )

    tmp_table_path = tmp_path / "path" / "to" / "table"
    first_failed_log_path = (
        tmp_table_path / "_delta_log" / "00000000000000000000.json.tmp"
    )
    first_log_path = tmp_table_path / "_delta_log" / "00000000000000000000.json"
    second_log_path = tmp_table_path / "_delta_log" / "00000000000000000001.json"
    second_failed_log_path = (
        tmp_table_path / "_delta_log" / "00000000000000000002.json.tmp"
    )
    third_log_path = tmp_table_path / "_delta_log" / "00000000000000000002.json"

    assert first_log_path.exists()
    assert first_failed_log_path.exists()
    assert second_log_path.exists()
    assert third_log_path.exists()
    assert second_failed_log_path.exists()


def test_cleanup_metadata_no_checkpoint(tmp_path: pathlib.Path, sample_table: Table):
    delta_table = setup_cleanup_metadata(tmp_path, sample_table)
    delta_table.cleanup_metadata()

    tmp_table_path = tmp_path / "path" / "to" / "table"
    first_failed_log_path = (
        tmp_table_path / "_delta_log" / "00000000000000000000.json.tmp"
    )
    first_log_path = tmp_table_path / "_delta_log" / "00000000000000000000.json"
    second_log_path = tmp_table_path / "_delta_log" / "00000000000000000001.json"
    second_failed_log_path = (
        tmp_table_path / "_delta_log" / "00000000000000000002.json.tmp"
    )
    third_log_path = tmp_table_path / "_delta_log" / "00000000000000000002.json"

    assert first_log_path.exists()
    assert first_failed_log_path.exists()
    assert second_log_path.exists()
    assert third_log_path.exists()
    assert second_failed_log_path.exists()


@pytest.mark.pyarrow
def test_features_maintained_after_checkpoint(tmp_path: pathlib.Path):
    from datetime import datetime

    import pyarrow as pa

    data = pa.table(
        {
            "timestamp": pa.array([datetime(2022, 1, 1)]),
        }
    )
    write_deltalake(tmp_path, data)

    dt = DeltaTable(tmp_path)
    current_protocol = dt.protocol()

    dt.create_checkpoint()

    dt = DeltaTable(tmp_path)
    protocol_after_checkpoint = dt.protocol()

    assert protocol_after_checkpoint.reader_features == ["timestampNtz"]
    assert current_protocol == protocol_after_checkpoint


@pytest.mark.pyarrow
def test_features_null_on_below_v3_v7(tmp_path: pathlib.Path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    data = pa.table(
        {
            "int": pa.array([1]),
        }
    )
    write_deltalake(tmp_path, data)

    dt = DeltaTable(tmp_path)
    current_protocol = dt.protocol()

    dt.create_checkpoint()

    dt = DeltaTable(tmp_path)
    protocol_after_checkpoint = dt.protocol()

    assert protocol_after_checkpoint.reader_features is None
    assert protocol_after_checkpoint.writer_features is None
    assert current_protocol == protocol_after_checkpoint

    checkpoint = pq.read_table(
        os.path.join(tmp_path, "_delta_log/00000000000000000000.checkpoint.parquet")
    )

    assert checkpoint["protocol"][0]["writerFeatures"].as_py() is None
    assert checkpoint["protocol"][0]["readerFeatures"].as_py() is None


@pytest.fixture
def sample_all_types():
    from datetime import timezone

    import pyarrow as pa

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
            # "decimal": pa.array([Decimal("10.000") + x for x in range(nrows)]), # Some issue with decimal and Rust engine at the moment.
            "date32": pa.array(
                [date(2022, 1, 1) + timedelta(days=x) for x in range(nrows)]
            ),
            "timestampNtz": pa.array(
                [datetime(2022, 1, 1) + timedelta(hours=x) for x in range(nrows)]
            ),
            "timestamp": pa.array(
                [
                    datetime(2022, 1, 1, tzinfo=timezone.utc) + timedelta(hours=x)
                    for x in range(nrows)
                ]
            ),
            "struct": pa.array([{"x": x, "y": str(x)} for x in range(nrows)]),
            "list": pa.array([list(range(x + 1)) for x in range(nrows)]),
        }
    )


@pytest.mark.pyarrow
@pytest.mark.parametrize(
    "part_col",
    [
        "timestampNtz",
        "timestamp",
    ],
)
def test_checkpoint_partition_timestamp_2380(
    tmp_path: pathlib.Path, sample_all_types: "pa.Table", part_col: str
):
    tmp_table_path = tmp_path / "path" / "to" / "table"
    checkpoint_path = tmp_table_path / "_delta_log" / "_last_checkpoint"
    last_checkpoint_path = (
        tmp_table_path / "_delta_log" / "00000000000000000000.checkpoint.parquet"
    )

    # TODO: Include binary after fixing issue "Json error: binary type is not supported"
    sample_data_pyarrow = sample_all_types.drop(["binary"])
    write_deltalake(
        str(tmp_table_path),
        sample_data_pyarrow,
        partition_by=[part_col],
    )

    assert not checkpoint_path.exists()

    delta_table = DeltaTable(str(tmp_table_path))

    delta_table.create_checkpoint()

    assert last_checkpoint_path.exists()
    assert checkpoint_path.exists()


def test_checkpoint_with_binary_column(tmp_path: pathlib.Path):
    data = Table(
        {
            "intColumn": Array(
                [1],
                ArrowField("intColumn", type=DataType.int64(), nullable=True),
            ),
            "binaryColumn": Array(
                [b"a"],
                ArrowField("binaryColumn", type=DataType.binary(), nullable=True),
            ),
        }
    )

    write_deltalake(
        str(tmp_path),
        data,
        partition_by=["intColumn"],
        mode="append",
    )

    dt = DeltaTable(tmp_path)
    dt.create_checkpoint()

    dt = DeltaTable(tmp_path)

    assert (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select intColumn, binaryColumn from tbl")
        .read_all()
        == data
    )


@pytest.mark.pyarrow
def test_checkpoint_post_commit_config(
    tmp_path: pathlib.Path, sample_data_pyarrow: "pa.Table"
):
    """Checks whether checkpoints are properly written based on commit_interval"""
    tmp_table_path = tmp_path / "path" / "to" / "table"
    checkpoint_path = tmp_table_path / "_delta_log" / "_last_checkpoint"
    first_checkpoint_path = (
        tmp_table_path / "_delta_log" / "00000000000000000004.checkpoint.parquet"
    )
    second_checkpoint_path = (
        tmp_table_path / "_delta_log" / "00000000000000000009.checkpoint.parquet"
    )

    # TODO: Include binary after fixing issue "Json error: binary type is not supported"
    sample_data_pyarrow = sample_data_pyarrow.drop(["binary"])
    for i in range(2):
        write_deltalake(
            str(tmp_table_path),
            sample_data_pyarrow,
            mode="append",
            configuration={"delta.checkpointInterval": "5"},
        )

    assert not checkpoint_path.exists()
    assert not first_checkpoint_path.exists()
    assert not second_checkpoint_path.exists()

    for i in range(10):
        write_deltalake(
            str(tmp_table_path),
            sample_data_pyarrow,
            mode="append",
            configuration={"delta.checkpointInterval": "5"},
        )

    assert checkpoint_path.exists()
    assert first_checkpoint_path.exists()
    assert second_checkpoint_path.exists()

    for i in range(12):
        if i in [4, 9]:
            continue
        random_checkpoint_path = (
            tmp_table_path / "_delta_log" / f"{str(i).zfill(20)}.checkpoint.parquet"
        )
        assert not random_checkpoint_path.exists()

    dt = DeltaTable(str(tmp_table_path))
    assert dt.version() == 11


def test_checkpoint_post_commit_config_multiple_operations(
    tmp_path: pathlib.Path, sample_table: Table
):
    """Checks whether checkpoints are properly written based on commit_interval"""
    tmp_table_path = tmp_path / "path" / "to" / "table"
    checkpoint_path = tmp_table_path / "_delta_log" / "_last_checkpoint"
    first_checkpoint_path = (
        tmp_table_path / "_delta_log" / "00000000000000000004.checkpoint.parquet"
    )
    second_checkpoint_path = (
        tmp_table_path / "_delta_log" / "00000000000000000009.checkpoint.parquet"
    )

    for i in range(4):
        write_deltalake(
            str(tmp_table_path),
            sample_table,
            mode="append",
            configuration={"delta.checkpointInterval": "5"},
        )

    assert not checkpoint_path.exists()
    assert not first_checkpoint_path.exists()
    assert not second_checkpoint_path.exists()

    dt = DeltaTable(str(tmp_table_path))

    dt.optimize.compact()

    assert checkpoint_path.exists()
    assert first_checkpoint_path.exists()

    for i in range(4):
        write_deltalake(
            str(tmp_table_path),
            sample_table,
            mode="append",
            configuration={"delta.checkpointInterval": "5"},
        )

    dt = DeltaTable(str(tmp_table_path))
    dt.delete()

    assert second_checkpoint_path.exists()

    for i in range(12):
        if i in [4, 9]:
            continue
        random_checkpoint_path = (
            tmp_table_path / "_delta_log" / f"{str(i).zfill(20)}.checkpoint.parquet"
        )
        assert not random_checkpoint_path.exists()

    delta_table = DeltaTable(str(tmp_table_path))
    assert delta_table.version() == 9


@pytest.mark.pyarrow
def test_checkpoint_with_nullable_false(tmp_path: pathlib.Path):
    tmp_table_path = tmp_path / "path" / "to" / "table"
    checkpoint_path = tmp_table_path / "_delta_log" / "_last_checkpoint"
    import pyarrow as pa

    pylist = [{"year": 2023, "n_party": 0}, {"year": 2024, "n_party": 1}]
    my_schema = pa.schema(
        [
            pa.field("year", pa.int64(), nullable=False),
            pa.field("n_party", pa.int64(), nullable=False),
        ]
    )

    data = pa.Table.from_pylist(pylist, schema=my_schema)

    write_deltalake(
        str(tmp_table_path),
        data,
        configuration={"delta.dataSkippingNumIndexedCols": "1"},
    )

    DeltaTable(str(tmp_table_path)).create_checkpoint()

    assert checkpoint_path.exists()

    assert DeltaTable(str(tmp_table_path)).to_pyarrow_table() == data


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_checkpoint_with_multiple_writes(tmp_path: pathlib.Path):
    import pandas as pd

    write_deltalake(
        tmp_path,
        pd.DataFrame(
            {
                "a": ["a"],
                "b": [3],
            }
        ),
    )
    dt = DeltaTable(tmp_path)
    dt.create_checkpoint()
    assert dt.version() == 0
    df = pd.DataFrame(
        {
            "a": ["a"],
            "b": [100],
        }
    )
    write_deltalake(tmp_path, df, mode="overwrite")

    dt = DeltaTable(tmp_path)
    assert dt.version() == 1
    new_df = dt.to_pandas()
    print(dt.to_pandas())
    assert len(new_df) == 1, "We overwrote! there should only be one row"


@pytest.mark.polars
@pytest.mark.xfail(reason="polars needs update")
def test_refresh_snapshot_after_log_cleanup_3057(tmp_path):
    """https://github.com/delta-io/delta-rs/issues/3057"""
    import polars as pl

    configuration = {
        "delta.deletedFileRetentionDuration": "interval 0 days",
        "delta.logRetentionDuration": "interval 0 days",
        "delta.targetFileSize": str(128 * 1024 * 1024),
    }

    for i in range(2):
        df = pl.DataFrame({"foo": [i]})
        df.write_delta(
            str(tmp_path),
            delta_write_options={"configuration": configuration},
            mode="append",
        )

    # create checkpoint so that logs before checkpoint can get removed
    dt = DeltaTable(tmp_path)
    dt.create_checkpoint()

    # Write to table again, snapshot should be correctly refreshed so that clean_up metadata can run after this
    df = pl.DataFrame({"foo": [1]})
    df.write_delta(dt, mode="append")

    # Vacuum is noop, since we already removed logs before and snapshot doesn't reference them anymore

    vacuum_log = dt.vacuum(
        retention_hours=0, enforce_retention_duration=False, dry_run=False
    )

    assert vacuum_log == []
