import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from random import random
from threading import Barrier, Thread
from typing import Any, List, Tuple, Type
from unittest.mock import Mock

from deltalake._util import encode_partition_value
from deltalake.exceptions import DeltaProtocolError
from deltalake.table import ProtocolVersions
from deltalake.writer import write_deltalake

try:
    import pandas as pd
except ModuleNotFoundError:
    _has_pandas = False
else:
    _has_pandas = True

import multiprocessing
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor

import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from pyarrow.dataset import ParquetReadOptions
from pyarrow.fs import LocalFileSystem, SubTreeFileSystem

from deltalake import DeltaTable


def test_read_table_with_edge_timestamps():
    table_path = "../crates/test/tests/data/table_with_edge_timestamps"
    dt = DeltaTable(table_path)
    dataset = dt.to_pyarrow_dataset(
        parquet_read_options=ParquetReadOptions(coerce_int96_timestamp_unit="ms")
    )
    assert dataset.to_table().to_pydict() == {
        "BIG_DATE": [
            datetime(9999, 12, 31, 0, 0, 0, tzinfo=timezone.utc),
            datetime(9999, 12, 30, 0, 0, 0, tzinfo=timezone.utc),
        ],
        "NORMAL_DATE": [
            datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2022, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
        ],
        "SOME_VALUE": [1, 2],
    }
    # Can push down filters to these timestamps.
    predicate = ds.field("BIG_DATE") == datetime(
        9999, 12, 31, 0, 0, 0, tzinfo=timezone.utc
    )
    assert len(list(dataset.get_fragments(predicate))) == 1


def test_read_simple_table_to_dict():
    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"id": [5, 7, 9]}


class _SerializableException(BaseException):
    pass


def _recursively_read_simple_table(executor_class: Type[Executor], depth):
    try:
        test_read_simple_table_to_dict()
    except BaseException as e:  # Ideally this would catch `pyo3_runtime.PanicException` but its seems that is not possible.
        # Re-raise as something that can be serialized and therefore sent back to parent processes.
        raise _SerializableException(f"Seraializatble exception: {e}") from e

    if depth == 0:
        return
    # We use concurrent.futures.Executors instead of `threading.Thread` or `multiprocessing.Process` to that errors
    # are re-rasied in the parent process/thread when we call `future.result()`.
    with executor_class(max_workers=1) as executor:
        future = executor.submit(
            _recursively_read_simple_table, executor_class, depth - 1
        )
        future.result()


@pytest.mark.parametrize(
    "executor_class,multiprocessing_start_method,expect_panic",
    [
        (ThreadPoolExecutor, None, False),
        (ProcessPoolExecutor, "forkserver", False),
        (ProcessPoolExecutor, "spawn", False),
        (ProcessPoolExecutor, "fork", True),
    ],
)
def test_read_simple_in_threads_and_processes(
    executor_class, multiprocessing_start_method, expect_panic
):
    if multiprocessing_start_method is not None:
        multiprocessing.set_start_method(multiprocessing_start_method, force=True)
    if expect_panic:
        with pytest.raises(
            _SerializableException,
            match="The tokio runtime does not support forked processes",
        ):
            _recursively_read_simple_table(executor_class=executor_class, depth=5)
    else:
        _recursively_read_simple_table(executor_class=executor_class, depth=5)


def test_read_simple_table_by_version_to_dict():
    table_path = "../crates/test/tests/data/delta-0.2.0"
    dt = DeltaTable(table_path, version=2)
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"value": [1, 2, 3]}


def test_read_simple_table_using_options_to_dict():
    table_path = "../crates/test/tests/data/delta-0.2.0"
    dt = DeltaTable(table_path, version=2, storage_options={})
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"value": [1, 2, 3]}


@pytest.mark.parametrize(
    ["date_value", "expected_version"],
    [
        ("2020-05-01T00:47:31-07:00", 0),
        ("2020-05-02T22:47:31-07:00", 1),
        ("2020-05-25T22:47:31-07:00", 4),
    ],
)
def test_load_as_version_datetime(date_value: str, expected_version):
    log_dir = "../crates/test/tests/data/simple_table/_delta_log"
    log_mtime_pair = [
        ("00000000000000000000.json", 1588398451.0),
        ("00000000000000000001.json", 1588484851.0),
        ("00000000000000000002.json", 1588571251.0),
        ("00000000000000000003.json", 1588657651.0),
        ("00000000000000000004.json", 1588744051.0),
    ]
    for file_name, dt_epoch in log_mtime_pair:
        file_path = os.path.join(log_dir, file_name)
        os.utime(file_path, (dt_epoch, dt_epoch))

    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)
    dt.load_as_version(date_value)
    assert dt.version() == expected_version
    dt = DeltaTable(table_path)
    dt.load_as_version(datetime.fromisoformat(date_value))
    assert dt.version() == expected_version


@pytest.mark.parametrize(
    ["date_value", "expected_version", "log_mtime_pairs"],
    [
        ("2020-05-01T00:47:31-07:00", 1, [("00000000000000000000.json", 158839841.0)]),
        (
            "2020-05-02T22:47:31-07:00",
            2,
            [
                ("00000000000000000000.json", 158839841.0),
                ("00000000000000000001.json", 1588484851.0),
            ],
        ),
    ],
)
def test_load_as_version_datetime_with_logs_removed(
    tmp_path,
    sample_table,
    date_value: str,
    expected_version,
    log_mtime_pairs: List[Tuple[str, int]],
):
    log_path = tmp_path / "_delta_log"
    for i in range(6):
        write_deltalake(tmp_path, data=sample_table, mode="append")

    for file_name, dt_epoch in log_mtime_pairs:
        file_path = log_path / file_name
        print(file_path)
        os.utime(file_path, (dt_epoch, dt_epoch))

    dt = DeltaTable(tmp_path, version=expected_version)
    dt.create_checkpoint()
    file = log_path / f"0000000000000000000{expected_version}.checkpoint.parquet"
    assert file.exists()
    dt.cleanup_metadata()

    file = log_path / f"0000000000000000000{expected_version-1}.json"
    assert not file.exists()
    dt = DeltaTable(tmp_path)
    dt.load_as_version(date_value)
    assert dt.version() == expected_version
    dt = DeltaTable(tmp_path)
    dt.load_as_version(datetime.fromisoformat(date_value))
    assert dt.version() == expected_version


def test_load_as_version_datetime_bad_format():
    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)

    for bad_format in [
        "2020-05-01T00:47:31",
        "2020-05-01 00:47:31",
        "2020-05-01T00:47:31+08",
    ]:
        with pytest.raises(Exception, match="Failed to parse datetime string:"):
            dt.load_as_version(bad_format)


def test_read_simple_table_update_incremental():
    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path, version=0)
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"id": [0, 1, 2, 3, 4]}
    dt.update_incremental()
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"id": [5, 7, 9]}


def test_read_simple_table_file_sizes_failure(mocker):
    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)
    add_actions = dt.get_add_actions().to_pydict()

    # set all sizes to -1, the idea is to break the reading, to check
    # that input file sizes are actually used
    add_actions_modified = {x: -1 for x in add_actions["path"]}
    mocker.patch(
        "deltalake._internal.RawDeltaTable.get_add_file_sizes",
        return_value=add_actions_modified,
    )

    with pytest.raises(OSError, match="Cannot seek past end of file."):
        dt.to_pyarrow_dataset().to_table().to_pydict()


def test_read_partitioned_table_to_dict():
    table_path = "../crates/test/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    expected = {
        "value": ["1", "2", "3", "6", "7", "5", "4"],
        "year": ["2020", "2020", "2020", "2021", "2021", "2021", "2021"],
        "month": ["1", "2", "2", "12", "12", "12", "4"],
        "day": ["1", "3", "5", "20", "20", "4", "5"],
    }
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == expected


def test_read_partitioned_table_with_partitions_filters_to_dict():
    table_path = "../crates/test/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    partitions = [("year", "=", "2021")]
    expected = {
        "value": ["6", "7", "5", "4"],
        "year": ["2021", "2021", "2021", "2021"],
        "month": ["12", "12", "12", "4"],
        "day": ["20", "20", "4", "5"],
    }

    assert dt.to_pyarrow_dataset(partitions).to_table().to_pydict() == expected


def test_read_empty_delta_table_after_delete():
    table_path = "../crates/test/tests/data/delta-0.8-empty"
    dt = DeltaTable(table_path)
    expected = {"column": []}

    assert dt.to_pyarrow_dataset().to_table().to_pydict() == expected


def test_read_table_with_column_subset():
    table_path = "../crates/test/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    expected = {
        "value": ["1", "2", "3", "6", "7", "5", "4"],
        "day": ["1", "3", "5", "20", "20", "4", "5"],
    }
    assert (
        dt.to_pyarrow_dataset().to_table(columns=["value", "day"]).to_pydict()
        == expected
    )


def test_read_table_as_category():
    table_path = "../crates/test/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)

    assert dt.schema().to_pyarrow().field("value").type == pa.string()

    read_options = ds.ParquetReadOptions(dictionary_columns={"value"})

    data = dt.to_pyarrow_dataset(parquet_read_options=read_options).to_table()

    assert data.schema.field("value").type == pa.dictionary(pa.int32(), pa.string())
    assert data.schema.field("day").type == pa.string()


def test_read_table_with_filter():
    table_path = "../crates/test/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    expected = {
        "value": ["6", "7", "5"],
        "year": ["2021", "2021", "2021"],
        "month": ["12", "12", "12"],
        "day": ["20", "20", "4"],
    }
    filter_expr = (ds.field("year") == "2021") & (ds.field("month") == "12")

    dataset = dt.to_pyarrow_dataset()

    assert len(list(dataset.get_fragments(filter=filter_expr))) == 2
    assert dataset.to_table(filter=filter_expr).to_pydict() == expected


def test_read_table_with_stats():
    table_path = "../crates/test/tests/data/COVID-19_NYT"
    dt = DeltaTable(table_path)
    dataset = dt.to_pyarrow_dataset()

    filter_expr = ds.field("date") > "2021-02-20"
    assert len(list(dataset.get_fragments(filter=filter_expr))) == 2

    data = dataset.to_table(filter=filter_expr)
    assert data.num_rows < 147181 + 47559

    filter_expr = ds.field("cases") < 0
    assert len(list(dataset.get_fragments(filter=filter_expr))) == 0

    data = dataset.to_table(filter=filter_expr)
    assert data.num_rows == 0

    filter_expr = ds.field("cases").is_null()
    assert len(list(dataset.get_fragments(filter=filter_expr))) == 0

    data = dataset.to_table(filter=filter_expr)
    assert data.num_rows == 0


def test_read_special_partition():
    table_path = "../crates/test/tests/data/delta-0.8.0-special-partition"
    dt = DeltaTable(table_path)

    file1 = (
        r"x=A%2FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet"
    )
    file2 = (
        r"x=B%20B/part-00015-e9abbc6f-85e9-457b-be8e-e9f5b8a22890.c000.snappy.parquet"
    )

    assert set(dt.files()) == {file1, file2}

    assert dt.files([("x", "=", "A/A")]) == [file1]
    assert dt.files([("x", "=", "B B")]) == [file2]
    assert dt.files([("x", "=", "c")]) == []

    table = dt.to_pyarrow_table()

    assert set(table["x"].to_pylist()) == {"A/A", "B B"}


def test_read_partitioned_table_metadata():
    table_path = "../crates/test/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    metadata = dt.metadata()
    assert metadata.id == "fe5a3c11-30d4-4dd7-b115-a1c121e66a4e"
    assert metadata.name is None
    assert metadata.description is None
    assert metadata.partition_columns == ["year", "month", "day"]
    assert metadata.created_time == 1615555644515
    assert metadata.configuration == {}


def test_read_partitioned_table_protocol():
    table_path = "../crates/test/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    protocol = dt.protocol()
    assert protocol.min_reader_version == 1
    assert protocol.min_writer_version == 2


def test_read_table_with_cdc():
    table_path = "../crates/test/tests/data/simple_table_with_cdc"
    dt = DeltaTable(table_path)
    assert dt.to_pyarrow_table().to_pydict() == {
        "id": [0],
        "name": ["Mino"],
    }


def test_history_partitioned_table_metadata():
    table_path = "../crates/test/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    history = dt.history()
    commit_info = history[0]

    assert len(history) == 1
    assert commit_info == {
        "timestamp": 1615555646188,
        "operation": "WRITE",
        "operationParameters": {
            "mode": "ErrorIfExists",
            "partitionBy": '["year","month","day"]',
        },
        "isBlindAppend": True,
        "operationMetrics": {
            "numFiles": "6",
            "numOutputBytes": "2477",
            "numOutputRows": "7",
        },
        "version": 0,
    }


@pytest.mark.parametrize("flatten", [True, False])
def test_add_actions_table(flatten: bool):
    table_path = "../crates/test/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    actions_df = dt.get_add_actions(flatten)
    # RecordBatch doesn't have a sort_by method yet
    actions_df = pa.Table.from_batches([actions_df]).sort_by("path").to_batches()[0]

    assert actions_df.num_rows == 6
    assert actions_df["path"] == pa.array(
        [
            "year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet",
            "year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet",
            "year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet",
            "year=2021/month=12/day=20/part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet",
            "year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet",
            "year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet",
        ]
    )
    assert actions_df["size_bytes"] == pa.array([414, 414, 414, 407, 414, 414])
    assert actions_df["data_change"] == pa.array([True] * 6)
    assert actions_df["modification_time"] == pa.array(
        [1615555646000] * 6, type=pa.timestamp("ms")
    )

    if flatten:
        partition_year = actions_df["partition.year"]
        partition_month = actions_df["partition.month"]
        partition_day = actions_df["partition.day"]
    else:
        partition_year = actions_df["partition_values"].field("year")
        partition_month = actions_df["partition_values"].field("month")
        partition_day = actions_df["partition_values"].field("day")

    assert partition_year == pa.array(["2020"] * 3 + ["2021"] * 3)
    assert partition_month == pa.array(["1", "2", "2", "12", "12", "4"])
    assert partition_day == pa.array(["1", "3", "5", "20", "4", "5"])


def assert_correct_files(dt: DeltaTable, partition_filters, expected_paths):
    assert dt.files(partition_filters) == expected_paths
    absolute_paths = [os.path.join(dt.table_uri, path) for path in expected_paths]
    assert dt.file_uris(partition_filters) == absolute_paths


def test_get_files_partitioned_table():
    table_path = "../crates/test/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    table_path = (
        Path.cwd().parent / "crates/test/tests/data/delta-0.8.0-partitioned"
    ).as_posix()

    partition_filters = [("day", "=", "3")]
    paths = [
        "year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet"
    ]
    assert_correct_files(dt, partition_filters, paths)

    # Also accepts integers
    partition_filters = [("day", "=", 3)]
    assert_correct_files(dt, partition_filters, paths)

    partition_filters = [("day", "!=", "3")]
    paths = [
        "year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet",
        "year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet",
        "year=2021/month=12/day=20/part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet",
        "year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet",
        "year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet",
    ]
    assert_correct_files(dt, partition_filters, paths)

    partition_filters = [("day", "in", ["3", "20"])]
    paths = [
        "year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet",
        "year=2021/month=12/day=20/part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet",
    ]
    assert_correct_files(dt, partition_filters, paths)

    partition_filters = [("day", "not in", ["3", "20"])]
    paths = [
        "year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet",
        "year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet",
        "year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet",
        "year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet",
    ]
    assert_correct_files(dt, partition_filters, paths)

    partition_filters = [("day", "not in", ["3", "20"]), ("year", "=", "2021")]
    paths = [
        "year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet",
        "year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet",
    ]
    assert_correct_files(dt, partition_filters, paths)

    partition_filters = [("invalid_operation", "=>", "3")]
    with pytest.raises(Exception) as exception:
        dt.files(partition_filters)
    assert (
        str(exception.value)
        == 'Invalid partition filter found: ("invalid_operation", "=>", "3").'
    )

    partition_filters = [("invalid_operation", "=", ["3", "20"])]
    with pytest.raises(Exception) as exception:
        dt.files(partition_filters)
    assert (
        str(exception.value)
        == 'Invalid partition filter found: ("invalid_operation", "=", ["3", "20"]).'
    )

    partition_filters = [("unknown", "=", "3")]
    with pytest.raises(Exception) as exception:
        dt.files(partition_filters)
    assert (
        str(exception.value)
        == 'Tried to filter partitions on non-partitioned columns: [\n    "unknown",\n]'
    )


@pytest.mark.pandas
def test_delta_table_to_pandas():
    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)
    assert dt.to_pandas().equals(pd.DataFrame({"id": [5, 7, 9]}))


@pytest.mark.pandas
def test_delta_table_with_filesystem():
    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)
    filesystem = SubTreeFileSystem(table_path, LocalFileSystem())
    assert dt.to_pandas(filesystem=filesystem).equals(pd.DataFrame({"id": [5, 7, 9]}))


@pytest.mark.pandas
def test_delta_table_with_filters():
    table_path = "../crates/test/tests/data/COVID-19_NYT"
    dt = DeltaTable(table_path)
    dataset = dt.to_pyarrow_dataset()

    filter_expr = ds.field("date") > "2021-02-20"
    data = dataset.to_table(filter=filter_expr)
    assert (
        len(dt.to_pandas(filters=[("date", ">", "2021-02-20")]))
        == len(dt.to_pandas(filters=filter_expr))
        == data.num_rows
    )

    filter_expr = (ds.field("date") > "2021-02-20") | (
        ds.field("state").isin(["Alabama", "Wyoming"])
    )
    data = dataset.to_table(filter=filter_expr)
    assert (
        len(
            dt.to_pandas(
                filters=[
                    [("date", ">", "2021-02-20")],
                    [("state", "in", ["Alabama", "Wyoming"])],
                ]
            )
        )
        == len(dt.to_pandas(filters=filter_expr))
        == data.num_rows
    )

    filter_expr = (ds.field("date") > "2021-02-20") & (
        ds.field("state").isin(["Alabama", "Wyoming"])
    )
    data = dataset.to_table(filter=filter_expr)
    assert (
        len(
            dt.to_pandas(
                filters=[
                    ("date", ">", "2021-02-20"),
                    ("state", "in", ["Alabama", "Wyoming"]),
                ]
            )
        )
        == len(dt.to_pandas(filters=filter_expr))
        == data.num_rows
    )


def test_writer_fails_on_protocol():
    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)
    dt.protocol = Mock(return_value=ProtocolVersions(2, 1, None, None))
    with pytest.raises(DeltaProtocolError):
        dt.to_pyarrow_dataset()
    with pytest.raises(DeltaProtocolError):
        dt.to_pyarrow_table()
    with pytest.raises(DeltaProtocolError):
        dt.to_pandas()


class ExcPassThroughThread(Thread):
    """Wrapper around `threading.Thread` that propagates exceptions."""

    def __init__(self, target, *args):
        Thread.__init__(self, target=target, *args)
        self.exc = None

    def run(self):
        """Method representing the thread's activity.
        You may override this method in a subclass. The standard run() method
        invokes the callable object passed to the object's constructor as the
        target argument, if any, with sequential and keyword arguments taken
        from the args and kwargs arguments, respectively.
        """
        try:
            Thread.run(self)
        except BaseException as e:
            self.exc = e

    def join(self, timeout=None):
        """Wait until the thread terminates.
        This blocks the calling thread until the thread whose join() method is
        called terminates -- either normally or through an unhandled exception
        or until the optional timeout occurs.
        When the timeout argument is present and not None, it should be a
        floating point number specifying a timeout for the operation in seconds
        (or fractions thereof). As join() always returns None, you must call
        is_alive() after join() to decide whether a timeout happened -- if the
        thread is still alive, the join() call timed out.
        When the timeout argument is not present or None, the operation will
        block until the thread terminates.
        A thread can be join()ed many times.
        join() raises a RuntimeError if an attempt is made to join the current
        thread as that would cause a deadlock. It is also an error to join() a
        thread before it has been started and attempts to do so raises the same
        exception.
        """
        super(ExcPassThroughThread, self).join(timeout)
        if self.exc:
            raise self.exc


@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=5, method="thread")
def test_read_multiple_tables_from_s3(s3_localstack):
    """Should be able to create multiple cloud storage based DeltaTable instances
    without blocking on async crates/test function calls.
    """
    for path in ["s3://deltars/simple", "s3://deltars/simple"]:
        t = DeltaTable(path)
        assert t.files() == [
            "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
            "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
            "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
            "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
            "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
        ]


@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=10, method="thread")
def test_read_multiple_tables_from_s3_multi_threaded(s3_localstack):
    thread_count = 10
    b = Barrier(thread_count, timeout=5)

    # make sure it works within multiple threads as well
    def read_table():
        b.wait()
        t = DeltaTable("s3://deltars/simple")
        assert t.files() == [
            "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
            "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
            "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
            "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
            "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
        ]

    threads = [ExcPassThroughThread(target=read_table) for _ in range(thread_count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def assert_num_fragments(table, predicate, count):
    frags = table.to_pyarrow_dataset().get_fragments(filter=predicate)
    assert len(list(frags)) == count


def test_filter_nulls(tmp_path: Path):
    def assert_scan_equals(table, predicate, expected):
        data = table.to_pyarrow_dataset().to_table(filter=predicate).sort_by("part")
        assert data == expected

    # 1 all-valid part, 1 all-null part, and 1 mixed part.
    data = pa.table(
        {"part": ["a", "a", "b", "b", "c", "c"], "value": [1, 1, None, None, 2, None]}
    )

    write_deltalake(tmp_path, data, partition_by="part")

    table = DeltaTable(tmp_path)

    # Note: we assert number of fragments returned because that verifies
    # that file skipping is working properly.

    # is valid predicate
    predicate = ds.field("value").is_valid()
    assert_num_fragments(table, predicate, 2)
    expected = pa.table({"part": ["a", "a", "c"], "value": [1, 1, 2]})
    assert_scan_equals(table, predicate, expected)

    # is null predicate
    predicate = ds.field("value").is_null()
    assert_num_fragments(table, predicate, 2)
    expected = pa.table(
        {"part": ["b", "b", "c"], "value": pa.array([None, None, None], pa.int64())}
    )
    assert_scan_equals(table, predicate, expected)

    # inequality predicate
    predicate = ds.field("value") > 1
    assert_num_fragments(table, predicate, 1)
    expected = pa.table({"part": ["c"], "value": pa.array([2], pa.int64())})
    assert_scan_equals(table, predicate, expected)

    # also test nulls in partition values
    data = pa.table({"part": pa.array([None], pa.string()), "value": [3]})
    write_deltalake(
        table,
        data,
        mode="append",
        partition_by="part",
    )

    # null predicate
    predicate = ds.field("part").is_null()
    assert_num_fragments(table, predicate, 1)
    expected = pa.table({"part": pa.array([None], pa.string()), "value": [3]})
    assert_scan_equals(table, predicate, expected)

    # valid predicate
    predicate = ds.field("part").is_valid()
    assert_num_fragments(table, predicate, 3)
    expected = pa.table(
        {"part": ["a", "a", "b", "b", "c", "c"], "value": [1, 1, None, None, 2, None]}
    )
    assert_scan_equals(table, predicate, expected)

    # inequality predicate
    predicate = ds.field("part") < "c"
    assert_num_fragments(table, predicate, 2)
    expected = pa.table({"part": ["a", "a", "b", "b"], "value": [1, 1, None, None]})
    assert_scan_equals(table, predicate, expected)


def test_issue_1653_filter_bool_partition(tmp_path: Path):
    ta = pa.Table.from_pydict(
        {
            "bool_col": [True, False, True, False],
            "int_col": [0, 1, 2, 3],
            "str_col": ["a", "b", "c", "d"],
        }
    )
    write_deltalake(
        tmp_path, ta, partition_by=["bool_col", "int_col"], mode="overwrite"
    )
    dt = DeltaTable(tmp_path)

    assert (
        dt.to_pyarrow_table(
            filters=[
                ("int_col", "=", 0),
                ("bool_col", "=", True),
            ]
        ).num_rows
        == 1
    )
    assert (
        len(
            dt.file_uris(
                partition_filters=[
                    ("int_col", "=", 0),
                    ("bool_col", "=", "true"),
                ]
            )
        )
        == 1
    )
    assert (
        len(
            dt.file_uris(
                partition_filters=[
                    ("int_col", "=", 0),
                    ("bool_col", "=", True),
                ]
            )
        )
        == 1
    )


@pytest.mark.parametrize(
    "input_value, expected",
    [
        (True, "true"),
        (False, "false"),
        (1, "1"),
        (1.5, "1.5"),
        ("string", "string"),
        (date(2023, 10, 17), "2023-10-17"),
        (datetime(2023, 10, 17, 12, 34, 56), "2023-10-17 12:34:56"),
        (b"bytes", "bytes"),
        ([True, False], ["true", "false"]),
        ([1, 2], ["1", "2"]),
        ([1.5, 2.5], ["1.5", "2.5"]),
        (["a", "b"], ["a", "b"]),
        ([date(2023, 10, 17), date(2023, 10, 18)], ["2023-10-17", "2023-10-18"]),
        (
            [datetime(2023, 10, 17, 12, 34, 56), datetime(2023, 10, 18, 12, 34, 56)],
            ["2023-10-17 12:34:56", "2023-10-18 12:34:56"],
        ),
        ([b"bytes", b"testbytes"], ["bytes", "testbytes"]),
    ],
)
def test_encode_partition_value(input_value: Any, expected: str) -> None:
    if isinstance(input_value, list):
        assert [encode_partition_value(val) for val in input_value] == expected
    else:
        assert encode_partition_value(input_value) == expected


def test_partitions_partitioned_table():
    table_path = "../crates/test/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    expected = [
        {"year": "2020", "month": "2", "day": "5"},
        {"year": "2021", "month": "12", "day": "4"},
        {"year": "2020", "month": "2", "day": "3"},
        {"year": "2021", "month": "4", "day": "5"},
        {"year": "2020", "month": "1", "day": "1"},
        {"year": "2021", "month": "12", "day": "20"},
    ]
    actual = dt.partitions()
    for partition in expected:
        assert partition in actual


def test_partitions_filtering_partitioned_table():
    table_path = "../crates/test/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    expected = [
        {"day": "5", "month": "4", "year": "2021"},
        {"day": "20", "month": "12", "year": "2021"},
        {"day": "4", "month": "12", "year": "2021"},
    ]

    partition_filters = [("year", ">=", "2021")]
    actual = dt.partitions(partition_filters=partition_filters)
    assert len(expected) == len(actual)
    for partition in expected:
        partition in actual


def test_partitions_date_partitioned_table():
    table_path = tempfile.gettempdir() + "/date_partition_table"
    date_partitions = [
        date(2024, 8, 1),
        date(2024, 8, 2),
        date(2024, 8, 3),
        date(2024, 8, 4),
    ]
    sample_data = pa.table(
        {
            "date_field": pa.array(date_partitions, pa.date32()),
            "numeric_data": pa.array([1, 2, 3, 4], pa.int64()),
        }
    )
    write_deltalake(
        table_path, sample_data, mode="overwrite", partition_by=["date_field"]
    )

    delta_table = DeltaTable(table_path)
    expected = [
        {"date_field": "2024-08-01"},
        {"date_field": "2024-08-02"},
        {"date_field": "2024-08-03"},
        {"date_field": "2024-08-04"},
    ]
    actual = sorted(delta_table.partitions(), key=lambda x: x["date_field"])
    assert expected == actual


def test_partitions_special_partitioned_table():
    table_path = "../crates/test/tests/data/delta-0.8.0-special-partition"
    dt = DeltaTable(table_path)

    expected = [{"x": "A/A"}, {"x": "B B"}]
    actual = dt.partitions()
    for partition in expected:
        partition in actual


def test_partitions_unpartitioned_table():
    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)
    assert len(dt.partitions()) == 0


def test_read_table_last_checkpoint_not_updated():
    dt = DeltaTable("../crates/test/tests/data/table_failed_last_checkpoint_update")

    assert dt.version() == 3


def test_is_deltatable_valid_path():
    table_path = "../crates/test/tests/data/simple_table"
    assert DeltaTable.is_deltatable(table_path)


def test_is_deltatable_invalid_path():
    # Nonce ensures that the table_path always remains an invalid table path.
    nonce = int(random() * 10000)
    table_path = "../crates/test/tests/data/simple_table_invalid_%s" % nonce
    assert not DeltaTable.is_deltatable(table_path)


def test_is_deltatable_with_storage_opts():
    table_path = "../crates/test/tests/data/simple_table"
    storage_options = {
        "AWS_ACCESS_KEY_ID": "THE_AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY": "THE_AWS_SECRET_ACCESS_KEY",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_S3_LOCKING_PROVIDER": "dynamodb",
        "DELTA_DYNAMO_TABLE_NAME": "custom_table_name",
    }
    assert DeltaTable.is_deltatable(table_path, storage_options=storage_options)
