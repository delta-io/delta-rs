import os
from datetime import datetime
from pathlib import Path
from threading import Barrier, Thread
from unittest.mock import Mock

from packaging import version

from deltalake.exceptions import DeltaProtocolError
from deltalake.table import ProtocolVersions

try:
    import pandas as pd
except ModuleNotFoundError:
    _has_pandas = False
else:
    _has_pandas = True

import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from pyarrow.dataset import ParquetReadOptions
from pyarrow.fs import LocalFileSystem, SubTreeFileSystem

from deltalake import DeltaTable


def test_read_table_with_edge_timestamps():
    table_path = "../rust/tests/data/table_with_edge_timestamps"
    dt = DeltaTable(table_path)
    dataset = dt.to_pyarrow_dataset(
        parquet_read_options=ParquetReadOptions(coerce_int96_timestamp_unit="ms")
    )
    assert dataset.to_table().to_pydict() == {
        "BIG_DATE": [datetime(9999, 12, 31, 0, 0, 0), datetime(9999, 12, 30, 0, 0, 0)],
        "NORMAL_DATE": [datetime(2022, 1, 1, 0, 0, 0), datetime(2022, 2, 1, 0, 0, 0)],
        "SOME_VALUE": [1, 2],
    }
    # Can push down filters to these timestamps.
    predicate = ds.field("BIG_DATE") == datetime(9999, 12, 31, 0, 0, 0)
    assert len(list(dataset.get_fragments(predicate))) == 1


def test_read_simple_table_to_dict():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"id": [5, 7, 9]}


def test_read_simple_table_by_version_to_dict():
    table_path = "../rust/tests/data/delta-0.2.0"
    dt = DeltaTable(table_path, version=2)
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"value": [1, 2, 3]}


def test_read_simple_table_using_options_to_dict():
    table_path = "../rust/tests/data/delta-0.2.0"
    dt = DeltaTable(table_path, version=2, storage_options={})
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"value": [1, 2, 3]}


def test_load_with_datetime():
    log_dir = "../rust/tests/data/simple_table/_delta_log"
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

    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    dt.load_with_datetime("2020-05-01T00:47:31-07:00")
    assert dt.version() == 0
    dt.load_with_datetime("2020-05-02T22:47:31-07:00")
    assert dt.version() == 1
    dt.load_with_datetime("2020-05-25T22:47:31-07:00")
    assert dt.version() == 4


def test_load_with_datetime_bad_format():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    with pytest.raises(Exception) as exception:
        dt.load_with_datetime("2020-05-01T00:47:31")
    assert (
        str(exception.value)
        == "Failed to parse datetime string: premature end of input"
    )
    with pytest.raises(Exception) as exception:
        dt.load_with_datetime("2020-05-01 00:47:31")
    assert (
        str(exception.value)
        == "Failed to parse datetime string: input contains invalid characters"
    )
    with pytest.raises(Exception) as exception:
        dt.load_with_datetime("2020-05-01T00:47:31+08")
    assert (
        str(exception.value)
        == "Failed to parse datetime string: premature end of input"
    )


def test_read_simple_table_update_incremental():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path, version=0)
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"id": [0, 1, 2, 3, 4]}
    dt.update_incremental()
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"id": [5, 7, 9]}


def test_read_partitioned_table_to_dict():
    table_path = "../rust/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    expected = {
        "value": ["1", "2", "3", "6", "7", "5", "4"],
        "year": ["2020", "2020", "2020", "2021", "2021", "2021", "2021"],
        "month": ["1", "2", "2", "12", "12", "12", "4"],
        "day": ["1", "3", "5", "20", "20", "4", "5"],
    }
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == expected


def test_read_partitioned_table_with_partitions_filters_to_dict():
    table_path = "../rust/tests/data/delta-0.8.0-partitioned"
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
    table_path = "../rust/tests/data/delta-0.8-empty"
    dt = DeltaTable(table_path)
    expected = {"column": []}

    assert dt.to_pyarrow_dataset().to_table().to_pydict() == expected


def test_read_table_with_column_subset():
    table_path = "../rust/tests/data/delta-0.8.0-partitioned"
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
    table_path = "../rust/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)

    assert dt.schema().to_pyarrow().field("value").type == pa.string()

    read_options = ds.ParquetReadOptions(dictionary_columns={"value"})

    data = dt.to_pyarrow_dataset(parquet_read_options=read_options).to_table()

    assert data.schema.field("value").type == pa.dictionary(pa.int32(), pa.string())
    assert data.schema.field("day").type == pa.string()


def test_read_table_with_filter():
    table_path = "../rust/tests/data/delta-0.8.0-partitioned"
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
    table_path = "../rust/tests/data/COVID-19_NYT"
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

    # PyArrow added support for is_null and is_valid simplification in 8.0.0
    if version.parse(pa.__version__).major >= 8:
        filter_expr = ds.field("cases").is_null()
        assert len(list(dataset.get_fragments(filter=filter_expr))) == 0

        data = dataset.to_table(filter=filter_expr)
        assert data.num_rows == 0


def test_read_partitioned_table_metadata():
    table_path = "../rust/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    metadata = dt.metadata()
    assert metadata.id == "fe5a3c11-30d4-4dd7-b115-a1c121e66a4e"
    assert metadata.name is None
    assert metadata.description is None
    assert metadata.partition_columns == ["year", "month", "day"]
    assert metadata.created_time == 1615555644515
    assert metadata.configuration == {}


def test_read_partitioned_table_protocol():
    table_path = "../rust/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    protocol = dt.protocol()
    assert protocol.min_reader_version == 1
    assert protocol.min_writer_version == 2


def test_read_table_with_cdc():
    table_path = "../rust/tests/data/simple_table_with_cdc"
    dt = DeltaTable(table_path)
    assert dt.to_pyarrow_table().to_pydict() == {
        "id": [0],
        "name": ["Mino"],
    }


def test_history_partitioned_table_metadata():
    table_path = "../rust/tests/data/delta-0.8.0-partitioned"
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
    }


@pytest.mark.parametrize("flatten", [True, False])
def test_add_actions_table(flatten: bool):
    table_path = "../rust/tests/data/delta-0.8.0-partitioned"
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
    table_path = "../rust/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    table_path = (
        Path.cwd().parent / "rust/tests/data/delta-0.8.0-partitioned"
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
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    assert dt.to_pandas().equals(pd.DataFrame({"id": [5, 7, 9]}))


@pytest.mark.pandas
def test_delta_table_with_filesystem():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    filesystem = SubTreeFileSystem(table_path, LocalFileSystem())
    assert dt.to_pandas(filesystem=filesystem).equals(pd.DataFrame({"id": [5, 7, 9]}))


@pytest.mark.pandas
def test_delta_table_with_filters():
    table_path = "../rust/tests/data/COVID-19_NYT"
    dt = DeltaTable(table_path)
    dataset = dt.to_pyarrow_dataset()

    filter_expr = ds.field("date") > "2021-02-20"
    data = dataset.to_table(filter=filter_expr)
    assert len(dt.to_pandas(filters=[("date", ">", "2021-02-20")])) == data.num_rows

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
        == data.num_rows
    )


def test_writer_fails_on_protocol():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    dt.protocol = Mock(return_value=ProtocolVersions(2, 1))
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
    without blocking on async rust function calls.
    """
    for path in ["s3://deltars/simple", "s3://deltars/simple"]:
        t = DeltaTable(path)
        assert t.files() == [
            "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
            "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
            "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
            "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
            "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
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
            "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
            "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
            "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
            "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
            "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
        ]

    threads = [ExcPassThroughThread(target=read_table) for _ in range(thread_count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
