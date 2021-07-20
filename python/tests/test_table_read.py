from threading import Barrier, Thread

import pandas as pd
import pytest

from deltalake import DeltaTable, Metadata


def test_read_simple_table_to_dict():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"id": [5, 7, 9]}


def test_read_simple_table_by_version_to_dict():
    table_path = "../rust/tests/data/delta-0.2.0"
    dt = DeltaTable(table_path, version=2)
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"value": [1, 2, 3]}


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


def test_vacuum_dry_run_simple_table():
    table_path = "../rust/tests/data/delta-0.2.0"
    dt = DeltaTable(table_path)
    retention_periods = 169
    tombstones = dt.vacuum(retention_periods)
    tombstones.sort()
    assert tombstones == [
        "../rust/tests/data/delta-0.2.0/part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet",
        "../rust/tests/data/delta-0.2.0/part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet",
        "../rust/tests/data/delta-0.2.0/part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet",
        "../rust/tests/data/delta-0.2.0/part-00001-4327c977-2734-4477-9507-7ccf67924649-c000.snappy.parquet",
    ]

    retention_periods = -1
    with pytest.raises(Exception) as exception:
        dt.vacuum(retention_periods)
    assert str(exception.value) == "The retention periods should be positive."

    retention_periods = 167
    with pytest.raises(Exception) as exception:
        dt.vacuum(retention_periods)
    assert (
        str(exception.value)
        == "Invalid retention period, retention for Vacuum must be greater than 1 week (168 hours)"
    )


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


def test_get_files_partitioned_table():
    table_path = "../rust/tests/data/delta-0.8.0-partitioned"
    dt = DeltaTable(table_path)
    partition_filters = [("day", "=", "3")]
    assert dt.files_by_partitions(partition_filters=partition_filters) == [
        f"{table_path}/year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet"
    ]
    partition_filters = [("day", "!=", "3")]
    assert dt.files_by_partitions(partition_filters=partition_filters) == [
        f"{table_path}/year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet",
        f"{table_path}/year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet",
        f"{table_path}/year=2021/month=12/day=20/part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet",
        f"{table_path}/year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet",
        f"{table_path}/year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet",
    ]
    partition_filters = [("day", "in", ["3", "20"])]
    assert dt.files_by_partitions(partition_filters=partition_filters) == [
        f"{table_path}/year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet",
        f"{table_path}/year=2021/month=12/day=20/part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet",
    ]
    partition_filters = [("day", "not in", ["3", "20"])]
    assert dt.files_by_partitions(partition_filters=partition_filters) == [
        f"{table_path}/year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet",
        f"{table_path}/year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet",
        f"{table_path}/year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet",
        f"{table_path}/year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet",
    ]
    partition_filters = [("day", "not in", ["3", "20"]), ("year", "=", "2021")]
    assert dt.files_by_partitions(partition_filters=partition_filters) == [
        f"{table_path}/year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet",
        f"{table_path}/year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet",
    ]
    partition_filters = [("invalid_operation", "=>", "3")]
    with pytest.raises(Exception) as exception:
        dt.files_by_partitions(partition_filters=partition_filters)
    assert (
        str(exception.value)
        == 'Invalid partition filter found: ("invalid_operation", "=>", "3").'
    )

    partition_filters = [("invalid_operation", "=", ["3", "20"])]
    with pytest.raises(Exception) as exception:
        dt.files_by_partitions(partition_filters=partition_filters)
    assert (
        str(exception.value)
        == 'Invalid partition filter found: ("invalid_operation", "=", ["3", "20"]).'
    )

    partition_filters = [("day", "=", 3)]
    with pytest.raises(Exception) as exception:
        dt.files_by_partitions(partition_filters=partition_filters)
    assert (
        str(exception.value)
        == "Only the type String is currently allowed inside the partition filters."
    )

    partition_filters = [("unknown", "=", "3")]
    assert dt.files_by_partitions(partition_filters=partition_filters) == []


def test_delta_table_to_pandas():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    assert dt.to_pandas().equals(pd.DataFrame({"id": [5, 7, 9]}))


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
def test_read_multiple_tables_from_s3(s3cred):
    """
    Should be able to create multiple cloud storage based DeltaTable instances
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
def test_read_multiple_tables_from_s3_multi_threaded(s3cred):
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
