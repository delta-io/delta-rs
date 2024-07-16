from collections import Counter
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pytest

from deltalake.fs import DeltaStorageHandler


@pytest.fixture
def file_systems(tmp_path: Path):
    store = fs.PyFileSystem(DeltaStorageHandler(str(tmp_path.absolute())))
    arrow_fs = fs.SubTreeFileSystem(str(tmp_path.absolute()), fs.LocalFileSystem())
    return (store, arrow_fs)


@pytest.fixture
def table_data():
    return pa.Table.from_arrays(
        [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["int", "str"]
    )


def test_file_info(file_systems, table_data):
    store, arrow_fs = file_systems
    file_path = "table.parquet"

    pq.write_table(table_data, file_path, filesystem=arrow_fs)

    info = store.get_file_info(file_path)
    arrow_info = arrow_fs.get_file_info(file_path)

    assert type(info) is type(arrow_info)
    assert info.path == arrow_info.path
    assert info.type == arrow_info.type
    assert info.size == arrow_info.size
    assert info.mtime_ns == arrow_info.mtime_ns
    assert info.mtime == arrow_info.mtime


def test_get_file_info_selector(file_systems):
    store, arrow_fs = file_systems
    table = pa.table({"a": range(10), "b": np.random.randn(10), "c": [1, 2] * 5})
    partitioning = ds.partitioning(pa.schema([("c", pa.int64())]), flavor="hive")
    ds.write_dataset(
        table,
        "/",
        partitioning=partitioning,
        format="parquet",
        filesystem=arrow_fs,
    )

    selector = fs.FileSelector("/", recursive=True)
    infos = store.get_file_info(selector)
    arrow_infos = arrow_fs.get_file_info(selector)

    assert Counter([i.type for i in infos]) == Counter([i.type for i in arrow_infos])


def test_open_input_file(file_systems, table_data):
    store, arrow_fs = file_systems
    file_path = "table.parquet"

    pq.write_table(table_data, file_path, filesystem=arrow_fs)

    file = store.open_input_file(file_path)
    arrow_file = arrow_fs.open_input_file(file_path)

    # Check the metadata
    assert file.mode == arrow_file.mode
    assert file.closed == arrow_file.closed
    assert file.size() == arrow_file.size()
    assert file.isatty() == arrow_file.isatty()
    assert file.readable() == arrow_file.readable()
    assert file.seekable() == arrow_file.seekable()

    # Check reading the same content
    assert file.read() == arrow_file.read()
    # Check subsequent read (should return no data anymore)
    assert file.read() == arrow_file.read()
    assert file.read() == b""

    file = store.open_input_file(file_path)
    arrow_file = arrow_fs.open_input_file(file_path)

    # Check seeking works as expected
    assert file.tell() == arrow_file.tell()
    assert file.seek(2) == arrow_file.seek(2)
    assert file.tell() == arrow_file.tell()
    assert file.tell() == 2

    # check reading works as expected
    assert file.read(10) == arrow_file.read(10)
    assert file.read1(10) == arrow_file.read1(10)
    assert file.read_at(10, 0) == arrow_file.read_at(10, 0)


def test_open_input_file_with_size(tmp_path, table_data):
    file_path = "table.parquet"
    input_size = 12345  # incorrect file size for testing purposes

    # test that injected file size gets stored correctly
    store1 = DeltaStorageHandler(
        str(tmp_path.absolute()), known_sizes={file_path: input_size}
    )
    wrapped_fs = fs.PyFileSystem(store1)
    arrow_fs = fs.SubTreeFileSystem(str(tmp_path.absolute()), fs.LocalFileSystem())
    pq.write_table(table_data, file_path, filesystem=arrow_fs)
    file = wrapped_fs.open_input_file(file_path)
    assert file.size() == input_size

    # confirm that true size is different
    store2 = DeltaStorageHandler(str(tmp_path.absolute()))
    wrapped_fs = fs.PyFileSystem(store2)
    arrow_fs = fs.SubTreeFileSystem(str(tmp_path.absolute()), fs.LocalFileSystem())
    pq.write_table(table_data, file_path, filesystem=arrow_fs)
    file = wrapped_fs.open_input_file(file_path)
    assert file.size() != input_size


def test_read_table(file_systems, table_data):
    store, arrow_fs = file_systems
    file_path = "table.parquet"

    pq.write_table(table_data, file_path, filesystem=arrow_fs)

    table = pq.read_table(file_path, filesystem=store)
    arrow_table = pq.read_table(file_path, filesystem=arrow_fs)

    assert isinstance(table, pa.Table)
    assert table.equals(arrow_table)


def test_read_dataset(file_systems):
    store, arrow_fs = file_systems
    table = pa.table({"a": range(10), "b": np.random.randn(10), "c": [1, 2] * 5})

    pq.write_table(table.slice(0, 5), "data1.parquet", filesystem=arrow_fs)
    pq.write_table(table.slice(5, 10), "data2.parquet", filesystem=arrow_fs)

    dataset = ds.dataset("/", format="parquet", filesystem=store)
    ds_table = dataset.to_table()

    assert table.schema == dataset.schema
    assert table.equals(ds_table)


def test_write_table(file_systems):
    store, _ = file_systems
    table = pa.table({"a": range(10), "b": np.random.randn(10), "c": [1, 2] * 5})

    pq.write_table(table.slice(0, 5), "data1.parquet", filesystem=store)
    pq.write_table(table.slice(5, 10), "data2.parquet", filesystem=store)

    dataset = ds.dataset("/", format="parquet", filesystem=store)
    ds_table = dataset.to_table()

    assert table.schema == ds_table.schema
    assert table.equals(ds_table)


def test_write_partitioned_dataset(file_systems):
    store, arrow_fs = file_systems
    table = pa.table({"a": range(10), "b": np.random.randn(10), "c": [1, 2] * 5})

    partitioning = ds.partitioning(pa.schema([("c", pa.int64())]), flavor="hive")
    ds.write_dataset(
        table,
        "/",
        partitioning=partitioning,
        format="parquet",
        filesystem=store,
    )

    dataset = ds.dataset(
        "/", format="parquet", filesystem=arrow_fs, partitioning=partitioning
    )
    ds_table = dataset.to_table().select(["a", "b", "c"])

    dataset = ds.dataset(
        "/", format="parquet", filesystem=store, partitioning=partitioning
    )
    ds_table2 = dataset.to_table().select(["a", "b", "c"])

    assert table.schema == ds_table.schema
    assert table.schema == ds_table2.schema
    assert table.shape == ds_table.shape
    assert table.shape == ds_table2.shape
