from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from arro3.core import Array, ChunkedArray, DataType, Table
from numpy.random import standard_normal

from deltalake import DeltaTable, write_deltalake

if TYPE_CHECKING:
    from minio import Minio

# NOTE: make sure to run these in release mode with
# PROFILE=python-release make develop
# When profiling, use:
# PROFILE=profiling make develop


@pytest.fixture()
def sample_table() -> Table:
    max_size_bytes = 128 * 1024 * 1024
    ncols = 20
    nrows = max_size_bytes // 20 // 8
    rows = {f"x{i}": standard_normal(nrows) for i in range(ncols)}
    rows["i"] = Array(range(nrows), type=DataType.int64())
    return Table.from_pydict(rows)


@pytest.mark.benchmark(group="write")
def test_benchmark_write(benchmark, sample_table: Table, tmp_path: Path):
    def setup() -> None:
        table_path = tmp_path / str(uuid.uuid4())
        table_path.mkdir()
        return (table_path,), dict()

    def func(table_path: str) -> None:
        write_deltalake(table_path, sample_table)

    benchmark.pedantic(func, setup=setup, rounds=5, warmup_rounds=3)

    # TODO: figure out why this assert is failing
    # dt = DeltaTable(str(tmp_path))
    # table = (
    #     QueryBuilder()
    #     .register("tbl", dt)
    #     .execute("select * from tbl order by i asc")
    #     .read_all()
    # )
    # assert table == sample_table


@pytest.mark.benchmark(group="write")
def test_benchmark_write_minio(
    benchmark, sample_table: Table, minio_container: tuple[dict, Minio]
):
    import uuid

    bucket_name = f"delta-bench-{uuid.uuid4()}"
    storage_options, minio = minio_container
    minio.make_bucket(bucket_name)

    def setup() -> None:
        table_path = f"s3://{bucket_name}/{uuid.uuid4()}"
        return (table_path,), dict()

    def func(table_path: str) -> None:
        write_deltalake(table_path, sample_table, storage_options=storage_options)

    benchmark.pedantic(func, setup=setup, rounds=5, warmup_rounds=3)


@pytest.mark.pyarrow
@pytest.mark.benchmark(group="read")
def test_benchmark_read(benchmark, sample_table: Table, tmp_path: Path):
    import pyarrow as pa

    write_deltalake(str(tmp_path), sample_table)
    dt = DeltaTable(str(tmp_path))

    result = benchmark(dt.to_pyarrow_table)
    assert result.sort_by("i") == pa.table(sample_table)


@pytest.mark.pyarrow
@pytest.mark.benchmark(group="read")
def test_benchmark_read_pyarrow(benchmark, sample_table: Table, tmp_path: Path):
    import pyarrow as pa
    import pyarrow.fs as pa_fs

    write_deltalake(str(tmp_path), sample_table)
    dt = DeltaTable(str(tmp_path))

    fs = pa_fs.SubTreeFileSystem(str(tmp_path), pa_fs.LocalFileSystem())
    result = benchmark(dt.to_pyarrow_table, filesystem=fs)
    assert result.sort_by("i") == pa.table(sample_table)


@pytest.mark.benchmark(group="optimize")
@pytest.mark.parametrize("max_tasks", [1, 5])
def test_benchmark_optimize(
    benchmark, sample_table: Table, tmp_path: Path, max_tasks: int
):
    # Create 2 partitions, each partition with 10 files.
    # Each file is about 100MB, so the total size is 2GB.
    files_per_part = 10
    parts = ["a", "b", "c", "d", "e"]

    nrows = int(sample_table.num_rows / files_per_part)
    for part in parts:
        tab = sample_table.slice(0, nrows)
        tab = tab.append_column(
            "part", ChunkedArray(Array([part] * nrows, type=DataType.utf8()))
        )
        for _ in range(files_per_part):
            write_deltalake(tmp_path, tab, mode="append", partition_by=["part"])

    dt = DeltaTable(tmp_path)

    assert len(dt.files()) == files_per_part * len(parts)
    initial_version = dt.version()

    def setup():
        # Instead of recreating the table for each benchmark run, we just delete
        # the optimize log file
        optimize_version = initial_version + 1
        try:
            os.remove(
                os.path.join(tmp_path, "_delta_log", f"{optimize_version:020}.json")
            )
        except FileNotFoundError:
            pass

        # Reload the table after we have altered the log
        dt = DeltaTable(tmp_path)
        assert dt.version() == initial_version

        return (dt,), dict(max_concurrent_tasks=max_tasks)

    def func(dt, max_concurrent_tasks):
        return dt.optimize.compact(
            max_concurrent_tasks=max_concurrent_tasks, target_size=1024 * 1024 * 1024
        )

    # We need to recreate the table for each benchmark run
    results = benchmark.pedantic(func, setup=setup, rounds=5, warmup_rounds=3)

    assert results["numFilesRemoved"] == 50
    assert results["numFilesAdded"] == 5
    assert results["partitionsOptimized"] == 5


@pytest.mark.benchmark(group="optimize", warmup=False)
@pytest.mark.parametrize("max_tasks", [1, 5])
def test_benchmark_optimize_minio(
    benchmark, sample_table: Table, minio_container: tuple[dict, Minio], max_tasks: int
):
    bucket_name = f"delta-bench-{uuid.uuid4()}"
    table_path = f"s3://{bucket_name}/optimize-test"

    storage_options, minio = minio_container
    minio.make_bucket(bucket_name)

    # Create 2 partitions, each partition with 10 files.
    # Each file is about 100MB, so the total size is 2GB.
    files_per_part = 10
    parts = ["a", "b", "c", "d", "e"]

    nrows = int(sample_table.num_rows / files_per_part)
    for part in parts:
        tab = sample_table.slice(0, nrows)
        tab = tab.append_column(
            "part", ChunkedArray(Array([part] * nrows, type=DataType.utf8()))
        )
        for _ in range(files_per_part):
            write_deltalake(
                table_path,
                tab,
                mode="append",
                partition_by=["part"],
                storage_options=storage_options,
            )

    dt = DeltaTable(table_path, storage_options=storage_options)

    assert len(dt.files()) == files_per_part * len(parts)
    initial_version = dt.version()

    def setup():
        # Instead of recreating the table for each benchmark run, we just delete
        # the optimize log file
        optimize_version = initial_version + 1
        try:
            minio.remove_object(
                bucket_name, f"optimize-test/_delta_log/{optimize_version:020}.json"
            )
        except Exception:
            pass
        dt = DeltaTable(table_path, storage_options=storage_options)
        return (dt,), dict(max_concurrent_tasks=max_tasks)

    def func(dt, max_concurrent_tasks):
        return dt.optimize.compact(
            max_concurrent_tasks=max_concurrent_tasks, target_size=1024 * 1024 * 1024
        )

    results = benchmark.pedantic(func, setup=setup, rounds=5, warmup_rounds=3)

    assert results["numFilesRemoved"] == 50
    assert results["numFilesAdded"] == 5
    assert results["partitionsOptimized"] == 5
