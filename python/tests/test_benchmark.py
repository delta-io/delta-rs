import os

import pytest
from arro3.core import Array, ChunkedArray, DataType, Table
from numpy.random import standard_normal

from deltalake import DeltaTable, QueryBuilder, write_deltalake

# NOTE: make sure to run these in release mode with
# MATURIN_EXTRA_ARGS=--release make develop
# When profiling, use:
# MATURIN_EXTRA_ARGS="--profile release-with-debug" make develop


@pytest.fixture()
def sample_table() -> Table:
    max_size_bytes = 128 * 1024 * 1024
    ncols = 20
    nrows = max_size_bytes // 20 // 8
    tab = Table.from_pydict({f"x{i}": standard_normal(nrows) for i in range(ncols)})
    # Add index column for sorting
    tab = tab.append_column(
        "i", ChunkedArray(Array(range(nrows), type=DataType.int64()))
    )
    return tab


@pytest.mark.benchmark(group="write")
def test_benchmark_write(benchmark, sample_table, tmp_path):
    benchmark(write_deltalake, str(tmp_path), sample_table, mode="overwrite")

    dt = DeltaTable(str(tmp_path))
    assert (
        QueryBuilder().register("tbl", dt).execute("select * from tbl order by id")
        == sample_table
    )


@pytest.mark.pyarrow
@pytest.mark.benchmark(group="read")
def test_benchmark_read(benchmark, sample_table, tmp_path):
    import pyarrow as pa

    write_deltalake(str(tmp_path), sample_table)
    dt = DeltaTable(str(tmp_path))

    result = benchmark(dt.to_pyarrow_table)
    assert result.sort_by("i") == pa.table(sample_table)


@pytest.mark.pyarrow
@pytest.mark.benchmark(group="read")
def test_benchmark_read_pyarrow(benchmark, sample_table, tmp_path):
    import pyarrow as pa
    import pyarrow.fs as pa_fs

    write_deltalake(str(tmp_path), sample_table)
    dt = DeltaTable(str(tmp_path))

    fs = pa_fs.SubTreeFileSystem(str(tmp_path), pa_fs.LocalFileSystem())
    result = benchmark(dt.to_pyarrow_table, filesystem=fs)
    assert result.sort_by("i") == pa.table(sample_table)


@pytest.mark.benchmark(group="optimize")
@pytest.mark.parametrize("max_tasks", [1, 5])
def test_benchmark_optimize(benchmark, sample_table, tmp_path, max_tasks):
    # Create 2 partitions, each partition with 10 files.
    # Each file is about 100MB, so the total size is 2GB.
    files_per_part = 10
    parts = ["a", "b", "c", "d", "e"]

    nrows = int(sample_table.num_rows / files_per_part)
    for part in parts:
        tab = sample_table.slice(0, nrows)
        tab = tab.append_column(
            "part", ChunkedArray(Array([part] * nrows), DataType.int64())
        )
        for _ in range(files_per_part):
            write_deltalake(tmp_path, tab, mode="append", partition_by=["part"])

        dt = DeltaTable(tmp_path)
        dt = DeltaTable(tmp_path)

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
    results = benchmark.pedantic(func, setup=setup, rounds=5)

    assert results["numFilesRemoved"] == 50
    assert results["numFilesAdded"] == 5
    assert results["partitionsOptimized"] == 5
