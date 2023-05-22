from itertools import product

import pyarrow as pa
import pyarrow.fs as pa_fs
import pytest
from numpy.random import standard_normal

from deltalake import DeltaTable, write_deltalake

# NOTE: make sure to run these in release mode with
# MATURIN_EXTRA_ARGS=--release make develop


@pytest.fixture()
def sample_table() -> pa.Table:
    max_size_bytes = 100 * 1024 * 1024
    ncols = 20
    nrows = max_size_bytes // 20 // 8
    tab = pa.table({f"x{i}": standard_normal(nrows) for i in range(ncols)})
    # Add index column for sorting
    tab = tab.append_column("i", pa.array(range(nrows), type=pa.int64()))
    return tab


@pytest.mark.benchmark(group="write")
def test_benchmark_write(benchmark, sample_table, tmp_path):
    benchmark(write_deltalake, str(tmp_path), sample_table, mode="overwrite")

    dt = DeltaTable(str(tmp_path))
    assert dt.to_pyarrow_table().sort_by("i") == sample_table


# TODO: support wrapping PyArrow filesystems
# @pytest.mark.benchmark(
#     group="write"
# )
# def test_benchmark_write_pyarrow(benchmark, sample_table, tmp_path):
#     fs = pa_fs.SubTreeFileSystem(str(tmp_path), pa_fs.LocalFileSystem())

#     benchmark(write_deltalake, str(tmp_path), sample_table, mode="overwrite", filesystem=fs)

#     dt = DeltaTable(str(tmp_path))
#     assert dt.to_pyarrow_table(filesystem=fs).sort_by("i") == sample_table


@pytest.mark.benchmark(group="read")
def test_benchmark_read(benchmark, sample_table, tmp_path):
    write_deltalake(str(tmp_path), sample_table)
    dt = DeltaTable(str(tmp_path))

    result = benchmark(dt.to_pyarrow_table)
    assert result.sort_by("i") == sample_table


@pytest.mark.benchmark(group="read")
def test_benchmark_read_pyarrow(benchmark, sample_table, tmp_path):
    write_deltalake(str(tmp_path), sample_table)
    dt = DeltaTable(str(tmp_path))

    fs = pa_fs.SubTreeFileSystem(str(tmp_path), pa_fs.LocalFileSystem())
    result = benchmark(dt.to_pyarrow_table, filesystem=fs)
    assert result.sort_by("i") == sample_table


@pytest.mark.benchmark(group="optimize")
@pytest.mark.parametrize("max_tasks", [1, 5, 10])
def test_benchmark_optimize(benchmark, sample_table, tmp_path, max_tasks):
    def setup():
        # Create 2 partitions, each partition with 10 files.
        # Each file is about 100MB, so the total size is 2GB.
        files_per_part = 10
        parts = ["a", "b"]
        for part, _ in product(parts, range(files_per_part)):
            # Default row group size is 1 million rows. In order to get close to
            # the target size, we need a tall dataset. Wide datasets will have
            # huge row groups.
            nrows = sample_table.num_rows
            tab = sample_table.select(["x0", "x1", "i"])
            tab = tab.append_column("part", pa.array([part] * nrows))
            write_deltalake(tmp_path, tab, mode="append", partition_by=["part"])

        dt = DeltaTable(tmp_path)

        assert len(dt.files()) == files_per_part * len(parts)

        target_size = 100 * 1024 * 1024
        sizes = dt.get_add_actions().column("size_bytes")
        assert all(size.as_py() <= target_size for size in sizes)

        return (dt,), dict(max_concurrent_tasks=max_tasks, target_size=target_size)

    def func(dt, max_concurrent_tasks, target_size):
        return dt.optimize(
            max_concurrent_tasks=max_concurrent_tasks, target_size=target_size
        )

    # We need to recreate the table for each benchmark run
    results = benchmark.pedantic(func, setup=setup)
    print(results)

    assert results["numFilesRemoved"] == 20
    assert results["numFilesAdded"] == 4
    assert results["partitionsOptimized"] == 2
