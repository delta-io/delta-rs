import pyarrow as pa
import pyarrow.fs as pa_fs
import pytest
from numpy.random import standard_normal

from deltalake import DeltaTable, write_deltalake

# NOTE: make sure to run these in release mode with
# MATURIN_EXTRA_ARGS=--release make develop


@pytest.fixture()
def sample_table() -> pa.Table:
    max_size_bytes = 1024 * 1024 * 1024
    ncols = 20
    nrows = max_size_bytes // 20 // 64
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
