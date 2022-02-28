from asyncore import write
from datetime import date, datetime, timedelta
import os
import pathlib
import pytest
import pyarrow as pa
from decimal import Decimal

from deltalake import DeltaTable, write_deltalake


@pytest.fixture()
def sample_data():
    nrows = 5
    return pa.table({
        'utf8': pa.array([str(x) for x in range(nrows)]),
        'int64': pa.array(list(range(nrows)), pa.int64()),
        'int32': pa.array(list(range(nrows)), pa.int32()),
        'int16': pa.array(list(range(nrows)), pa.int16()),
        'int8': pa.array(list(range(nrows)), pa.int8()),
        'float32': pa.array([float(x) for x in range(nrows)], pa.float32()),
        'float64': pa.array([float(x) for x in range(nrows)], pa.float64()),
        'bool': pa.array([x % 2 == 0 for x in range(nrows)]),
        'binary': pa.array([str(x).encode() for x in range(nrows)]),
        'decimal': pa.array([Decimal("10.000") + x for x in range(nrows)]),
        'date32': pa.array([date(2022, 1, 1) + timedelta(days=x) for x in range(nrows)]),
        'timestamp': pa.array([datetime(2022, 1, 1) + timedelta(hours=x) for x in range(nrows)]),
        'struct': pa.array([{'x': x, 'y': str(x)} for x in range(nrows)]),
        'list': pa.array([list(range(x)) for x in range(nrows)]),
        # NOTE: https://github.com/apache/arrow-rs/issues/477
        #'map': pa.array([[(str(y), y) for y in range(x)] for x in range(nrows)], pa.map_(pa.string(), pa.int64())),
    })


def test_handle_existing(tmp_path: pathlib.Path, sample_data: pa.Table):
    # if uri points to a non-empty directory that isn't a delta table, error
    tmp_path
    p = tmp_path / "hello.txt"
    p.write_text("hello")

    with pytest.raises(OSError) as exception:
        write_deltalake(str(tmp_path), sample_data, mode='overwrite')

    assert "directory is not empty" in str(exception)


# round trip, no partitioning

def test_roundtrip_basic(tmp_path: pathlib.Path, sample_data):
    write_deltalake(str(tmp_path), sample_data)

    assert ("0" * 20 + ".json") in os.listdir(tmp_path / "_delta_log")

    table = DeltaTable(str(tmp_path)).to_pyarrow_table()

    assert table == sample_data


# round trip, one partitioning, parameterized part column

# round trip, nested partitioning


# test behaviors
