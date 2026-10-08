import math

import pytest
from arro3.core import Array, DataType, Table

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError
from deltalake.query import QueryBuilder


def test_unsafe_cast(tmp_path):
    tbl = Table.from_pydict({"foo": Array([1, 2, 3, 200], DataType.uint8())})

    with pytest.raises(
        DeltaError,
        match="Cast error: Failed to convert into Arrow schema: Cast error: Failed to cast foo from Int8 to UInt8: Can't cast value 200 to type Int8",
    ):
        write_deltalake(tmp_path, tbl)


def test_safe_cast(tmp_path):
    tbl = Table.from_pydict({"foo": Array([1, 2, 3, 4], DataType.uint8())})

    write_deltalake(tmp_path, tbl)


@pytest.mark.parametrize("schema_mode", [None, "merge"])
def test_append_fractional_float64_to_int64_rejected(tmp_path, schema_mode):
    """Appending fractional float64 values into an int64 column must raise
    instead of silently truncating the values.

    See <https://github.com/delta-io/delta-rs/issues/4811>
    """
    write_deltalake(tmp_path, Table.from_pydict({"x": Array([1], DataType.int64())}))

    kwargs = {} if schema_mode is None else {"schema_mode": schema_mode}
    with pytest.raises(DeltaError):
        write_deltalake(
            tmp_path,
            Table.from_pydict({"x": Array([2.9, -2.9], DataType.float64())}),
            mode="append",
            **kwargs,
        )


@pytest.mark.parametrize("schema_mode", [None, "merge"])
def test_append_float64_to_float32(tmp_path, schema_mode):
    """Appending float64 values to a float32 column is allowed.
    In-range values are rounded to float32 precision and
    out-of-range values saturate to +/- inf.
    """
    write_deltalake(
        tmp_path, Table.from_pydict({"x": Array([0.0], DataType.float32())})
    )

    kwargs = {} if schema_mode is None else {"schema_mode": schema_mode}
    write_deltalake(
        tmp_path,
        Table.from_pydict(
            {"x": Array([1e300, -1e300, 1.5, float("nan")], DataType.float64())}
        ),
        mode="append",
        **kwargs,
    )

    result = (
        QueryBuilder()
        .register("tbl", DeltaTable(tmp_path))
        .execute("select x from tbl")
        .read_all()
    )
    assert result.schema.field("x").type == DataType.float32()
    values = result["x"].to_pylist()
    assert len(values) == 5
    assert sorted(v for v in values if not math.isnan(v)) == [
        float("-inf"),
        0.0,
        1.5,
        float("inf"),
    ]
