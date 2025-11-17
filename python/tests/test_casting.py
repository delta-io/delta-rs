import pytest
from arro3.core import Array, DataType, Table

from deltalake import write_deltalake
from deltalake.exceptions import DeltaError


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
