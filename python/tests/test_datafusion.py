import pytest
from arro3.core import Array, DataType, Field, Table

from deltalake import DeltaTable, write_deltalake


@pytest.mark.datafusion
def test_datafusion_table_provider(tmp_path):
    nrows = 5
    table = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                Field("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                list(range(nrows)), Field("price", type=DataType.int64(), nullable=True)
            ),
            "sold": Array(
                list(range(nrows)), Field("sold", type=DataType.int32(), nullable=True)
            ),
            "deleted": Array(
                [False] * nrows, Field("deleted", type=DataType.bool(), nullable=True)
            ),
        },
    )

    from datafusion import SessionContext

    write_deltalake(tmp_path, table)

    dt = DeltaTable(tmp_path)

    session = SessionContext()
    session.register_table("tbl", dt)
    data = session.sql("SELECT * FROM tbl")

    assert Table.from_arrow(data) == table
