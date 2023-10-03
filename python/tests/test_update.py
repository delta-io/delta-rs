import pathlib

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake


@pytest.fixture()
def sample_table():
    nrows = 5
    return pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array(list(range(nrows)), pa.int32()),
            "deleted": pa.array([False] * nrows),
        }
    )


def test_update_with_predicate(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    nrows = 5
    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array(list(range(nrows)), pa.int32()),
            "deleted": pa.array([False, False, False, False, True]),
        }
    )

    dt.update(updates={"deleted": "True"}, predicate="price > 3")

    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "UPDATE"
    assert result == expected


def test_update_wo_predicate(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    nrows = 5
    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array(list(range(nrows)), pa.int32()),
            "deleted": pa.array([True] * 5),
        }
    )

    dt.update(updates={"deleted": "True"})

    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "UPDATE"
    assert result == expected
