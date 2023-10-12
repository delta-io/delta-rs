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
            "sold": pa.array(list(range(nrows)), pa.int64()),
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
            "sold": pa.array(list(range(nrows)), pa.int64()),
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
            "sold": pa.array(list(range(nrows)), pa.int64()),
            "deleted": pa.array([True] * 5),
        }
    )

    dt.update(updates={"deleted": "True"})

    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "UPDATE"
    assert result == expected


def test_update_wrong_types_cast(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    with pytest.raises(Exception) as excinfo:
        dt.update(updates={"deleted": "'hello_world'"})

    assert (
        str(excinfo.value)
        == "Cast error: Cannot cast value 'hello_world' to value of Boolean type"
    )


def test_update_wo_predicate_multiple_updates(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    expected = pa.table(
        {
            "id": pa.array(["1_1", "2_1", "3_1", "4_1", "5_1"]),
            "price": pa.array([0, 1, 2, 3, 4], pa.int64()),
            "sold": pa.array([0, 1, 4, 9, 16], pa.int64()),
            "deleted": pa.array([True] * 5),
        }
    )

    dt.update(
        updates={"deleted": "True", "sold": "sold * price", "id": "concat(id,'_1')"},
        error_on_type_mismatch=False,
    )

    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "UPDATE"
    assert result == expected
