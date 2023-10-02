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


def test_merge_when_matched_delete_wo_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["5"]),
            "price": pa.array([1], pa.int64()),
            "sold": pa.array([1], pa.int32()),
            "deleted": pa.array([False]),
        }
    )

    dt.merge(
        source=source_table, source_alias="source", predicate="id = source.id"
    ).when_matched_delete().execute()

    nrows = 4
    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array(list(range(nrows)), pa.int32()),
            "deleted": pa.array([False] * nrows),
        }
    )
    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected
