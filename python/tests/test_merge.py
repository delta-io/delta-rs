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


def test_merge_when_matched_delete_with_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["5", "4"]),
            "price": pa.array([1, 2], pa.int64()),
            "sold": pa.array([1, 2], pa.int32()),
            "deleted": pa.array([True, False]),
        }
    )

    dt.merge(
        source=source_table, source_alias="source", predicate="id = source.id"
    ).when_matched_delete("source.deleted = True").execute()

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


def test_merge_when_matched_update_wo_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["4", "5"]),
            "price": pa.array([10, 100], pa.int64()),
            "sold": pa.array([10, 20], pa.int32()),
            "deleted": pa.array([False, False]),
        }
    )

    dt.merge(
        source=source_table, source_alias="source", predicate="id = source.id"
    ).when_matched_update({"price": "source.price", "sold": "source.sold"}).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array([1, 2, 3, 10, 100], pa.int64()),
            "sold": pa.array([1, 2, 3, 10, 20], pa.int32()),
            "deleted": pa.array([False] * 5),
        }
    )
    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_when_matched_update_with_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["4", "5"]),
            "price": pa.array([10, 100], pa.int64()),
            "sold": pa.array([10, 20], pa.int32()),
            "deleted": pa.array([False, True]),
        }
    )

    dt.merge(
        source=source_table, source_alias="source", predicate="id = source.id"
    ).when_matched_update(
        updates={"price": "source.price", "sold": "source.sold"},
        predicate="source.deleted = False",
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array([1, 2, 3, 10, 5], pa.int64()),
            "sold": pa.array([1, 2, 3, 10, 5], pa.int32()),
            "deleted": pa.array([False] * 5),
        }
    )
    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_when_not_matched_insert_wo_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["4", "10"]),
            "price": pa.array([10, 100], pa.int64()),
            "sold": pa.array([10, 20], pa.int32()),
            "deleted": pa.array([False, False]),
        }
    )

    dt.merge(
        source=source_table, source_alias="source", predicate="id = source.id"
    ).when_not_matched_insert(
        updates={
            "id": "source.id",
            "price": "source.price",
            "sold": "source.sold",
            "deleted": "False",
        }
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5", "10"]),
            "price": pa.array([1, 2, 3, 4, 5, 100], pa.int64()),
            "sold": pa.array([1, 2, 3, 4, 5, 20], pa.int32()),
            "deleted": pa.array([False] * 6),
        }
    )
    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_when_not_matched_insert_with_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["6", "10"]),
            "price": pa.array([10, 100], pa.int64()),
            "sold": pa.array([10, 20], pa.int32()),
            "deleted": pa.array([False, False]),
        }
    )

    dt.merge(
        source=source_table, source_alias="source", predicate="id = source.id"
    ).when_not_matched_insert(
        updates={
            "id": "source.id",
            "price": "source.price",
            "sold": "source.sold",
            "deleted": "False",
        },
        predicate="source.price < 50",
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5", "6"]),
            "price": pa.array([1, 2, 3, 4, 5, 10], pa.int64()),
            "sold": pa.array([1, 2, 3, 4, 5, 10], pa.int32()),
            "deleted": pa.array([False] * 6),
        }
    )
    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_when_not_matched_by_source_update_wo_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["6", "7"]),
            "price": pa.array([10, 100], pa.int64()),
            "sold": pa.array([10, 20], pa.int32()),
            "deleted": pa.array([False, False]),
        }
    )

    dt.merge(
        source=source_table, source_alias="source", predicate="id = source.id"
    ).when_not_matched_by_source_update(
        updates={
            "sold": "10",
        }
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array([1, 2, 3, 4, 5], pa.int64()),
            "sold": pa.array([10,10,10,10,10], pa.int32()),
            "deleted": pa.array([False] * 5),
        }
    )
    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected
    
def test_merge_when_not_matched_by_source_update_with_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["6", "7"]),
            "price": pa.array([10, 100], pa.int64()),
            "sold": pa.array([10, 20], pa.int32()),
            "deleted": pa.array([False, False]),
        }
    )

    dt.merge(
        source=source_table, source_alias="source", predicate="id = source.id"
    ).when_not_matched_by_source_update(
        updates={
            "sold": "10",
        },
        predicate="price > 3"
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array([1, 2, 3, 4, 5], pa.int64()),
            "sold": pa.array([1,2,3,10,10], pa.int32()),
            "deleted": pa.array([False] * 5),
        }
    )
    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected
    
    
def test_merge_when_not_matched_by_source_delete_with_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["6", "7"]),
            "price": pa.array([10, 100], pa.int64()),
            "sold": pa.array([10, 20], pa.int32()),
            "deleted": pa.array([False, False]),
        }
    )

    dt.merge(
        source=source_table, source_alias="source", predicate="id = source.id"
    ).when_not_matched_by_source_delete(
        predicate="price > 3"
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3"]),
            "price": pa.array([1, 2, 3,], pa.int64()),
            "sold": pa.array([1,2,3], pa.int32()),
            "deleted": pa.array([False] * 3),
        }
    )
    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected
    
## Add when_not_matched_by_source_delete_wo_predicate ?