import pathlib

import pyarrow as pa

from deltalake import DeltaTable, write_deltalake


def test_merge_when_matched_delete_wo_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["5"]),
            "weight": pa.array([105], pa.int32()),
        }
    )

    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
        custom_metadata={"userName": "John Doe"},
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
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert last_action["userName"] == "John Doe"
    assert result == expected


def test_merge_when_matched_delete_with_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["5", "4"]),
            "weight": pa.array([1, 2], pa.int64()),
            "sold": pa.array([1, 2], pa.int32()),
            "deleted": pa.array([True, False]),
            "customer": pa.array(["Adam", "Patrick"]),
        }
    )

    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
    ).when_matched_delete("s.deleted = True").execute()

    nrows = 4
    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array(list(range(nrows)), pa.int32()),
            "deleted": pa.array([False] * nrows),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
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
        }
    )

    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
    ).when_matched_update({"price": "s.price", "sold": "s.sold"}).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array([0, 1, 2, 10, 100], pa.int64()),
            "sold": pa.array([0, 1, 2, 10, 20], pa.int32()),
            "deleted": pa.array([False] * 5),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_when_matched_update_all_wo_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["4", "5"]),
            "price": pa.array([10, 100], pa.int64()),
            "sold": pa.array([10, 20], pa.int32()),
            "deleted": pa.array([True, True]),
            "weight": pa.array([10, 15], pa.int64()),
        }
    )

    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
    ).when_matched_update_all().execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array([0, 1, 2, 10, 100], pa.int64()),
            "sold": pa.array([0, 1, 2, 10, 20], pa.int32()),
            "deleted": pa.array([False, False, False, True, True]),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
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
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
    ).when_matched_update(
        updates={"price": "source.price", "sold": "source.sold"},
        predicate="source.deleted = False",
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array([0, 1, 2, 10, 4], pa.int64()),
            "sold": pa.array([0, 1, 2, 10, 4], pa.int32()),
            "deleted": pa.array([False] * 5),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
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
            "id": pa.array(["4", "6"]),
            "price": pa.array([10, 100], pa.int64()),
            "sold": pa.array([10, 20], pa.int32()),
            "deleted": pa.array([False, False]),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
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
            "id": pa.array(["1", "2", "3", "4", "5", "6"]),
            "price": pa.array([0, 1, 2, 3, 4, 100], pa.int64()),
            "sold": pa.array([0, 1, 2, 3, 4, 20], pa.int32()),
            "deleted": pa.array([False] * 6),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
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
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
    ).when_not_matched_insert(
        updates={
            "id": "source.id",
            "price": "source.price",
            "sold": "source.sold",
            "deleted": "False",
        },
        predicate="source.price < bigint'50'",
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5", "6"]),
            "price": pa.array([0, 1, 2, 3, 4, 10], pa.int64()),
            "sold": pa.array([0, 1, 2, 3, 4, 10], pa.int32()),
            "deleted": pa.array([False] * 6),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_when_not_matched_insert_all_with_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["6", "10"]),
            "price": pa.array([10, 100], pa.int64()),
            "sold": pa.array([10, 20], pa.int32()),
            "deleted": pa.array([None, None], pa.bool_()),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
    ).when_not_matched_insert_all(
        predicate="source.price < bigint'50'",
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5", "6"]),
            "price": pa.array([0, 1, 2, 3, 4, 10], pa.int64()),
            "sold": pa.array([0, 1, 2, 3, 4, 10], pa.int32()),
            "deleted": pa.array([False, False, False, False, False, None]),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
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
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
    ).when_not_matched_by_source_update(
        updates={
            "sold": "int'10'",
        }
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array([0, 1, 2, 3, 4], pa.int64()),
            "sold": pa.array([10, 10, 10, 10, 10], pa.int32()),
            "deleted": pa.array([False] * 5),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
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
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
    ).when_not_matched_by_source_update(
        updates={
            "sold": "int'10'",
        },
        predicate="target.price > bigint'3'",
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array([0, 1, 2, 3, 4], pa.int64()),
            "sold": pa.array([0, 1, 2, 3, 10], pa.int32()),
            "deleted": pa.array([False] * 5),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
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
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
    ).when_not_matched_by_source_delete(predicate="target.price > bigint'3'").execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4"]),
            "price": pa.array(
                [0, 1, 2, 3],
                pa.int64(),
            ),
            "sold": pa.array([0, 1, 2, 3], pa.int32()),
            "deleted": pa.array([False] * 4),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_when_not_matched_by_source_delete_wo_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {"id": pa.array(["4", "5"]), "weight": pa.array([1.5, 1.6], pa.float64())}
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
    ).when_not_matched_by_source_delete().execute()

    expected = pa.table(
        {
            "id": pa.array(["4", "5"]),
            "price": pa.array(
                [3, 4],
                pa.int64(),
            ),
            "sold": pa.array([3, 4], pa.int32()),
            "deleted": pa.array([False] * 2),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_multiple_when_matched_update_with_predicate(
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
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
    ).when_matched_update(
        updates={"price": "source.price", "sold": "source.sold"},
        predicate="source.deleted = False",
    ).when_matched_update(
        updates={"price": "source.price", "sold": "source.sold"},
        predicate="source.deleted = True",
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array([0, 1, 2, 10, 100], pa.int64()),
            "sold": pa.array([0, 1, 2, 10, 20], pa.int32()),
            "deleted": pa.array([False] * 5),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_multiple_when_matched_update_all_with_predicate(
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
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
    ).when_matched_update_all(
        predicate="source.deleted = False",
    ).when_matched_update_all(
        predicate="source.deleted = True",
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array([0, 1, 2, 10, 100], pa.int64()),
            "sold": pa.array([0, 1, 2, 10, 20], pa.int32()),
            "deleted": pa.array([False, False, False, False, True]),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_multiple_when_not_matched_insert_with_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["6", "9"]),
            "price": pa.array([10, 100], pa.int64()),
            "sold": pa.array([10, 20], pa.int32()),
            "deleted": pa.array([False, False]),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
    ).when_not_matched_insert(
        updates={
            "id": "source.id",
            "price": "source.price",
            "sold": "source.sold",
            "deleted": "False",
        },
        predicate="source.price < bigint'50'",
    ).when_not_matched_insert(
        updates={
            "id": "source.id",
            "price": "source.price",
            "sold": "source.sold",
            "deleted": "False",
        },
        predicate="source.price > bigint'50'",
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5", "6", "9"]),
            "price": pa.array([0, 1, 2, 3, 4, 10, 100], pa.int64()),
            "sold": pa.array([0, 1, 2, 3, 4, 10, 20], pa.int32()),
            "deleted": pa.array([False] * 7),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_multiple_when_matched_delete_with_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["5", "4"]),
            "weight": pa.array([1, 2], pa.int64()),
            "sold": pa.array([1, 2], pa.int32()),
            "deleted": pa.array([True, False]),
            "customer": pa.array(["Adam", "Patrick"]),
        }
    )

    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
    ).when_matched_delete("s.deleted = True").when_matched_delete(
        "s.deleted = false"
    ).execute()

    nrows = 3
    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array(list(range(nrows)), pa.int32()),
            "deleted": pa.array([False] * nrows),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected


def test_merge_multiple_when_not_matched_by_source_update_wo_predicate(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    """The first match clause that meets the predicate will be executed, so if you do an update
    without an other predicate the first clause will be matched and therefore the other ones are skipped.
    """
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
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.id = source.id",
    ).when_not_matched_by_source_update(
        updates={
            "sold": "int'10'",
        }
    ).when_not_matched_by_source_update(
        updates={
            "sold": "int'100'",
        }
    ).execute()

    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array([0, 1, 2, 3, 4], pa.int64()),
            "sold": pa.array([10, 10, 10, 10, 10], pa.int32()),
            "deleted": pa.array([False] * 5),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected
