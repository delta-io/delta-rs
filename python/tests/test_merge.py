import pathlib

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake
from deltalake.table import CommitProperties


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

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.merge(
        source=source_table,
        predicate="t.id = s.id",
        source_alias="s",
        target_alias="t",
        commit_properties=commit_properties,
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
        predicate="source.price < 50",
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
        predicate="source.price < 50",
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


def test_merge_when_not_matched_insert_all_with_predicate_special_column_names(
    tmp_path: pathlib.Path, sample_table_with_spaces_numbers: pa.Table
):
    write_deltalake(tmp_path, sample_table_with_spaces_numbers, mode="append")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "1id": pa.array(["6", "10"]),
            "price": pa.array([10, 100], pa.int64()),
            "sold items": pa.array([10, 20], pa.int32()),
            "deleted": pa.array([None, None], pa.bool_()),
        }
    )

    dt.merge(
        source=source_table,
        source_alias="source",
        target_alias="target",
        predicate="target.`1id` = source.`1id`",
    ).when_not_matched_insert_all(
        predicate="source.price < 50",
    ).execute()

    expected = pa.table(
        {
            "1id": pa.array(["1", "2", "3", "4", "5", "6"]),
            "price": pa.array([0, 1, 2, 3, 4, 10], pa.int64()),
            "sold items": pa.array([0, 1, 2, 3, 4, 10], pa.int32()),
            "deleted": pa.array([False, False, False, False, False, None]),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("1id", "ascending")])
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
        predicate="target.price > 3",
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
    ).when_not_matched_by_source_delete(predicate="target.price > 3").execute()

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
        predicate="source.price < 50",
    ).when_not_matched_insert(
        updates={
            "id": "source.id",
            "price": "source.price",
            "sold": "source.sold",
            "deleted": "False",
        },
        predicate="source.price > 50",
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


def test_merge_date_partitioned_2344(tmp_path: pathlib.Path):
    from datetime import date

    schema = pa.schema(
        [
            ("date", pa.date32()),
            ("foo", pa.string()),
            ("bar", pa.string()),
        ]
    )

    dt = DeltaTable.create(
        tmp_path, schema=schema, partition_by=["date"], mode="overwrite"
    )

    data = pa.table(
        {
            "date": pa.array([date(2022, 2, 1)]),
            "foo": pa.array(["hello"]),
            "bar": pa.array(["world"]),
        }
    )

    dt.merge(
        data,
        predicate="s.date = t.date",
        source_alias="s",
        target_alias="t",
    ).when_matched_update_all().when_not_matched_insert_all().execute()

    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == data
    assert last_action["operationParameters"].get("predicate") == "2022-02-01 = date"


@pytest.mark.parametrize(
    "timezone,predicate",
    [
        (
            None,
            "arrow_cast('2022-02-01T00:00:00.000000', 'Timestamp(Microsecond, None)') = datetime",
        ),
        (
            "UTC",
            "arrow_cast('2022-02-01T00:00:00.000000', 'Timestamp(Microsecond, Some(\"UTC\"))') = datetime",
        ),
    ],
)
def test_merge_timestamps_partitioned_2344(tmp_path: pathlib.Path, timezone, predicate):
    from datetime import datetime

    schema = pa.schema(
        [
            ("datetime", pa.timestamp("us", tz=timezone)),
            ("foo", pa.string()),
            ("bar", pa.string()),
        ]
    )

    dt = DeltaTable.create(
        tmp_path, schema=schema, partition_by=["datetime"], mode="overwrite"
    )

    data = pa.table(
        {
            "datetime": pa.array(
                [datetime(2022, 2, 1)], pa.timestamp("us", tz=timezone)
            ),
            "foo": pa.array(["hello"]),
            "bar": pa.array(["world"]),
        }
    )

    dt.merge(
        data,
        predicate="s.datetime = t.datetime",
        source_alias="s",
        target_alias="t",
    ).when_matched_update_all().when_not_matched_insert_all().execute()

    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == data
    assert last_action["operationParameters"].get("predicate") == predicate


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_merge_stats_columns_stats_provided(tmp_path: pathlib.Path, engine):
    data = pa.table(
        {
            "foo": pa.array(["a", "b", None, None]),
            "bar": pa.array([1, 2, 3, None]),
            "baz": pa.array([1, 1, None, None]),
        }
    )
    write_deltalake(
        tmp_path,
        data,
        mode="append",
        engine=engine,
        configuration={"delta.dataSkippingStatsColumns": "foo,baz"},
    )
    dt = DeltaTable(tmp_path)
    add_actions_table = dt.get_add_actions(flatten=True)
    stats = add_actions_table.to_pylist()[0]

    assert stats["null_count.foo"] == 2
    assert stats["min.foo"] == "a"
    assert stats["max.foo"] == "b"
    assert stats["null_count.bar"] is None
    assert stats["min.bar"] is None
    assert stats["max.bar"] is None
    assert stats["null_count.baz"] == 2
    assert stats["min.baz"] == 1
    assert stats["max.baz"] == 1

    data = pa.table(
        {
            "foo": pa.array(["a"]),
            "bar": pa.array([10]),
            "baz": pa.array([10]),
        }
    )

    dt.merge(
        data,
        predicate="source.foo = target.foo",
        source_alias="source",
        target_alias="target",
    ).when_matched_update_all().execute()

    dt = DeltaTable(tmp_path)
    add_actions_table = dt.get_add_actions(flatten=True)
    stats = add_actions_table.to_pylist()[0]

    assert dt.version() == 1
    assert stats["null_count.foo"] == 2
    assert stats["min.foo"] == "a"
    assert stats["max.foo"] == "b"
    assert stats["null_count.bar"] is None
    assert stats["min.bar"] is None
    assert stats["max.bar"] is None
    assert stats["null_count.baz"] == 2
    assert stats["min.baz"] == 1
    assert stats["max.baz"] == 10


def test_merge_field_special_characters_delete_2438(tmp_path: pathlib.Path):
    ## See issue: https://github.com/delta-io/delta-rs/issues/2438
    data = pa.table({"x": [1, 2, 3], "y--1": [4, 5, 6]})
    write_deltalake(tmp_path, data, mode="append")

    dt = DeltaTable(tmp_path)
    new_data = pa.table({"x": [2, 3]})

    (
        dt.merge(
            source=new_data,
            predicate="target.x = source.x",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_delete()
        .execute()
    )

    expected = pa.table({"x": [1], "y--1": [4]})

    assert dt.to_pyarrow_table() == expected


@pytest.mark.pandas
def test_struct_casting(tmp_path: pathlib.Path):
    import pandas as pd

    cols = ["id", "name", "address", "scores"]
    data = [
        (
            2,
            "Marry Doe",
            {"street": "123 Main St", "city": "Anytown", "state": "CA"},
            [0, 0, 0],
        )
    ]
    df = pd.DataFrame(data, columns=cols)
    df_merge = pd.DataFrame(
        [
            (
                2,
                "Merged",
                {"street": "1 Front", "city": "San Francisco", "state": "CA"},
                [7, 0, 7],
            )
        ],
        columns=cols,
    )
    assert not df.empty

    schema = pa.Table.from_pandas(df=df).schema
    dt = DeltaTable.create(tmp_path, schema, name="test")
    metadata = dt.metadata()
    assert metadata.name == "test"

    result = (
        dt.merge(
            source=df_merge,
            predicate="t.id = s.id",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all()
        .execute()
    )
    assert result is not None


def test_merge_isin_partition_pruning(
    tmp_path: pathlib.Path,
):
    nrows = 5
    data = pa.table(
        {
            "id": pa.array([str(x) for x in range(nrows)]),
            "partition": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array(list(range(nrows)), pa.int32()),
        }
    )

    write_deltalake(tmp_path, data, mode="append", partition_by="partition")

    dt = DeltaTable(tmp_path)

    source_table = pa.table(
        {
            "id": pa.array(["3", "4"]),
            "partition": pa.array([3, 4], pa.int64()),
            "sold": pa.array([10, 20], pa.int32()),
        }
    )

    metrics = (
        dt.merge(
            source=source_table,
            predicate="t.id = s.id and t.partition in (3,4)",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all()
        .execute()
    )

    expected = pa.table(
        {
            "id": pa.array(["0", "1", "2", "3", "4"]),
            "partition": pa.array([0, 1, 2, 3, 4], pa.int64()),
            "sold": pa.array([0, 1, 2, 10, 20], pa.int32()),
        }
    )
    result = dt.to_pyarrow_table().sort_by([("id", "ascending")])
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "MERGE"
    assert result == expected
    assert metrics["num_target_files_scanned"] == 2
    assert metrics["num_target_files_skipped_during_scan"] == 3
