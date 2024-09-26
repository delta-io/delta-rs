import pathlib

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake
from deltalake.table import CommitProperties


@pytest.fixture()
def sample_table():
    nrows = 5
    return pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array(list(range(nrows)), pa.int64()),
            "price_float": pa.array(list(range(nrows)), pa.float64()),
            "items_in_bucket": pa.array([["item1", "item2", "item3"]] * nrows),
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
            "price_float": pa.array(list(range(nrows)), pa.float64()),
            "items_in_bucket": pa.array([["item1", "item2", "item3"]] * nrows),
            "deleted": pa.array([False, False, False, False, True]),
        }
    )

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.update(
        updates={"deleted": "True"},
        predicate="price > 3",
        commit_properties=commit_properties,
    )

    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "UPDATE"
    assert last_action["userName"] == "John Doe"
    assert result == expected


def test_update_with_predicate_large_dtypes(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append", large_dtypes=True)

    dt = DeltaTable(tmp_path)

    nrows = 5
    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array(list(range(nrows)), pa.int64()),
            "price_float": pa.array(list(range(nrows)), pa.float64()),
            "items_in_bucket": pa.array([["item1", "item2", "item3"]] * nrows),
            "deleted": pa.array([True, False, False, False, False]),
        }
    )

    dt.update(
        updates={"deleted": "True"},
        predicate="id = '1'",
    )

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
            "price_float": pa.array(list(range(nrows)), pa.float64()),
            "items_in_bucket": pa.array([["item1", "item2", "item3"]] * nrows),
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

    expected = """Generic DeltaTable error: type_coercion\ncaused by\nError during planning: Failed to coerce then ([Utf8]) and else (Some(Boolean)) to common types in CASE WHEN expression"""
    assert str(excinfo.value) == expected


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
            "price_float": pa.array(list(range(5)), pa.float64()),
            "items_in_bucket": pa.array([["item1", "item2", "item3"]] * 5),
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


def test_update_with_predicate_and_new_values(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    nrows = 5
    expected = pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "new_id"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array([0, 1, 2, 3, 100], pa.int64()),
            "price_float": pa.array([0, 1, 2, 3, 9999], pa.float64()),
            "items_in_bucket": pa.array(
                [["item1", "item2", "item3"]] * 4 + [["item4", "item5", "item6"]]
            ),
            "deleted": pa.array([False, False, False, False, True]),
        }
    )

    dt.update(
        new_values={
            "id": "new_id",
            "deleted": True,
            "sold": 100,
            "price_float": 9999,
            "items_in_bucket": ["item4", "item5", "item6"],
        },
        predicate="price > 3",
    )

    result = dt.to_pyarrow_table()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "UPDATE"
    assert result == expected


def test_update_no_inputs(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    with pytest.raises(Exception) as excinfo:
        dt.update()

    assert (
        str(excinfo.value)
        == "Either updates or new_values need to be passed to update the table."
    )


def test_update_to_many_inputs(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    with pytest.raises(Exception) as excinfo:
        dt.update(updates={}, new_values={})

    assert (
        str(excinfo.value)
        == "Passing updates and new_values at same time is not allowed, pick one."
    )


def test_update_with_incorrect_updates_input(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)
    updates = {"col": {}}
    with pytest.raises(Exception) as excinfo:
        dt.update(new_values=updates)

    assert (
        str(excinfo.value)
        == "Invalid datatype provided in new_values, only int, float, bool, list, str or datetime or accepted."
    )


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_update_stats_columns_stats_provided(tmp_path: pathlib.Path, engine):
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

    dt.update({"foo": "'hello world'"})

    dt = DeltaTable(tmp_path)
    add_actions_table = dt.get_add_actions(flatten=True)
    stats = add_actions_table.to_pylist()[0]

    assert dt.version() == 1
    assert stats["null_count.foo"] == 0
    assert stats["min.foo"] == "hello world"
    assert stats["max.foo"] == "hello world"
    assert stats["null_count.bar"] is None
    assert stats["min.bar"] is None
    assert stats["max.bar"] is None
    assert stats["null_count.baz"] == 2
    assert stats["min.baz"] == 1
    assert stats["max.baz"] == 1
