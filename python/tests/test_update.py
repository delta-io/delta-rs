import pathlib

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import CommitProperties, DeltaTable, write_deltalake
from deltalake.query import QueryBuilder


@pytest.fixture()
def sample_table():
    nrows = 5
    return Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                list(range(nrows)),
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                list(range(nrows)),
                ArrowField("sold", type=DataType.int64(), nullable=True),
            ),
            "price_float": Array(
                list(range(nrows)),
                ArrowField("price_float", type=DataType.float64(), nullable=True),
            ),
            "deleted": Array(
                [False] * nrows,
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )


def test_update_with_predicate(tmp_path: pathlib.Path, sample_table: Table):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3, 4],
                ArrowField("sold", type=DataType.int64(), nullable=True),
            ),
            "price_float": Array(
                [0.0, 1.0, 2.0, 3.0, 4.0],
                ArrowField("price_float", type=DataType.float64(), nullable=True),
            ),
            "deleted": Array(
                [False, False, False, False, True],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.update(
        updates={"deleted": "True"},
        predicate="price > 3",
        commit_properties=commit_properties,
    )

    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "UPDATE"
    assert last_action["userName"] == "John Doe"
    assert result == expected


def test_update_wo_predicate(tmp_path: pathlib.Path, sample_table: Table):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3, 4],
                ArrowField("sold", type=DataType.int64(), nullable=True),
            ),
            "price_float": Array(
                [0.0, 1.0, 2.0, 3.0, 4.0],
                ArrowField("price_float", type=DataType.float64(), nullable=True),
            ),
            "deleted": Array(
                [True, True, True, True, True],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.update(updates={"deleted": "True"})

    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "UPDATE"
    assert result == expected


def test_update_wrong_types_cast(tmp_path: pathlib.Path, sample_table: Table):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    with pytest.raises(Exception) as excinfo:
        dt.update(updates={"deleted": "'hello_world'"})

    expected = """Generic DeltaTable error: type_coercion\ncaused by\nError during planning: Failed to coerce then (Utf8) and else (Boolean) to common types in CASE WHEN expression"""
    assert str(excinfo.value) == expected


def test_update_wo_predicate_multiple_updates(
    tmp_path: pathlib.Path, sample_table: Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    expected = Table(
        {
            "id": Array(
                ["1_1", "2_1", "3_1", "4_1", "5_1"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 4, 9, 16],
                ArrowField("sold", type=DataType.int64(), nullable=True),
            ),
            "price_float": Array(
                [0.0, 1.0, 2.0, 3.0, 4.0],
                ArrowField("price_float", type=DataType.float64(), nullable=True),
            ),
            "deleted": Array(
                [True, True, True, True, True],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
        }
    )

    dt.update(
        updates={"deleted": "True", "sold": "sold * price", "id": "concat(id,'_1')"},
        error_on_type_mismatch=False,
    )

    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "UPDATE"
    assert result == expected


def test_update_with_predicate_and_new_values(
    tmp_path: pathlib.Path, sample_table: Table
):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    expected = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "new_id"],
                ArrowField("id", type=DataType.string_view(), nullable=True),
            ),
            "price": Array(
                [0, 1, 2, 3, 4],
                ArrowField("price", type=DataType.int64(), nullable=True),
            ),
            "sold": Array(
                [0, 1, 2, 3, 100],
                ArrowField("sold", type=DataType.int64(), nullable=True),
            ),
            "price_float": Array(
                [0.0, 1.0, 2.0, 3.0, 9999.0],
                ArrowField("price_float", type=DataType.float64(), nullable=True),
            ),
            "deleted": Array(
                [False, False, False, False, True],
                ArrowField("deleted", type=DataType.bool(), nullable=True),
            ),
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

    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    last_action = dt.history(1)[0]

    assert last_action["operation"] == "UPDATE"
    assert result == expected


def test_update_no_inputs(tmp_path: pathlib.Path, sample_table: Table):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    with pytest.raises(Exception) as excinfo:
        dt.update()

    assert (
        str(excinfo.value)
        == "Either updates or new_values need to be passed to update the table."
    )


def test_update_to_many_inputs(tmp_path: pathlib.Path, sample_table: Table):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    with pytest.raises(Exception) as excinfo:
        dt.update(updates={}, new_values={})

    assert (
        str(excinfo.value)
        == "Passing updates and new_values at same time is not allowed, pick one."
    )


def test_update_with_incorrect_updates_input(
    tmp_path: pathlib.Path, sample_table: Table
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


def test_update_stats_columns_stats_provided(tmp_path: pathlib.Path):
    data = Table(
        {
            "foo": Array(
                ["a", "b", None, None],
                ArrowField("foo", type=DataType.string(), nullable=True),
            ),
            "bar": Array(
                [1, 2, 3, None],
                ArrowField("bar", type=DataType.int64(), nullable=True),
            ),
            "baz": Array(
                [1, 1, None, None],
                ArrowField("baz", type=DataType.int64(), nullable=True),
            ),
        }
    )

    write_deltalake(
        tmp_path,
        data,
        mode="append",
        configuration={"delta.dataSkippingStatsColumns": "foo,baz"},
    )
    dt = DeltaTable(tmp_path)
    add_actions_table = dt.get_add_actions(flatten=True)

    def get_value(name: str):
        return add_actions_table.column(name)[0].as_py()

    # x1 has no max, since inf was the highest value
    assert get_value("null_count.foo") == 2
    assert get_value("min.foo") == "a"
    assert get_value("max.foo") == "b"
    assert get_value("null_count.baz") == 2
    assert get_value("min.baz") == 1
    assert get_value("max.baz") == 1

    with pytest.raises(Exception):
        get_value("null_count.bar")

    dt.update({"foo": "'hello world'"})

    dt = DeltaTable(tmp_path)
    add_actions_table = dt.get_add_actions(flatten=True)

    def get_value(name: str):
        return add_actions_table.column(name)[0].as_py()

    # x1 has no max, since inf was the highest value
    assert get_value("null_count.foo") == 0
    assert get_value("min.foo") == "hello world"
    assert get_value("max.foo") == "hello world"
    assert get_value("null_count.baz") == 2
    assert get_value("min.baz") == 1
    assert get_value("max.baz") == 1

    with pytest.raises(Exception):
        get_value("null_count.bar")
