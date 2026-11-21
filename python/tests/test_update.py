import datetime
import pathlib
import time
from typing import TYPE_CHECKING

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import CommitProperties, DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError
from deltalake.query import QueryBuilder

if TYPE_CHECKING:
    import pyarrow as pa


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
    last_action = next(dt.history(1))

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
    last_action = next(dt.history(1))

    assert last_action["operation"] == "UPDATE"
    assert result == expected


def test_update_wrong_types_cast(tmp_path: pathlib.Path, sample_table: Table):
    write_deltalake(tmp_path, sample_table, mode="append")

    dt = DeltaTable(tmp_path)

    with pytest.raises(Exception) as excinfo:
        dt.update(updates={"deleted": "'hello_world'"})

    expected = """Generic DeltaTable error: Optimizer rule 'simplify_expressions' failed\ncaused by\nArrow error: Cast error: Cannot cast value 'hello_world' to value of Boolean type"""
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
    last_action = next(dt.history(1))

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
        },
        predicate="price > 3",
    )

    result = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    last_action = next(dt.history(1))

    assert last_action["operation"] == "UPDATE"
    assert result == expected


@pytest.mark.parametrize("value", ["Alice", "O'Reilly", "a' || 'b", ""])
def test_update_new_values_preserves_strings(tmp_path, value):
    data = Table(
        {
            "name": Array(
                ["before"],
                ArrowField("name", type=DataType.string(), nullable=True),
            )
        }
    )
    write_deltalake(tmp_path, data)
    dt = DeltaTable(tmp_path)

    dt.update(new_values={"name": value})

    result = (
        QueryBuilder().register("tbl", dt).execute("SELECT name FROm tbl").read_all()
    )
    assert result.column("name")[0].as_py() == value


@pytest.mark.pyarrow
@pytest.mark.parametrize("tz", ["UTC", "Asia/Seoul", "America/Los_Angeles"])
def test_update_new_values_naive_datetime_is_utc(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, tz: str
):
    import pyarrow as pa

    if not hasattr(time, "tzset"):
        pytest.skip("time.tzset is not available on this platform")

    data = pa.table(
        {
            "ts": pa.array(
                [datetime.datetime(2026, 1, 1)], type=pa.timestamp("us", "UTC")
            )
        }
    )
    write_deltalake(tmp_path, data)
    dt = DeltaTable(tmp_path)

    monkeypatch.setenv("TZ", tz)
    time.tzset()

    dt.update(new_values={"ts": datetime.datetime(2026, 6, 1, 12, 30)})

    result = QueryBuilder().register("tbl", dt).execute("SELECT ts FROM tbl").read_all()
    assert result.column("ts")[0].as_py() == datetime.datetime(
        2026, 6, 1, 12, 30, tzinfo=datetime.timezone.utc
    )


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


def test_update_unknown_column(tmp_path: pathlib.Path, sample_table: Table):
    write_deltalake(tmp_path, sample_table, mode="append")
    dt = DeltaTable(tmp_path)

    with pytest.raises(DeltaError, match="No field named"):
        dt.update(updates={"Price": "0"}, predicate="id = '1'")

    assert dt.version() == 0


@pytest.mark.pyarrow
def test_update_struct_field(tmp_path: pathlib.Path, struct_table: "pa.Table"):
    write_deltalake(tmp_path, struct_table)
    dt = DeltaTable(tmp_path)

    dt.update({"s.a": "0"}, predicate="id = 1")
    dt.update({"s.inner.x": "coalesce(s.inner.x, 0) + 1", "`s`.b": "'new'"})

    result = dt.to_pyarrow_table().sort_by("id").to_pydict()
    assert result == {
        "id": [1, 2, 3],
        "s": [
            {"a": 0, "b": "new", "inner": {"x": 11, "y": 11}},
            # A null struct is built: the fields that the update does not set are null
            {"a": 2, "b": "new", "inner": {"x": 1, "y": None}},
            {"a": None, "b": "new", "inner": {"x": 1, "y": None}},
        ],
    }


@pytest.mark.pyarrow
def test_update_struct_field_conflict(tmp_path: pathlib.Path, struct_table: "pa.Table"):
    write_deltalake(tmp_path, struct_table)
    dt = DeltaTable(tmp_path)

    with pytest.raises(DeltaError, match="set the same field"):
        dt.update({"s.inner": "NULL", "s.inner.x": "0"})

    assert dt.version() == 0


@pytest.mark.pyarrow
def test_update_struct_field_to_struct_with_not_null_field(tmp_path: pathlib.Path):
    import pyarrow as pa

    inner = pa.struct([pa.field("x", pa.int64(), nullable=False)])
    struct = pa.struct([("inner", inner), ("b", pa.string())])
    data = pa.table(
        {
            "id": pa.array([1], pa.int64()),
            "s": pa.array([{"inner": {"x": 1}, "b": "one"}], type=struct),
        }
    )
    write_deltalake(tmp_path, data)
    dt = DeltaTable(tmp_path)

    # The key sets the struct `s.inner` as a whole. Its field `x` is NOT NULL.
    dt.update({"s.inner": "named_struct('x', 5)"})

    result = dt.to_pyarrow_table().to_pydict()
    assert result == {"id": [1], "s": [{"inner": {"x": 5}, "b": "one"}]}


@pytest.mark.pyarrow
def test_update_key_that_needs_backticks(
    tmp_path: pathlib.Path, struct_table: "pa.Table"
):
    write_deltalake(tmp_path, struct_table)
    dt = DeltaTable(tmp_path)

    # `my field` is not an identifier without backticks. The error must tell the user that.
    with pytest.raises(DeltaError) as error:
        dt.update({"s.my field": "0"})

    assert str(error.value) == (
        'Generic DeltaTable error: Schema error: No field named "s.my field".\n'
        "Valid fields are id, s.\n"
        "'s.my field' is read as one column name, because it is not a list of identifiers. "
        "Put backticks around each name that has a space or another special character, "
        "for example `my col`.a"
    )

    assert dt.version() == 0


@pytest.mark.pyarrow
@pytest.mark.parametrize("key", ["x.a", "x.a.b.c", "x.a.b.c.d", "x.a.b.c.d.e"])
def test_update_unknown_key_with_many_parts(tmp_path: pathlib.Path, key: str):
    import pyarrow as pa

    write_deltalake(tmp_path, pa.table({"id": [1]}))
    dt = DeltaTable(tmp_path)

    # The error must name the unknown key for each number of parts
    with pytest.raises(DeltaError) as error:
        dt.update({key: "9"})

    # DataFusion puts quotes around a name that it cannot split into a table and a column
    named = f'"{key}"' if key.count(".") >= 4 else key
    assert str(error.value) == (
        f"Generic DeltaTable error: Schema error: No field named {named}.\n"
        "Valid fields are id."
    )

    assert dt.version() == 0


@pytest.mark.pyarrow
def test_update_struct_field_of_column_with_dot(tmp_path: pathlib.Path):
    import pyarrow as pa

    write_deltalake(tmp_path, pa.table({"id": [1], "my.col": [{"a": 1, "b": 2}]}))
    dt = DeltaTable(tmp_path)

    dt.update({"`my.col`.a": "0"})

    result = dt.to_pyarrow_table().to_pydict()
    assert result == {"id": [1], "my.col": [{"a": 0, "b": 2}]}


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
