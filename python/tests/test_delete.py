import pathlib
from typing import TYPE_CHECKING

import pytest
from arro3.core import Array, DataType, Field, Table

from deltalake import CommitProperties, write_deltalake
from deltalake.table import DeltaTable

if TYPE_CHECKING:
    import pyarrow as pa


def test_delete_no_predicates(existing_sample_table: DeltaTable):
    old_version = existing_sample_table.version()

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    existing_sample_table.delete(commit_properties=commit_properties)

    last_action = existing_sample_table.history(1)[0]
    assert last_action["operation"] == "DELETE"
    assert existing_sample_table.version() == old_version + 1
    assert last_action["userName"] == "John Doe"

    from deltalake.query import QueryBuilder

    qb = QueryBuilder()
    qb = QueryBuilder().register("tbl", existing_sample_table)
    data = qb.execute("select * from tbl").read_all()

    assert data.num_rows == 0
    assert len(existing_sample_table.files()) == 0


@pytest.mark.pyarrow
def test_delete_a_partition(tmp_path: pathlib.Path, sample_data_pyarrow: "pa.Table"):
    write_deltalake(tmp_path, sample_data_pyarrow, partition_by=["bool"])
    import pyarrow.compute as pc

    dt = DeltaTable(tmp_path)
    old_version = dt.version()

    mask = pc.equal(sample_data_pyarrow["bool"], False)
    expected_table = sample_data_pyarrow.filter(mask)

    dt.delete(predicate="bool = true")

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "DELETE"
    assert dt.version() == old_version + 1

    table = dt.to_pyarrow_table()
    assert table.equals(expected_table)
    assert len(dt.files()) == 1


@pytest.mark.pyarrow
def test_delete_some_rows(existing_table: DeltaTable):
    import pyarrow as pa
    import pyarrow.compute as pc

    old_version = existing_table.version()

    existing = existing_table.to_pyarrow_table()
    mask = pc.invert(pc.is_in(existing["utf8"], pa.array(["0", "1"])))
    expected_table = existing.filter(mask)

    existing_table.delete(predicate="utf8 in ('0', '1')")

    last_action = existing_table.history(1)[0]
    assert last_action["operation"] == "DELETE"
    assert existing_table.version() == old_version + 1

    table = existing_table.to_pyarrow_table()
    assert table.equals(expected_table)


def test_delete_stats_columns_stats_provided(tmp_path: pathlib.Path):
    data = Table(
        {
            "foo": Array(
                ["a", "b", None, None],
                type=Field("foo", DataType.string(), nullable=True),
            ),
            "bar": Array(
                [1, 2, 3, None], type=Field("bar", DataType.int64(), nullable=True)
            ),
            "baz": Array(
                [1, 1, None, None], type=Field("baz", DataType.int64(), nullable=True)
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
    assert get_value("null_count.bar") is None
    assert get_value("min.bar") is None
    assert get_value("max.bar") is None
    assert get_value("null_count.baz") == 2
    assert get_value("min.baz") == 1
    assert get_value("max.baz") == 1

    dt.delete("bar == 3")

    dt = DeltaTable(tmp_path)
    add_actions_table = dt.get_add_actions(flatten=True)

    assert dt.version() == 1

    def get_value(name: str):
        return add_actions_table.column(name)[0].as_py()

    # x1 has no max, since inf was the highest value
    assert get_value("null_count.foo") == 1
    assert get_value("min.foo") == "a"
    assert get_value("max.foo") == "b"
    assert get_value("null_count.bar") is None
    assert get_value("min.bar") is None
    assert get_value("max.bar") is None
    assert get_value("null_count.baz") == 1
    assert get_value("min.baz") == 1
    assert get_value("max.baz") == 1
