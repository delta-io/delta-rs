import pathlib

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from deltalake.table import CommitProperties, DeltaTable
from deltalake.writer import write_deltalake


def test_delete_no_predicates(existing_table: DeltaTable):
    old_version = existing_table.version()

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    existing_table.delete(commit_properties=commit_properties)

    last_action = existing_table.history(1)[0]
    assert last_action["operation"] == "DELETE"
    assert existing_table.version() == old_version + 1
    assert last_action["userName"] == "John Doe"

    dataset = existing_table.to_pyarrow_dataset()
    assert dataset.count_rows() == 0
    assert len(existing_table.files()) == 0


def test_delete_a_partition(tmp_path: pathlib.Path, sample_data: pa.Table):
    write_deltalake(tmp_path, sample_data, partition_by=["bool"])

    dt = DeltaTable(tmp_path)
    old_version = dt.version()

    mask = pc.equal(sample_data["bool"], False)
    expected_table = sample_data.filter(mask)

    dt.delete(predicate="bool = true")

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "DELETE"
    assert dt.version() == old_version + 1

    table = dt.to_pyarrow_table()
    assert table.equals(expected_table)
    assert len(dt.files()) == 1


def test_delete_some_rows(existing_table: DeltaTable):
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


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_delete_large_dtypes(
    tmp_path: pathlib.Path, sample_table: pa.table, engine: str
):
    write_deltalake(tmp_path, sample_table, large_dtypes=True, engine=engine)  # type: ignore

    dt = DeltaTable(tmp_path)
    old_version = dt.version()

    existing = dt.to_pyarrow_table()
    mask = pc.invert(pc.is_in(existing["id"], pa.array(["1"])))
    expected_table = existing.filter(mask)

    dt.delete(predicate="id = '1'")

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "DELETE"
    assert dt.version() == old_version + 1

    table = dt.to_pyarrow_table()
    assert table.equals(expected_table)


@pytest.mark.parametrize("engine", ["pyarrow", "rust"])
def test_delete_stats_columns_stats_provided(tmp_path: pathlib.Path, engine):
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

    dt.delete("bar == 3")

    dt = DeltaTable(tmp_path)
    add_actions_table = dt.get_add_actions(flatten=True)
    stats = add_actions_table.to_pylist()[0]

    assert dt.version() == 1
    assert stats["null_count.foo"] == 1
    assert stats["min.foo"] == "a"
    assert stats["max.foo"] == "b"
    assert stats["null_count.bar"] is None
    assert stats["min.bar"] is None
    assert stats["max.bar"] is None
    assert stats["null_count.baz"] == 1
    assert stats["min.baz"] == 1
    assert stats["max.baz"] == 1
