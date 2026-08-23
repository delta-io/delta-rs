import json
import pathlib
from typing import TYPE_CHECKING

import pytest
from arro3.core import Array, DataType, Field, Table

from deltalake import CommitProperties, write_deltalake
from deltalake.table import DeltaTable

if TYPE_CHECKING:
    import pyarrow as pa


def _strip_add_stats(table_path: pathlib.Path) -> None:
    # Simulates a writer that elided optional Add.stats per Delta spec.
    log_path = table_path / "_delta_log" / "00000000000000000000.json"
    rewritten_lines = []
    for line in log_path.read_text().splitlines():
        payload = json.loads(line)
        if "add" in payload:
            payload["add"].pop("stats", None)
        rewritten_lines.append(json.dumps(payload, separators=(",", ":")))

    log_path.write_text("\n".join(rewritten_lines) + "\n")


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
    assert len(existing_sample_table.file_uris()) == 0


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
    assert len(dt.file_uris()) == 1


@pytest.mark.pyarrow
def test_delete_partition_only_reports_deleted_rows_when_stats_exist(
    tmp_path: pathlib.Path,
):
    import pyarrow as pa

    write_deltalake(
        tmp_path,
        pa.table(
            {
                "part": pa.array(["a", "a", "a", "b"]),
                "value": pa.array([1, 2, 3, 4], pa.int64()),
            }
        ),
        partition_by=["part"],
    )

    dt = DeltaTable(tmp_path)
    metrics = dt.delete(predicate="part = 'a'")

    assert metrics["num_removed_files"] == 1
    assert metrics["num_deleted_rows"] == 3
    assert metrics["num_copied_rows"] == 0


@pytest.mark.pyarrow
def test_delete_partition_only_omits_deleted_rows_when_stats_missing(
    tmp_path: pathlib.Path,
):
    import pyarrow as pa

    write_deltalake(
        tmp_path,
        pa.table(
            {
                "part": pa.array(["a", "a", "a", "b"]),
                "value": pa.array([1, 2, 3, 4], pa.int64()),
            }
        ),
        partition_by=["part"],
    )
    _strip_add_stats(tmp_path)

    dt = DeltaTable(tmp_path)
    metrics = dt.delete(predicate="part = 'a'")

    assert metrics["num_removed_files"] == 1
    assert "num_deleted_rows" not in metrics
    assert metrics["num_copied_rows"] == 0


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
    assert get_value("null_count.baz") == 2
    assert get_value("min.baz") == 1
    assert get_value("max.baz") == 1

    with pytest.raises(Exception):
        get_value("null_count.bar")

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
    assert get_value("null_count.baz") == 1
    assert get_value("min.baz") == 1
    assert get_value("max.baz") == 1

    with pytest.raises(Exception):
        get_value("null_count.bar")
