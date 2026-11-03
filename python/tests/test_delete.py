import json
import pathlib
from typing import TYPE_CHECKING

import pytest
from arro3.core import Array, DataType, Field, Table

from deltalake import CommitProperties, write_deltalake
from deltalake.exceptions import CommitFailedError
from deltalake.query import QueryBuilder
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

    last_action = next(existing_sample_table.history(1))
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

    last_action = next(dt.history(1))
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

    last_action = next(existing_table.history(1))
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


@pytest.mark.pandas
def test_delete_concurrent_with_non_overlapping_append(tmp_path: pathlib.Path):
    """A delete must not report an error for a commit that succeeded.

    Regression test for https://github.com/delta-io/delta-rs/issues/2509.
    """
    import pandas as pd

    df = pd.DataFrame.from_dict({"k": [1], "v": [1]})
    write_deltalake(tmp_path, df, mode="overwrite")

    # Read the table before the append, so that the delete below is concurrent.
    table_2 = DeltaTable(tmp_path)

    data_1 = pd.DataFrame.from_dict({"k": [3], "v": [-3]})
    write_deltalake(tmp_path, data_1, mode="append")

    table_2.delete("k = 1")

    assert table_2.version() == 2
    assert next(table_2.history(1))["operation"] == "DELETE"

    from deltalake.query import QueryBuilder

    expected = Table.from_pydict(
        {
            "k": Array([3], Field("k", type=DataType.int64(), nullable=True)),
            "v": Array([-3], Field("v", type=DataType.int64(), nullable=True)),
        }
    )
    result = (
        QueryBuilder()
        .register("tbl", DeltaTable(tmp_path))
        .execute("select k, v from tbl")
        .read_all()
    )
    assert result == expected


def _write_partitioned_table(path: pathlib.Path) -> None:
    write_deltalake(
        path,
        Table(
            {
                "id": Array(
                    [0, 1, 2, 3], type=Field("id", DataType.int64(), nullable=True)
                ),
                "x": Array(
                    [0, 1, 2, None], type=Field("x", DataType.int64(), nullable=True)
                ),
                "p": Array(
                    [0, 1, None, 2], type=Field("p", DataType.int32(), nullable=True)
                ),
                "q": Array(
                    ["a", "b", None, "c"],
                    type=Field("q", DataType.string(), nullable=True),
                ),
            }
        ),
        partition_by=["p", "q"],
    )


@pytest.mark.parametrize(
    ("predicate", "expected_remaining"),
    [
        ("p + 1 = 2", [0, 2, 3]),
        ("CASE WHEN p = 1 THEN true ELSE false END", [0, 2, 3]),
        ("NOT coalesce(q > 'a', false)", [1, 3]),
        ("x = NULL", [0, 1, 2, 3]),
        ("p = 1 AND x = NULL", [0, 1, 2, 3]),
        ("p = 1", [0, 2, 3]),
        ("p IS NULL", [0, 1, 3]),
    ],
)
def test_delete_partition_predicate_follows_sql_semantics(
    tmp_path: pathlib.Path, predicate: str, expected_remaining: list[int]
):
    _write_partitioned_table(tmp_path)

    metrics = DeltaTable(tmp_path).delete(predicate)

    remaining = (
        QueryBuilder()
        .register("tbl", DeltaTable(tmp_path))
        .execute("select id from tbl")
        .read_all()["id"]
        .to_pylist()
    )
    assert sorted(remaining) == expected_remaining
    assert metrics["num_deleted_rows"] == 4 - len(expected_remaining)


@pytest.mark.pyarrow
def test_delete_without_predicate_conflicts_with_concurrent_merge(
    tmp_path: pathlib.Path,
):
    """https://github.com/delta-io/delta-rs/issues/4985

    A delete() without a predicate on a stale snapshot must conflict with an
    insert-only merge committed after the snapshot was loaded, matching the
    behavior of delete() with a predicate.
    """
    import pyarrow as pa

    write_deltalake(tmp_path, pa.table({"id": [1]}))
    stale = DeltaTable(tmp_path)
    (
        DeltaTable(tmp_path)
        .merge(
            pa.table({"id": [1, 2]}),
            "t.id = s.id",
            source_alias="s",
            target_alias="t",
        )
        .when_not_matched_insert_all()
        .execute()
    )

    with pytest.raises(CommitFailedError):
        stale.delete()


@pytest.mark.pyarrow
def test_delete_is_distinct_from_binds_tighter_than_and(tmp_path: pathlib.Path):
    """https://github.com/delta-io/delta-rs/issues/4987

    `f IS DISTINCT FROM TRUE AND id = 2` must parse as
    `(f IS DISTINCT FROM TRUE) AND id = 2` so only id 2 is deleted.
    """
    import pyarrow as pa

    write_deltalake(tmp_path, pa.table({"id": [1, 2, 3], "f": [True, False, None]}))

    DeltaTable(tmp_path).delete("f IS DISTINCT FROM TRUE AND id = 2")

    remaining = (
        QueryBuilder()
        .register("tbl", DeltaTable(tmp_path))
        .execute("select id from tbl")
        .read_all()["id"]
        .to_pylist()
    )
    assert sorted(remaining) == [1, 3]
