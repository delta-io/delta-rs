import pathlib

import pytest
from arro3.core import Array, DataType, Field, Table

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError
from deltalake.query import QueryBuilder

TABLE_PATH = "../crates/test/tests/data/simple_table"


def _setting(qb: QueryBuilder, name: str) -> str:
    result = qb.execute(
        f"select value from information_schema.df_settings where name = '{name}'"
    ).read_all()
    return result["value"].to_pylist()[0]


def test_query_builder_without_session_config():
    dt = DeltaTable(TABLE_PATH)
    result = (
        QueryBuilder()
        .register("tbl", dt)
        .execute("select * from tbl ORDER BY id")
        .read_all()
    )
    assert result["id"].to_pylist() == [5, 7, 9]


def test_query_builder_session_config_applies_override():
    qb = QueryBuilder(
        {
            "datafusion.catalog.information_schema": "true",
            "datafusion.execution.batch_size": "7",
        }
    )
    assert _setting(qb, "datafusion.execution.batch_size") == "7"


def test_query_builder_session_config_preserves_delta_defaults():
    qb = QueryBuilder(
        {
            "datafusion.catalog.information_schema": "true",
            "datafusion.execution.batch_size": "7",
        }
    )
    # deltalake turns this off, while DataFusion's own default is on, so it only
    # reads as "false" here if Delta's defaults survived the override.
    assert _setting(qb, "datafusion.sql_parser.enable_ident_normalization") == "false"


def test_query_builder_session_config_rejects_unknown_key():
    with pytest.raises(DeltaError, match="datafusion.execution.bogus_option"):
        QueryBuilder({"datafusion.execution.bogus_option": "1"})


def test_query_builder_session_config_still_queries_tables():
    dt = DeltaTable(TABLE_PATH)
    result = (
        QueryBuilder({"datafusion.execution.batch_size": "1"})
        .register("tbl", dt)
        .execute("select * from tbl ORDER BY id")
        .read_all()
    )
    assert result["id"].to_pylist() == [5, 7, 9]


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
    ("where", "expected"),
    [
        ("CASE WHEN p = 1 THEN true ELSE false END", [1]),
        ("NOT coalesce(q > 'a', false)", [0, 2]),
        ("p = 1 AND x = NULL", []),
        ("x NOT IN (1, NULL)", []),
        ("p = 1", [1]),
        ("p IS NULL", [2]),
    ],
)
def test_query_builder_partition_filter_follows_sql_semantics(
    tmp_path: pathlib.Path, where: str, expected: list[int]
):
    _write_partitioned_table(tmp_path)

    result = (
        QueryBuilder()
        .register("tbl", DeltaTable(tmp_path))
        .execute(f"select id from tbl where {where}")
        .read_all()
    )

    assert sorted(result["id"].to_pylist()) == expected


def test_query_builder_nan_float_with_equal_min_max_not_constant_folded(
    tmp_path: pathlib.Path,
):
    # Regression test for #5007: a file holding [NaN, 9.0] in a float column
    # records min = max = 9.0 in the Delta log, because Parquet statistics
    # leave NaN out of float min/max. The scan must not constant-fold the
    # column to 9.0, or the NaN row is returned (and later overwritten or
    # deleted) as 9.0.
    import math

    import pyarrow as pa

    def _write_table(name: str) -> pathlib.Path:
        path = tmp_path / name
        write_deltalake(path, pa.table({"id": [1, 2], "f": [math.nan, 9.0]}))
        return path

    def _rows(path: pathlib.Path, where: str | None = None) -> list[tuple[int, float]]:
        sql = "SELECT id, f FROM t"
        if where:
            sql += f" WHERE {where}"
        result = (
            QueryBuilder()
            .register("t", DeltaTable(path))
            .execute(sql + " ORDER BY id")
            .read_all()
        )
        return list(zip(result["id"].to_pylist(), result["f"].to_pylist()))

    path = _write_table("read")
    stored = _rows(path)
    assert [row[0] for row in stored] == [1, 2]
    assert math.isnan(stored[0][1])
    assert stored[1][1] == 9.0
    assert _rows(path, "f = 9.0") == [(2, 9.0)]

    path = _write_table("update")
    DeltaTable(path).update(predicate="f < 10.0", updates={"id": "100"})
    updated = _rows(path)
    assert [row[0] for row in updated] == [1, 100]
    assert math.isnan(updated[0][1])

    path = _write_table("delete")
    DeltaTable(path).delete("f = 9.0")
    remaining = _rows(path)
    assert [row[0] for row in remaining] == [1]
    assert math.isnan(remaining[0][1])
