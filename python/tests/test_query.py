import pytest

from deltalake import DeltaTable
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
