"""Engine selection on to_pyarrow_table / to_pandas.

Both engines are exercised side by side: where they agree, the tests assert
the shared result; where SQL and pyarrow semantics genuinely diverge, each
engine's behavior is pinned separately so the differences stay visible and
documented.
"""

import pathlib

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError, DeltaProtocolError

pytestmark = pytest.mark.pyarrow

COLUMN_MAPPED_TABLE = "../crates/test/tests/data/table_with_column_mapping"
DV_TABLE = "../crates/test/tests/data/table-with-dv-small"

# the ids avoid the conftest keyword-skip reserved for datafusion-python
# wheel integration tests
ENGINES = [pytest.param("pyarrow", id="pa"), pytest.param("datafusion", id="df")]


@pytest.fixture
def edge_table(tmp_path: pathlib.Path) -> DeltaTable:
    """s: ["", "a", "b", None], v: [1, 2, 3, 4]."""
    data = Table(
        {
            "s": Array(
                ["", "a", "b", None], ArrowField("s", DataType.string(), nullable=True)
            ),
            "v": Array([1, 2, 3, 4], ArrowField("v", DataType.int64(), nullable=True)),
        }
    )
    write_deltalake(tmp_path, data)
    return DeltaTable(tmp_path)


def read(dt: DeltaTable, engine: str, **kwargs) -> dict:
    table = dt.to_pyarrow_table(engine=engine, **kwargs)
    return table.sort_by("v").to_pydict()


@pytest.mark.parametrize("engine", ENGINES)
def test_full_read_agrees(edge_table, engine):
    assert read(edge_table, engine) == {
        "s": ["", "a", "b", None],
        "v": [1, 2, 3, 4],
    }


@pytest.mark.parametrize("engine", ENGINES)
def test_typed_comparison_agrees(edge_table, engine):
    assert read(edge_table, engine, filters=[("v", ">=", 3)])["v"] == [3, 4]
    assert read(edge_table, engine, filters=[("v", "in", [1, 3])])["v"] == [1, 3]


@pytest.mark.parametrize("engine", ENGINES)
def test_dnf_or_agrees(edge_table, engine):
    result = read(edge_table, engine, filters=[[("v", "=", 1)], [("s", "=", "b")]])
    assert result["v"] == [1, 3]


@pytest.mark.parametrize("engine", ENGINES)
def test_empty_string_equality_agrees(edge_table, engine):
    # the SQL-string routing this replaces rewrote "" to IS NULL; with the
    # filters compiled to typed expressions both engines agree "" is data
    result = read(edge_table, engine, filters=[("s", "=", "")])
    assert result == {"s": [""], "v": [1]}


@pytest.mark.parametrize("engine", ENGINES)
def test_columns_projection_agrees(edge_table, engine):
    table = edge_table.to_pyarrow_table(columns=["v"], engine=engine)
    assert table.column_names == ["v"]
    assert sorted(table["v"].to_pylist()) == [1, 2, 3, 4]


def test_not_in_null_divergence(edge_table):
    # SQL three-valued logic: NULL NOT IN (...) is NULL, so the row is dropped;
    # pyarrow keeps it
    filters = [("s", "not in", ["a"])]
    assert read(edge_table, "pyarrow", filters=filters)["s"] == ["", "b", None]
    assert read(edge_table, "datafusion", filters=filters)["s"] == ["", "b"]


def test_null_equality_divergence(edge_table):
    # tuple filters have always allowed equality against null (see the listing
    # APIs); in SQL `= NULL` matches nothing, pyarrow follows the SQL reading
    filters = [("s", "=", None)]
    assert read(edge_table, "pyarrow", filters=filters)["s"] == []
    assert read(edge_table, "datafusion", filters=filters)["s"] == [None]
    assert read(edge_table, "datafusion", filters=[("s", "!=", None)])["s"] == [
        "",
        "a",
        "b",
    ]


def test_duplicate_projection_divergence(edge_table):
    # legal (and documented) on the pyarrow path, a planning error in DataFusion
    table = edge_table.to_pyarrow_table(columns=["v", "v"], engine="pyarrow")
    assert table.column_names == ["v", "v"]
    with pytest.raises(DeltaError):
        edge_table.to_pyarrow_table(columns=["v", "v"], engine="datafusion")


def test_datafusion_reads_column_mapped_table():
    dt = DeltaTable(COLUMN_MAPPED_TABLE)
    with pytest.raises(DeltaProtocolError, match="reader version|columnMapping"):
        dt.to_pyarrow_table(engine="pyarrow")
    table = dt.to_pyarrow_table(
        engine="datafusion", filters=[("Company Very Short", "=", "BME")]
    )
    assert table["Super Name"].to_pylist() == ["Timothy Lamb"]


def test_datafusion_reads_deletion_vector_table():
    dt = DeltaTable(DV_TABLE)
    with pytest.raises(DeltaProtocolError, match="deletionVectors"):
        dt.to_pyarrow_table(engine="pyarrow")
    table = dt.to_pyarrow_table(engine="datafusion")
    assert sorted(table["value"].to_pylist()) == [1, 2, 3, 4, 5, 6, 7, 8]


def test_datafusion_rejects_pyarrow_only_arguments(edge_table):
    import pyarrow.dataset as ds
    import pyarrow.fs as pa_fs

    with pytest.raises(ValueError, match="partitions"):
        edge_table.to_pyarrow_table(engine="datafusion", partitions=[("v", "=", 1)])
    with pytest.raises(ValueError, match="filesystem"):
        edge_table.to_pyarrow_table(
            engine="datafusion", filesystem=pa_fs.LocalFileSystem()
        )
    with pytest.raises(ValueError, match="tuple filters"):
        edge_table.to_pyarrow_table(engine="datafusion", filters=ds.field("v") == 1)


def test_unknown_engine_errors(edge_table):
    with pytest.raises(ValueError, match="unknown engine"):
        edge_table.to_pyarrow_table(engine="polars")


def test_sql_string_filters_datafusion(edge_table):
    by_string = read(edge_table, "datafusion", filters="v >= 3 AND s IS NOT NULL")
    by_tuples = read(
        edge_table, "datafusion", filters=[("v", ">=", 3), ("s", "!=", None)]
    )
    assert by_string == by_tuples
    assert by_string["v"] == [3]


def test_sql_string_filters_rejected_under_pyarrow(edge_table):
    with pytest.raises(ValueError, match="SQL string filters"):
        edge_table.to_pyarrow_table(filters="v >= 3", engine="pyarrow")


@pytest.mark.pandas
def test_to_pandas_datafusion(edge_table):
    df = edge_table.to_pandas(engine="datafusion", filters=[("v", "<", 3)])
    assert sorted(df["v"].tolist()) == [1, 2]


def test_pyarrow_engine_warns(edge_table):
    with pytest.warns(DeprecationWarning, match="pyarrow engine"):
        edge_table.to_pyarrow_table()
    with pytest.warns(DeprecationWarning, match="pyarrow engine"):
        edge_table.to_pyarrow_table(engine="pyarrow")


@pytest.mark.pandas
def test_to_pandas_pyarrow_engine_warns(edge_table):
    with pytest.warns(DeprecationWarning, match="pyarrow engine"):
        edge_table.to_pandas()


def test_datafusion_engine_does_not_warn(edge_table):
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        edge_table.to_pyarrow_table(engine="datafusion")
