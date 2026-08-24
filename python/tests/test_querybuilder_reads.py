"""Reads through the QueryBuilder path of `to_pyarrow_table` and `to_pandas`.

`to_pyarrow_table` reads through the embedded DataFusion engine when the call
only projects columns and filters rows with tuple filters; everything else
scans through a pyarrow dataset as before. These tests pin the equivalence of
the two paths, the path selection, and the reader features the DataFusion path
unlocks.
"""

from pathlib import Path

import pytest

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError, DeltaProtocolError
from deltalake.table import _read_query

PARTITIONED_TABLE = "../crates/test/tests/data/delta-0.8.0-partitioned"
COLUMN_MAPPED_TABLE = "../crates/test/tests/data/table_with_column_mapping"


def _rows(table) -> list[dict]:
    return sorted(table.to_pylist(), key=lambda row: tuple(map(str, row.values())))


def _dataset_path_table(dt: DeltaTable, columns=None, filters=None):
    """Read through the pyarrow dataset path directly, bypassing the router."""
    from pyarrow.parquet import filters_to_expression

    expression = filters_to_expression(filters) if filters is not None else None
    return dt.to_pyarrow_dataset().to_table(columns=columns, filter=expression)


def _spy_dataset_path(monkeypatch) -> list:
    """Record every call that reaches `to_pyarrow_dataset`."""
    calls = []
    original = DeltaTable.to_pyarrow_dataset

    def wrapper(self, *args, **kwargs):
        calls.append(1)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(DeltaTable, "to_pyarrow_dataset", wrapper)
    return calls


def test_read_query_rendering():
    assert _read_query(None, None) == "SELECT * FROM tbl"
    assert _read_query(["day", "value"], None) == 'SELECT "day", "value" FROM tbl'
    assert (
        _read_query(None, [("name", "=", "O'Brien")])
        == "SELECT * FROM tbl WHERE \"name\" = 'O''Brien'"
    )
    assert (
        _read_query(None, [('a"b', "<=", 3)]) == 'SELECT * FROM tbl WHERE "a""b" <= 3'
    )
    assert (
        _read_query(None, [[("year", "=", "2021")], [("month", "in", [1, 2])]])
        == 'SELECT * FROM tbl WHERE (("year" = \'2021\') OR ("month" IN (1, 2)))'
    )
    assert (
        _read_query(None, [("part", "=", ""), ("day", "in", []), ("value", "!=", "")])
        == 'SELECT * FROM tbl WHERE "part" IS NULL AND FALSE AND "value" IS NOT NULL'
    )

    with pytest.raises(ValueError, match="invalid filter operator"):
        _read_query(None, [("day", "=>", "3")])
    with pytest.raises(ValueError, match="requires a collection"):
        _read_query(None, [("day", "in", "3")])


@pytest.mark.pyarrow
@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"columns": ["day", "value"]},
        {"filters": [("year", "=", "2021")]},
        {"filters": [[("year", "=", "2020")], [("month", "=", "12")]]},
        {"filters": [("day", "in", ["4", "5"]), ("year", "=", "2021")]},
        {"filters": [("day", "not in", ["4", "5"])], "columns": ["value"]},
        {"filters": [("value", ">=", "5")], "columns": ["value", "year"]},
    ],
)
def test_matches_pyarrow_dataset_path(kwargs):
    dt = DeltaTable(PARTITIONED_TABLE)
    via_query = dt.to_pyarrow_table(**kwargs)
    via_dataset = _dataset_path_table(dt, **kwargs)
    assert via_query.schema.equals(via_dataset.schema)
    assert _rows(via_query) == _rows(via_dataset)


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_to_pandas_matches_pyarrow_path():
    dt = DeltaTable(PARTITIONED_TABLE)
    df = dt.to_pandas(filters=[("year", "=", "2020")], columns=["value", "day"])
    assert list(df.columns) == ["value", "day"]
    assert sorted(df["value"]) == ["1", "2", "3"]


@pytest.mark.pyarrow
def test_expression_filters_fall_back_to_dataset(monkeypatch):
    import pyarrow.dataset as ds

    calls = _spy_dataset_path(monkeypatch)
    dt = DeltaTable(PARTITIONED_TABLE)

    tuple_result = dt.to_pyarrow_table(filters=[("year", "=", "2021")])
    assert not calls

    expr_result = dt.to_pyarrow_table(filters=ds.field("year") == "2021")
    assert len(calls) == 1
    assert _rows(expr_result) == _rows(tuple_result)


@pytest.mark.pyarrow
def test_file_pruning_predicate_falls_back_to_dataset(monkeypatch):
    calls = _spy_dataset_path(monkeypatch)
    dt = DeltaTable(PARTITIONED_TABLE)

    by_tuples = dt.to_pyarrow_table(file_pruning_predicate=[("year", "=", "2021")])
    assert len(calls) == 1
    assert by_tuples.num_rows == 4

    by_sql = dt.to_pyarrow_table(file_pruning_predicate="year = '2021'")
    assert len(calls) == 2
    assert _rows(by_sql) == _rows(by_tuples)

    # the deprecated partitions parameter takes the same path
    by_partitions = dt.to_pyarrow_table(partitions=[("year", "=", "2021")])
    assert len(calls) == 3
    assert _rows(by_partitions) == _rows(by_tuples)


@pytest.mark.pyarrow
def test_custom_filesystem_falls_back_to_dataset(monkeypatch):
    import pyarrow.fs as pa_fs

    calls = _spy_dataset_path(monkeypatch)
    root = (Path.cwd().parent / PARTITIONED_TABLE.removeprefix("../")).as_posix()
    filesystem = pa_fs.SubTreeFileSystem(root, pa_fs.LocalFileSystem())

    table = DeltaTable(PARTITIONED_TABLE).to_pyarrow_table(filesystem=filesystem)
    assert len(calls) == 1
    assert table.num_rows == 7


@pytest.mark.pyarrow
def test_unrenderable_filter_value_falls_back_to_dataset(tmp_path, monkeypatch):
    import pyarrow as pa

    write_deltalake(tmp_path, pa.table({"x": [1.0, 2.0]}))
    calls = _spy_dataset_path(monkeypatch)

    table = DeltaTable(tmp_path).to_pyarrow_table(filters=[("x", "=", float("nan"))])
    assert len(calls) == 1
    assert table.num_rows == 0


@pytest.mark.pyarrow
def test_empty_projection_falls_back_to_dataset(monkeypatch):
    calls = _spy_dataset_path(monkeypatch)
    table = DeltaTable(PARTITIONED_TABLE).to_pyarrow_table(columns=[])
    assert len(calls) == 1
    assert table.num_columns == 0
    assert table.num_rows == 7


@pytest.mark.pandas
@pytest.mark.pyarrow
def test_column_mapped_table_reads():
    dt = DeltaTable(COLUMN_MAPPED_TABLE)

    df = dt.to_pandas()
    assert list(df.columns) == ["Company Very Short", "Super Name"]
    assert len(df) == 5

    filtered = dt.to_pyarrow_table(filters=[("Company Very Short", "=", "BME")])
    assert filtered["Super Name"].to_pylist() == ["Timothy Lamb"]


@pytest.mark.pyarrow
def test_column_mapped_table_still_rejected_on_dataset_path():
    import pyarrow.dataset as ds

    dt = DeltaTable(COLUMN_MAPPED_TABLE)
    with pytest.raises(DeltaProtocolError, match="minimum reader version"):
        dt.to_pyarrow_table(filters=ds.field("Super Name") == "Timothy Lamb")


@pytest.mark.pyarrow
def test_null_matching_filters(tmp_path):
    import pyarrow as pa

    data = pa.table({"value": [1, 2, 3], "part": ["a", None, "b"]})
    write_deltalake(tmp_path, data, partition_by=["part"])
    dt = DeltaTable(tmp_path)

    nulls = dt.to_pyarrow_table(filters=[("part", "=", "")])
    assert nulls["value"].to_pylist() == [2]
    assert nulls["part"].to_pylist() == [None]

    not_nulls = dt.to_pyarrow_table(filters=[("part", "!=", "")])
    assert sorted(not_nulls["value"].to_pylist()) == [1, 3]


@pytest.mark.pyarrow
def test_without_files_raises():
    dt = DeltaTable(PARTITIONED_TABLE, without_files=True)
    with pytest.raises(DeltaError, match="Table is instantiated without files\\."):
        dt.to_pyarrow_table()
