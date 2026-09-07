"""Tests for DeltaTable.scan, the lazy engine-native read entrypoint."""

import pytest

from deltalake import DeltaTable
from deltalake.exceptions import DeltaError

PARTITIONED_TABLE = "../crates/test/tests/data/delta-0.8.0-partitioned"
COLUMN_MAPPED_TABLE = "../crates/test/tests/data/table_with_column_mapping"
DV_TABLE = "../crates/test/tests/data/table-with-dv-small"


@pytest.mark.pyarrow
def test_scan_matches_to_pyarrow_table():
    import pyarrow as pa

    dt = DeltaTable(PARTITIONED_TABLE)
    expected = dt.to_pyarrow_table().sort_by("value")
    # the engine emits view types; cast to the pyarrow schema before sorting
    scanned = pa.table(dt.scan().read_all()).cast(expected.schema).sort_by("value")
    assert scanned == expected


def test_scan_columns_projection():
    dt = DeltaTable(PARTITIONED_TABLE)
    result = dt.scan(columns=["value", "year"]).read_all()
    assert result.schema.names == ["value", "year"]
    assert sorted(result["value"].to_pylist()) == ["1", "2", "3", "4", "5", "6", "7"]


def test_scan_predicate_returns_exact_rows():
    # the year=2021/month=12/day=20 file holds values 6 and 7; a row filter
    # must return only the matching row, not the whole file
    dt = DeltaTable(PARTITIONED_TABLE)
    result = dt.scan(predicate="value = '6'").read_all()
    assert result.num_rows == 1
    assert result["value"].to_pylist() == ["6"]


def test_scan_predicate_on_partition_column():
    dt = DeltaTable(PARTITIONED_TABLE)
    result = dt.scan(columns=["value"], predicate="year = '2021'").read_all()
    assert sorted(result["value"].to_pylist()) == ["4", "5", "6", "7"]


def test_scan_predicate_can_reference_unprojected_column():
    dt = DeltaTable(PARTITIONED_TABLE)
    result = dt.scan(columns=["year"], predicate="value = '6'").read_all()
    assert result.schema.names == ["year"]
    assert result["year"].to_pylist() == ["2021"]


def test_scan_column_mapped_table():
    # Spark-written name-mode fixture: data lives under physical col-<uuid>
    # names, the scan must expose the logical ones
    dt = DeltaTable(COLUMN_MAPPED_TABLE)
    full = dt.scan().read_all()
    assert full.schema.names == ["Company Very Short", "Super Name"]

    filtered = dt.scan(
        columns=["Super Name"],
        predicate="\"Company Very Short\" = 'BME'",
    ).read_all()
    assert filtered["Super Name"].to_pylist() == ["Timothy Lamb"]


def test_scan_deletion_vector_table():
    dt = DeltaTable(DV_TABLE)
    result = dt.scan().read_all()
    assert result.num_rows == 8
    assert sorted(result["value"].to_pylist()) == [1, 2, 3, 4, 5, 6, 7, 8]


def test_scan_unknown_column_in_predicate_errors():
    dt = DeltaTable(PARTITIONED_TABLE)
    with pytest.raises(DeltaError, match="nonexistent"):
        dt.scan(predicate="nonexistent = 1")


def test_scan_unknown_column_in_projection_errors():
    dt = DeltaTable(PARTITIONED_TABLE)
    with pytest.raises(DeltaError, match="nonexistent"):
        dt.scan(columns=["nonexistent"])


def test_scan_reader_is_consumable_batch_by_batch():
    dt = DeltaTable(PARTITIONED_TABLE)
    reader = dt.scan()
    values = []
    for batch in reader:
        assert batch.num_rows > 0
        values.extend(batch["value"].to_pylist())
    assert sorted(values) == ["1", "2", "3", "4", "5", "6", "7"]
