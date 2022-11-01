from typing import NamedTuple, Dict, Any
from pathlib import Path
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from deltalake import DeltaTable

class ReadCase(NamedTuple):
    root: Path
    metadata: Dict[str, Any]

cases = []

project_root = Path("../dat")
for path in (project_root / "out" / "tables" / "generated").iterdir():
    if path.is_dir():
        with open(path / "table-metadata.json") as f:
            metadata = json.load(f)
        cases.append(ReadCase(path, metadata))

# TODO: external-tables should be added to cases as well

@pytest.mark.parametrize("case", cases)
def test_dat(case: ReadCase):
    root, metadata = case

    # Get Delta Table path
    delta_root = root / "delta"

    # Load table
    dt = DeltaTable(str(delta_root))

    # Compare protocol versions
    # TODO: this is incorrect in dat
    # assert dt.protocol().min_reader_version == metadata["reader_protocol_version"]
    assert dt.protocol().min_writer_version == metadata["writer_protocol_version"]

    # Perhaps?
    # assert dt.version == metadata["current_version"]

    # If supported protocol version, try to read, load parquet, and compare
    if dt.protocol().min_reader_version <= 1:
        parquet_root = root / "parquet"
        expected = pq.read_table(parquet_root)
        actual = dt.to_pyarrow_table()
        assert_tables_equal(expected, actual)
    else:
        # We should raise an error when attempting to read too advanced protocol
        with pytest.raises(Exception):
            dt.to_pyarrow_table()
    

def assert_tables_equal(first: pa.Table, second: pa.Table) -> None:
    assert first.schema == second.schema
    sort_keys = [(col, "ascending") for col in first.column_names]
    first_sorted = first.sort_by(sort_keys)
    second_sorted = second.sort_by(sort_keys)
    assert first_sorted == second_sorted
