import pathlib

import pyarrow as pa
import pyarrow.compute as pc

from deltalake.table import DeltaTable
from deltalake.writer import write_deltalake


def test_delete_no_predicates(existing_table: DeltaTable):
    old_version = existing_table.version()

    existing_table.delete(custom_metadata={"userName": "John Doe"})

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
