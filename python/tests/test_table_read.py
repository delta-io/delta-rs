import os

import pytest

from deltalake import DeltaTable


def test_read_simple_table_to_dict():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"id": [5, 7, 9]}


@pytest.mark.timeout(timeout=5, method="thread")
def test_create_multiple_tables_from_s3(s3cred):
    """
    Should be able to create multiple cloud storage based DeltaTable instances
    without blocking on async rust function calls.
    """
    for path in ["s3://deltars/simple", "s3://deltars/simple"]:
        t = DeltaTable(path)
        assert t.files() == [
            "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
            "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
            "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
            "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
            "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
        ]
