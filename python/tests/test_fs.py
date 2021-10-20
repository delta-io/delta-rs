import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from deltalake import DeltaTable
from deltalake.deltalake import DeltaStorageFsBackend
from deltalake.fs import DeltaStorageHandler


def test_normalize_path():
    backend = DeltaStorageFsBackend("")
    assert backend.normalize_path("s3://foo/bar") == "s3://foo/bar"
    assert backend.normalize_path("s3://foo/bar/") == "s3://foo/bar"
    assert backend.normalize_path("/foo/bar//") == "/foo/bar"


@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=5, method="thread")
def test_read_files(s3_localstack):
    table_path = "s3://deltars/simple"
    handler = DeltaStorageHandler(table_path)
    dt = DeltaTable(table_path)
    files = dt.file_uris()
    assert len(files) > 0
    for file in files:
        with handler.open_input_file(file) as f_:
            table = pq.read_table(f_)
            assert isinstance(table, pa.Table)
            assert table.shape > (0, 0)


@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=5, method="thread")
def test_read_simple_table_from_remote(s3_localstack):
    table_path = "s3://deltars/simple"
    dt = DeltaTable(table_path)
    assert dt.to_pandas().equals(pd.DataFrame({"id": [5, 7, 9]}))
