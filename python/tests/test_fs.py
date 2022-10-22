import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import S3FileSystem

from deltalake import DeltaTable
from deltalake.fs import DeltaStorageHandler
from deltalake.writer import write_deltalake
from tests.conftest import s3_localstack


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
        rel_path = file[len(table_path) :]
        with handler.open_input_file(rel_path) as f_:
            table = pq.read_table(f_)
            assert isinstance(table, pa.Table)
            assert table.shape > (0, 0)


@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=5, method="thread")
def test_read_simple_table_from_remote(s3_localstack):
    table_path = "s3://deltars/simple"
    dt = DeltaTable(table_path)
    assert dt.to_pyarrow_table().equals(pa.table({"id": [5, 7, 9]}))


@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=5, method="thread")
def test_roundtrip_s3_env(s3_localstack, sample_data: pa.Table):
    table_path = "s3://deltars/roundtrip"

    # Create new table with path
    write_deltalake(table_path, sample_data)
    dt = DeltaTable(table_path)
    table = dt.to_pyarrow_table()
    assert table == sample_data

    # Write with existing DeltaTable
    write_deltalake(dt, sample_data, mode="overwrite")
    dt.update_incremental()
    assert dt.version == 2

    table = dt.to_pyarrow_table()
    assert table == sample_data


@pytest.mark.skip()
@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=5, method="thread")
def test_roundtrip_s3_direct(s3_localstack_creds, sample_data: pa.Table):
    table_path = "s3://deltars/roundtrip2"

    # Fails without any credentials
    with pytest.raises(Exception):
        # TODO: what is the default timeout?
        write_deltalake(
            table_path,
            sample_data,
            storage_options={"AWS_ENDPOINT_URL": s3_localstack_creds["endpoint_url"]},
        )

    # Can pass storage_options in directly
    storage_opts = {
        "AWS_ACCESS_KEY_ID": s3_localstack_creds["access_key"],
        "AWS_SECRET_ACCESS_KEY": s3_localstack_creds["secrey_key"],
        "AWS_ENDPOINT_URL": s3_localstack_creds["endpoint_url"],
        "AWS_STORAGE_ALLOW_HTTP": "true",
    }
    write_deltalake(table_path, sample_data, storage_options=storage_opts)
    dt = DeltaTable(table_path, storage_options=storage_opts)
    table = dt.to_pyarrow_table()
    assert table == sample_data

    # Can pass storage_options into DeltaTable and then write
    write_deltalake(dt, sample_data, mode="overwrite")
    dt.update_incremental()
    assert dt.version == 2
    table = dt.to_pyarrow_table()
    assert table == sample_data

    # Can provide S3Filesystem from pyarrow
    pa_s3fs = S3FileSystem(
        access_key=s3_localstack_creds["access_key"],
        secret_key=s3_localstack_creds["secrey_key"],
        endpoint_override=s3_localstack_creds["endpoint_url"],
        scheme="http",
    )

    write_deltalake(table_path, sample_data, filesystem=pa_s3fs, mode="overwrite")
