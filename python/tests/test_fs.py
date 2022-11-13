import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import S3FileSystem

from deltalake import DeltaTable, PyDeltaTableError
from deltalake.fs import DeltaStorageHandler
from deltalake.writer import write_deltalake


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
@pytest.mark.timeout(timeout=4, method="thread")
def test_s3_authenticated_read_write(s3_localstack_creds):
    # Create unauthenticated handler
    storage_handler = DeltaStorageHandler(
        "s3://deltars/",
        {
            "AWS_ENDPOINT_URL": s3_localstack_creds["AWS_ENDPOINT_URL"],
            # Grants anonymous access. If we don't do this, will timeout trying
            # to reading from EC2 instance provider.
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
        },
    )

    # Make a get request on an object
    with pytest.raises(Exception):
        storage_handler.open_input_stream("nonexistant")

    # Try to write an object
    with pytest.raises(Exception):
        storage_handler.open_output_stream("nonexistant")


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
def test_roundtrip_s3_env(s3_localstack, sample_data: pa.Table, monkeypatch):
    table_path = "s3://deltars/roundtrip"

    # Create new table with path
    with pytest.raises(PyDeltaTableError, match="Atomic rename requires a LockClient"):
        write_deltalake(table_path, sample_data)

    monkeypatch.setenv("AWS_S3_ALLOW_UNSAFE_RENAME", "true")

    # Create new table with path
    write_deltalake(table_path, sample_data)
    dt = DeltaTable(table_path)
    table = dt.to_pyarrow_table()
    assert table == sample_data
    assert dt.version() == 0

    # Write with existing DeltaTable
    write_deltalake(dt, sample_data, mode="overwrite")
    dt.update_incremental()
    assert dt.version() == 1

    table = dt.to_pyarrow_table()
    assert table == sample_data


@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=5, method="thread")
def test_roundtrip_s3_direct(s3_localstack_creds, sample_data: pa.Table):
    table_path = "s3://deltars/roundtrip2"

    # Fails without any credentials
    with pytest.raises(PyDeltaTableError):
        anon_storage_options = {
            "AWS_ENDPOINT_URL": s3_localstack_creds["AWS_ENDPOINT_URL"],
            # Grants anonymous access. If we don't do this, will timeout trying
            # to reading from EC2 instance provider.
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
        }
        write_deltalake(
            table_path,
            sample_data,
            storage_options=anon_storage_options,
        )

    # Can pass storage_options in directly
    storage_opts = {
        "AWS_STORAGE_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    storage_opts.update(s3_localstack_creds)
    write_deltalake(table_path, sample_data, storage_options=storage_opts)
    dt = DeltaTable(table_path, storage_options=storage_opts)
    assert dt.version() == 0
    table = dt.to_pyarrow_table()
    assert table == sample_data

    # Can pass storage_options into DeltaTable and then write
    write_deltalake(dt, sample_data, mode="overwrite")
    dt.update_incremental()
    assert dt.version() == 1
    table = dt.to_pyarrow_table()
    assert table == sample_data

    # TODO: Refactor so DeltaTable can be instantiated with a storage backend
    # Can provide S3Filesystem from pyarrow
    # pa_s3fs = S3FileSystem(
    #     access_key=s3_localstack_creds["AWS_ACCESS_KEY_ID"],
    #     secret_key=s3_localstack_creds["AWS_SECRET_ACCESS_KEY"],
    #     endpoint_override=s3_localstack_creds["AWS_ENDPOINT_URL"],
    #     scheme="http",
    # )

    # write_deltalake(table_path, sample_data, filesystem=pa_s3fs, mode="overwrite")
    # assert dt.version() == 2
    # table = dt.to_pyarrow_table()
    # assert table == sample_data


@pytest.mark.azure
@pytest.mark.integration
@pytest.mark.timeout(timeout=5, method="thread")
def test_roundtrip_azure_env(azurite_env_vars, sample_data: pa.Table):
    table_path = "az://deltars/roundtrip"

    # Create new table with path
    write_deltalake(table_path, sample_data)
    dt = DeltaTable(table_path)
    table = dt.to_pyarrow_table()
    assert table == sample_data
    assert dt.version() == 0

    # Write with existing DeltaTable
    write_deltalake(dt, sample_data, mode="overwrite")
    dt.update_incremental()
    assert dt.version() == 1

    table = dt.to_pyarrow_table()
    assert table == sample_data


@pytest.mark.azure
@pytest.mark.integration
@pytest.mark.timeout(timeout=5, method="thread")
def test_roundtrip_azure_direct(azurite_creds, sample_data: pa.Table):
    table_path = "az://deltars/roundtrip2"

    # Fails without any creds
    with pytest.raises(PyDeltaTableError):
        anon_storage_options = {
            key: value for key, value in azurite_creds.items() if "ACCOUNT" not in key
        }
        write_deltalake(table_path, sample_data, storage_options=anon_storage_options)

    # Can pass storage_options in directly
    write_deltalake(table_path, sample_data, storage_options=azurite_creds)
    dt = DeltaTable(table_path, storage_options=azurite_creds)
    table = dt.to_pyarrow_table()
    assert table == sample_data
    assert dt.version() == 0

    # Can pass storage_options into DeltaTable and then write
    write_deltalake(dt, sample_data, mode="overwrite")
    dt.update_incremental()
    assert dt.version() == 1

    table = dt.to_pyarrow_table()
    assert table == sample_data


@pytest.mark.azure
@pytest.mark.integration
@pytest.mark.timeout(timeout=5, method="thread")
def test_roundtrip_azure_sas(azurite_sas_creds, sample_data: pa.Table):
    table_path = "az://deltars/roundtrip3"

    write_deltalake(table_path, sample_data, storage_options=azurite_sas_creds)
    dt = DeltaTable(table_path, storage_options=azurite_sas_creds)
    table = dt.to_pyarrow_table()
    assert table == sample_data
    assert dt.version() == 0
