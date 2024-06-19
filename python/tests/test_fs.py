import pickle
import urllib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import FileType

from deltalake import DeltaTable
from deltalake.exceptions import DeltaProtocolError
from deltalake.fs import DeltaStorageHandler
from deltalake.writer import write_deltalake


@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=15, method="thread")
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
@pytest.mark.timeout(timeout=15, method="thread")
def test_read_file_info(s3_localstack):
    table_path = "s3://deltars/simple"
    handler = DeltaStorageHandler(table_path)
    meta = handler.get_file_info(
        ["part-00000-a72b1fb3-f2df-41fe-a8f0-e65b746382dd-c000.snappy.parquet"]
    )
    assert len(meta) == 1
    assert meta[0].type == FileType.File


@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=15, method="thread")
def test_s3_authenticated_read_write(s3_localstack_creds, monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    # Create unauthenticated handler
    storage_handler = DeltaStorageHandler(
        "s3://deltars/",
        options={
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
@pytest.mark.timeout(timeout=15, method="thread")
def test_read_simple_table_from_remote(s3_localstack):
    table_path = "s3://deltars/simple"
    dt = DeltaTable(table_path)
    assert dt.to_pyarrow_table().equals(pa.table({"id": [5, 7, 9]}))

    expected_files = [
        "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
        "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
        "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
        "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
        "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
    ]

    assert dt.files() == expected_files
    assert dt.file_uris() == [table_path + "/" + path for path in expected_files]


@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=15, method="thread")
@pytest.mark.skip(
    reason="Temporarily disabled until we can resolve https://github.com/delta-io/delta-rs/pull/2120#issuecomment-1912367573"
)
def test_roundtrip_s3_env(s3_localstack, sample_data: pa.Table, monkeypatch):
    table_path = "s3://deltars/roundtrip"

    # Create new table with path
    with pytest.raises(DeltaProtocolError, match="Atomic rename requires a LockClient"):
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
@pytest.mark.timeout(timeout=15, method="thread")
def test_roundtrip_s3_direct(s3_localstack_creds, sample_data: pa.Table):
    table_path = "s3://deltars/roundtrip2"

    # Fails without any credentials
    with pytest.raises(IOError):
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
        "AWS_ALLOW_HTTP": "true",
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


@pytest.mark.azure
@pytest.mark.integration
@pytest.mark.timeout(timeout=60, method="thread")
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
@pytest.mark.timeout(timeout=60, method="thread")
def test_roundtrip_azure_direct(azurite_creds, sample_data: pa.Table):
    table_path = "az://deltars/roundtrip2"

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
@pytest.mark.timeout(timeout=60, method="thread")
def test_roundtrip_azure_sas(azurite_sas_creds, sample_data: pa.Table):
    table_path = "az://deltars/roundtrip3"
    write_deltalake(table_path, sample_data, storage_options=azurite_sas_creds)
    dt = DeltaTable(table_path, storage_options=azurite_sas_creds)
    table = dt.to_pyarrow_table()
    assert table == sample_data
    assert dt.version() == 0


@pytest.mark.azure
@pytest.mark.integration
@pytest.mark.timeout(timeout=60, method="thread")
def test_roundtrip_azure_decoded_sas(azurite_sas_creds, sample_data: pa.Table):
    table_path = "az://deltars/roundtrip4"
    azurite_sas_creds["SAS_TOKEN"] = urllib.parse.unquote(
        azurite_sas_creds["SAS_TOKEN"]
    )

    write_deltalake(table_path, sample_data, storage_options=azurite_sas_creds)
    dt = DeltaTable(table_path, storage_options=azurite_sas_creds)
    table = dt.to_pyarrow_table()
    assert table == sample_data
    assert dt.version() == 0


@pytest.mark.parametrize("storage_size", [1, 4 * 1024 * 1024, 5 * 1024 * 1024 - 1])
def test_warning_for_small_max_buffer_size(tmp_path, storage_size):
    storage_opts = {"max_buffer_size": str(storage_size)}
    store = DeltaStorageHandler(str(tmp_path.absolute()), options=storage_opts)
    with pytest.warns(UserWarning) as warnings:
        store.open_output_stream("test")

    assert len(warnings) == 1
    assert (
        f"You specified a `max_buffer_size` of {storage_size} bits less than {5*1024*1024} bits"
        in str(warnings[0].message)
    )


def test_pickle_roundtrip(tmp_path):
    store = DeltaStorageHandler(str(tmp_path.absolute()))

    with (tmp_path / "asd.pkl").open("wb") as handle:
        pickle.dump(store, handle)

    with (tmp_path / "asd.pkl").open("rb") as handle:
        store_pkl = pickle.load(handle)

    infos = store_pkl.get_file_info(["asd.pkl"])
    assert infos[0].size > 0
