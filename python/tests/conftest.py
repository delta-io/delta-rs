import os
import pathlib
import subprocess
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from time import sleep

import pyarrow as pa
import pytest
from azure.storage import blob

from deltalake import DeltaTable, WriterProperties, write_deltalake


def wait_till_host_is_available(host: str, timeout_sec: int = 0.5):
    spacing = 2
    start = time.monotonic()
    while True:
        if time.monotonic() - start > timeout_sec:
            raise TimeoutError(f"Host {host} is not available after {timeout_sec} sec")

        try:
            subprocess.run(["curl", host], timeout=timeout_sec * 1000, check=True)
        except Exception:
            pass
        else:
            break

        sleep(spacing)


@pytest.fixture(scope="session")
def s3_localstack_creds():
    endpoint_url = "http://localhost:4566"

    config = dict(
        AWS_REGION="us-east-1",
        AWS_ACCESS_KEY_ID="deltalake",
        AWS_SECRET_ACCESS_KEY="weloverust",
        AWS_ENDPOINT_URL=endpoint_url,
    )

    env = os.environ.copy()
    env.update(config)

    setup_commands = [
        [
            "aws",
            "s3api",
            "create-bucket",
            "--bucket",
            "deltars",
            "--endpoint-url",
            endpoint_url,
        ],
        [
            "aws",
            "s3",
            "sync",
            "--quiet",
            "../crates/test/tests/data/simple_table",
            "s3://deltars/simple",
            "--endpoint-url",
            endpoint_url,
        ],
    ]

    wait_till_host_is_available(endpoint_url)

    try:
        for args in setup_commands:
            subprocess.run(args, env=env)
    except OSError:
        pytest.skip("aws cli not installed")

    yield config

    shutdown_commands = [
        [
            "aws",
            "s3",
            "rm",
            "--quiet",
            "--recursive",
            "s3://deltars",
            "--endpoint-url",
            endpoint_url,
        ],
        [
            "aws",
            "s3api",
            "delete-bucket",
            "--bucket",
            "deltars",
            "--endpoint-url",
            endpoint_url,
        ],
    ]

    for args in shutdown_commands:
        subprocess.run(args, env=env)


@pytest.fixture()
def s3_localstack(monkeypatch, s3_localstack_creds):
    monkeypatch.setenv("AWS_ALLOW_HTTP", "TRUE")
    for key, value in s3_localstack_creds.items():
        monkeypatch.setenv(key, value)


@pytest.fixture(scope="session")
def azurite_creds():
    # These are the well-known values
    # https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio#well-known-storage-account-and-key
    account_name = "devstoreaccount1"
    config = dict(
        AZURE_STORAGE_ACCOUNT_NAME=account_name,
        AZURE_STORAGE_ACCOUNT_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        AZURE_STORAGE_CONTAINER_NAME="deltars",
        AZURE_STORAGE_USE_EMULATOR="true",
        AZURE_STORAGE_USE_HTTP="true",
        AZURE_STORAGE_ENDPOINT=f"http://localhost:10000/{account_name}",
    )

    env = os.environ.copy()
    env.update(config)
    conn_str = (
        "DefaultEndpointsProtocol=http;"
        f"AccountName={config['AZURE_STORAGE_ACCOUNT_NAME']};"
        f"AccountKey={config['AZURE_STORAGE_ACCOUNT_KEY']};"
        f"BlobEndpoint={config['AZURE_STORAGE_ENDPOINT']};"
    )
    env["AZURE_STORAGE_CONNECTION_STRING"] = conn_str
    wait_till_host_is_available(config["AZURE_STORAGE_ENDPOINT"])
    try:
        blob_client = blob.BlobServiceClient.from_connection_string(conn_str=conn_str)
        container = blob_client.create_container(
            name=config["AZURE_STORAGE_CONTAINER_NAME"]
        )
        yield config
    finally:
        container.delete_container()


@pytest.fixture()
def azurite_env_vars(monkeypatch, azurite_creds):
    for key, value in azurite_creds.items():
        monkeypatch.setenv(key, value)


@pytest.fixture(scope="session")
def azurite_sas_creds(azurite_creds):
    env = os.environ.copy()
    env.update(azurite_creds)
    env["AZURE_STORAGE_CONNECTION_STRING"] = (
        "DefaultEndpointsProtocol=http;"
        f"AccountName={azurite_creds['AZURE_STORAGE_ACCOUNT_NAME']};"
        f"AccountKey={azurite_creds['AZURE_STORAGE_ACCOUNT_KEY']};"
        f"BlobEndpoint={azurite_creds['AZURE_STORAGE_ENDPOINT']};"
    )
    sas_token = blob.generate_container_sas(
        account_name=azurite_creds["AZURE_STORAGE_ACCOUNT_NAME"],
        container_name=azurite_creds["AZURE_STORAGE_CONTAINER_NAME"],
        account_key=azurite_creds["AZURE_STORAGE_ACCOUNT_KEY"],
        permission=blob.ContainerSasPermissions(
            read=True,
            write=True,
            list=True,
            delete=True,
        ),
        expiry=datetime.now(tz=timezone.utc) + timedelta(hours=1),
        start=datetime.now(tz=timezone.utc),
        protocol="http",
    )
    creds = {key: value for key, value in azurite_creds.items() if "KEY" not in key}
    creds["SAS_TOKEN"] = sas_token

    return creds


@pytest.fixture()
def sample_data():
    nrows = 5
    return pa.table(
        {
            "utf8": pa.array([str(x) for x in range(nrows)]),
            "int64": pa.array(list(range(nrows)), pa.int64()),
            "int32": pa.array(list(range(nrows)), pa.int32()),
            "int16": pa.array(list(range(nrows)), pa.int16()),
            "int8": pa.array(list(range(nrows)), pa.int8()),
            "float32": pa.array([float(x) for x in range(nrows)], pa.float32()),
            "float64": pa.array([float(x) for x in range(nrows)], pa.float64()),
            "bool": pa.array([x % 2 == 0 for x in range(nrows)]),
            "binary": pa.array([str(x).encode() for x in range(nrows)]),
            "decimal": pa.array([Decimal("10.000") + x for x in range(nrows)]),
            "date32": pa.array(
                [date(2022, 1, 1) + timedelta(days=x) for x in range(nrows)]
            ),
            "timestamp": pa.array(
                [datetime(2022, 1, 1) + timedelta(hours=x) for x in range(nrows)]
            ),
            "struct": pa.array([{"x": x, "y": str(x)} for x in range(nrows)]),
            "list": pa.array([list(range(x + 1)) for x in range(nrows)]),
            # NOTE: https://github.com/apache/arrow-rs/issues/477
            #'map': pa.array([[(str(y), y) for y in range(x)] for x in range(nrows)], pa.map_(pa.string(), pa.int64())),
        }
    )


@pytest.fixture()
def existing_table(tmp_path: pathlib.Path, sample_data: pa.Table):
    path = str(tmp_path)
    write_deltalake(path, sample_data)
    return DeltaTable(path)


@pytest.fixture()
def sample_table():
    nrows = 5
    return pa.table(
        {
            "id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold": pa.array(list(range(nrows)), pa.int32()),
            "deleted": pa.array([False] * nrows),
        }
    )


@pytest.fixture()
def existing_sample_table(tmp_path: pathlib.Path, sample_table: pa.Table):
    path = str(tmp_path)
    write_deltalake(path, sample_table)
    return DeltaTable(path)


@pytest.fixture()
def sample_table_with_spaces_numbers():
    nrows = 5
    return pa.table(
        {
            "1id": pa.array(["1", "2", "3", "4", "5"]),
            "price": pa.array(list(range(nrows)), pa.int64()),
            "sold items": pa.array(list(range(nrows)), pa.int32()),
            "deleted": pa.array([False] * nrows),
        }
    )


@pytest.fixture()
def writer_properties():
    return WriterProperties(compression="GZIP", compression_level=0)
