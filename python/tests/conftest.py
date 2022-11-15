import os
import pathlib
import subprocess
from datetime import date, datetime, timedelta
from decimal import Decimal

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake


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
            "../rust/tests/data/simple_table",
            "s3://deltars/simple",
            "--endpoint-url",
            endpoint_url,
        ],
    ]

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
    monkeypatch.setenv("AWS_STORAGE_ALLOW_HTTP", "TRUE")
    for key, value in s3_localstack_creds.items():
        monkeypatch.setenv(key, value)


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
