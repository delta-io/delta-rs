import pathlib

import pyarrow as pa
import pytest

from deltalake import DeltaTable
from deltalake.exceptions import DeltaError


def test_create_roundtrip_metadata(tmp_path: pathlib.Path, sample_data: pa.Table):
    dt = DeltaTable.create(
        tmp_path,
        sample_data.schema,
        name="test_name",
        description="test_desc",
        configuration={"delta.appendOnly": "false", "foo": "bar"},
        custom_metadata={"userName": "John Doe"},
    )

    metadata = dt.metadata()

    assert metadata.name == "test_name"
    assert metadata.description == "test_desc"
    assert metadata.configuration == {"delta.appendOnly": "false", "foo": "bar"}
    assert dt.history()[0]["userName"] == "John Doe"


def test_create_modes(tmp_path: pathlib.Path, sample_data: pa.Table):
    dt = DeltaTable.create(tmp_path, sample_data.schema, mode="error")
    last_action = dt.history(1)[0]

    with pytest.raises(DeltaError):
        dt = DeltaTable.create(tmp_path, sample_data.schema, mode="error")

    assert last_action["operation"] == "CREATE TABLE"
    with pytest.raises(DeltaError):
        dt = DeltaTable.create(tmp_path, sample_data.schema, mode="append")

    dt = DeltaTable.create(tmp_path, sample_data.schema, mode="ignore")
    assert dt.version() == 0

    dt = DeltaTable.create(tmp_path, sample_data.schema, mode="overwrite")
    assert dt.version() == 1

    last_action = dt.history(1)[0]

    assert last_action["operation"] == "CREATE OR REPLACE TABLE"


def test_create_schema(tmp_path: pathlib.Path, sample_data: pa.Table):
    dt = DeltaTable.create(
        tmp_path,
        sample_data.schema,
    )

    assert dt.schema().to_pyarrow() == sample_data.schema
