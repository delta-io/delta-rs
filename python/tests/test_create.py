import pathlib

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError


def test_create_roundtrip_metadata(tmp_path: pathlib.Path, sample_data: pa.Table):
    dt = DeltaTable.create(
        tmp_path,
        sample_data.schema,
        name="test_name",
        description="test_desc",
        configuration={
            "delta.appendOnly": "true",
            "delta.logRetentionDuration": "interval 2 days",
        },
        custom_metadata={"userName": "John Doe"},
    )

    metadata = dt.metadata()

    assert metadata.name == "test_name"
    assert metadata.description == "test_desc"
    assert metadata.configuration == {
        "delta.appendOnly": "true",
        "delta.logRetentionDuration": "interval 2 days",
    }
    assert dt.history()[0]["userName"] == "John Doe"

    assert {*dt.protocol().writer_features} == {"appendOnly", "timestampNtz"}  # type: ignore


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


def test_create_with_deletion_vectors_enabled(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    """append only is set to false so shouldn't be converted to a feature"""
    dt = DeltaTable.create(
        tmp_path,
        sample_table.schema,
        name="test_name",
        description="test_desc",
        configuration={
            "delta.appendOnly": "false",
            "delta.enableDeletionVectors": "true",
        },
        custom_metadata={"userName": "John Doe"},
    )

    metadata = dt.metadata()
    protocol = dt.protocol()
    assert metadata.name == "test_name"
    assert metadata.description == "test_desc"
    assert metadata.configuration == {
        "delta.appendOnly": "false",
        "delta.enableDeletionVectors": "true",
    }
    assert protocol.min_reader_version == 3
    assert protocol.min_writer_version == 7
    assert protocol.writer_features == ["deletionVectors"]  # type: ignore
    assert protocol.reader_features == ["deletionVectors"]
    assert dt.history()[0]["userName"] == "John Doe"


def test_create_higher_protocol_versions(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    dt = DeltaTable.create(
        tmp_path,
        sample_table.schema,
        name="test_name",
        description="test_desc",
        configuration={
            "delta.appendOnly": "false",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
        },
        custom_metadata={"userName": "John Doe"},
    )

    metadata = dt.metadata()
    protocol = dt.protocol()
    assert metadata.name == "test_name"
    assert metadata.description == "test_desc"
    assert metadata.configuration == {
        "delta.appendOnly": "false",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    }
    assert protocol.min_reader_version == 2
    assert protocol.min_writer_version == 5
    assert dt.history()[0]["userName"] == "John Doe"


def test_create_or_replace_existing_table(
    tmp_path: pathlib.Path, sample_data: pa.Table
):
    write_deltalake(table_or_uri=tmp_path, data=sample_data)
    dt = DeltaTable.create(
        tmp_path, sample_data.schema, partition_by=["utf8"], mode="overwrite"
    )

    assert dt.files() == []
