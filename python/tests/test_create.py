import pathlib
from typing import TYPE_CHECKING

import pytest
from arro3.core import Table

from deltalake import CommitProperties, DeltaTable, Field, Schema, write_deltalake
from deltalake.exceptions import DeltaError
from deltalake.schema import PrimitiveType, VariantType

if TYPE_CHECKING:
    pass

schema = Schema(
    fields=[
        Field("id", type=PrimitiveType("string"), nullable=True),
        Field("price", type=PrimitiveType("integer"), nullable=True),
        Field("timestamp", type=PrimitiveType("timestamp_ntz"), nullable=True),
        Field("deleted", type=PrimitiveType("boolean"), nullable=True),
    ]
)


def test_create_roundtrip_metadata(
    tmp_path: pathlib.Path,
):
    dt = DeltaTable.create(
        tmp_path,
        schema,
        name="test_name",
        description="test_desc",
        configuration={
            "delta.appendOnly": "true",
            "delta.logRetentionDuration": "interval 2 days",
        },
        commit_properties=CommitProperties(custom_metadata={"userName": "John Doe"}),
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


def test_create_modes(tmp_path: pathlib.Path):
    dt = DeltaTable.create(tmp_path, schema, mode="error")
    last_action = dt.history(1)[0]

    with pytest.raises(DeltaError):
        dt = DeltaTable.create(tmp_path, schema, mode="error")

    assert last_action["operation"] == "CREATE TABLE"
    with pytest.raises(DeltaError):
        dt = DeltaTable.create(tmp_path, schema, mode="append")

    dt = DeltaTable.create(tmp_path, schema, mode="ignore")
    assert dt.version() == 0

    dt = DeltaTable.create(tmp_path, schema, mode="overwrite")
    assert dt.version() == 1

    last_action = dt.history(1)[0]

    assert last_action["operation"] == "CREATE OR REPLACE TABLE"


def test_create_schema(tmp_path: pathlib.Path):
    dt = DeltaTable.create(
        tmp_path,
        schema,
    )

    assert dt.schema() == schema


def test_create_schema_with_variant_type(tmp_path: pathlib.Path):
    variant_schema = Schema(
        fields=[
            Field("id", type=PrimitiveType("string"), nullable=True),
            Field("payload", type=VariantType(), nullable=True),
        ]
    )

    dt = DeltaTable.create(tmp_path, variant_schema)
    protocol = dt.protocol()

    assert protocol.min_reader_version == 3
    assert protocol.min_writer_version == 7
    assert "variantType" in protocol.reader_features
    assert "variantType" in protocol.writer_features
    assert dt.schema() == variant_schema


def test_create_with_deletion_vectors_enabled(tmp_path: pathlib.Path):
    """append only is set to false so shouldn't be converted to a feature"""
    dt = DeltaTable.create(
        tmp_path,
        schema,
        name="test_name",
        description="test_desc",
        configuration={
            "delta.appendOnly": "false",
            "delta.enableDeletionVectors": "true",
        },
        commit_properties=CommitProperties(custom_metadata={"userName": "John Doe"}),
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
    assert "deletionVectors" in protocol.writer_features
    assert "deletionVectors" in protocol.reader_features
    assert dt.history()[0]["userName"] == "John Doe"


def test_create_higher_protocol_versions(tmp_path: pathlib.Path):
    schema = Schema(
        fields=[
            Field("id", type=PrimitiveType("string"), nullable=True),
            Field("price", type=PrimitiveType("integer"), nullable=True),
            Field("deleted", type=PrimitiveType("boolean"), nullable=True),
        ]
    )

    dt = DeltaTable.create(
        tmp_path,
        schema,
        name="test_name",
        description="test_desc",
        configuration={
            "delta.appendOnly": "false",
            "delta.minReaderVersion": "1",
            "delta.minWriterVersion": "5",
        },
        commit_properties=CommitProperties(custom_metadata={"userName": "John Doe"}),
    )

    metadata = dt.metadata()
    protocol = dt.protocol()
    assert metadata.name == "test_name"
    assert metadata.description == "test_desc"
    assert metadata.configuration == {
        "delta.appendOnly": "false",
        "delta.minReaderVersion": "1",
        "delta.minWriterVersion": "5",
    }
    assert protocol.min_reader_version == 1
    assert protocol.min_writer_version == 5
    assert dt.history()[0]["userName"] == "John Doe"


def test_create_or_replace_existing_table(tmp_path: pathlib.Path, sample_table: Table):
    write_deltalake(table_or_uri=tmp_path, data=sample_table)
    dt = DeltaTable.create(
        tmp_path,
        schema,
        partition_by=["id"],
        mode="overwrite",
    )

    assert dt.file_uris() == []


def test_delta_table_invalid_path_raises_error(tmp_path: pathlib.Path):
    nonexistent_path = tmp_path / "does_not_exist"
    assert not nonexistent_path.exists()

    with pytest.raises(DeltaError):
        DeltaTable(str(nonexistent_path))

    assert not nonexistent_path.exists()


def test_positional_commit_args_raise_deprecation_warning(tmp_path: pathlib.Path):
    commit = CommitProperties(custom_metadata={"userName": "John Doe"})
    with pytest.warns(DeprecationWarning, match="positionally"):
        DeltaTable.create(
            tmp_path, schema, "error", None, None, None, None, None, commit, None, False
        )


def test_create_partial_legacy_positional_args_preserve_defaults(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
):
    import deltalake.table as table_module

    captured: dict[str, tuple] = {}

    def fake_create(*args):
        captured["args"] = args

    def fake_init(self, *args, **kwargs):
        pass

    monkeypatch.setattr(table_module, "_create_deltalake", fake_create)
    monkeypatch.setattr(DeltaTable, "__init__", fake_init)

    commit = CommitProperties(custom_metadata={"userName": "John Doe"})
    with pytest.warns(DeprecationWarning, match="positionally"):
        DeltaTable.create(
            tmp_path, schema, "error", None, None, None, None, None, commit
        )

    create_args = captured["args"]
    assert create_args[4] is True
    assert create_args[9] is commit
    assert create_args[10] is None


def test_create_positional_and_keyword_commit_conflict_raises(
    tmp_path: pathlib.Path,
):
    commit = CommitProperties(custom_metadata={"userName": "John Doe"})
    with pytest.raises(TypeError, match="multiple values for 'commit_properties'"):
        DeltaTable.create(
            tmp_path,
            schema,
            "error",
            None,
            None,
            None,
            None,
            None,
            commit,
            commit_properties=commit,
        )


def test_create_positional_and_keyword_raise_if_key_not_exists_conflict_raises(
    tmp_path: pathlib.Path,
    monkeypatch,
):
    import deltalake.table as table_module

    def fake_create(*args):
        pass

    def fake_init(self, *args, **kwargs):
        pass

    monkeypatch.setattr(table_module, "_create_deltalake", fake_create)
    monkeypatch.setattr(DeltaTable, "__init__", fake_init)

    commit = CommitProperties(custom_metadata={"userName": "John Doe"})
    with pytest.raises(
        TypeError, match="multiple values for 'raise_if_key_not_exists'"
    ):
        DeltaTable.create(
            tmp_path,
            schema,
            "error",
            None,
            None,
            None,
            None,
            None,
            commit,
            None,
            False,
            raise_if_key_not_exists=True,
        )
