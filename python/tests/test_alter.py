import pathlib
from typing import List

import pyarrow as pa
import pytest

from deltalake import DeltaTable, TableFeatures, write_deltalake
from deltalake.exceptions import DeltaError, DeltaProtocolError
from deltalake.schema import Field, PrimitiveType, StructType
from deltalake.table import CommitProperties


def test_add_constraint(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table)

    dt = DeltaTable(tmp_path)

    dt.alter.add_constraint({"check_price": "price >= 0"})

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "ADD CONSTRAINT"
    assert dt.version() == 1
    assert dt.metadata().configuration == {
        "delta.constraints.check_price": "price >= 0"
    }
    assert dt.protocol().min_writer_version == 3

    with pytest.raises(DeltaError):
        # Invalid constraint
        dt.alter.add_constraint({"check_price": "price < 0"})

    with pytest.raises(DeltaProtocolError):
        data = pa.table(
            {
                "id": pa.array(["1"]),
                "price": pa.array([-1], pa.int64()),
                "sold": pa.array(list(range(1)), pa.int32()),
                "deleted": pa.array([False] * 1),
            }
        )
        write_deltalake(tmp_path, data, engine="rust", mode="append")


def test_add_multiple_constraints(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table)

    dt = DeltaTable(tmp_path)

    with pytest.raises(ValueError):
        dt.alter.add_constraint(
            {"check_price": "price >= 0", "check_price2": "price >= 0"}
        )


def test_add_constraint_roundtrip_metadata(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")

    dt = DeltaTable(tmp_path)

    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.alter.add_constraint(
        {"check_price2": "price >= 0"}, commit_properties=commit_properties
    )

    assert dt.history(1)[0]["userName"] == "John Doe"


def test_drop_constraint(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table)

    dt = DeltaTable(tmp_path)

    dt.alter.add_constraint({"check_price": "price >= 0"})
    assert dt.protocol().min_writer_version == 3
    dt.alter.drop_constraint(name="check_price")
    last_action = dt.history(1)[0]
    assert last_action["operation"] == "DROP CONSTRAINT"
    assert dt.version() == 2
    assert dt.metadata().configuration == {}
    assert dt.protocol().min_writer_version == 3


def test_drop_constraint_invalid(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table)

    dt = DeltaTable(tmp_path)

    dt.alter.add_constraint({"check_price": "price >= 0"})
    with pytest.raises(DeltaError):
        dt.alter.drop_constraint(name="invalid_constraint_name")

    assert dt.metadata().configuration == {
        "delta.constraints.check_price": "price >= 0"
    }
    assert dt.protocol().min_writer_version == 3


def test_drop_constraint_invalid_ignore(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table)

    dt = DeltaTable(tmp_path)

    dt.alter.add_constraint({"check_price": "price >= 0"})
    dt.alter.drop_constraint(name="invalid_constraint_name", raise_if_not_exists=False)


def test_drop_constraint_roundtrip_metadata(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")

    dt = DeltaTable(tmp_path)

    dt.alter.add_constraint({"check_price2": "price >= 0"})
    commit_properties = CommitProperties(custom_metadata={"userName": "John Doe"})
    dt.alter.drop_constraint("check_price2", commit_properties=commit_properties)

    assert dt.history(1)[0]["userName"] == "John Doe"


@pytest.mark.parametrize("min_writer_version", ["2", "3", "4", "5", "6", "7"])
def test_set_table_properties_min_writer_version(
    tmp_path: pathlib.Path,
    sample_table: pa.Table,
    min_writer_version: str,
):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")
    dt = DeltaTable(tmp_path)

    configuration = {"delta.minWriterVersion": min_writer_version}
    dt.alter.set_table_properties(configuration)

    protocol = dt.protocol()

    assert dt.metadata().configuration == configuration
    assert protocol.min_reader_version == 1
    assert protocol.min_writer_version == int(min_writer_version)


def test_set_table_properties_invalid_min_writer_version(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")
    dt = DeltaTable(tmp_path)
    with pytest.raises(DeltaError):
        dt.alter.set_table_properties({"delta.minWriterVersion": "8"})

    protocol = dt.protocol()
    assert dt.metadata().configuration == {}
    assert protocol.min_reader_version == 1
    assert protocol.min_writer_version == 2


@pytest.mark.parametrize("min_reader_version", ["1", "2", "3"])
def test_set_table_properties_min_reader_version(
    tmp_path: pathlib.Path,
    sample_table: pa.Table,
    min_reader_version: str,
):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")
    dt = DeltaTable(tmp_path)
    configuration = {"delta.minReaderVersion": min_reader_version}
    dt.alter.set_table_properties(configuration)

    protocol = dt.protocol()
    assert dt.metadata().configuration == configuration
    assert protocol.min_reader_version == int(min_reader_version)
    assert protocol.min_writer_version == 2


def test_set_table_properties_invalid_min_reader_version(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")
    dt = DeltaTable(tmp_path)
    with pytest.raises(DeltaError):
        dt.alter.set_table_properties({"delta.minReaderVersion": "8"})

    protocol = dt.protocol()
    assert dt.metadata().configuration == {}
    assert protocol.min_reader_version == 1
    assert protocol.min_writer_version == 2


def test_set_table_properties_enable_cdf(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")
    dt = DeltaTable(tmp_path)
    dt.alter.set_table_properties({"delta.enableChangeDataFeed": "true"})

    protocol = dt.protocol()
    assert dt.metadata().configuration == {"delta.enableChangeDataFeed": "true"}
    assert protocol.min_reader_version == 1
    assert protocol.min_writer_version == 4


def test_set_table_properties_enable_cdf_invalid(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")
    dt = DeltaTable(tmp_path)
    with pytest.raises(DeltaError):
        dt.alter.set_table_properties({"delta.enableChangeDataFeed": "wrong"})

    protocol = dt.protocol()
    assert dt.metadata().configuration == {}
    assert protocol.min_reader_version == 1
    assert protocol.min_writer_version == 2


def test_set_table_properties_enable_cdf_value_false(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")
    dt = DeltaTable(tmp_path)
    dt.alter.set_table_properties({"delta.enableChangeDataFeed": "false"})

    protocol = dt.protocol()
    assert dt.metadata().configuration == {"delta.enableChangeDataFeed": "false"}
    assert protocol.min_reader_version == 1
    assert protocol.min_writer_version == 2


def test_set_table_properties_enable_cdf_with_writer_version_bumped(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")
    dt = DeltaTable(tmp_path)
    dt.alter.set_table_properties(
        {"delta.enableChangeDataFeed": "true", "delta.minWriterVersion": "7"}
    )

    protocol = dt.protocol()
    assert dt.metadata().configuration == {
        "delta.enableChangeDataFeed": "true",
        "delta.minWriterVersion": "7",
    }
    assert protocol.min_reader_version == 1
    assert protocol.min_writer_version == 7
    assert protocol.writer_features == ["changeDataFeed"]


def test_set_table_properties_enable_cdf_and_deletion_vectors(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")
    dt = DeltaTable(tmp_path)
    dt.alter.set_table_properties(
        {"delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "true"}
    )

    protocol = dt.protocol()
    assert dt.metadata().configuration == {
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
    }
    assert protocol.min_reader_version == 3
    assert protocol.min_writer_version == 7
    assert list(sorted(protocol.writer_features)) == [  # type: ignore
        "changeDataFeed",
        "deletionVectors",
    ]
    assert protocol.reader_features == ["deletionVectors"]


def test_convert_checkConstraints_to_feature_after_version_upgrade(
    tmp_path: pathlib.Path, sample_table: pa.Table
):
    write_deltalake(tmp_path, sample_table)

    dt = DeltaTable(tmp_path)

    dt.alter.add_constraint({"check_price": "price >= 0"})

    last_action = dt.history(1)[0]
    assert last_action["operation"] == "ADD CONSTRAINT"
    assert dt.version() == 1
    assert dt.metadata().configuration == {
        "delta.constraints.check_price": "price >= 0"
    }
    assert dt.protocol().min_writer_version == 3

    dt.alter.set_table_properties({"delta.enableDeletionVectors": "true"})

    protocol = dt.protocol()
    assert dt.metadata().configuration == {
        "delta.constraints.check_price": "price >= 0",
        "delta.enableDeletionVectors": "true",
    }
    assert protocol.min_reader_version == 3
    assert protocol.min_writer_version == 7
    assert list(sorted(protocol.writer_features)) == [  # type: ignore
        "checkConstraints",
        "deletionVectors",
    ]
    assert protocol.reader_features == ["deletionVectors"]


def test_set_table_properties_enable_dv(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")
    dt = DeltaTable(tmp_path)
    dt.alter.set_table_properties({"delta.enableDeletionVectors": "true"})

    protocol = dt.protocol()
    assert dt.metadata().configuration == {"delta.enableDeletionVectors": "true"}
    assert protocol.min_reader_version == 3
    assert protocol.min_writer_version == 7
    assert protocol.writer_features == ["deletionVectors"]
    assert protocol.reader_features == ["deletionVectors"]


def _sort_fields(fields: List[Field]) -> List[Field]:
    return list(sorted(iter(fields), key=lambda x: (x.name, str(x.type))))


def test_add_column_primitive(existing_table: DeltaTable):
    current_fields = existing_table.schema().fields

    new_fields_to_add = [
        Field("foo", PrimitiveType("integer")),
        Field("bar", PrimitiveType("float")),
    ]

    existing_table.alter.add_columns(new_fields_to_add)
    new_fields = existing_table.schema().fields

    assert _sort_fields(new_fields) == _sort_fields(
        [*current_fields, *new_fields_to_add]
    )


def test_add_field_in_struct_column(existing_table: DeltaTable):
    current_fields = existing_table.schema().fields

    new_fields_to_add = [
        Field("struct", StructType([Field("z", PrimitiveType("float"))])),
    ]

    existing_table.alter.add_columns(new_fields_to_add)
    new_fields = existing_table.schema().fields

    new_field = Field(
        "struct",
        StructType(
            [
                Field("x", PrimitiveType("long")),
                Field("y", PrimitiveType("string")),
                Field("z", PrimitiveType("float")),
            ]
        ),
    )
    assert _sort_fields(new_fields) == _sort_fields(
        [*[field for field in current_fields if field.name != "struct"], new_field]
    )


def test_add_timestamp_ntz_column(tmp_path: pathlib.Path, sample_table: pa.Table):
    write_deltalake(tmp_path, sample_table, mode="append", engine="rust")
    dt = DeltaTable(tmp_path)
    current_fields = dt.schema().fields

    new_fields_to_add = Field("timestamp_ntz_col", PrimitiveType("timestamp_ntz"))

    dt.alter.add_columns(new_fields_to_add)
    new_fields = dt.schema().fields
    new_protocol = dt.protocol()

    assert _sort_fields(new_fields) == _sort_fields(
        [*current_fields, new_fields_to_add]
    )
    assert new_protocol.min_reader_version == 3
    assert new_protocol.min_writer_version == 7
    assert new_protocol.reader_features == ["timestampNtz"]
    assert new_protocol.writer_features == ["timestampNtz"]


features = [
    TableFeatures.ChangeDataFeed,
    TableFeatures.DeletionVectors,
    TableFeatures.ColumnMapping,
    TableFeatures.TimestampWithoutTimezone,
    TableFeatures.V2Checkpoint,
    TableFeatures.AppendOnly,
    TableFeatures.AppendOnly,
    TableFeatures.Invariants,
    TableFeatures.CheckConstraints,
    TableFeatures.GeneratedColumns,
    TableFeatures.IdentityColumns,
    TableFeatures.RowTracking,
    TableFeatures.DomainMetadata,
    TableFeatures.IcebergCompatV1,
]

all_features = []
all_features.extend(features)
all_features.append(features)


@pytest.mark.parametrize("feature", all_features)
def test_add_feature_variations(existing_table: DeltaTable, feature):
    """Existing table already has timestampNtz so it's already at v3,7"""
    existing_table.alter.add_feature(
        feature=feature,
        allow_protocol_versions_increase=False,
    )
    last_action = existing_table.history(1)[0]
    assert last_action["operation"] == "ADD FEATURE"
    assert existing_table.version() == 1


def test_add_features_disallowed_protocol_increase(existing_sample_table: DeltaTable):
    with pytest.raises(
        DeltaError,
        match="Generic DeltaTable error: Table feature enables writer feature, but min_writer is not v7. Set allow_protocol_versions_increase or increase version explicitly through set_tbl_properties",
    ):
        existing_sample_table.alter.add_feature(
            feature=TableFeatures.ChangeDataFeed,
            allow_protocol_versions_increase=False,
        )
    with pytest.raises(
        DeltaError,
        match="Generic DeltaTable error: Table feature enables reader and writer feature, but reader is not v3, and writer not v7. Set allow_protocol_versions_increase or increase versions explicitly through set_tbl_properties",
    ):
        existing_sample_table.alter.add_feature(
            feature=TableFeatures.DeletionVectors,
            allow_protocol_versions_increase=False,
        )


def test_add_feautres(existing_sample_table: DeltaTable):
    existing_sample_table.alter.add_feature(
        feature=features,
        allow_protocol_versions_increase=True,
    )
    protocol = existing_sample_table.protocol()

    assert sorted(protocol.reader_features) == sorted(  # type: ignore
        ["v2Checkpoint", "columnMapping", "deletionVectors", "timestampNtz"]
    )
    assert sorted(protocol.writer_features) == sorted(  # type: ignore
        [
            "appendOnly",
            "changeDataFeed",
            "checkConstraints",
            "columnMapping",
            "deletionVectors",
            "domainMetadata",
            "generatedColumns",
            "icebergCompatV1",
            "identityColumns",
            "invariants",
            "rowTracking",
            "timestampNtz",
            "v2Checkpoint",
        ]
    )  # type: ignore
