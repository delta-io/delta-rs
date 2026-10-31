"""Write-path tests against an existing column-mapped Delta table.

delta-rs cannot create/enable column mapping yet, so these round-trips run against the
Spark-written, name-mode, string-partitioned fixture copied into a temp dir. They cover
append/overwrite/delete/merge under the logical schema (the data is stored under physical
``col-<uuid>`` names on disk) and assert that schema evolution is rejected.
"""

import pathlib

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import DeltaTable, TableFeatures, write_deltalake
from deltalake.exceptions import DeltaError
from deltalake.query import QueryBuilder


def _column_mapping_table(tmp_path: pathlib.Path) -> str:
    """Copy the Spark-written, name-mode, string-partitioned column-mapped fixture to a writable
    temp dir. delta-rs cannot create column-mapped tables yet, so the write round-trips below run
    against an existing one."""
    import shutil

    dst = tmp_path / "table_with_column_mapping"
    shutil.copytree("../crates/test/tests/data/table_with_column_mapping", dst)
    return str(dst)


def _cm_data(companies: list[str], names: list[str]) -> Table:
    """Build a logical-schema arro3 table matching the fixture's two string columns."""
    return Table(
        {
            "Company Very Short": Array(
                companies,
                ArrowField("Company Very Short", type=DataType.string(), nullable=True),
            ),
            "Super Name": Array(
                names, ArrowField("Super Name", type=DataType.string(), nullable=True)
            ),
        }
    )


def _cm_read(table_uri: str, order_by: str) -> list[dict]:
    """Read the fixture's two logical columns back via QueryBuilder as a list of row dicts."""
    result = (
        QueryBuilder()
        .register("tbl", DeltaTable(table_uri))
        .execute(
            f'select "Company Very Short", "Super Name" from tbl order by {order_by}'
        )
        .read_all()
    )
    return result.to_struct_array().to_pylist()


def test_column_mapping_append_roundtrip(tmp_path: pathlib.Path):
    """Append to an existing column-mapped table and read it back via QueryBuilder under the
    logical schema (the data is written under physical names on disk)."""
    table_uri = _column_mapping_table(tmp_path)
    write_deltalake(table_uri, _cm_data(["BMS"], ["New Customer"]), mode="append")

    assert _cm_read(table_uri, '"Company Very Short", "Super Name"') == [
        {"Company Very Short": "BME", "Super Name": "Timothy Lamb"},
        {"Company Very Short": "BMS", "Super Name": "Anthony Johnson"},
        {"Company Very Short": "BMS", "Super Name": "Mr. Daniel Ferguson MD"},
        {"Company Very Short": "BMS", "Super Name": "Nathan Bennett"},
        {"Company Very Short": "BMS", "Super Name": "New Customer"},
        {"Company Very Short": "BMS", "Super Name": "Stephanie Mcgrath"},
    ]


def test_column_mapping_overwrite_roundtrip(tmp_path: pathlib.Path):
    """Overwrite a column-mapped table and read the replacement data back via QueryBuilder."""
    table_uri = _column_mapping_table(tmp_path)
    write_deltalake(
        table_uri, _cm_data(["BMS", "BME"], ["Solo", "Duo"]), mode="overwrite"
    )

    assert _cm_read(table_uri, '"Company Very Short"') == [
        {"Company Very Short": "BME", "Super Name": "Duo"},
        {"Company Very Short": "BMS", "Super Name": "Solo"},
    ]


def test_column_mapping_delete_roundtrip(tmp_path: pathlib.Path):
    """Delete from a column-mapped table and read the survivors back via QueryBuilder."""
    table_uri = _column_mapping_table(tmp_path)
    DeltaTable(table_uri).delete("\"Super Name\" = 'Timothy Lamb'")

    assert _cm_read(table_uri, '"Super Name"') == [
        {"Company Very Short": "BMS", "Super Name": "Anthony Johnson"},
        {"Company Very Short": "BMS", "Super Name": "Mr. Daniel Ferguson MD"},
        {"Company Very Short": "BMS", "Super Name": "Nathan Bennett"},
        {"Company Very Short": "BMS", "Super Name": "Stephanie Mcgrath"},
    ]


def test_column_mapping_merge_roundtrip(tmp_path: pathlib.Path):
    """Merge a not-matched row into a column-mapped table and read it back via QueryBuilder."""
    table_uri = _column_mapping_table(tmp_path)
    (
        DeltaTable(table_uri)
        .merge(
            source=_cm_data(["BMS"], ["Merged Row"]),
            predicate='t."Super Name" = s."Super Name"',
            source_alias="s",
            target_alias="t",
        )
        .when_not_matched_insert_all()
        .execute()
    )

    assert _cm_read(table_uri, '"Company Very Short", "Super Name"') == [
        {"Company Very Short": "BME", "Super Name": "Timothy Lamb"},
        {"Company Very Short": "BMS", "Super Name": "Anthony Johnson"},
        {"Company Very Short": "BMS", "Super Name": "Merged Row"},
        {"Company Very Short": "BMS", "Super Name": "Mr. Daniel Ferguson MD"},
        {"Company Very Short": "BMS", "Super Name": "Nathan Bennett"},
        {"Company Very Short": "BMS", "Super Name": "Stephanie Mcgrath"},
    ]


def test_column_mapping_write_schema_evolution_rejected(tmp_path: pathlib.Path):
    """Schema evolution via write (schema_mode='merge') is rejected on column-mapped tables."""
    table_uri = _column_mapping_table(tmp_path)
    evolving = Table(
        {
            "Company Very Short": Array(
                ["BMS"],
                ArrowField("Company Very Short", type=DataType.string(), nullable=True),
            ),
            "Super Name": Array(
                ["New Customer"],
                ArrowField("Super Name", type=DataType.string(), nullable=True),
            ),
            "extra": Array(
                [1], ArrowField("extra", type=DataType.int32(), nullable=True)
            ),
        }
    )
    with pytest.raises(
        DeltaError,
        match="Schema evolution on column-mapped tables is not yet supported",
    ):
        write_deltalake(table_uri, evolving, mode="append", schema_mode="merge")


def test_column_mapping_merge_schema_evolution_rejected(tmp_path: pathlib.Path):
    """Schema-evolving merge (merge_schema=True) is rejected on column-mapped tables."""
    table_uri = _column_mapping_table(tmp_path)
    source = Table(
        {
            "Company Very Short": Array(
                ["BMS"],
                ArrowField("Company Very Short", type=DataType.string(), nullable=True),
            ),
            "Super Name": Array(
                ["Merged Row"],
                ArrowField("Super Name", type=DataType.string(), nullable=True),
            ),
            "extra": Array(
                [1], ArrowField("extra", type=DataType.int32(), nullable=True)
            ),
        }
    )
    with pytest.raises(
        DeltaError,
        match="Schema evolution on column-mapped tables is not yet supported",
    ):
        (
            DeltaTable(table_uri)
            .merge(
                source=source,
                predicate='t."Super Name" = s."Super Name"',
                source_alias="s",
                target_alias="t",
                merge_schema=True,
            )
            .when_not_matched_insert_all()
            .execute()
        )


def _new_column_mapping_table(tmp_path: pathlib.Path, configuration=None) -> str:
    table_uri = str(tmp_path / "new_column_mapping")
    write_deltalake(
        table_uri,
        _cm_data(["BME", "BMS"], ["Timothy Lamb", "Nathan Bennett"]),
        configuration={"delta.columnMapping.mode": "name", **(configuration or {})},
    )
    return table_uri


def _assert_column_mapping_feature(table_uri: str) -> None:
    protocol = DeltaTable(table_uri).protocol()
    assert (protocol.min_reader_version, protocol.min_writer_version) == (3, 7)
    assert protocol.reader_features is not None
    assert protocol.writer_features is not None
    assert "columnMapping" in protocol.reader_features
    assert "columnMapping" in protocol.writer_features


@pytest.mark.parametrize(
    "enable_deletion_vectors",
    [
        lambda dt: dt.alter.set_table_properties(
            {"delta.enableDeletionVectors": "true"}
        ),
        lambda dt: dt.alter.add_feature(
            TableFeatures.DeletionVectors, allow_protocol_versions_increase=True
        ),
    ],
    ids=["set_table_properties", "add_feature"],
)
def test_column_mapping_kept_when_upgrading_to_table_features(
    tmp_path: pathlib.Path, enable_deletion_vectors
):
    table_uri = _new_column_mapping_table(tmp_path)

    enable_deletion_vectors(DeltaTable(table_uri))
    write_deltalake(table_uri, _cm_data(["BMT"], ["New Customer"]), mode="append")

    assert _cm_read(table_uri, '"Super Name"') == [
        {"Company Very Short": "BMS", "Super Name": "Nathan Bennett"},
        {"Company Very Short": "BMT", "Super Name": "New Customer"},
        {"Company Very Short": "BME", "Super Name": "Timothy Lamb"},
    ]
    _assert_column_mapping_feature(table_uri)


def test_create_column_mapping_table_with_deletion_vectors(tmp_path: pathlib.Path):
    table_uri = _new_column_mapping_table(
        tmp_path, {"delta.enableDeletionVectors": "true"}
    )

    assert _cm_read(table_uri, '"Super Name"') == [
        {"Company Very Short": "BMS", "Super Name": "Nathan Bennett"},
        {"Company Very Short": "BME", "Super Name": "Timothy Lamb"},
    ]
    _assert_column_mapping_feature(table_uri)
    assert "deletionVectors" in DeltaTable(table_uri).protocol().reader_features


def test_column_mapping_protocol_unchanged_without_table_features(
    tmp_path: pathlib.Path,
):
    table_uri = _new_column_mapping_table(tmp_path)

    DeltaTable(table_uri).alter.set_table_properties(
        {
            "delta.enableChangeDataFeed": "true",
            "delta.logRetentionDuration": "interval 2 days",
        }
    )

    protocol = DeltaTable(table_uri).protocol()
    assert (
        protocol.min_reader_version,
        protocol.min_writer_version,
        protocol.reader_features,
        protocol.writer_features,
    ) == (2, 5, None, None)


@pytest.mark.pyarrow
def test_conflict_check_partition_predicate_with_column_mapping(
    tmp_path: pathlib.Path,
):
    """https://github.com/delta-io/delta-rs/issues/4979

    A delete predicated on a partition column must not panic when another
    writer appended a file after the table was loaded: the conflict checker
    looks up partition values by logical name while a column-mapped table
    keys them by physical name.
    """
    import pyarrow as pa

    table_path = tmp_path / "t"
    data = pa.table({"k": [1, 2], "p": ["a", "b"]})
    write_deltalake(
        table_path,
        data,
        partition_by=["p"],
        configuration={"delta.columnMapping.mode": "name"},
    )

    dt = DeltaTable(table_path)
    write_deltalake(table_path, pa.table({"k": [3], "p": ["b"]}), mode="append")

    # PanicException: called `Option::unwrap()` on a `None` value
    dt.delete("p = 'a'")

    rows = (
        QueryBuilder()
        .register("tbl", DeltaTable(table_path))
        .execute("select k, p from tbl order by k")
        .read_all()
        .to_struct_array()
        .to_pylist()
    )
    assert rows == [{"k": 3, "p": "b"}]
