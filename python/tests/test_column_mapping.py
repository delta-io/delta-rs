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

from deltalake import DeltaTable, write_deltalake
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
