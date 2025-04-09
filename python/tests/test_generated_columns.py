import pyarrow as pa
import pytest

from deltalake import DeltaTable, Field, Schema, write_deltalake
from deltalake.exceptions import DeltaError, SchemaMismatchError
from deltalake.schema import PrimitiveType


@pytest.fixture
def gc_schema() -> Schema:
    return Schema(
        [
            Field(name="id", type=PrimitiveType("integer")),
            Field(
                name="gc",
                type=PrimitiveType("integer"),
                metadata={"delta.generationExpression": "5"},
            ),
        ]
    )


@pytest.fixture
def valid_gc_data() -> pa.Table:
    id_col = pa.field("id", pa.int32())
    gc = pa.field("gc", pa.int32()).with_metadata({"delta.generationExpression": "10"})
    data = pa.Table.from_pydict(
        {"id": [1, 2], "gc": [10, 10]}, schema=pa.schema([id_col, gc])
    )
    return data


@pytest.fixture
def data_without_gc() -> pa.Table:
    id_col = pa.field("id", pa.int32())
    data = pa.Table.from_pydict({"id": [1, 2]}, schema=pa.schema([id_col]))
    return data


@pytest.fixture
def invalid_gc_data() -> pa.Table:
    id_col = pa.field("id", pa.int32())
    gc = pa.field("gc", pa.int32()).with_metadata({"delta.generationExpression": "10"})
    data = pa.Table.from_pydict(
        {"id": [1, 2], "gc": [5, 10]}, schema=pa.schema([id_col, gc])
    )
    return data


@pytest.fixture
def table_with_gc(tmp_path, gc_schema) -> DeltaTable:
    dt = DeltaTable.create(
        tmp_path,
        schema=gc_schema,
    )
    return dt


def test_create_table_with_generated_columns(tmp_path, gc_schema: Schema):
    dt = DeltaTable.create(
        tmp_path,
        schema=gc_schema,
    )
    protocol = dt.protocol()
    assert protocol.min_writer_version == 4

    dt = DeltaTable.create(
        tmp_path,
        schema=gc_schema,
        mode="overwrite",
        configuration={"delta.minWriterVersion": "7"},
    )
    protocol = dt.protocol()

    assert dt.version() == 1
    assert protocol.writer_features is not None
    assert "generatedColumns" in protocol.writer_features


def test_write_with_gc(tmp_path, valid_gc_data):
    write_deltalake(tmp_path, mode="append", data=valid_gc_data)
    dt = DeltaTable(tmp_path)

    assert dt.protocol().min_writer_version == 4
    assert dt.to_pyarrow_table() == valid_gc_data


def test_write_with_gc_higher_writer_version(tmp_path, valid_gc_data):
    write_deltalake(
        tmp_path,
        mode="append",
        data=valid_gc_data,
        configuration={"delta.minWriterVersion": "7"},
    )
    dt = DeltaTable(tmp_path)
    protocol = dt.protocol()
    assert protocol.min_writer_version == 7
    assert protocol.writer_features is not None
    assert "generatedColumns" in protocol.writer_features
    assert dt.to_pyarrow_table() == valid_gc_data


def test_write_with_invalid_gc(tmp_path, invalid_gc_data):
    import re

    with pytest.raises(
        DeltaError,
        match=re.escape(
            'Invariant violations: ["Check or Invariant (gc = 10 OR (gc IS NULL AND 10 IS NULL)) violated by value in row: [5]"]'
        ),
    ):
        write_deltalake(tmp_path, mode="append", data=invalid_gc_data)


def test_write_with_invalid_gc_to_table(table_with_gc, invalid_gc_data):
    import re

    with pytest.raises(
        DeltaError,
        match=re.escape(
            'Invariant violations: ["Check or Invariant (gc = 5 OR (gc IS NULL AND 5 IS NULL)) violated by value in row: [10]"]'
        ),
    ):
        write_deltalake(table_with_gc, mode="append", data=invalid_gc_data)


def test_write_to_table_generating_data(table_with_gc: DeltaTable):
    id_col = pa.field("id", pa.int32())
    data = pa.Table.from_pydict({"id": [1, 2]}, schema=pa.schema([id_col]))
    write_deltalake(table_with_gc, mode="append", data=data)

    id_col = pa.field("id", pa.int32())
    gc = pa.field("gc", pa.int32())
    expected_data = pa.Table.from_pydict(
        {"id": [1, 2], "gc": [5, 5]}, schema=pa.schema([id_col, gc])
    )

    assert table_with_gc.version() == 1
    assert table_with_gc.to_pyarrow_table() == expected_data


def test_raise_when_gc_passed_during_schema_evolution(
    tmp_path, data_without_gc, valid_gc_data
):
    write_deltalake(
        tmp_path,
        mode="append",
        data=data_without_gc,
    )
    dt = DeltaTable(tmp_path)
    assert dt.protocol().min_writer_version == 2

    with pytest.raises(
        SchemaMismatchError,
        match="Schema evolved fields cannot have generated expressions. Recreate the table to achieve this.",
    ):
        write_deltalake(
            dt,
            mode="append",
            data=valid_gc_data,
            schema_mode="merge",
        )


def test_raise_when_gc_passed_during_adding_new_columns(tmp_path, data_without_gc):
    write_deltalake(
        tmp_path,
        mode="append",
        data=data_without_gc,
    )
    dt = DeltaTable(tmp_path)
    assert dt.protocol().min_writer_version == 2

    with pytest.raises(DeltaError, match="New columns cannot be a generated column"):
        dt.alter.add_columns(
            fields=[
                Field(
                    name="gc",
                    type=PrimitiveType("integer"),
                    metadata={"delta.generationExpression": "5"},
                )
            ]
        )


def test_merge_with_gc(table_with_gc: DeltaTable, data_without_gc):
    (
        table_with_gc.merge(
            data_without_gc, predicate="s.id = t.id", source_alias="s", target_alias="t"
        )
        .when_not_matched_insert_all()
        .execute()
    )
    id_col = pa.field("id", pa.int32())
    gc = pa.field("gc", pa.int32())
    expected_data = pa.Table.from_pydict(
        {"id": [1, 2], "gc": [5, 5]}, schema=pa.schema([id_col, gc])
    )
    assert (
        table_with_gc.to_pyarrow_table().sort_by([("id", "ascending")]) == expected_data
    )


def test_merge_with_g_during_schema_evolution(
    table_with_gc: DeltaTable, data_without_gc
):
    (
        table_with_gc.merge(
            data_without_gc,
            predicate="s.id = t.id",
            source_alias="s",
            target_alias="t",
            merge_schema=True,
        )
        .when_not_matched_insert_all()
        .execute()
    )
    id_col = pa.field("id", pa.int32())
    gc = pa.field("gc", pa.int32())
    expected_data = pa.Table.from_pydict(
        {"id": [1, 2], "gc": [5, 5]}, schema=pa.schema([id_col, gc])
    )
    assert (
        table_with_gc.to_pyarrow_table().sort_by([("id", "ascending")]) == expected_data
    )


def test_raise_when_gc_passed_merge_statement_during_schema_evolution(
    tmp_path, data_without_gc, valid_gc_data
):
    write_deltalake(
        tmp_path,
        mode="append",
        data=data_without_gc,
    )
    dt = DeltaTable(tmp_path)
    assert dt.protocol().min_writer_version == 2

    with pytest.raises(
        SchemaMismatchError,
        match="Schema evolved fields cannot have generated expressions. Recreate the table to achieve this.",
    ):
        (
            dt.merge(
                valid_gc_data,
                predicate="s.id = t.id",
                source_alias="s",
                target_alias="t",
                merge_schema=True,
            )
            .when_not_matched_insert_all()
            .execute()
        )


def test_merge_with_gc_invalid(table_with_gc: DeltaTable, invalid_gc_data):
    import re

    with pytest.raises(
        DeltaError,
        match=re.escape(
            'Invariant violations: ["Check or Invariant (gc = 5 OR (gc IS NULL AND 5 IS NULL)) violated by value in row: [10]"]'
        ),
    ):
        (
            table_with_gc.merge(
                invalid_gc_data,
                predicate="s.id = t.id",
                source_alias="s",
                target_alias="t",
            )
            .when_not_matched_insert_all()
            .execute()
        )
