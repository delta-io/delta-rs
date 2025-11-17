from typing import TYPE_CHECKING

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import DeltaTable, Field, write_deltalake
from deltalake import Schema as DeltaSchema
from deltalake.exceptions import DeltaError, SchemaMismatchError
from deltalake.query import QueryBuilder
from deltalake.schema import PrimitiveType

if TYPE_CHECKING:
    import pyarrow as pa


@pytest.fixture
def gc_schema() -> DeltaSchema:
    return DeltaSchema(
        [
            Field(name="id", type=PrimitiveType("integer"), nullable=True),
            Field(
                name="gc",
                type=PrimitiveType("integer"),
                nullable=True,
                metadata={"delta.generationExpression": "5"},
            ),
        ]
    )


@pytest.fixture
def valid_gc_data() -> Table:
    id_col = ArrowField("id", DataType.int32(), nullable=True)
    gc = ArrowField("gc", DataType.int32(), nullable=True).with_metadata(
        {"delta.generationExpression": "10"}
    )
    data = Table.from_pydict(
        {"id": Array([1, 2], type=id_col), "gc": Array([10, 10], type=gc)},
    )
    return data


@pytest.fixture
def data_without_gc() -> Table:
    id_col = ArrowField("id", DataType.int32(), nullable=True)
    data = Table.from_pydict({"id": Array([1, 2], type=id_col)})
    return data


@pytest.fixture
def invalid_gc_data() -> "pa.Table":
    id_col = ArrowField("id", DataType.int32(), nullable=True)
    gc = ArrowField("gc", DataType.int32(), nullable=True).with_metadata(
        {"delta.generationExpression": "10"}
    )
    data = Table.from_pydict(
        {"id": Array([1, 2], type=id_col), "gc": Array([5, 10], type=gc)},
    )
    return data


@pytest.fixture
def table_with_gc(tmp_path, gc_schema) -> DeltaTable:
    dt = DeltaTable.create(
        tmp_path,
        schema=gc_schema,
    )
    return dt


def test_create_table_with_generated_columns(tmp_path, gc_schema: DeltaSchema):
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

    from deltalake.query import QueryBuilder

    data = QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
    assert data == valid_gc_data


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
    assert (
        QueryBuilder().register("tbl", dt).execute("select * from tbl").read_all()
        == valid_gc_data
    )


def test_write_with_invalid_gc(tmp_path, invalid_gc_data):
    import re

    with pytest.raises(
        DeltaError,
        match=re.escape(
            'Invariant violations: ["Check or Invariant (gc <=> 10) violated by value in row: [5]"]'
        ),
    ):
        write_deltalake(tmp_path, mode="append", data=invalid_gc_data)


def test_write_with_invalid_gc_to_table(table_with_gc, invalid_gc_data):
    import re

    with pytest.raises(
        DeltaError,
        match=re.escape(
            'Invariant violations: ["Check or Invariant (gc <=> 5) violated by value in row: [10]"]'
        ),
    ):
        write_deltalake(table_with_gc, mode="append", data=invalid_gc_data)


def test_write_to_table_generating_data(table_with_gc: DeltaTable):
    id_col = ArrowField("id", DataType.int32(), nullable=True)

    data = Table.from_pydict({"id": Array([1, 2], type=id_col)})
    write_deltalake(table_with_gc, mode="append", data=data)

    gc = ArrowField("gc", DataType.int32(), nullable=True).with_metadata(
        {"delta.generationExpression": "5"}
    )
    expected_data = Table.from_pydict(
        {"id": Array([1, 2], type=id_col), "gc": Array([5, 5], type=gc)},
    )

    assert table_with_gc.version() == 1
    result = (
        QueryBuilder()
        .register("tbl", table_with_gc)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    assert result.schema == expected_data.schema


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

    id_col = ArrowField("id", DataType.int32(), nullable=True)

    gc = ArrowField("gc", DataType.int32(), nullable=True).with_metadata(
        {"delta.generationExpression": "5"}
    )
    expected_data = Table.from_pydict(
        {"id": Array([1, 2], type=id_col), "gc": Array([5, 5], type=gc)},
    )

    result = (
        QueryBuilder()
        .register("tbl", table_with_gc)
        .execute("select * from tbl order by id asc")
        .read_all()
    )

    assert result == expected_data


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

    id_col = ArrowField("id", DataType.int32(), nullable=True)
    gc = ArrowField("gc", DataType.int32(), nullable=True)
    expected_data = Table.from_pydict(
        {"id": Array([1, 2], type=id_col), "gc": Array([5, 5], type=gc)},
    )

    result = (
        QueryBuilder()
        .register("tbl", table_with_gc)
        .execute("select * from tbl order by id asc")
        .read_all()
    )
    assert result == expected_data


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
            'Invariant violations: ["Check or Invariant (gc <=> 5) violated by value in row: [10]"]'
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
