from deltalake import (
    DeltaTable,
    DeltaTableField,
    DeltaTableSchema,
    DeltaTableSchemaFormat,
)
from deltalake.schema import ArrayType
import pyarrow


def test_table_schema_format_delta():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    schema = dt.schema(format=DeltaTableSchemaFormat.DELTA)
    field = schema.fields[0]
    assert len(schema.fields) == 1
    assert field.name == "id"
    assert field.type == "long"
    assert field.nullable is True
    assert field.metadata == {}


def test_table_schema_format_arrow():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    schema = dt.schema(format=DeltaTableSchemaFormat.ARROW)
    field = schema.fields[0]
    assert len(schema.fields) == 1
    assert field.name == "id"
    assert field.type == "int64"
    assert field.nullable is True
    assert field.metadata == {}


def test_table_schema_pyarrow():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    schema = dt.to_pyarrow_schema()
    field = schema.field(0)
    assert len(schema.types) == 1
    assert field.name == "id"
    assert field.type == pyarrow.int64()
    assert field.nullable is True
    assert field.metadata is None


def test_schema_delta_types():
    primitive_name = "int_type"
    primitive_type = "integer"
    metadata = {"metadata_k": "metadata_v"}
    delta_field = DeltaTableField(
        name=primitive_name,
        type='{"name": "%s" }' % primitive_type,
        metadata={"metadata_k": "metadata_v"},
        nullable=False,
        format=DeltaTableSchemaFormat.DELTA,
    )
    assert delta_field.name == primitive_name
    assert delta_field.type == primitive_type
    assert delta_field.metadata == metadata
    assert delta_field.nullable is False

    array_name = "array_type"
    element_type = "integer"
    metadata = {"metadata_k": "metadata_v"}
    delta_field = DeltaTableField(
        name=array_name,
        type='{"name": "array", "elementType": {"name": "%s"}, "containsNull": true}'
        % element_type,
        metadata={"metadata_k": "metadata_v"},
        nullable=False,
        format=DeltaTableSchemaFormat.DELTA,
    )
    assert delta_field.name == array_name
    assert delta_field.type.element_type == element_type
    assert delta_field.type.contains_null is True
    assert delta_field.metadata == metadata
    assert delta_field.nullable is False
    assert delta_field.type.name == "array"

    map_name = "map_type"
    key_type = "integer"
    value_type = "integer"
    metadata = {"metadata_k": "metadata_v"}
    delta_field = DeltaTableField(
        name=map_name,
        type='{"name": "map", "keyType": {"name": "%s"}, "valueType": {"name": "%s"}, "valueContainsNull": true}'
        % (key_type, value_type),
        metadata={"metadata_k": "metadata_v"},
        nullable=False,
        format=DeltaTableSchemaFormat.DELTA,
    )
    assert delta_field.name == map_name
    assert delta_field.type.key_type == key_type
    assert delta_field.type.value_type == value_type
    assert delta_field.metadata == metadata
    assert delta_field.nullable is False
    assert delta_field.type.name == "map"

    map_name = "map_type"
    key_type = "integer"
    value_type = "integer"
    metadata = {"metadata_k": "metadata_v"}
    delta_field = DeltaTableField(
        name=map_name,
        type='{"name": "map", "keyType": {"name": "%s"}, "valueType": {"name": "%s"}, "valueContainsNull": true}'
        % (key_type, value_type),
        metadata={"metadata_k": "metadata_v"},
        nullable=False,
        format=DeltaTableSchemaFormat.DELTA,
    )
    assert delta_field.name == map_name
    assert delta_field.type.key_type == key_type
    assert delta_field.type.value_type == value_type
    assert delta_field.metadata == metadata
    assert delta_field.nullable is False
    assert delta_field.type.name == "map"

    struct_name = "struct_type"
    metadata = {"metadata_k": "metadata_v"}
    delta_field = DeltaTableField(
        name=struct_name,
        type='{"name": "struct", "fields": [{"name": "x", "type": {"name": "integer"}, "nullable": true, "metadata": {}}]}',
        metadata={"metadata_k": "metadata_v"},
        nullable=False,
        format=DeltaTableSchemaFormat.DELTA,
    )
    assert delta_field.name == struct_name
    assert delta_field.type.fields[0].name == "x"
    assert delta_field.type.fields[0].type == "integer"
    assert delta_field.type.fields[0].nullable is True
    assert delta_field.metadata == metadata
    assert delta_field.nullable is False
    assert delta_field.type.name == "struct"
