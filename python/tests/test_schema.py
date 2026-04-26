import json

import pytest

from deltalake import DeltaTable, Field
from deltalake.schema import (
    ArrayType,
    MapType,
    PrimitiveType,
    Schema,
    StructType,
)


def test_table_schema():
    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)
    schema = dt.schema()
    assert json.loads(schema.to_json()) == {
        "fields": [{"metadata": {}, "name": "id", "nullable": True, "type": "long"}],
        "type": "struct",
    }
    assert len(schema.fields) == 1
    field = schema.fields[0]
    assert field.name == "id"
    assert field.type == PrimitiveType("long")
    assert field.nullable is True
    assert field.metadata == {}

    json_buf = '{"type":"struct","fields":[{"name":"x","type":{"type":"array","elementType":"long","containsNull":true},"nullable":true,"metadata":{}}]}'
    schema = Schema.from_json(json_buf)
    assert schema.fields[0] == Field(
        "x", ArrayType(PrimitiveType("long"), True), True, {}
    )


@pytest.mark.pyarrow
def test_table_schema_pyarrow_simple():
    import pyarrow as pa

    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)
    schema = dt.schema().to_arrow()
    field = schema.field(0)
    assert len(schema.types) == 1
    assert field.name == "id"
    assert field.type == pa.int64()
    assert field.nullable is True
    assert field.metadata == {}


@pytest.mark.pyarrow
def test_table_schema_pyarrow_020():
    import pyarrow as pa

    table_path = "../crates/test/tests/data/delta-0.2.0"
    dt = DeltaTable(table_path)
    schema = dt.schema().to_arrow()
    field = schema.field(0)
    assert len(schema.types) == 1
    assert field.name == "value"
    assert field.type == pa.int32()
    assert field.nullable is True
    assert field.metadata == {}


@pytest.mark.pyarrow
def test_primitive_delta_types():
    valid_types = [
        "string",
        "long",
        "integer",
        "short",
        "byte",
        "float",
        "double",
        "boolean",
        "binary",
        "date",
        "timestamp",
        "decimal(10,2)",
    ]

    invalid_types = ["int", "decimal", "decimal()", "decimal(39,1)", "decimal(1,39)"]

    for data_type in valid_types:
        delta_type = PrimitiveType(data_type)
        assert delta_type.type == data_type
        assert data_type in str(delta_type)
        assert data_type in repr(delta_type)

        pa_type = delta_type.to_arrow()
        assert delta_type == PrimitiveType.from_arrow(pa_type)

        json_type = delta_type.to_json()
        assert delta_type == PrimitiveType.from_json(json_type)

    for data_type in invalid_types:
        with pytest.raises(ValueError):
            PrimitiveType(data_type)


@pytest.mark.pyarrow
def test_void_type_maps_to_pyarrow_null():
    """
    Regression test for: https://github.com/delta-io/delta-rs/issues/1947

    Delta tables can contain columns with `void` data type (e.g. columns that
    were created but never populated). When converting a Delta schema to PyArrow
    via `DeltaTable.schema().to_pyarrow()`, `void` should map to `pyarrow.null()`
    rather than raising a `Schema error: Invalid data type for Arrow: void`.

    PyArrow's `null` type is the correct semantic equivalent:
    https://arrow.apache.org/docs/python/generated/pyarrow.null.html
    """
    import pyarrow as pa

    # Construct a schema containing a void-typed column via JSON
    # (void columns appear in Delta tables that have been schema-evolved
    # or where a column was added but never written to)
    schema_json = json.dumps({
        "type": "struct",
        "fields": [
            {
                "name": "id",
                "type": "long",
                "nullable": False,
                "metadata": {},
            },
            {
                "name": "empty_col",
                "type": "void",
                "nullable": True,
                "metadata": {},
            },
        ],
    })

    schema = Schema.from_json(schema_json)

    # Prior to the fix this raised:
    # Exception: Schema error: Invalid data type for Arrow: void
    pa_schema = schema.to_arrow()

    assert pa_schema.field("id").type == pa.int64()

    # void in Delta should map to null in PyArrow — semantically equivalent:
    # both represent the absence of a typed value
    assert pa_schema.field("empty_col").type == pa.null(), (
        f"Expected pa.null() for Delta 'void' type, got {pa_schema.field('empty_col').type}"
    )
    assert pa_schema.field("empty_col").nullable is True


@pytest.mark.pyarrow
def test_void_type_in_struct_field():
    """
    Regression test: void type nested inside a struct should also map correctly.
    Covers the case where void columns appear in nested schemas.
    """
    import pyarrow as pa

    schema_json = json.dumps({
        "type": "struct",
        "fields": [
            {
                "name": "nested",
                "type": {
                    "type": "struct",
                    "fields": [
                        {"name": "value", "type": "string", "nullable": True, "metadata": {}},
                        {"name": "placeholder", "type": "void", "nullable": True, "metadata": {}},
                    ],
                },
                "nullable": True,
                "metadata": {},
            }
        ],
    })

    schema = Schema.from_json(schema_json)
    pa_schema = schema.to_arrow()

    nested_field = pa_schema.field("nested")
    assert isinstance(nested_field.type, pa.StructType)

    placeholder_idx = nested_field.type.get_field_index("placeholder")
    assert nested_field.type.field(placeholder_idx).type == pa.null()


@pytest.mark.pyarrow
def test_array_delta_types():
    init_values = [
        (PrimitiveType("string"), False),
        (ArrayType(PrimitiveType("string"), True), True),
    ]

    for element_type, contains_null in init_values:
        array_type = ArrayType(element_type, contains_null)

        assert array_type.type == "array"
        assert array_type.element_type == element_type
        assert array_type.contains_null == contains_null

        pa_type = array_type.to_arrow()
        assert array_type == ArrayType.from_arrow(pa_type)

        json_type = array_type.to_json()
        assert array_type == ArrayType.from_json(json_type)


def test_map_delta_types():
    init_values = [
        (PrimitiveType("string"), PrimitiveType("decimal(20,9)"), False),
        (PrimitiveType("float"), PrimitiveType("string"), True),
        (
            PrimitiveType("string"),
            MapType(PrimitiveType("date"), PrimitiveType("date"), True),
            False,
        ),
    ]
    for key_type, value_type, value_contains_null in init_values:
        map_type = MapType(key_type, value_type, value_contains_null)

        assert map_type.type == "map"
        assert map_type.key_type == key_type
        assert map_type.value_type == value_type
        assert map_type.value_contains_null == value_contains_null

        # Map type is not yet supported in C Data Interface
        # https://github.com/apache/arrow-rs/issues/2037
        # pa_type = map_type.to_arrow()
        # assert map_type == PrimitiveType.from_arrow(pa_type)

        json_type = map_type.to_json()
        assert map_type == MapType.from_json(json_type)


@pytest.mark.pyarrow
def test_struct_delta_types():
    fields = [
        Field("x", "integer", nullable=True, metadata={"x": {"y": 3}}),
        Field("y", PrimitiveType("string"), nullable=False),
    ]

    struct_type = StructType(fields)

    assert struct_type.type == "struct"
    assert struct_type.fields == fields

    json_type = struct_type.to_json()
    assert struct_type == StructType.from_json(json_type)

    # Field metadata doesn't roundtrip currently
    # See: https://github.com/apache/arrow-rs/issues/478
    fields = [
        Field("x", "integer", nullable=True),
        Field("y", PrimitiveType("string"), nullable=False),
    ]
    struct_type = StructType(fields)
    pa_type = struct_type.to_arrow()
    assert struct_type == StructType.from_arrow(pa_type)


@pytest.mark.pyarrow
def test_delta_field():
    args = [
        ("x", PrimitiveType("string"), True, {}),
        ("y", "float", False, {"x": {"y": 3}}),
        ("z", ArrayType(StructType([Field("x", "integer", True)]), True), True, None),
    ]

    # TODO: are there field names we should reject?

    for name, ty, nullable, metadata in args:
        field = Field(name=name, type=ty, nullable=nullable, metadata=metadata)

        assert field.name == name
        assert field.type == (PrimitiveType(ty) if isinstance(ty, str) else ty)
        assert field.nullable == nullable
        if metadata:
            assert json.loads(field.metadata["x"]) == {"y": 3}
        else:
            assert field.metadata == {}

        # Field metadata doesn't roundtrip currently
        # See: https://github.com/apache/arrow-rs/issues/478
        if len(field.metadata) == 0:
            pa_field = field.to_arrow()
            assert field == Field.from_arrow(pa_field)

        json_field = field.to_json()
        assert field == Field.from_json(json_field)


@pytest.mark.pyarrow
def test_delta_schema():
    fields = [
        Field("x", "integer", nullable=True, metadata={"x": {"y": 3}}),
        Field("y", PrimitiveType("string"), nullable=False),
    ]

    schema = Schema(fields)

    assert schema.fields == fields

    empty_schema = Schema([])
    pa_schema = empty_schema.to_arrow()
    assert empty_schema == Schema.from_arrow(pa_schema)

    # Field metadata doesn't roundtrip currently
    # See: https://github.com/apache/arrow-rs/issues/478
    fields = [
        Field("x", "integer", nullable=True),
        Field("y", ArrayType("string", contains_null=True), nullable=False),
    ]
    schema_without_metadata = schema = Schema(fields)
    pa_schema = schema_without_metadata.to_arrow()
    assert schema_without_metadata == Schema.from_arrow(pa_schema)


# <https://github.com/delta-io/delta-rs/issues/3174>
def test_field_serialization():
    from deltalake import Field

    f = Field("fieldname", "binary", metadata={"key": "value"})
    assert f.name == "fieldname"
    assert f.metadata == {"key": "value"}
