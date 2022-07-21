from array import array

import pyarrow
import pytest

from deltalake import DeltaTable, Field
from deltalake.schema import (
    ArrayType,
    DataType,
    MapType,
    PrimitiveType,
    Schema,
    StructType,
)


def test_table_schema():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    schema = dt.schema()
    assert schema.json() == {
        "fields": [{"metadata": {}, "name": "id", "nullable": True, "type": "long"}],
        "type": "struct",
    }
    assert len(schema.fields) == 1
    field = schema.fields[0]
    assert field.name == "id"
    assert field.type == PrimitiveType("long")
    assert field.nullable is True
    assert field.metadata == {}

    json = '{"type":"struct","fields":[{"name":"x","type":{"type":"array","elementType":"long","containsNull":true},"nullable":true,"metadata":{}}]}'
    schema = Schema.from_json(json)
    assert schema.fields[0] == Field(
        "x", ArrayType(PrimitiveType("long"), True), True, {}
    )


def test_table_schema_pyarrow_simple():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    schema = dt.pyarrow_schema()
    field = schema.field(0)
    assert len(schema.types) == 1
    assert field.name == "id"
    assert field.type == pyarrow.int64()
    assert field.nullable is True
    assert field.metadata is None


def test_table_schema_pyarrow_020():
    table_path = "../rust/tests/data/delta-0.2.0"
    dt = DeltaTable(table_path)
    schema = dt.pyarrow_schema()
    field = schema.field(0)
    assert len(schema.types) == 1
    assert field.name == "value"
    assert field.type == pyarrow.int32()
    assert field.nullable is True
    assert field.metadata is None


# def test_schema_pyarrow_from_decimal_and_floating_types():
#     field_name = "decimal_test"
#     metadata = {b"metadata_k": b"metadata_v"}
#     precision = 20
#     scale = 2
#     pyarrow_field = pyarrow_field_from_dict(
#         {
#             "name": field_name,
#             "nullable": False,
#             "metadata": metadata,
#             "type": {"name": "decimal", "precision": precision, "scale": scale},
#         }
#     )
#     assert pyarrow_field.name == field_name
#     assert pyarrow_field.type == pyarrow.decimal128(precision=precision, scale=scale)
#     assert dict(pyarrow_field.metadata) == metadata
#     assert pyarrow_field.nullable is False

#     field_name = "floating_test"
#     metadata = {b"metadata_k": b"metadata_v"}
#     pyarrow_field = pyarrow_field_from_dict(
#         {
#             "name": field_name,
#             "nullable": False,
#             "metadata": metadata,
#             "type": {"name": "floatingpoint", "precision": "HALF"},
#         }
#     )
#     assert pyarrow_field.name == field_name
#     assert pyarrow_field.type == pyarrow.float16()
#     assert dict(pyarrow_field.metadata) == metadata
#     assert pyarrow_field.nullable is False


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

    invalid_types = ["int", "decimal", "decimal()"]

    for data_type in valid_types:
        delta_type = PrimitiveType(data_type)
        assert delta_type.type == data_type
        assert data_type in str(delta_type)
        assert data_type in repr(delta_type)

        pa_type = delta_type.to_pyarrow()
        assert delta_type == PrimitiveType.from_pyarrow(pa_type)

        json_type = delta_type.to_json()
        assert delta_type == PrimitiveType.from_json(json_type)

    for data_type in invalid_types:
        with pytest.raises(ValueError):
            PrimitiveType(data_type)


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

        pa_type = array_type.to_pyarrow()
        assert array_type == ArrayType.from_pyarrow(pa_type)

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
        # pa_type = map_type.to_pyarrow()
        # assert map_type == PrimitiveType.from_pyarrow(pa_type)

        json_type = map_type.to_json()
        assert map_type == MapType.from_json(json_type)


def test_struct_delta_types():
    fields = [
        Field("x", "integer", nullable=True, metadata={"x": {"y": 3}}),
        Field("y", PrimitiveType("string"), nullable=False),
    ]

    struct_type = StructType(fields)

    assert struct_type.type == "struct"
    assert struct_type.fields == fields

    pa_type = struct_type.to_pyarrow()
    assert struct_type == StructType.from_pyarrow(pa_type)

    json_type = struct_type.to_json()
    assert struct_type == StructType.from_json(json_type)


def test_delta_field():
    args = [
        ("x", PrimitiveType("string"), True, {}),
        ("y", "float", False, {"x": {"y": 3}}),
        ("z", ArrayType(StructType([Field("x", "integer", True)]), True), True, None),
    ]

    # TODO: are there field names we should reject?

    for name, ty, nullable, metadata in args:
        field = Field(name, ty, nullable=nullable, metadata=metadata)

        assert field.name == name        
        assert field.type == (PrimitiveType(ty) if isinstance(ty, str) else ty)
        assert field.nullable == nullable
        assert field.metadata == metadata

        pa_field = field.to_pyarrow()
        assert field == Field.from_pyarrow(pa_field)

        json_field = field.to_json()
        assert field == Field.from_json(json_field)


def test_delta_schema():
    fields = [
        Field("x", "integer", nullable=True, metadata={"x": {"y": 3}}),
        Field("y", PrimitiveType("string"), nullable=False),
    ]

    schema = Schema(fields)

    assert schema.fields == fields

    pa_schema = schema.to_pyarrow()
    assert schema == Schema.from_pyarrow(pa_schema)

    empty_schema = Schema([])
    pa_schema = empty_schema.to_pyarrow()
    assert schema == Schema.from_pyarrow(empty_schema)


# def test_schema_delta_types():
#     field_name = "column1"
#     metadata = {"metadata_k": "metadata_v"}
#     delta_field = Field(
#         name=field_name,
#         type=DataType.from_dict({"type": "integer"}),
#         metadata={"metadata_k": "metadata_v"},
#         nullable=False,
#     )
#     assert delta_field.name == field_name
#     assert delta_field.type == DataType("integer")
#     assert delta_field.metadata == metadata
#     assert delta_field.nullable is False

#     delta_field = Field(
#         name=field_name,
#         type=DataType.from_dict(
#             {"type": "array", "elementType": {"type": "integer"}, "containsNull": True}
#         ),
#         metadata={"metadata_k": "metadata_v"},
#         nullable=False,
#     )
#     assert delta_field.name == field_name
#     assert delta_field.type == ArrayType(DataType("integer"), True)
#     assert delta_field.metadata == metadata
#     assert delta_field.nullable is False

#     delta_field = Field(
#         name=field_name,
#         type=DataType.from_dict(
#             {
#                 "type": "map",
#                 "keyType": "integer",
#                 "valueType": "integer",
#                 "valueContainsNull": True,
#             }
#         ),
#         metadata={"metadata_k": "metadata_v"},
#         nullable=False,
#     )
#     assert delta_field.name == field_name
#     key_type = DataType("integer")
#     value_type = DataType("integer")
#     assert delta_field.type == MapType(key_type, value_type, True)
#     assert delta_field.metadata == metadata
#     assert delta_field.nullable is False

#     delta_field = Field(
#         name=field_name,
#         type=DataType.from_dict(
#             {
#                 "type": "struct",
#                 "fields": [
#                     {
#                         "name": "x",
#                         "type": {"type": "integer"},
#                         "nullable": True,
#                         "metadata": {},
#                     }
#                 ],
#             }
#         ),
#         metadata={"metadata_k": "metadata_v"},
#         nullable=False,
#     )
#     assert delta_field.name == field_name
#     assert delta_field.type == StructType([Field("x", DataType("integer"), True, {})])
#     assert delta_field.metadata == metadata
#     assert delta_field.nullable is False
