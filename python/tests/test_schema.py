import json

import pyarrow as pa
import pytest

from deltalake import DeltaTable, Field
from deltalake.schema import (
    ArrayType,
    ArrowSchemaConversionMode,
    MapType,
    PrimitiveType,
    Schema,
    StructType,
    _convert_pa_schema_to_delta,
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


def test_table_schema_pyarrow_simple():
    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)
    schema = dt.schema().to_pyarrow()
    field = schema.field(0)
    assert len(schema.types) == 1
    assert field.name == "id"
    assert field.type == pa.int64()
    assert field.nullable is True
    assert field.metadata is None


def test_table_schema_pyarrow_020():
    table_path = "../crates/test/tests/data/delta-0.2.0"
    dt = DeltaTable(table_path)
    schema = dt.schema().to_pyarrow()
    field = schema.field(0)
    assert len(schema.types) == 1
    assert field.name == "value"
    assert field.type == pa.int32()
    assert field.nullable is True
    assert field.metadata is None


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

    json_type = struct_type.to_json()
    assert struct_type == StructType.from_json(json_type)

    # Field metadata doesn't roundtrip currently
    # See: https://github.com/apache/arrow-rs/issues/478
    fields = [
        Field("x", "integer", nullable=True),
        Field("y", PrimitiveType("string"), nullable=False),
    ]
    struct_type = StructType(fields)
    pa_type = struct_type.to_pyarrow()
    assert struct_type == StructType.from_pyarrow(pa_type)


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

    empty_schema = Schema([])
    pa_schema = empty_schema.to_pyarrow()
    assert empty_schema == Schema.from_pyarrow(pa_schema)

    # Field metadata doesn't roundtrip currently
    # See: https://github.com/apache/arrow-rs/issues/478
    fields = [
        Field("x", "integer", nullable=True),
        Field("y", ArrayType("string", contains_null=True), nullable=False),
    ]
    schema_without_metadata = schema = Schema(fields)
    pa_schema = schema_without_metadata.to_pyarrow()
    assert schema_without_metadata == Schema.from_pyarrow(pa_schema)


def _generate_test_type():
    class UuidType(pa.ExtensionType):
        def __init__(self):
            pa.ExtensionType.__init__(self, pa.binary(16), "my_package.uuid")

        def __arrow_ext_serialize__(self):
            # since we don't have a parameterized type, we don't need extra
            # metadata to be deserialized
            return b""

        @classmethod
        def __arrow_ext_deserialize__(self, storage_type, serialized):
            # return an instance of this subclass given the serialized
            # metadata.
            return UuidType()

    pa.register_extension_type(UuidType())
    return UuidType()


def _generate_test_tuples():
    test_tuples = [
        (
            pa.schema([("some_int", pa.uint32()), ("some_string", pa.string_view())]),
            pa.schema([("some_int", pa.int32()), ("some_string", pa.string_view())]),
            ArrowSchemaConversionMode.PASSTHROUGH,
        ),
        (
            pa.schema(
                [
                    ("some_int", pa.uint32()),
                    ("some_string", pa.list_view(pa.large_string())),
                ]
            ),
            pa.schema(
                [
                    ("some_int", pa.int32()),
                    ("some_string", pa.list_view(pa.large_string())),
                ]
            ),
            ArrowSchemaConversionMode.PASSTHROUGH,
        ),
        (
            pa.schema([("some_int", pa.uint32()), ("some_string", pa.string())]),
            pa.schema([("some_int", pa.int32()), ("some_string", pa.string())]),
            ArrowSchemaConversionMode.NORMAL,
        ),
        (
            pa.schema([("some_int", pa.uint32()), ("some_string", pa.string())]),
            pa.schema([("some_int", pa.int32()), ("some_string", pa.string())]),
            ArrowSchemaConversionMode.PASSTHROUGH,
        ),
        (
            pa.schema([("some_int", pa.uint32()), ("some_string", pa.large_string())]),
            pa.schema([("some_int", pa.int32()), ("some_string", pa.large_string())]),
            ArrowSchemaConversionMode.PASSTHROUGH,
        ),
        (
            pa.schema([("some_int", pa.uint32()), ("some_binary", pa.large_binary())]),
            pa.schema([("some_int", pa.int32()), ("some_binary", pa.large_binary())]),
            ArrowSchemaConversionMode.PASSTHROUGH,
        ),
        (
            pa.schema(
                [
                    ("some_int", pa.uint32()),
                    ("some_string", pa.large_list(pa.large_string())),
                ]
            ),
            pa.schema(
                [
                    ("some_int", pa.int32()),
                    ("some_string", pa.large_list(pa.large_string())),
                ]
            ),
            ArrowSchemaConversionMode.PASSTHROUGH,
        ),
        (
            pa.schema(
                [
                    ("some_int", pa.uint32()),
                    ("some_string", pa.list_(pa.large_string())),
                ]
            ),
            pa.schema(
                [("some_int", pa.int32()), ("some_string", pa.list_(pa.large_string()))]
            ),
            ArrowSchemaConversionMode.PASSTHROUGH,
        ),
        (
            pa.schema(
                [
                    pa.field("some_int", pa.uint32(), nullable=True),
                    pa.field("some_string", pa.string(), nullable=False),
                    pa.field("some_fixed_binary", pa.binary(5), nullable=False),
                    pa.field("some_decimal", pa.decimal128(10, 2), nullable=False),
                ]
            ),
            pa.schema(
                [
                    pa.field("some_int", pa.int32(), nullable=True),
                    pa.field("some_string", pa.string(), nullable=False),
                    pa.field("some_fixed_binary", pa.binary(), nullable=False),
                    pa.field("some_decimal", pa.decimal128(10, 2), nullable=False),
                ]
            ),
            ArrowSchemaConversionMode.NORMAL,
        ),
        (
            pa.schema(
                [
                    pa.field("some_int", pa.uint32(), nullable=True),
                    pa.field("some_string", pa.string(), nullable=False),
                ]
            ),
            pa.schema(
                [
                    pa.field("some_int", pa.int32(), nullable=True),
                    pa.field("some_string", pa.large_string(), nullable=False),
                ]
            ),
            ArrowSchemaConversionMode.LARGE,
        ),
        (
            pa.schema([("some_int", pa.uint32()), ("some_string", pa.string())]),
            pa.schema([("some_int", pa.int32()), ("some_string", pa.large_string())]),
            ArrowSchemaConversionMode.LARGE,
        ),
        (
            pa.schema([("some_int", pa.uint32()), ("some_string", pa.large_string())]),
            pa.schema([("some_int", pa.int32()), ("some_string", pa.string())]),
            ArrowSchemaConversionMode.NORMAL,
        ),
        (
            pa.schema(
                [
                    ("some_int", pa.uint8()),
                    ("some_int1", pa.uint16()),
                    ("some_int2", pa.uint32()),
                    ("some_int3", pa.uint64()),
                ]
            ),
            pa.schema(
                [
                    ("some_int", pa.int8()),
                    ("some_int1", pa.int16()),
                    ("some_int2", pa.int32()),
                    ("some_int3", pa.int64()),
                ]
            ),
            ArrowSchemaConversionMode.LARGE,
        ),
        (
            pa.schema(
                [
                    ("some_list", pa.list_(pa.string())),
                    ("some_fixed_list_int", pa.list_(pa.uint32(), 5)),
                    ("some_list_binary", pa.list_(pa.binary())),
                    ("some_string", pa.large_string()),
                ]
            ),
            pa.schema(
                [
                    ("some_list", pa.large_list(pa.large_string())),
                    ("some_fixed_list_int", pa.large_list(pa.int32())),
                    ("some_list_binary", pa.large_list(pa.large_binary())),
                    ("some_string", pa.large_string()),
                ]
            ),
            ArrowSchemaConversionMode.LARGE,
        ),
        (
            pa.schema(
                [
                    ("some_list", pa.large_list(pa.string())),
                    ("some_string", pa.large_string()),
                    ("some_binary", pa.large_binary()),
                ]
            ),
            pa.schema(
                [
                    ("some_list", pa.list_(pa.string())),
                    ("some_string", pa.string()),
                    ("some_binary", pa.binary()),
                ]
            ),
            ArrowSchemaConversionMode.NORMAL,
        ),
        (
            pa.schema(
                [
                    ("highly_nested_list", pa.list_(pa.list_(pa.list_(pa.string())))),
                    (
                        "highly_nested_list_binary",
                        pa.list_(pa.list_(pa.list_(pa.binary()))),
                    ),
                    ("some_string", pa.large_string()),
                    ("some_binary", pa.large_binary()),
                ]
            ),
            pa.schema(
                [
                    (
                        "highly_nested_list",
                        pa.large_list(pa.large_list(pa.large_list(pa.large_string()))),
                    ),
                    (
                        "highly_nested_list_binary",
                        pa.large_list(pa.large_list(pa.large_list(pa.large_binary()))),
                    ),
                    ("some_string", pa.large_string()),
                    ("some_binary", pa.large_binary()),
                ]
            ),
            ArrowSchemaConversionMode.LARGE,
        ),
        (
            pa.schema(
                [
                    (
                        "highly_nested_list",
                        pa.large_list(pa.list_(pa.large_list(pa.string()))),
                    ),
                    (
                        "highly_nested_list_int",
                        pa.large_list(pa.list_(pa.large_list(pa.uint64()))),
                    ),
                    ("some_string", pa.large_string()),
                    ("some_binary", pa.large_binary()),
                ]
            ),
            pa.schema(
                [
                    ("highly_nested_list", pa.list_(pa.list_(pa.list_(pa.string())))),
                    (
                        "highly_nested_list_int",
                        pa.list_(pa.list_(pa.list_(pa.int64()))),
                    ),
                    ("some_string", pa.string()),
                    ("some_binary", pa.binary()),
                ]
            ),
            ArrowSchemaConversionMode.NORMAL,
        ),
        (
            pa.schema(
                [
                    ("timestamp", pa.timestamp("s")),
                    ("timestamp1", pa.timestamp("ms")),
                    ("timestamp2", pa.timestamp("us")),
                    ("timestamp3", pa.timestamp("ns")),
                    ("timestamp4", pa.timestamp("s", tz="UTC")),
                    ("timestamp5", pa.timestamp("ms", tz="UTC")),
                    ("timestamp6", pa.timestamp("ns", tz="UTC")),
                    ("timestamp7", pa.timestamp("ns", tz="UTC")),
                ]
            ),
            pa.schema(
                [
                    ("timestamp", pa.timestamp("us")),
                    ("timestamp1", pa.timestamp("us")),
                    ("timestamp2", pa.timestamp("us")),
                    ("timestamp3", pa.timestamp("us")),
                    ("timestamp4", pa.timestamp("us", tz="UTC")),
                    ("timestamp5", pa.timestamp("us", tz="UTC")),
                    ("timestamp6", pa.timestamp("us", tz="UTC")),
                    ("timestamp7", pa.timestamp("us", tz="UTC")),
                ]
            ),
            ArrowSchemaConversionMode.NORMAL,
        ),
        (
            pa.schema(
                [
                    (
                        "struct",
                        pa.struct(
                            {
                                "highly_nested_list": pa.large_list(
                                    pa.list_(pa.large_list(pa.string()))
                                ),
                                "highly_nested_list_int": pa.large_list(
                                    pa.list_(pa.large_list(pa.uint64()))
                                ),
                                "some_string": pa.large_string(),
                                "some_binary": pa.large_binary(),
                            }
                        ),
                    )
                ]
            ),
            pa.schema(
                [
                    (
                        "struct",
                        pa.struct(
                            {
                                "highly_nested_list": pa.list_(
                                    pa.list_(pa.list_(pa.string()))
                                ),
                                "highly_nested_list_int": pa.list_(
                                    pa.list_(pa.list_(pa.int64()))
                                ),
                                "some_string": pa.string(),
                                "some_binary": pa.binary(),
                            }
                        ),
                    )
                ]
            ),
            ArrowSchemaConversionMode.NORMAL,
        ),
        (
            pa.schema([("uuid", _generate_test_type())]),
            pa.schema([("uuid", pa.binary(16))]),
            ArrowSchemaConversionMode.NORMAL,
        ),
    ]

    return test_tuples


@pytest.mark.parametrize(
    "schema,expected_schema,conversion_mode",
    _generate_test_tuples(),
)
def test_schema_conversions(schema, expected_schema, conversion_mode):
    result_schema = _convert_pa_schema_to_delta(schema, conversion_mode)

    assert result_schema == expected_schema
