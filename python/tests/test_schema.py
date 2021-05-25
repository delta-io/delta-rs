import pyarrow

from deltalake import DeltaTable, Field
from deltalake.schema import (
    ArrayType,
    DataType,
    MapType,
    Schema,
    StructType,
    pyarrow_field_from_dict,
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
    assert field.type == DataType("long")
    assert field.nullable is True
    assert field.metadata == {}

    json = '{"type":"struct","fields":[{"name":"x","type":{"type":"array","elementType":"long","containsNull":true},"nullable":true,"metadata":{}}]}'
    schema = Schema.from_json(json)
    assert schema.fields[0] == Field("x", ArrayType(DataType("long"), True), True, {})


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


def test_schema_pyarrow_from_decimal_and_floating_types():
    field_name = "decimal_test"
    metadata = {b"metadata_k": b"metadata_v"}
    precision = 20
    scale = 2
    pyarrow_field = pyarrow_field_from_dict(
        {
            "name": field_name,
            "nullable": False,
            "metadata": metadata,
            "type": {"name": "decimal", "precision": precision, "scale": scale},
        }
    )
    assert pyarrow_field.name == field_name
    assert pyarrow_field.type == pyarrow.decimal128(precision=precision, scale=scale)
    assert dict(pyarrow_field.metadata) == metadata
    assert pyarrow_field.nullable is False

    field_name = "floating_test"
    metadata = {b"metadata_k": b"metadata_v"}
    pyarrow_field = pyarrow_field_from_dict(
        {
            "name": field_name,
            "nullable": False,
            "metadata": metadata,
            "type": {"name": "floatingpoint", "precision": "HALF"},
        }
    )
    assert pyarrow_field.name == field_name
    assert pyarrow_field.type == pyarrow.float16()
    assert dict(pyarrow_field.metadata) == metadata
    assert pyarrow_field.nullable is False


def test_schema_delta_types():
    field_name = "column1"
    metadata = {"metadata_k": "metadata_v"}
    delta_field = Field(
        name=field_name,
        type=DataType.from_dict({"type": "integer"}),
        metadata={"metadata_k": "metadata_v"},
        nullable=False,
    )
    assert delta_field.name == field_name
    assert delta_field.type == DataType("integer")
    assert delta_field.metadata == metadata
    assert delta_field.nullable is False

    delta_field = Field(
        name=field_name,
        type=DataType.from_dict(
            {"type": "array", "elementType": {"type": "integer"}, "containsNull": True}
        ),
        metadata={"metadata_k": "metadata_v"},
        nullable=False,
    )
    assert delta_field.name == field_name
    assert delta_field.type == ArrayType(DataType("integer"), True)
    assert delta_field.metadata == metadata
    assert delta_field.nullable is False

    delta_field = Field(
        name=field_name,
        type=DataType.from_dict(
            {
                "type": "map",
                "keyType": "integer",
                "valueType": "integer",
                "valueContainsNull": True,
            }
        ),
        metadata={"metadata_k": "metadata_v"},
        nullable=False,
    )
    assert delta_field.name == field_name
    key_type = DataType("integer")
    value_type = DataType("integer")
    assert delta_field.type == MapType(key_type, value_type, True)
    assert delta_field.metadata == metadata
    assert delta_field.nullable is False

    delta_field = Field(
        name=field_name,
        type=DataType.from_dict(
            {
                "type": "struct",
                "fields": [
                    {
                        "name": "x",
                        "type": {"type": "integer"},
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            }
        ),
        metadata={"metadata_k": "metadata_v"},
        nullable=False,
    )
    assert delta_field.name == field_name
    assert delta_field.type == StructType([Field("x", DataType("integer"), True, {})])
    assert delta_field.metadata == metadata
    assert delta_field.nullable is False


def test_schema_pyarrow_types():
    field_name = "column1"
    metadata = {b"metadata_k": b"metadata_v"}
    pyarrow_field = pyarrow_field_from_dict(
        {
            "name": field_name,
            "nullable": False,
            "metadata": metadata,
            "type": {"name": "int", "bitWidth": 8, "isSigned": True},
        }
    )
    assert pyarrow_field.name == field_name
    assert pyarrow_field.type == pyarrow.int8()
    assert dict(pyarrow_field.metadata) == metadata
    assert pyarrow_field.nullable is False

    field_name = "column_timestamp_no_unit"
    metadata = {b"metadata_k": b"metadata_v"}
    pyarrow_field = pyarrow_field_from_dict(
        {
            "name": field_name,
            "nullable": False,
            "metadata": metadata,
            "type": {"name": "timestamp"},
        }
    )
    assert pyarrow_field.name == field_name
    assert pyarrow_field.type == pyarrow.timestamp("ns")
    assert dict(pyarrow_field.metadata) == metadata
    assert pyarrow_field.nullable is False

    field_name = "column_timestamp_with_unit"
    metadata = {b"metadata_k": b"metadata_v"}
    pyarrow_field = pyarrow_field_from_dict(
        {
            "name": field_name,
            "nullable": False,
            "metadata": metadata,
            "type": {"name": "timestamp", "unit": "MICROSECOND"},
        }
    )
    assert pyarrow_field.name == field_name
    assert pyarrow_field.type == pyarrow.timestamp("us")
    assert dict(pyarrow_field.metadata) == metadata
    assert pyarrow_field.nullable is False

    field_name = "date_with_day_unit"
    metadata = {b"metadata_k": b"metadata_v"}
    pyarrow_field = pyarrow_field_from_dict(
        {
            "name": field_name,
            "nullable": False,
            "metadata": metadata,
            "type": {"name": "date", "unit": "DAY"},
        }
    )
    assert pyarrow_field.name == field_name
    assert pyarrow_field.type == pyarrow.date32()
    assert dict(pyarrow_field.metadata) == metadata
    assert pyarrow_field.nullable is False

    field_name = "simple_list"
    pyarrow_field = pyarrow_field_from_dict(
        {
            "name": field_name,
            "nullable": False,
            "metadata": metadata,
            "type": {"name": "list"},
            "children": [{"type": {"name": "int", "bitWidth": 32, "isSigned": True}}],
        }
    )
    assert pyarrow_field.name == field_name
    assert pyarrow_field.type == pyarrow.list_(
        pyarrow.field("element", pyarrow.int32())
    )
    assert pyarrow_field.metadata == metadata
    assert pyarrow_field.nullable is False

    field_name = "dictionary"
    pyarrow_field = pyarrow_field_from_dict(
        {
            "name": field_name,
            "nullable": False,
            "metadata": metadata,
            "type": {"name": "int", "bitWidth": 32, "isSigned": True},
            "children": [],
            "dictionary": {
                "id": 0,
                "indexType": {"name": "int", "bitWidth": 16, "isSigned": True},
            },
        }
    )
    assert pyarrow_field.name == field_name
    assert pyarrow_field.type == pyarrow.map_(pyarrow.int16(), pyarrow.int32())
    assert pyarrow_field.metadata == metadata
    assert pyarrow_field.nullable is False

    field_name = "struct_array"
    pyarrow_field = pyarrow_field_from_dict(
        {
            "name": field_name,
            "nullable": False,
            "metadata": metadata,
            "type": {"name": "list"},
            "children": [],
            "dictionary": {
                "id": 0,
                "indexType": {"name": "int", "bitWidth": 32, "isSigned": True},
            },
        }
    )
    assert pyarrow_field.name == field_name
    assert pyarrow_field.type == pyarrow.map_(
        pyarrow.int32(),
        pyarrow.list_(
            pyarrow.field(
                "element",
                pyarrow.struct(
                    [pyarrow.field("val", pyarrow.int32(), False, metadata)]
                ),
            )
        ),
    )
    assert pyarrow_field.metadata == metadata
    assert pyarrow_field.nullable is False

    field_name = "simple_dictionary"
    pyarrow_field = pyarrow_field_from_dict(
        {
            "name": field_name,
            "metadata": {"metadata_k": "metadata_v"},
            "nullable": False,
            "type": {"name": "dictionary"},
            "dictionary": {"indexType": {"type": {"name": "int", "bitWidth": 8}}},
            "children": [{"type": {"name": "int", "bitWidth": 32}}],
        }
    )
    assert pyarrow_field.name == field_name
    assert pyarrow_field.type == pyarrow.map_(pyarrow.int8(), pyarrow.int32())
    assert pyarrow_field.metadata == metadata
    assert pyarrow_field.nullable is False

    pyarrow_field = pyarrow_field_from_dict(
        {
            "name": field_name,
            "type": {"name": "struct"},
            "children": [
                {
                    "name": "x",
                    "type": {"name": "int", "bitWidth": 64},
                    "nullable": True,
                    "metadata": {},
                }
            ],
            "metadata": {"metadata_k": "metadata_v"},
            "nullable": False,
        }
    )
    assert pyarrow_field.name == field_name
    assert pyarrow_field.type == pyarrow.struct(
        [pyarrow.field("x", pyarrow.int64(), True, {})]
    )
    assert pyarrow_field.metadata == metadata
    assert pyarrow_field.nullable is False
