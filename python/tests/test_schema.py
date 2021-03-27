import pyarrow
from deltalake import (
    DeltaTable,
    Field,
)
from deltalake.schema import (
    DataType,
    ArrayType,
    MapType,
    StructType,
    pyarrow_field_from_dict,
    Schema,
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


def test_schema_pyarrow_from_decimal_type():
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
                "keyType": {"type": "integer"},
                "valueType": {"type": "integer"},
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
    assert pyarrow_field.type == pyarrow.list_(pyarrow.int32())
    assert pyarrow_field.metadata == metadata
    assert pyarrow_field.nullable is False

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
