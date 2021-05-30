import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pyarrow

# TODO: implement this module in Rust land to avoid JSON serialization
# https://github.com/delta-io/delta-rs/issues/95


@dataclass
class DataType:
    """
    Base class of all Delta data types.
    """

    type: str

    def __str__(self) -> str:
        return f"DataType({self.type})"

    @classmethod
    def from_dict(cls, json_dict: Dict[str, Any]) -> "DataType":
        """
        Generate a DataType from a DataType in json format.

        :param json_dict: the data type in json format
        :return: the Delta DataType
        """
        type_class = json_dict["type"]
        if type_class == "map":
            key_type_dict = {"type": json_dict["keyType"]}
            value_type_dict = {"type": json_dict["valueType"]}
            value_contains_null = json_dict["valueContainsNull"]
            key_type = cls.from_dict(json_dict=key_type_dict)
            value_type = cls.from_dict(json_dict=value_type_dict)
            return MapType(
                key_type=key_type,
                value_type=value_type,
                value_contains_null=value_contains_null,
            )
        if type_class == "array":
            field = json_dict["elementType"]
            if isinstance(field, str):
                element_type = cls(field)
            else:
                element_type = cls.from_dict(json_dict=field)
            return ArrayType(
                element_type=element_type,
                contains_null=json_dict["containsNull"],
            )
        if type_class == "struct":
            fields = []
            for json_field in json_dict["fields"]:
                if isinstance(json_field["type"], str):
                    data_type = cls(json_field["type"])
                else:
                    data_type = cls.from_dict(json_field["type"])
                field = Field(
                    name=json_field["name"],
                    type=data_type,
                    nullable=json_field["nullable"],
                    metadata=json_field.get("metadata"),
                )
                fields.append(field)
            return StructType(fields=fields)

        return DataType(type_class)


@dataclass(init=False)
class MapType(DataType):
    """Concrete class for map data types."""

    key_type: DataType
    value_type: DataType
    value_contains_null: bool
    type: str

    def __init__(
        self, key_type: "DataType", value_type: "DataType", value_contains_null: bool
    ):
        super().__init__("map")
        self.key_type = key_type
        self.value_type = value_type
        self.value_contains_null = value_contains_null

    def __str__(self) -> str:
        return f"DataType(map<{self.key_type}, {self.value_type}, {self.value_contains_null}>)"


@dataclass(init=False)
class ArrayType(DataType):
    """Concrete class for array data types."""

    element_type: DataType
    contains_null: bool
    type: str

    def __init__(self, element_type: DataType, contains_null: bool):
        super().__init__("array")
        self.element_type = element_type
        self.contains_null = contains_null

    def __str__(self) -> str:
        return f"DataType(array<{self.element_type}> {self.contains_null})"


@dataclass(init=False)
class StructType(DataType):
    """Concrete class for struct data types."""

    fields: List["Field"]
    type: str

    def __init__(self, fields: List["Field"]):
        super().__init__("struct")
        self.fields = fields

    def __str__(self) -> str:
        field_strs = [str(f) for f in self.fields]
        return f"DataType(struct<{', '.join(field_strs)}>)"


@dataclass
class Field:
    """Create a DeltaTable Field instance."""

    name: str
    type: DataType
    nullable: bool
    metadata: Optional[Dict[str, str]] = None

    def __str__(self) -> str:
        return f"Field({self.name}: {self.type} nullable({self.nullable}) metadata({self.metadata}))"


@dataclass
class Schema:
    """Create a DeltaTable Schema instance."""

    fields: List[Field]
    json_value: Dict[str, Any]

    def __str__(self) -> str:
        field_strs = [str(f) for f in self.fields]
        return f"Schema({', '.join(field_strs)})"

    def json(self) -> Dict[str, Any]:
        return self.json_value

    @classmethod
    def from_json(cls, json_data: str) -> "Schema":
        """
        Generate a DeltaTable Schema from a json format.

        :param json_data: the schema in json format
        :return: the DeltaTable schema
        """
        json_value = json.loads(json_data)
        fields = []
        for json_field in json_value["fields"]:
            if isinstance(json_field["type"], str):
                data_type = DataType(json_field["type"])
            else:
                data_type = DataType.from_dict(json_field["type"])
            field = Field(
                name=json_field["name"],
                type=data_type,
                nullable=json_field["nullable"],
                metadata=json_field.get("metadata"),
            )
            fields.append(field)
        return cls(fields=fields, json_value=json_value)


def pyarrow_datatype_from_dict(json_dict: Dict[str, Any]) -> pyarrow.DataType:
    """
    Create a DataType in PyArrow format from a Schema json format.

    :param json_dict: the DataType in json format
    :return: the DataType in PyArrow format
    """
    type_class = json_dict["type"]["name"]
    if type_class == "dictionary":
        key_type = json_dict["dictionary"]["indexType"]
        value_type = json_dict["children"][0]
        key_type = pyarrow_datatype_from_dict(key_type)
        value_type = pyarrow_datatype_from_dict(value_type)
        return pyarrow.map_(key_type, value_type)
    elif "dictionary" in json_dict:
        key_type = {
            "name": "key",
            "type": json_dict["dictionary"]["indexType"],
            "nullable": json_dict["nullable"],
        }
        key = pyarrow_datatype_from_dict(key_type)
        if type_class == "list":
            value_type = {
                "name": "val",
                "type": json_dict["dictionary"]["indexType"],
                "nullable": json_dict["nullable"],
            }
            return pyarrow.map_(
                key,
                pyarrow.list_(
                    pyarrow.field(
                        "element", pyarrow.struct([pyarrow_field_from_dict(value_type)])
                    )
                ),
            )
        value_type = {
            "name": "value",
            "type": json_dict["type"],
            "nullable": json_dict["nullable"],
        }
        return pyarrow.map_(key, pyarrow_datatype_from_dict(value_type))
    elif type_class == "list":
        field = json_dict["children"][0]
        element_type = pyarrow_datatype_from_dict(field)
        return pyarrow.list_(pyarrow.field("element", element_type))
    elif type_class == "struct":
        fields = [pyarrow_field_from_dict(field) for field in json_dict["children"]]
        return pyarrow.struct(fields)
    elif type_class == "int":
        return pyarrow.type_for_alias(f'{type_class}{json_dict["type"]["bitWidth"]}')
    elif type_class == "date":
        type_info = json_dict["type"]
        if type_info["unit"] == "DAY":
            return pyarrow.date32()
        else:
            return pyarrow.date64()
    elif type_class == "time":
        type_info = json_dict["type"]
        if type_info["unit"] == "MICROSECOND":
            unit = "us"
        elif type_info["unit"] == "NANOSECOND":
            unit = "ns"
        elif type_info["unit"] == "MILLISECOND":
            unit = "ms"
        else:
            unit = "s"
        return pyarrow.type_for_alias(f'{type_class}{type_info["bitWidth"]}[{unit}]')
    elif type_class == "timestamp":
        type_info = json_dict["type"]
        if "unit" in type_info:
            if type_info["unit"] == "MICROSECOND":
                unit = "us"
            elif type_info["unit"] == "NANOSECOND":
                unit = "ns"
            elif type_info["unit"] == "MILLISECOND":
                unit = "ms"
            elif type_info["unit"] == "SECOND":
                unit = "s"
        else:
            unit = "ns"
        return pyarrow.type_for_alias(f"{type_class}[{unit}]")
    elif type_class.startswith("decimal"):
        type_info = json_dict["type"]
        return pyarrow.decimal128(
            precision=type_info["precision"], scale=type_info["scale"]
        )
    elif type_class.startswith("floatingpoint"):
        type_info = json_dict["type"]
        if type_info["precision"] == "HALF":
            return pyarrow.float16()
        elif type_info["precision"] == "SINGLE":
            return pyarrow.float32()
        elif type_info["precision"] == "DOUBLE":
            return pyarrow.float64()
    else:
        return pyarrow.type_for_alias(type_class)


def pyarrow_field_from_dict(field: Dict[str, Any]) -> pyarrow.Field:
    """
    Create a Field in PyArrow format from a Field in json format.
    :param field: the field in json format
    :return: the Field in PyArrow format
    """
    return pyarrow.field(
        field["name"],
        pyarrow_datatype_from_dict(field),
        field["nullable"],
        field.get("metadata"),
    )


def pyarrow_schema_from_json(json_data: str) -> pyarrow.Schema:
    """
    Create a Schema in PyArrow format from a Schema in json format.

    :param json_data: the field in json format
    :return: the Schema in PyArrow format
    """
    schema_json = json.loads(json_data)
    arrow_fields = [pyarrow_field_from_dict(field) for field in schema_json["fields"]]
    return pyarrow.schema(arrow_fields)
