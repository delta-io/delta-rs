import json
from typing import Dict, List, Any
from enum import Enum


class DeltaTableSchemaFormat(Enum):
    DELTA = "DELTA"
    ARROW = "ARROW"


class DeltaTableField:
    def __init__(
        self,
        name: str,
        type: str,
        nullable: bool,
        metadata: Dict[str, str],
        format: DeltaTableSchemaFormat,
    ):
        type = json.loads(type)
        if format == DeltaTableSchemaFormat.ARROW:
            self.type = DataType.from_arrow_json(json_dict=type)
        else:
            self.type = DataType.from_delta_json(json_dict=type)
        self.name = name
        self.nullable = nullable
        self.metadata = metadata

    def __str__(self):
        return f'{self.name}: {self.type} {self.nullable} {self.metadata if self.metadata else ""}'


class DeltaTableSchema:
    def __init__(self, fields: List[DeltaTableField]):
        self.fields = fields

    def __str__(self):
        for field in self.fields:
            print(field)
        return ""

    def __repr__(self):
        return self.__str__()


class DataType:
    @classmethod
    def from_delta_json(cls, json_dict: Dict[str, Any]):
        name = json_dict["name"]
        if name == "map":
            key_type = json_dict["keyType"]
            value_type = json_dict["valueType"]
            key_type = cls.from_delta_json(json_dict=key_type)
            value_type = cls.from_delta_json(json_dict=value_type)
            return MapType(
                name=name,
                key_type=key_type,
                value_type=value_type,
            )
        if name == "array":
            field = json_dict["elementType"]
            element_type = cls.from_delta_json(json_dict=field)
            return ArrayType(
                name=name,
                element_type=element_type,
                contains_null=json_dict["containsNull"],
            )
        if name == "struct":
            fields = json_dict["fields"]
            fields = [
                DeltaTableField(
                    name=field["name"],
                    type=json.dumps(field["type"]),
                    nullable=field["nullable"],
                    metadata=field["metadata"],
                    format=DeltaTableSchemaFormat.DELTA,
                )
                for field in fields
            ]
            return StructType(name=name, fields=fields)

        return name

    @classmethod
    def from_arrow_json(cls, json_dict: Dict[str, Any]):
        name = json_dict["type"]["name"]
        if name == "dictionary":
            key_type = json_dict["dictionary"]["indexType"]
            value_type = json_dict["children"][0]
            key_type = cls.from_arrow_json(json_dict=key_type)
            value_type = cls.from_arrow_json(json_dict=value_type)
            return MapType(
                name=json_dict["name"],
                key_type=key_type,
                value_type=value_type,
            )
        if name == "list":
            field = json_dict["children"][0]
            element_type = cls.from_arrow_json(json_dict=field)
            return ArrayType(
                name=json_dict["name"],
                element_type=element_type,
                contains_null=json_dict["nullable"],
            )
        if name == "struct":
            fields = json_dict["children"]
            fields = [
                DeltaTableField(
                    name=field["name"],
                    type=json.dumps(field["type"]),
                    nullable=field["nullable"],
                    metadata=field["metadata"],
                    format=DeltaTableSchemaFormat.ARROW,
                )
                for field in fields
            ]
            return StructType(name=json_dict["name"], fields=fields)

        if name == "int" or name == "float":
            return f'{name}{json_dict["type"]["bitWidth"]}'
        else:
            return name


class MapType(DataType):
    def __init__(self, name: str, key_type: str, value_type: str):
        self.name = name
        self.type = "map"
        self.key_type = key_type
        self.value_type = value_type

    def __str__(self):
        return f"{self.name}: {self.type}<{self.key_type}, {self.value_type}>"

    def __repr__(self):
        return self.__str__()


class ArrayType(DataType):
    def __init__(self, name: str, element_type: DataType, contains_null: bool):
        self.name = name
        self.type = "array"
        self.element_type = element_type
        self.contains_null = contains_null

    def __str__(self):
        return f"{self.name} {self.type}<{self.element_type}> {self.contains_null}"

    def __repr__(self):
        return self.__str__()


class StructType(DataType):
    def __init__(self, name: str, fields: List[DataType]):
        self.name = name
        self.type = "struct"
        self.fields = fields

    def __str__(self):
        return f"{self.name} {self.type}<{self.fields}>"

    def __repr__(self):
        return self.__str__()
