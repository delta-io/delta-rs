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
        self.name = name
        self.type = DataType.from_json(json_dict=json.loads(type), format=format)
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
    def from_json(cls, json_dict: Dict[str, Any], format: DeltaTableSchemaFormat):
        print(json_dict)
        if json_dict["name"] == "map":
            key_type = cls.from_json(json_dict=json_dict["keyType"], format=format)
            value_type = cls.from_json(json_dict=json_dict["valueType"], format=format)
            return MapType(
                name=json_dict["name"],
                key_type=key_type,
                value_type=value_type,
            )
        if json_dict["name"] == "array":
            element_type = cls.from_json(
                json_dict=json_dict["elementType"], format=format
            )
            return ArrayType(
                name=json_dict["name"],
                element_type=element_type,
                contains_null=json_dict["containsNull"],
            )
        if json_dict["name"] == "struct":
            fields = [
                DeltaTableField(
                    name=field["name"],
                    type=json.dumps(field["type"]),
                    nullable=field["nullable"],
                    format=format,
                    metadata=field["metadata"],
                )
                for field in json_dict["fields"]
            ]
            return StructType(name=json_dict["name"], fields=fields)

        return get_primitive(json=json_dict, format=format)


def get_primitive(json: Dict[str, Any], format: DeltaTableSchemaFormat) -> str:
    if format.ARROW:
        if json["name"] == "int" or json["name"] == "float":
            return f'{json["name"]}{json["bitWidth"]}'

    return json["name"]


class MapType(DataType):
    def __init__(self, name: str, key_type: str, value_type: str):
        self.name = name
        self.key_type = key_type
        self.value_type = value_type

    def __str__(self):
        return f"{self.name}: map<{self.key_type}, {self.value_type}>"

    def __repr__(self):
        return self.__str__()


class ArrayType(DataType):
    def __init__(self, name: str, element_type: DataType, contains_null: bool):
        self.name = name
        self.element_type = element_type
        self.contains_null = contains_null

    def __str__(self):
        return f"{self.name} array<{self.element_type}> {self.contains_null}"

    def __repr__(self):
        return self.__str__()


class StructType(DataType):
    def __init__(self, name: str, fields: List[DataType]):
        self.name = name
        self.fields = fields

    def __str__(self):
        return f"{self.name} {self.fields}"

    def __repr__(self):
        return self.__str__()
