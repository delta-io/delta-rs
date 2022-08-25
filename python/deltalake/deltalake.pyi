from typing import Any, Callable, Dict, List, Optional, Union

import pyarrow as pa
from typing import Literal

from deltalake.writer import AddAction

RawDeltaTable: Any
rust_core_version: Callable[[], str]
DeltaStorageFsBackend: Any

class PyDeltaTableError(BaseException): ...

write_new_deltalake: Callable[[str, pa.Schema, List[AddAction], str, List[str]], None]

# Can't implement inheritance (see note in src/schema.rs), so this is next
# best thing.
DataType = Union["PrimitiveType", "MapType", "StructType", "ArrayType"]

class PrimitiveType:
    def __init__(self, data_type: str) -> None: ...
    type: str

    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> "PrimitiveType": ...
    def to_pyarrow(self) -> pa.DataType: ...
    @staticmethod
    def from_pyarrow(type: pa.DataType) -> "PrimitiveType": ...

class ArrayType:
    def __init__(
        self, element_type: DataType, *, contains_null: bool = True
    ) -> None: ...
    type: Literal["array"]
    element_type: DataType
    contains_null: bool

    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> "ArrayType": ...
    def to_pyarrow(
        self,
    ) -> pa.ListType: ...
    @staticmethod
    def from_pyarrow(type: pa.ListType) -> "ArrayType": ...

class MapType:
    def __init__(
        self,
        key_type: DataType,
        value_type: DataType,
        *,
        value_contains_null: bool = True
    ) -> None: ...
    type: Literal["map"]
    key_type: DataType
    value_type: DataType
    value_contains_null: bool

    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> "MapType": ...
    def to_pyarrow(self) -> pa.MapType: ...
    @staticmethod
    def from_pyarrow(type: pa.MapType) -> "MapType": ...

class Field:
    def __init__(
        self,
        name: str,
        type: DataType,
        *,
        nullable: bool = True,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None: ...
    name: str
    type: DataType
    nullable: bool
    metadata: Dict[str, Any]

    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> "Field": ...
    def to_pyarrow(self) -> pa.Field: ...
    @staticmethod
    def from_pyarrow(type: pa.Field) -> "Field": ...

class StructType:
    def __init__(self, fields: List[Field]) -> None: ...
    type: Literal["struct"]
    fields: List[Field]

    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> "StructType": ...
    def to_pyarrow(self) -> pa.StructType: ...
    @staticmethod
    def from_pyarrow(type: pa.StructType) -> "StructType": ...

class Schema:
    def __init__(self, fields: List[Field]) -> None: ...
    fields: List[Field]

    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> "Schema": ...
    def to_pyarrow(self) -> pa.Schema: ...
    @staticmethod
    def from_pyarrow(type: pa.Schema) -> "Schema": ...
