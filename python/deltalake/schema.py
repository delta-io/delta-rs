from dataclasses import dataclass
from typing import Dict, Optional, Union

import pyarrow as pa

from .deltalake import ArrayType, MapType, StructType, PrimitiveType
# For now, StructType and Schema are identical
from .deltalake import StructType as Schema

# Can't implement inheritance (see note in src/schema.rs), so this is next
# best thing.
DataType = Union[PrimitiveType, MapType, StructType, ArrayType]

@dataclass
class Field:
    """Create a DeltaTable Field instance."""

    name: str
    type: DataType
    nullable: bool
    metadata: Optional[Dict[str, str]] = None

    def __str__(self) -> str:
        return f"Field({self.name}: {self.type} nullable({self.nullable}) metadata({self.metadata}))"

    def to_pyarrow(self) -> pa.Field:
        return pa.field(
            name=self.name,
            type=self.type.to_pyarrow(),
            nullable=self.nullable,
            metadata=self.metadata,
        )

    @classmethod
    def from_pyarrow(cls, field: pa.Field) -> "Field":
        if isinstance(field.type, pa.StructType):
            data_type = StructType.from_pyarrow(field.type)
        elif isinstance(field.type, pa.ListType):
            data_type = ArrayType.from_pyarrow(field.type)
        elif isinstance(field.type, pa.MapType):
            data_type = MapType.from_pyarrow(field.type)
        else:
            data_type = PrimitiveType.from_pyarrow(field.type)

        return cls(
            name=field.name,
            type=data_type,
            nullable=field.nullable,
            metadata=field.metadata,
        )

class Schema(StructType):

    def __str__(self) -> str:
        pass

    def to_pyarrow(self) -> pa.Schema:
        return pa.schema(list(super().to_pyarrow()))

    @classmethod
    def from_pyarrow(cls, schema: pa.Schema) -> "Schema":
        return super().from_pyarrow(list(schema))
