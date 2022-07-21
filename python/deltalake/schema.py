from typing import Union

from .deltalake import ArrayType, Field, MapType, PrimitiveType, Schema, StructType

# Can't implement inheritance (see note in src/schema.rs), so this is next
# best thing.
DataType = Union["PrimitiveType", "MapType", "StructType", "ArrayType"]
