from typing import Union

from deltalake._internal import ArrayType as ArrayType
from deltalake._internal import Field as Field
from deltalake._internal import MapType as MapType
from deltalake._internal import PrimitiveType as PrimitiveType
from deltalake._internal import Schema as Schema
from deltalake._internal import StructType as StructType

# Can't implement inheritance (see note in src/schema.rs), so this is next
# best thing.
DataType = Union["PrimitiveType", "MapType", "StructType", "ArrayType"]
