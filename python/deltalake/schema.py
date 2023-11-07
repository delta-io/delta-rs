from typing import TYPE_CHECKING, Union

import pyarrow as pa

if TYPE_CHECKING:
    pass

from ._internal import ArrayType as ArrayType
from ._internal import Field as Field
from ._internal import MapType as MapType
from ._internal import PrimitiveType as PrimitiveType
from ._internal import Schema as Schema
from ._internal import StructType as StructType

# Can't implement inheritance (see note in src/schema.rs), so this is next
# best thing.
DataType = Union["PrimitiveType", "MapType", "StructType", "ArrayType"]


### Inspired from Pola-rs repo - licensed with MIT License, see licenses folder.###
def _convert_pa_schema_to_delta(
    schema: pa.schema, large_dtypes: bool = False
) -> pa.schema:
    """Convert a PyArrow schema to a schema compatible with Delta Lake. Converts unsigned to signed equivalent, and
    converts all timestamps to `us` timestamps. With the boolean flag large_dtypes you can control if the schema
    should keep cast normal to large types in the schema, or from large to normal.

    Args
        schema: Source schema
        large_dtypes: If True, the pyarrow schema is casted to large_dtypes
    """
    dtype_map = {
        pa.uint8(): pa.int8(),
        pa.uint16(): pa.int16(),
        pa.uint32(): pa.int32(),
        pa.uint64(): pa.int64(),
    }
    if large_dtypes:
        dtype_map = {
            **dtype_map,
            **{pa.string(): pa.large_string(), pa.binary(): pa.large_binary()},
        }
    else:
        dtype_map = {
            **dtype_map,
            **{pa.large_string(): pa.string(), pa.large_binary(): pa.binary()},
        }

    def dtype_to_delta_dtype(dtype: pa.DataType) -> pa.DataType:
        # Handle nested types
        if isinstance(dtype, (pa.LargeListType, pa.ListType)):
            return list_to_delta_dtype(dtype)
        elif isinstance(dtype, pa.StructType):
            return struct_to_delta_dtype(dtype)
        elif isinstance(dtype, pa.TimestampType):
            return pa.timestamp("us")
        try:
            return dtype_map[dtype]
        except KeyError:
            return dtype

    def list_to_delta_dtype(
        dtype: Union[pa.LargeListType, pa.ListType],
    ) -> Union[pa.LargeListType, pa.ListType]:
        nested_dtype = dtype.value_type
        nested_dtype_cast = dtype_to_delta_dtype(nested_dtype)
        if large_dtypes:
            return pa.large_list(nested_dtype_cast)
        else:
            return pa.list_(nested_dtype_cast)

    def struct_to_delta_dtype(dtype: pa.StructType) -> pa.StructType:
        fields = [dtype.field(i) for i in range(dtype.num_fields)]
        fields_cast = [pa.field(f.name, dtype_to_delta_dtype(f.type)) for f in fields]
        return pa.struct(fields_cast)

    return pa.schema([pa.field(f.name, dtype_to_delta_dtype(f.type)) for f in schema])


def convert_pyarrow_recordbatchreader(
    data: pa.RecordBatchReader, large_dtypes: bool
) -> pa.RecordBatchReader:
    """Converts a PyArrow RecordBatchReader to a PyArrow RecordBatchReader with a compatible delta schema"""
    schema = _convert_pa_schema_to_delta(data.schema, large_dtypes=large_dtypes)
    data = pa.RecordBatchReader.from_batches(
        schema,
        data.read_all().cast(schema).to_batches(),
    )
    return data, schema


def convert_pyarrow_recordbatch(
    data: pa.RecordBatch, large_dtypes: bool
) -> pa.RecordBatchReader:
    """Converts a PyArrow RecordBatch to a PyArrow RecordBatchReader with a compatible delta schema"""
    schema = _convert_pa_schema_to_delta(data.schema, large_dtypes=large_dtypes)
    data = pa.Table.from_batches(data).cast(schema).to_batches()
    return data, schema


def convert_pyarrow_table(
    data: pa.RecordBatch, large_dtypes: bool
) -> pa.RecordBatchReader:
    """Converts a PyArrow table to a PyArrow RecordBatchReader with a compatible delta schema"""
    schema = _convert_pa_schema_to_delta(data.schema, large_dtypes=large_dtypes)
    data = data.cast(schema).to_reader()
    return data, schema
