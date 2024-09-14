from enum import Enum
from typing import Generator, Union

import pyarrow as pa
import pyarrow.dataset as ds

from ._internal import ArrayType as ArrayType
from ._internal import Field as Field
from ._internal import MapType as MapType
from ._internal import PrimitiveType as PrimitiveType
from ._internal import Schema as Schema
from ._internal import StructType as StructType

# Can't implement inheritance (see note in src/schema.rs), so this is next
# best thing.
DataType = Union["PrimitiveType", "MapType", "StructType", "ArrayType"]


class ArrowSchemaConversionMode(Enum):
    NORMAL = "NORMAL"
    LARGE = "LARGE"
    PASSTHROUGH = "PASSTHROUGH"

    @classmethod
    def from_str(cls, value: str) -> "ArrowSchemaConversionMode":
        try:
            return cls(value.upper())
        except ValueError:
            raise ValueError(
                f"{value} is not a valid ArrowSchemaConversionMode. Valid values are: {[item.value for item in ArrowSchemaConversionMode]}"
            )


### Inspired from Pola-rs repo - licensed with MIT License, see license in python/licenses/polars_license.txt.###
def _convert_pa_schema_to_delta(
    schema: pa.schema,
    schema_conversion_mode: ArrowSchemaConversionMode = ArrowSchemaConversionMode.NORMAL,
) -> pa.schema:
    """Convert a PyArrow schema to a schema compatible with Delta Lake. Converts unsigned to signed equivalent, and
    converts all timestamps to `us` timestamps. With the boolean flag large_dtypes you can control if the schema
    should keep cast normal to large types in the schema, or from large to normal.

    Args
        schema: Source schema
        schema_conversion_mode: large mode will cast all string/binary/list to the large version arrow types, normal mode
            keeps the normal version of the types. Passthrough mode keeps string/binary/list flavored types in their original
            version, whether that is view/large/normal.
    """
    dtype_map = {
        pa.uint8(): pa.int8(),
        pa.uint16(): pa.int16(),
        pa.uint32(): pa.int32(),
        pa.uint64(): pa.int64(),
    }
    if schema_conversion_mode == ArrowSchemaConversionMode.LARGE:
        dtype_map = {
            **dtype_map,
            **{
                pa.string(): pa.large_string(),
                pa.string_view(): pa.large_string(),
                pa.binary(): pa.large_binary(),
                pa.binary_view(): pa.large_binary(),
            },
        }
    elif schema_conversion_mode == ArrowSchemaConversionMode.NORMAL:
        dtype_map = {
            **dtype_map,
            **{
                pa.large_string(): pa.string(),
                pa.string_view(): pa.string(),
                pa.large_binary(): pa.binary(),
                pa.binary_view(): pa.binary(),
            },
        }

    def dtype_to_delta_dtype(dtype: pa.DataType) -> pa.DataType:
        # Handle nested types
        if isinstance(
            dtype,
            (
                pa.LargeListType,
                pa.ListType,
                pa.FixedSizeListType,
                pa.ListViewType,
                pa.LargeListViewType,
            ),
        ):
            return list_to_delta_dtype(dtype)
        elif isinstance(dtype, pa.StructType):
            return struct_to_delta_dtype(dtype)
        elif isinstance(dtype, pa.TimestampType):
            if dtype.tz is None:
                return pa.timestamp("us")
            else:
                return pa.timestamp("us", "UTC")
        elif type(dtype) is pa.FixedSizeBinaryType:
            return pa.binary()
        elif isinstance(dtype, pa.ExtensionType):
            return dtype.storage_type
        try:
            return dtype_map[dtype]
        except KeyError:
            return dtype

    def list_to_delta_dtype(
        dtype: Union[
            pa.LargeListType,
            pa.ListType,
            pa.ListViewType,
            pa.LargeListViewType,
            pa.FixedSizeListType,
        ],
    ) -> Union[pa.LargeListType, pa.ListType]:
        nested_dtype = dtype.value_type
        nested_dtype_cast = dtype_to_delta_dtype(nested_dtype)
        if schema_conversion_mode == ArrowSchemaConversionMode.LARGE:
            return pa.large_list(nested_dtype_cast)
        elif schema_conversion_mode == ArrowSchemaConversionMode.NORMAL:
            return pa.list_(nested_dtype_cast)
        elif schema_conversion_mode == ArrowSchemaConversionMode.PASSTHROUGH:
            if isinstance(dtype, pa.LargeListType):
                return pa.large_list(nested_dtype_cast)
            elif isinstance(dtype, pa.ListType):
                return pa.list_(nested_dtype_cast)
            elif isinstance(dtype, pa.FixedSizeListType):
                return pa.list_(nested_dtype_cast)
            elif isinstance(dtype, pa.LargeListViewType):
                return pa.large_list_view(nested_dtype_cast)
            elif isinstance(dtype, pa.ListViewType):
                return pa.list_view(nested_dtype_cast)
            else:
                raise NotImplementedError
        else:
            raise NotImplementedError

    def struct_to_delta_dtype(dtype: pa.StructType) -> pa.StructType:
        fields = [dtype[i] for i in range(dtype.num_fields)]
        fields_cast = [f.with_type(dtype_to_delta_dtype(f.type)) for f in fields]
        return pa.struct(fields_cast)

    return pa.schema([f.with_type(dtype_to_delta_dtype(f.type)) for f in schema])


def _cast_schema_to_recordbatchreader(
    reader: pa.RecordBatchReader, schema: pa.schema
) -> Generator[pa.RecordBatch, None, None]:
    """Creates recordbatch generator."""
    for batch in reader:
        batchs = pa.Table.from_batches([batch]).cast(schema).to_batches()
        if len(batchs) > 0:
            yield batchs[0]


def convert_pyarrow_recordbatchreader(
    data: pa.RecordBatchReader, schema_conversion_mode: ArrowSchemaConversionMode
) -> pa.RecordBatchReader:
    """Converts a PyArrow RecordBatchReader to a PyArrow RecordBatchReader with a compatible delta schema"""
    schema = _convert_pa_schema_to_delta(
        data.schema, schema_conversion_mode=schema_conversion_mode
    )

    data = pa.RecordBatchReader.from_batches(
        schema,
        _cast_schema_to_recordbatchreader(data, schema),
    )
    return data


def convert_pyarrow_recordbatch(
    data: pa.RecordBatch, schema_conversion_mode: ArrowSchemaConversionMode
) -> pa.RecordBatchReader:
    """Converts a PyArrow RecordBatch to a PyArrow RecordBatchReader with a compatible delta schema"""
    schema = _convert_pa_schema_to_delta(
        data.schema, schema_conversion_mode=schema_conversion_mode
    )
    data = pa.Table.from_batches([data]).cast(schema).to_reader()
    return data


def convert_pyarrow_table(
    data: pa.Table, schema_conversion_mode: ArrowSchemaConversionMode
) -> pa.RecordBatchReader:
    """Converts a PyArrow table to a PyArrow RecordBatchReader with a compatible delta schema"""
    schema = _convert_pa_schema_to_delta(
        data.schema, schema_conversion_mode=schema_conversion_mode
    )
    data = data.cast(schema).to_reader()
    return data


def convert_pyarrow_dataset(
    data: ds.Dataset, schema_conversion_mode: ArrowSchemaConversionMode
) -> pa.RecordBatchReader:
    """Converts a PyArrow dataset to a PyArrow RecordBatchReader with a compatible delta schema"""
    data = data.scanner().to_reader()
    data = convert_pyarrow_recordbatchreader(
        data, schema_conversion_mode=schema_conversion_mode
    )
    return data
