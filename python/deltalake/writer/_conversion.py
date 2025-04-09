from __future__ import annotations

from collections.abc import Generator, Iterable
from enum import Enum
from typing import Protocol

import pyarrow as pa
import pyarrow.dataset as ds

try:
    import pandas as pd
except ModuleNotFoundError:
    _has_pandas = False
else:
    _has_pandas = True

from deltalake._internal import Schema


class ArrowSchemaConversionMode(Enum):
    NORMAL = "NORMAL"
    LARGE = "LARGE"
    PASSTHROUGH = "PASSTHROUGH"

    @classmethod
    def from_str(cls, value: str) -> ArrowSchemaConversionMode:
        try:
            return cls(value.upper())
        except ValueError:
            raise ValueError(
                f"{value} is not a valid ArrowSchemaConversionMode. Valid values are: {[item.value for item in ArrowSchemaConversionMode]}"
            )


class ArrowStreamExportable(Protocol):
    """Type hint for object exporting Arrow C Stream via Arrow PyCapsule Interface.

    https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
    """

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...


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
        dtype: pa.LargeListType
        | pa.ListType
        | pa.ListViewType
        | pa.LargeListViewType
        | pa.FixedSizeListType,
    ) -> pa.LargeListType | pa.ListType:
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


def _convert_data_and_schema(
    data: pd.DataFrame
    | ds.Dataset
    | pa.Table
    | pa.RecordBatch
    | Iterable[pa.RecordBatch]
    | pa.RecordBatchReader
    | ArrowStreamExportable,
    schema: pa.Schema | Schema | None,
    conversion_mode: ArrowSchemaConversionMode,
) -> tuple[pa.RecordBatchReader, pa.Schema]:
    if isinstance(data, pa.RecordBatchReader):
        data = convert_pyarrow_recordbatchreader(data, conversion_mode)
    elif isinstance(data, pa.RecordBatch):
        data = convert_pyarrow_recordbatch(data, conversion_mode)
    elif isinstance(data, pa.Table):
        data = convert_pyarrow_table(data, conversion_mode)
    elif isinstance(data, ds.Dataset):
        data = convert_pyarrow_dataset(data, conversion_mode)
    elif _has_pandas and isinstance(data, pd.DataFrame):
        if schema is not None:
            data = convert_pyarrow_table(
                pa.Table.from_pandas(data, schema=schema), conversion_mode
            )
        else:
            data = convert_pyarrow_table(pa.Table.from_pandas(data), conversion_mode)
    elif hasattr(data, "__arrow_c_array__"):
        data = convert_pyarrow_recordbatch(
            pa.record_batch(data),  # type:ignore[attr-defined]
            conversion_mode,
        )
    elif hasattr(data, "__arrow_c_stream__"):
        if not hasattr(pa.RecordBatchReader, "from_stream"):
            raise ValueError(
                "pyarrow 15 or later required to read stream via pycapsule interface"
            )

        data = convert_pyarrow_recordbatchreader(
            pa.RecordBatchReader.from_stream(data), conversion_mode
        )
    elif isinstance(data, Iterable):
        if schema is None:
            raise ValueError("You must provide schema if data is Iterable")
    else:
        raise TypeError(
            f"{type(data).__name__} is not a valid input. Only PyArrow RecordBatchReader, RecordBatch, Iterable[RecordBatch], Table, Dataset or Pandas DataFrame or objects implementing the Arrow PyCapsule Interface are valid inputs for source."
        )
    from deltalake.schema import Schema

    if (
        isinstance(schema, Schema)
        and conversion_mode == ArrowSchemaConversionMode.PASSTHROUGH
    ):
        raise NotImplementedError(
            "ArrowSchemaConversionMode.passthrough is not implemented to work with DeltaSchema, skip passing a schema or pass an arrow schema."
        )
    elif isinstance(schema, Schema):
        if conversion_mode == ArrowSchemaConversionMode.LARGE:
            schema = schema.to_pyarrow(as_large_types=True)
        else:
            schema = schema.to_pyarrow(as_large_types=False)
    elif schema is None:
        schema = data.schema

    return data, schema
