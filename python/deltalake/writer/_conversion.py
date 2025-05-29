from __future__ import annotations

from arro3.core import DataType
from arro3.core import Schema as Arro3Schema


def _convert_arro3_schema_to_delta(
    schema: Arro3Schema,
) -> Arro3Schema:
    """Convert a arro3 schema to a schema compatible with Delta Lake. Converts unsigned to signed equivalent, and
    converts all timestamps to `us` timestamps.
    Args
        schema: Source schema
    """

    def dtype_to_delta_dtype(dtype: DataType) -> DataType:
        # Handle nested types
        if (
            DataType.is_list(dtype)
            or DataType.is_large_list(dtype)
            or DataType.is_fixed_size_list(dtype)
            or DataType.is_list_view(dtype)
            or DataType.is_large_list_view(dtype)
        ):
            return list_to_delta_dtype(dtype)
        elif DataType.is_struct(dtype):
            return struct_to_delta_dtype(dtype)
        elif DataType.is_timestamp(dtype):
            if dtype.tz is None:
                return DataType.timestamp("us")
            else:
                return DataType.timestamp("us", tz="UTC")
        elif DataType.is_fixed_size_binary(dtype):
            return DataType.binary()
        elif DataType.is_unsigned_integer(dtype):
            if DataType.is_uint16(dtype):
                return DataType.int16()
            elif DataType.is_uint32(dtype):
                return DataType.int32()
            elif DataType.is_uint64(dtype):
                return DataType.int64()
            elif DataType.is_uint8(dtype):
                return DataType.int8()
            else:
                raise NotImplementedError
        else:
            return dtype

    def list_to_delta_dtype(
        dtype: DataType,
    ) -> DataType:
        nested_dtype = dtype.value_type
        inner_field = dtype.value_field

        assert nested_dtype is not None
        assert inner_field is not None

        inner_field_casted = inner_field.with_type(dtype_to_delta_dtype(nested_dtype))

        if DataType.is_large_list(dtype):
            return DataType.large_list(inner_field_casted)
        elif DataType.is_fixed_size_list(dtype):
            return DataType.list(inner_field_casted, dtype.list_size)
        elif DataType.is_large_list_view(dtype):
            return DataType.large_list_view(inner_field_casted)
        elif DataType.is_list_view(dtype):
            return DataType.list_view(inner_field_casted)
        elif DataType.is_list(dtype):
            return DataType.list(inner_field_casted)
        else:
            raise NotImplementedError

    def struct_to_delta_dtype(dtype: DataType) -> DataType:
        fields_cast = [f.with_type(dtype_to_delta_dtype(f.type)) for f in dtype.fields]
        return DataType.struct(fields_cast)

    return Arro3Schema([f.with_type(dtype_to_delta_dtype(f.type)) for f in schema])  # type: ignore[attr-defined]
