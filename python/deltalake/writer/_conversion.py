from __future__ import annotations

from arro3.core import DataType
from arro3.core import Schema as Arro3Schema


def _convert_arro3_schema_to_delta(
    schema: Arro3Schema,
    existing_schema: Arro3Schema | None = None,
) -> Arro3Schema:
    """Convert a arro3 schema to a schema compatible with Delta Lake. Converts unsigned to signed equivalent, and
    converts all timestamps to `us` timestamps. Also handles null column types by converting them to match
    corresponding fields in the existing table schema.

    Args:
        schema (Arro3Schema): Source schema.
        existing_schema (Arro3Schema, optional): Existing table schema to match null types against. Defaults to None.

    Returns:
        Arro3Schema: Delta-compatible schema with converted types.
    """

    def dtype_to_delta_dtype(
        dtype: DataType, field_name: str | None = None
    ) -> DataType:
        if DataType.is_null(dtype) and existing_schema is not None and field_name is not None:
            try:
                existing_field = existing_schema.field(field_name)
                # Prevent infinite recursion: if existing field is also null, keep as null
                if DataType.is_null(existing_field.type):
                    return dtype
                return dtype_to_delta_dtype(existing_field.type, None)
            except (KeyError, IndexError):
                return dtype

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

        inner_field_casted = inner_field.with_type(
            dtype_to_delta_dtype(nested_dtype, None)
        )

        if DataType.is_large_list(dtype):
            return DataType.large_list(inner_field_casted)
        elif DataType.is_fixed_size_list(dtype):
            # Fixed sized lists can come in from polars via their Array type.
            # These may carry array field names of "item" rather than "element"
            # which is expected everywhere else. Converting the field name and
            # then passing the field through for further casting in Rust will
            # accommodate this
            #
            # See also: <https://github.com/delta-io/delta-rs/issues/3566>
            if inner_field_casted.name == "item":
                inner_field_casted = inner_field_casted.with_name("element")
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
        fields_cast = [
            f.with_type(dtype_to_delta_dtype(f.type, f.name)) for f in dtype.fields
        ]
        return DataType.struct(fields_cast)

    return Arro3Schema(
        [f.with_type(dtype_to_delta_dtype(f.type, f.name)) for f in schema]  # type: ignore[attr-defined]
    )
