from typing import Tuple, Union

import pandas as pd
import pyarrow as pa

from ._internal import ArrayType, Field, MapType, PrimitiveType, Schema, StructType

# Can't implement inheritance (see note in src/schema.rs), so this is next
# best thing.
DataType = Union["PrimitiveType", "MapType", "StructType", "ArrayType"]


def delta_arrow_schema_from_pandas(
    data: pd.DataFrame,
) -> Tuple[pd.DataFrame, pa.Schema]:
    """ "
    Infers the schema for the delta table from the Pandas DataFrame.
    Necessary because of issues such as:  https://github.com/delta-io/delta-rs/issues/686

    :param data: Data to write.
    :returns A Pyarrow Table and the inferred schema for the Delta Table
    """

    table = pa.Table.from_pandas(data)
    _schema = table.schema
    schema_out = []
    for _field in _schema:
        if isinstance(_field.type, pa.TimestampType):
            f = pa.field(
                name=_field.name,
                type=pa.timestamp("us"),
                nullable=_field.nullable,
                metadata=_field.metadata,
            )
            schema_out.append(f)
        else:
            schema_out.append(_field)
    schema = pa.schema(schema_out, metadata=_schema.metadata)
    data = pa.Table.from_pandas(data, schema=schema)
    return data, schema
