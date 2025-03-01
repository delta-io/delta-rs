import json
from datetime import date, datetime
from decimal import Decimal
from math import inf
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    Union,
    overload,
)
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatchReader

from deltalake import Schema as DeltaSchema
from deltalake._internal import convert_to_deltalake as _convert_to_deltalake
from deltalake._internal import (
    get_num_idx_cols_and_stats_columns as get_num_idx_cols_and_stats_columns,
)
from deltalake._internal import write_to_deltalake as write_deltalake_rust
from deltalake.exceptions import TableNotFoundError
from deltalake.schema import (
    ArrowSchemaConversionMode,
    convert_pyarrow_dataset,
    convert_pyarrow_recordbatch,
    convert_pyarrow_recordbatchreader,
    convert_pyarrow_table,
)
from deltalake.table import (
    DeltaTable,
    WriterProperties,
)
from deltalake.transaction import (
    CommitProperties,
    PostCommitHookProperties,
)

try:
    import pandas as pd
except ModuleNotFoundError:
    _has_pandas = False
else:
    _has_pandas = True

DEFAULT_DATA_SKIPPING_NUM_INDEX_COLS = 32

DTYPE_MAP = {
    pa.large_string(): pa.string(),
}


class ArrowStreamExportable(Protocol):
    """Type hint for object exporting Arrow C Stream via Arrow PyCapsule Interface.

    https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
    """

    def __arrow_c_stream__(
        self, requested_schema: Optional[object] = None
    ) -> object: ...


@overload
def write_deltalake(
    table_or_uri: Union[str, Path, DeltaTable],
    data: Union[
        "pd.DataFrame",
        ds.Dataset,
        pa.Table,
        pa.RecordBatch,
        Iterable[pa.RecordBatch],
        RecordBatchReader,
        ArrowStreamExportable,
    ],
    *,
    schema: Optional[Union[pa.Schema, DeltaSchema]] = ...,
    partition_by: Optional[Union[List[str], str]] = ...,
    mode: Literal["error", "append", "ignore"] = ...,
    name: Optional[str] = ...,
    description: Optional[str] = ...,
    configuration: Optional[Mapping[str, Optional[str]]] = ...,
    schema_mode: Optional[Literal["merge", "overwrite"]] = ...,
    storage_options: Optional[Dict[str, str]] = ...,
    target_file_size: Optional[int] = ...,
    writer_properties: WriterProperties = ...,
    post_commithook_properties: Optional[PostCommitHookProperties] = ...,
    commit_properties: Optional[CommitProperties] = ...,
) -> None: ...


@overload
def write_deltalake(
    table_or_uri: Union[str, Path, DeltaTable],
    data: Union[
        "pd.DataFrame",
        ds.Dataset,
        pa.Table,
        pa.RecordBatch,
        Iterable[pa.RecordBatch],
        RecordBatchReader,
        ArrowStreamExportable,
    ],
    *,
    schema: Optional[Union[pa.Schema, DeltaSchema]] = ...,
    partition_by: Optional[Union[List[str], str]] = ...,
    mode: Literal["overwrite"],
    name: Optional[str] = ...,
    description: Optional[str] = ...,
    configuration: Optional[Mapping[str, Optional[str]]] = ...,
    schema_mode: Optional[Literal["merge", "overwrite"]] = ...,
    storage_options: Optional[Dict[str, str]] = ...,
    predicate: Optional[str] = ...,
    target_file_size: Optional[int] = ...,
    writer_properties: WriterProperties = ...,
    post_commithook_properties: Optional[PostCommitHookProperties] = ...,
    commit_properties: Optional[CommitProperties] = ...,
) -> None: ...


def write_deltalake(
    table_or_uri: Union[str, Path, DeltaTable],
    data: Union[
        "pd.DataFrame",
        ds.Dataset,
        pa.Table,
        pa.RecordBatch,
        Iterable[pa.RecordBatch],
        RecordBatchReader,
        ArrowStreamExportable,
    ],
    *,
    schema: Optional[Union[pa.Schema, DeltaSchema]] = None,
    partition_by: Optional[Union[List[str], str]] = None,
    mode: Literal["error", "append", "overwrite", "ignore"] = "error",
    name: Optional[str] = None,
    description: Optional[str] = None,
    configuration: Optional[Mapping[str, Optional[str]]] = None,
    schema_mode: Optional[Literal["merge", "overwrite"]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    predicate: Optional[str] = None,
    target_file_size: Optional[int] = None,
    writer_properties: Optional[WriterProperties] = None,
    post_commithook_properties: Optional[PostCommitHookProperties] = None,
    commit_properties: Optional[CommitProperties] = None,
) -> None:
    """Write to a Delta Lake table

    If the table does not already exist, it will be created.

    Args:
        table_or_uri: URI of a table or a DeltaTable object.
        data: Data to write. If passing iterable, the schema must also be given.
        schema: Optional schema to write.
        partition_by: List of columns to partition the table by. Only required
            when creating a new table.
        mode: How to handle existing data. Default is to error if table already exists.
            If 'append', will add new data.
            If 'overwrite', will replace table with new data.
            If 'ignore', will not write anything if table already exists.
        name: User-provided identifier for this table.
        description: User-provided description for this table.
        configuration: A map containing configuration options for the metadata action.
        schema_mode: If set to "overwrite", allows replacing the schema of the table. Set to "merge" to merge with existing schema.
        storage_options: options passed to the native delta filesystem.
        predicate: When using `Overwrite` mode, replace data that matches a predicate. Only used in rust engine.'
        target_file_size: Override for target file size for data files written to the delta table. If not passed, it's taken from `delta.targetFileSize`.
        writer_properties: Pass writer properties to the Rust parquet writer.
        post_commithook_properties: properties for the post commit hook. If None, default values are used.
        commit_properties: properties of the transaction commit. If None, default values are used.
    """
    table, table_uri = try_get_table_and_table_uri(table_or_uri, storage_options)
    if table is not None:
        storage_options = table._storage_options or {}
        storage_options.update(storage_options or {})
        table.update_incremental()

    _enforce_append_only(table=table, configuration=configuration, mode=mode)
    if isinstance(partition_by, str):
        partition_by = [partition_by]

    if table is not None and mode == "ignore":
        return

    data, schema = _convert_data_and_schema(
        data=data,
        schema=schema,
        conversion_mode=ArrowSchemaConversionMode.PASSTHROUGH,
    )
    data = RecordBatchReader.from_batches(schema, (batch for batch in data))
    if table:
        table._table.write(
            data=data,
            partition_by=partition_by,
            mode=mode,
            schema_mode=schema_mode,
            predicate=predicate,
            target_file_size=target_file_size,
            name=name,
            description=description,
            configuration=configuration,
            writer_properties=writer_properties,
            commit_properties=commit_properties,
            post_commithook_properties=post_commithook_properties,
        )
    else:
        write_deltalake_rust(
            table_uri=table_uri,
            data=data,
            partition_by=partition_by,
            mode=mode,
            schema_mode=schema_mode,
            predicate=predicate,
            target_file_size=target_file_size,
            name=name,
            description=description,
            configuration=configuration,
            storage_options=storage_options,
            writer_properties=writer_properties,
            commit_properties=commit_properties,
            post_commithook_properties=post_commithook_properties,
        )


def convert_to_deltalake(
    uri: Union[str, Path],
    mode: Literal["error", "ignore"] = "error",
    partition_by: Optional[pa.Schema] = None,
    partition_strategy: Optional[Literal["hive"]] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    configuration: Optional[Mapping[str, Optional[str]]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    commit_properties: Optional[CommitProperties] = None,
    post_commithook_properties: Optional[PostCommitHookProperties] = None,
) -> None:
    """
    `Convert` parquet tables `to delta` tables.

    Currently only HIVE partitioned tables are supported. `Convert to delta` creates
    a transaction log commit with add actions, and additional properties provided such
    as configuration, name, and description.

    Args:
        uri: URI of a table.
        partition_by: Optional partitioning schema if table is partitioned.
        partition_strategy: Optional partition strategy to read and convert
        mode: How to handle existing data. Default is to error if table already exists.
            If 'ignore', will not convert anything if table already exists.
        name: User-provided identifier for this table.
        description: User-provided description for this table.
        configuration: A map containing configuration options for the metadata action.
        storage_options: options passed to the native delta filesystem. Unused if 'filesystem' is defined.
        commit_properties: properties of the transaction commit. If None, default values are used.
        post_commithook_properties: properties for the post commit hook. If None, default values are used.
    """
    if partition_by is not None and partition_strategy is None:
        raise ValueError("Partition strategy has to be provided with partition_by.")

    if partition_strategy is not None and partition_strategy != "hive":
        raise ValueError(
            "Currently only `hive` partition strategy is supported to be converted."
        )

    if mode == "ignore" and try_get_deltatable(uri, storage_options) is not None:
        return

    _convert_to_deltalake(
        uri=str(uri),
        partition_schema=partition_by,
        partition_strategy=partition_strategy,
        name=name,
        description=description,
        configuration=configuration,
        storage_options=storage_options,
        commit_properties=commit_properties,
        post_commithook_properties=post_commithook_properties,
    )
    return


def _enforce_append_only(
    table: Optional[DeltaTable],
    configuration: Optional[Mapping[str, Optional[str]]],
    mode: str,
) -> None:
    """Throw ValueError if table configuration contains delta.appendOnly and mode is not append"""
    if table:
        configuration = table.metadata().configuration
    config_delta_append_only = (
        configuration and configuration.get("delta.appendOnly", "false") == "true"
    )
    if config_delta_append_only and mode != "append":
        raise ValueError(
            "If configuration has delta.appendOnly = 'true', mode must be 'append'."
            f" Mode is currently {mode}"
        )


def _convert_data_and_schema(
    data: Union[
        "pd.DataFrame",
        ds.Dataset,
        pa.Table,
        pa.RecordBatch,
        Iterable[pa.RecordBatch],
        RecordBatchReader,
        ArrowStreamExportable,
    ],
    schema: Optional[Union[pa.Schema, DeltaSchema]],
    conversion_mode: ArrowSchemaConversionMode,
) -> Tuple[pa.RecordBatchReader, pa.Schema]:
    if isinstance(data, RecordBatchReader):
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
        if not hasattr(RecordBatchReader, "from_stream"):
            raise ValueError(
                "pyarrow 15 or later required to read stream via pycapsule interface"
            )

        data = convert_pyarrow_recordbatchreader(
            RecordBatchReader.from_stream(data), conversion_mode
        )
    elif isinstance(data, Iterable):
        if schema is None:
            raise ValueError("You must provide schema if data is Iterable")
    else:
        raise TypeError(
            f"{type(data).__name__} is not a valid input. Only PyArrow RecordBatchReader, RecordBatch, Iterable[RecordBatch], Table, Dataset or Pandas DataFrame or objects implementing the Arrow PyCapsule Interface are valid inputs for source."
        )

    if (
        isinstance(schema, DeltaSchema)
        and conversion_mode == ArrowSchemaConversionMode.PASSTHROUGH
    ):
        raise NotImplementedError(
            "ArrowSchemaConversionMode.passthrough is not implemented to work with DeltaSchema, skip passing a schema or pass an arrow schema."
        )
    elif isinstance(schema, DeltaSchema):
        if conversion_mode == ArrowSchemaConversionMode.LARGE:
            schema = schema.to_pyarrow(as_large_types=True)
        else:
            schema = schema.to_pyarrow(as_large_types=False)
    elif schema is None:
        schema = data.schema

    return data, schema


def _sort_arrow_schema(schema: pa.schema) -> pa.schema:
    sorted_cols = sorted(iter(schema), key=lambda x: (x.name, str(x.type)))
    return pa.schema(sorted_cols)


def _large_to_normal_dtype(dtype: pa.DataType) -> pa.DataType:
    return DTYPE_MAP.get(dtype, dtype)


class DeltaJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, bytes):
            return obj.decode("unicode_escape", "backslashreplace")
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return str(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def try_get_table_and_table_uri(
    table_or_uri: Union[str, Path, DeltaTable],
    storage_options: Optional[Dict[str, str]] = None,
) -> Tuple[Optional[DeltaTable], str]:
    """Parses the `table_or_uri`.
    Raises [ValueError] If `table_or_uri` is not of type str, Path or DeltaTable.

    Args:
        table_or_uri: URI of a table or a DeltaTable object.
        storage_options: Options passed to the native delta filesystem.

    Returns:
        (DeltaTable object, URI of the table)
    """
    if not isinstance(table_or_uri, (str, Path, DeltaTable)):
        raise ValueError("table_or_uri must be a str, Path or DeltaTable")

    if isinstance(table_or_uri, (str, Path)):
        table = try_get_deltatable(table_or_uri, storage_options)
        table_uri = str(table_or_uri)
    else:
        table = table_or_uri
        table_uri = table._table.table_uri()

    return (table, table_uri)


def try_get_deltatable(
    table_uri: Union[str, Path], storage_options: Optional[Dict[str, str]]
) -> Optional[DeltaTable]:
    try:
        return DeltaTable(table_uri, storage_options=storage_options)
    except TableNotFoundError:
        return None


def get_partitions_from_path(path: str) -> Tuple[str, Dict[str, Optional[str]]]:
    if path[0] == "/":
        path = path[1:]
    parts = path.split("/")
    parts.pop()  # remove filename
    out: Dict[str, Optional[str]] = {}
    for part in parts:
        if part == "":
            continue
        key, value = part.split("=", maxsplit=1)
        if value == "__HIVE_DEFAULT_PARTITION__":
            out[key] = None
        else:
            out[key] = unquote(value)
    return path, out


def get_file_stats_from_metadata(
    metadata: Any,
    num_indexed_cols: int,
    columns_to_collect_stats: Optional[List[str]],
) -> Dict[str, Union[int, Dict[str, Any]]]:
    stats = {
        "numRecords": metadata.num_rows,
        "minValues": {},
        "maxValues": {},
        "nullCount": {},
    }

    def iter_groups(metadata: Any) -> Iterator[Any]:
        for i in range(metadata.num_row_groups):
            if metadata.row_group(i).num_rows > 0:
                yield metadata.row_group(i)

    schema_columns = metadata.schema.names
    if columns_to_collect_stats is not None:
        idx_to_iterate = []
        for col in columns_to_collect_stats:
            try:
                idx_to_iterate.append(schema_columns.index(col))
            except ValueError:
                pass
    elif num_indexed_cols == -1:
        idx_to_iterate = list(range(metadata.num_columns))
    elif num_indexed_cols >= 0:
        idx_to_iterate = list(range(min(num_indexed_cols, metadata.num_columns)))
    else:
        raise ValueError("delta.dataSkippingNumIndexedCols valid values are >=-1")

    for column_idx in idx_to_iterate:
        name = metadata.row_group(0).column(column_idx).path_in_schema

        # If stats missing, then we can't know aggregate stats
        if all(
            group.column(column_idx).is_stats_set for group in iter_groups(metadata)
        ):
            stats["nullCount"][name] = sum(
                group.column(column_idx).statistics.null_count
                for group in iter_groups(metadata)
            )

            # Min / max may not exist for some column types, or if all values are null
            if any(
                group.column(column_idx).statistics.has_min_max
                for group in iter_groups(metadata)
            ):
                # Min and Max are recorded in physical type, not logical type
                # https://stackoverflow.com/questions/66753485/decoding-parquet-min-max-statistics-for-decimal-type
                # TODO: Add logic to decode physical type for DATE, DECIMAL

                minimums = (
                    group.column(column_idx).statistics.min
                    for group in iter_groups(metadata)
                )
                # If some row groups have all null values, their min and max will be null too.
                min_value = min(minimum for minimum in minimums if minimum is not None)
                # Infinity cannot be serialized to JSON, so we skip it. Saying
                # min/max is infinity is equivalent to saying it is null, anyways.
                if min_value != -inf:
                    stats["minValues"][name] = min_value
                maximums = (
                    group.column(column_idx).statistics.max
                    for group in iter_groups(metadata)
                )
                max_value = max(maximum for maximum in maximums if maximum is not None)
                if max_value != inf:
                    stats["maxValues"][name] = max_value
    return stats
