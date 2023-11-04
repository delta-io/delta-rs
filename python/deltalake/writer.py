import json
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from math import inf
from pathlib import Path
from typing import (
    Any,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)
from urllib.parse import unquote

from deltalake.fs import DeltaStorageHandler

from ._util import encode_partition_value

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pa_fs
from pyarrow.lib import RecordBatchReader

from deltalake.schema import delta_arrow_schema_from_pandas

from ._internal import DeltaDataChecker as _DeltaDataChecker
from ._internal import batch_distinct
from ._internal import write_new_deltalake as _write_new_deltalake
from .exceptions import DeltaProtocolError, TableNotFoundError
from .table import MAX_SUPPORTED_WRITER_VERSION, DeltaTable

try:
    import pandas as pd  # noqa: F811
except ModuleNotFoundError:
    _has_pandas = False
else:
    _has_pandas = True

PYARROW_MAJOR_VERSION = int(pa.__version__.split(".", maxsplit=1)[0])


@dataclass
class AddAction:
    path: str
    size: int
    partition_values: Mapping[str, Optional[str]]
    modification_time: int
    data_change: bool
    stats: str


def write_deltalake(
    table_or_uri: Union[str, Path, DeltaTable],
    data: Union[
        "pd.DataFrame",
        ds.Dataset,
        pa.Table,
        pa.RecordBatch,
        Iterable[pa.RecordBatch],
        RecordBatchReader,
    ],
    *,
    schema: Optional[pa.Schema] = None,
    partition_by: Optional[Union[List[str], str]] = None,
    filesystem: Optional[pa_fs.FileSystem] = None,
    mode: Literal["error", "append", "overwrite", "ignore"] = "error",
    file_options: Optional[ds.ParquetFileWriteOptions] = None,
    max_partitions: Optional[int] = None,
    max_open_files: int = 1024,
    max_rows_per_file: int = 10 * 1024 * 1024,
    min_rows_per_group: int = 64 * 1024,
    max_rows_per_group: int = 128 * 1024,
    name: Optional[str] = None,
    description: Optional[str] = None,
    configuration: Optional[Mapping[str, Optional[str]]] = None,
    overwrite_schema: bool = False,
    storage_options: Optional[Dict[str, str]] = None,
    partition_filters: Optional[List[Tuple[str, str, Any]]] = None,
    large_dtypes: bool = False,
) -> None:
    """Write to a Delta Lake table

    If the table does not already exist, it will be created.

    This function only supports writer protocol version 2 currently. When
    attempting to write to an existing table with a higher min_writer_version,
    this function will throw DeltaProtocolError.

    Note that this function does NOT register this table in a data catalog.

    A locking mechanism is needed to prevent unsafe concurrent writes to a
    delta lake directory when writing to S3. DynamoDB is the only available
    locking provider at the moment in delta-rs. To enable DynamoDB as the
    locking provider, you need to set the `AWS_S3_LOCKING_PROVIDER` to 'dynamodb'
    as a storage_option or as an environment variable.

    Additionally, you must create a DynamoDB table with the name 'delta_rs_lock_table'
    so that it can be automatically discovered by delta-rs. Alternatively, you can
    use a table name of your choice, but you must set the `DYNAMO_LOCK_TABLE_NAME`
    variable to match your chosen table name. The required schema for the DynamoDB
    table is as follows:

    - Key Schema: AttributeName=key, KeyType=HASH
    - Attribute Definitions: AttributeName=key, AttributeType=S

    Please note that this locking mechanism is not compatible with any other
    locking mechanisms, including the one used by Spark.

    Args:
        table_or_uri: URI of a table or a DeltaTable object.
        data: Data to write. If passing iterable, the schema must also be given.
        schema: Optional schema to write.
        partition_by: List of columns to partition the table by. Only required
            when creating a new table.
        filesystem: Optional filesystem to pass to PyArrow. If not provided will
            be inferred from uri. The file system has to be rooted in the table root.
            Use the pyarrow.fs.SubTreeFileSystem, to adopt the root of pyarrow file systems.
        mode: How to handle existing data. Default is to error if table already exists.
            If 'append', will add new data.
            If 'overwrite', will replace table with new data.
            If 'ignore', will not write anything if table already exists.
        file_options: Optional write options for Parquet (ParquetFileWriteOptions).
            Can be provided with defaults using ParquetFileWriteOptions().make_write_options().
            Please refer to https://github.com/apache/arrow/blob/master/python/pyarrow/_dataset_parquet.pyx#L492-L533
            for the list of available options
        max_partitions: the maximum number of partitions that will be used.
        max_open_files: Limits the maximum number of
            files that can be left open while writing. If an attempt is made to open
            too many files then the least recently used file will be closed.
            If this setting is set too low you may end up fragmenting your
            data into many small files.
        max_rows_per_file: Maximum number of rows per file.
            If greater than 0 then this will limit how many rows are placed in any single file.
            Otherwise there will be no limit and one file will be created in each output directory
            unless files need to be closed to respect max_open_files
            min_rows_per_group: Minimum number of rows per group. When the value is set,
            the dataset writer will batch incoming data and only write the row groups to the disk
            when sufficient rows have accumulated.
        max_rows_per_group: Maximum number of rows per group.
            If the value is set, then the dataset writer may split up large incoming batches into multiple row groups.
            If this value is set, then min_rows_per_group should also be set.
        name: User-provided identifier for this table.
        description: User-provided description for this table.
        configuration: A map containing configuration options for the metadata action.
        overwrite_schema: If True, allows updating the schema of the table.
        storage_options: options passed to the native delta filesystem. Unused if 'filesystem' is defined.
        partition_filters: the partition filters that will be used for partition overwrite.
        large_dtypes: If True, the table schema is checked against large_dtypes
    """
    if _has_pandas and isinstance(data, pd.DataFrame):
        if schema is not None:
            data = pa.Table.from_pandas(data, schema=schema)
        else:
            data, schema = delta_arrow_schema_from_pandas(data)

    table, table_uri = try_get_table_and_table_uri(table_or_uri, storage_options)

    # We need to write against the latest table version
    if table:
        table.update_incremental()

    if schema is None:
        if isinstance(data, RecordBatchReader):
            schema = data.schema
        elif isinstance(data, Iterable):
            raise ValueError("You must provide schema if data is Iterable")
        else:
            schema = data.schema

    if filesystem is not None:
        raise NotImplementedError("Filesystem support is not yet implemented.  #570")

    if table is not None:
        storage_options = table._storage_options or {}
        storage_options.update(storage_options or {})

    filesystem = pa_fs.PyFileSystem(DeltaStorageHandler(table_uri, storage_options))

    __enforce_append_only(table=table, configuration=configuration, mode=mode)

    if isinstance(partition_by, str):
        partition_by = [partition_by]

    if table:  # already exists
        if schema != table.schema().to_pyarrow(as_large_types=large_dtypes) and not (
            mode == "overwrite" and overwrite_schema
        ):
            raise ValueError(
                "Schema of data does not match table schema\n"
                f"Data schema:\n{schema}\nTable Schema:\n{table.schema().to_pyarrow(as_large_types=large_dtypes)}"
            )

        if mode == "error":
            raise AssertionError("DeltaTable already exists.")
        elif mode == "ignore":
            return

        current_version = table.version()

        if partition_by:
            assert partition_by == table.metadata().partition_columns
        else:
            partition_by = table.metadata().partition_columns

        if table.protocol().min_writer_version > MAX_SUPPORTED_WRITER_VERSION:
            raise DeltaProtocolError(
                "This table's min_writer_version is "
                f"{table.protocol().min_writer_version}, "
                "but this method only supports version 2."
            )
    else:  # creating a new table
        current_version = -1

    dtype_map = {
        pa.large_string(): pa.string(),  # type: ignore
    }

    def _large_to_normal_dtype(dtype: pa.DataType) -> pa.DataType:
        try:
            return dtype_map[dtype]
        except KeyError:
            return dtype

    if partition_by:
        if PYARROW_MAJOR_VERSION < 12:
            partition_schema = pa.schema(
                [
                    pa.field(name, _large_to_normal_dtype(schema.field(name).type))
                    for name in partition_by
                ]
            )
        else:
            partition_schema = pa.schema([schema.field(name) for name in partition_by])
        partitioning = ds.partitioning(partition_schema, flavor="hive")
    else:
        partitioning = None

    add_actions: List[AddAction] = []

    def visitor(written_file: Any) -> None:
        path, partition_values = get_partitions_from_path(written_file.path)
        stats = get_file_stats_from_metadata(written_file.metadata)

        # PyArrow added support for written_file.size in 9.0.0
        if PYARROW_MAJOR_VERSION >= 9:
            size = written_file.size
        elif filesystem is not None:
            size = filesystem.get_file_info([path])[0].size
        else:
            size = 0

        add_actions.append(
            AddAction(
                path,
                size,
                partition_values,
                int(datetime.now().timestamp() * 1000),
                True,
                json.dumps(stats, cls=DeltaJSONEncoder),
            )
        )

    if table is not None:
        # We don't currently provide a way to set invariants
        # (and maybe never will), so only enforce if already exist.
        invariants = table.schema().invariants
        checker = _DeltaDataChecker(invariants)

        def check_data_is_aligned_with_partition_filtering(
            batch: pa.RecordBatch,
        ) -> None:
            if table is None:
                return
            existed_partitions: FrozenSet[
                FrozenSet[Tuple[str, Optional[str]]]
            ] = table._table.get_active_partitions()
            allowed_partitions: FrozenSet[
                FrozenSet[Tuple[str, Optional[str]]]
            ] = table._table.get_active_partitions(partition_filters)
            partition_values = pa.RecordBatch.from_arrays(
                [
                    batch.column(column_name)
                    for column_name in table.metadata().partition_columns
                ],
                table.metadata().partition_columns,
            )
            partition_values = batch_distinct(partition_values)
            for i in range(partition_values.num_rows):
                # Map will maintain order of partition_columns
                partition_map = {
                    column_name: encode_partition_value(
                        batch.column(column_name)[i].as_py()
                    )
                    for column_name in table.metadata().partition_columns
                }
                partition = frozenset(partition_map.items())
                if (
                    partition not in allowed_partitions
                    and partition in existed_partitions
                ):
                    partition_repr = " ".join(
                        f"{key}={value}" for key, value in partition_map.items()
                    )
                    raise ValueError(
                        f"Data should be aligned with partitioning. "
                        f"Data contained values for partition {partition_repr}"
                    )

        def validate_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
            checker.check_batch(batch)

            if mode == "overwrite" and partition_filters:
                check_data_is_aligned_with_partition_filtering(batch)

            return batch

        if isinstance(data, RecordBatchReader):
            batch_iter = data
        elif isinstance(data, pa.RecordBatch):
            batch_iter = [data]
        elif isinstance(data, pa.Table):
            batch_iter = data.to_batches()
        elif isinstance(data, ds.Dataset):
            batch_iter = data.to_batches()
        else:
            batch_iter = data

        data = RecordBatchReader.from_batches(
            schema, (validate_batch(batch) for batch in batch_iter)
        )

    if file_options is not None:
        file_options.update(use_compliant_nested_type=False)
    else:
        file_options = ds.ParquetFileFormat().make_write_options(
            use_compliant_nested_type=False
        )

    ds.write_dataset(
        data,
        base_dir="/",
        basename_template=f"{current_version + 1}-{uuid.uuid4()}-{{i}}.parquet",
        format="parquet",
        partitioning=partitioning,
        # It will not accept a schema if using a RBR
        schema=schema if not isinstance(data, RecordBatchReader) else None,
        file_visitor=visitor,
        existing_data_behavior="overwrite_or_ignore",
        file_options=file_options,
        max_open_files=max_open_files,
        max_rows_per_file=max_rows_per_file,
        min_rows_per_group=min_rows_per_group,
        max_rows_per_group=max_rows_per_group,
        filesystem=filesystem,
        max_partitions=max_partitions,
    )

    if table is None:
        _write_new_deltalake(
            table_uri,
            schema,
            add_actions,
            mode,
            partition_by or [],
            name,
            description,
            configuration,
            storage_options,
        )
    else:
        table._table.create_write_transaction(
            add_actions,
            mode,
            partition_by or [],
            schema,
            partition_filters,
        )
        table.update_incremental()


def __enforce_append_only(
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
) -> Dict[str, Union[int, Dict[str, Any]]]:
    stats = {
        "numRecords": metadata.num_rows,
        "minValues": {},
        "maxValues": {},
        "nullCount": {},
    }

    def iter_groups(metadata: Any) -> Iterator[Any]:
        for i in range(metadata.num_row_groups):
            yield metadata.row_group(i)

    for column_idx in range(metadata.num_columns):
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
                logical_type = (
                    metadata.row_group(0)
                    .column(column_idx)
                    .statistics.logical_type.type
                )

                if PYARROW_MAJOR_VERSION < 8 and logical_type not in [
                    "STRING",
                    "INT",
                    "TIMESTAMP",
                    "NONE",
                ]:
                    continue

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
