import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pa_fs
from pyarrow.lib import RecordBatchReader
from typing_extensions import Literal

from .deltalake import PyDeltaTableError
from .deltalake import write_new_deltalake as _write_new_deltalake
from .table import DeltaTable


class DeltaTableProtocolError(PyDeltaTableError):
    pass


@dataclass
class AddAction:
    path: str
    size: int
    partition_values: Mapping[str, Optional[str]]
    modification_time: int
    data_change: bool
    stats: str


def write_deltalake(
    table_or_uri: Union[str, DeltaTable],
    data: Union[
        pd.DataFrame,
        pa.Table,
        pa.RecordBatch,
        Iterable[pa.RecordBatch],
        RecordBatchReader,
    ],
    schema: Optional[pa.Schema] = None,
    partition_by: Optional[List[str]] = None,
    filesystem: Optional[pa_fs.FileSystem] = None,
    mode: Literal["error", "append", "overwrite", "ignore"] = "error",
    name: Optional[str] = None,
    description: Optional[str] = None,
    configuration: Optional[Mapping[str, Optional[str]]] = None,
) -> None:
    """Write to a Delta Lake table (Experimental)

    If the table does not already exist, it will be created.

    This function only supports protocol version 1 currently. If an attempting
    to write to an existing table with a higher min_writer_version, this
    function will throw DeltaTableProtocolError.

    Note that this function does NOT register this table in a data catalog.

    :param table_or_uri: URI of a table or a DeltaTable object.
    :param data: Data to write. If passing iterable, the schema must also be given.
    :param schema: Optional schema to write.
    :param partition_by: List of columns to partition the table by. Only required
        when creating a new table.
    :param filesystem: Optional filesystem to pass to PyArrow. If not provided will
        be inferred from uri.
    :param mode: How to handle existing data. Default is to error if table
        already exists. If 'append', will add new data. If 'overwrite', will
        replace table with new data. If 'ignore', will not write anything if
        table already exists.
    :param name: User-provided identifier for this table.
    :param description: User-provided description for this table.
    :param configuration: A map containing configuration options for the metadata action.
    """
    if isinstance(data, pd.DataFrame):
        data = pa.Table.from_pandas(data)

    if schema is None:
        if isinstance(data, RecordBatchReader):
            schema = data.schema
        elif isinstance(data, Iterable):
            raise ValueError("You must provide schema if data is Iterable")
        else:
            schema = data.schema

    if isinstance(table_or_uri, str):
        table = try_get_deltatable(table_or_uri)
        table_uri = table_or_uri
    else:
        table = table_or_uri
        table_uri = table_uri = table._table.table_uri()

    # TODO: Pass through filesystem once it is complete
    # if filesystem is None:
    #    filesystem = pa_fs.PyFileSystem(DeltaStorageHandler(table_uri))

    if table:  # already exists
        if mode == "error":
            raise AssertionError("DeltaTable already exists.")
        elif mode == "ignore":
            return

        current_version = table.version()

        if partition_by:
            assert partition_by == table.metadata().partition_columns

        if table.protocol().min_writer_version > 1:
            raise DeltaTableProtocolError(
                "This table's min_writer_version is "
                f"{table.protocol().min_writer_version}, "
                "but this method only supports version 1."
            )
    else:  # creating a new table
        current_version = -1

        # TODO: Don't allow writing to non-empty directory
        # Blocked on: Finish filesystem implementation in fs.py
        # assert len(filesystem.get_file_info(pa_fs.FileSelector(table_uri, allow_not_found=True))) == 0

    if partition_by:
        partition_schema = pa.schema([schema.field(name) for name in partition_by])
        partitioning = ds.partitioning(partition_schema, flavor="hive")
    else:
        partitioning = None

    add_actions: List[AddAction] = []

    def visitor(written_file: Any) -> None:
        partition_values = get_partitions_from_path(table_uri, written_file.path)
        stats = get_file_stats_from_metadata(written_file.metadata)

        add_actions.append(
            AddAction(
                written_file.path,
                written_file.metadata.serialized_size,
                partition_values,
                int(datetime.now().timestamp()),
                True,
                json.dumps(stats, cls=DeltaJSONEncoder),
            )
        )

    ds.write_dataset(
        data,
        base_dir=table_uri,
        basename_template=f"{current_version + 1}-{uuid.uuid4()}-{{i}}.parquet",
        format="parquet",
        partitioning=partitioning,
        # It will not accept a schema if using a RBR
        schema=schema if not isinstance(data, RecordBatchReader) else None,
        file_visitor=visitor,
        existing_data_behavior="overwrite_or_ignore",
    )

    if table is None:
        _write_new_deltalake(  # type: ignore[call-arg]
            table_uri,
            schema,
            add_actions,
            mode,
            partition_by or [],
            name,
            description,
            configuration,
        )
    else:
        table._table.create_write_transaction(
            add_actions,
            mode,
            partition_by or [],
        )


class DeltaJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, bytes):
            return obj.decode("unicode_escape")
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return str(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def try_get_deltatable(table_uri: str) -> Optional[DeltaTable]:
    try:
        return DeltaTable(table_uri)
    except PyDeltaTableError as err:
        if "Not a Delta table" not in str(err):
            raise
        return None


def get_partitions_from_path(base_path: str, path: str) -> Dict[str, str]:
    path = path.split(base_path, maxsplit=1)[1]
    parts = path.split("/")
    parts.pop()  # remove filename
    out = {}
    for part in parts:
        if part == "":
            continue
        key, value = part.split("=", maxsplit=1)
        out[key] = value
    return out


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

            # I assume for now this is based on data type, and thus is
            # consistent between groups
            if metadata.row_group(0).column(column_idx).statistics.has_min_max:
                # Min and Max are recorded in physical type, not logical type
                # https://stackoverflow.com/questions/66753485/decoding-parquet-min-max-statistics-for-decimal-type
                # TODO: Add logic to decode physical type for DATE, DECIMAL
                logical_type = (
                    metadata.row_group(0)
                    .column(column_idx)
                    .statistics.logical_type.type
                )
                #
                if logical_type not in ["STRING", "INT", "TIMESTAMP", "NONE"]:
                    continue
                # import pdb; pdb.set_trace()
                stats["minValues"][name] = min(
                    group.column(column_idx).statistics.min
                    for group in iter_groups(metadata)
                )
                stats["maxValues"][name] = max(
                    group.column(column_idx).statistics.max
                    for group in iter_groups(metadata)
                )
    return stats
