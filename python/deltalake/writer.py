from dataclasses import dataclass
from datetime import datetime
import json
from typing import Dict, Iterable, List, Literal, Optional, Union, overload
import uuid
from deltalake import DeltaTable, PyDeltaTableError
from .deltalake import RawDeltaTable, write_new_deltalake as _write_new_deltalake
import pyarrow as pa
import pyarrow.dataset as ds


@dataclass
class AddAction:
    path: str
    size: int
    partition_values: Dict[str, Optional[str]]
    modification_time: int
    data_change: bool
    stats: str


def create_empty_table(uri: str, schema: pa.Schema, partition_columns: List[str]) -> DeltaTable:
    return DeltaTable._from_raw(RawDeltaTable.create_empty(uri, schema, partition_columns))


@overload
def write_deltalake(
    table_or_uri: Union[str, DeltaTable],
    data: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    partition_by: Optional[Iterable[str]],
    mode: Literal['error', 'append', 'overwrite', 'ignore'] = 'error'
): ...


@overload
def write_deltalake(
    table_or_uri: Union[str, DeltaTable],
    data: Union[pa.Table, pa.RecordBatch], # TODO: there a type for a RecordBatchReader?
    schema: Optional[pa.Schema],
    partition_by: Optional[Iterable[str]],
    mode: Literal['error', 'append', 'overwrite', 'ignore'] = 'error'
): ...


def write_deltalake(table_or_uri, data, schema=None, partition_by=None, mode='error'):
    """Write to a Delta Lake table

    If the table does not already exist, it will be created.

    :param table: URI of a table or a DeltaTable object.
    :param data: Data to write. If passing iterable, the schema must also be given.
    :param schema: Optional schema to write. 
    :param mode: How to handle existing data. Default is to error if table 
        already exists. If 'append', will add new data. If 'overwrite', will
        replace table with new data. If 'ignore', will not write anything if
        table already exists.
    """
    if isinstance(data, Iterable) and schema is None:
        return ValueError("You must provide schema if data is Iterable")
    elif not isinstance(data, Iterable):
        schema = data.schema

    if isinstance(table_or_uri, str):
        table_uri = table_or_uri
        try:
            table = DeltaTable(table_or_uri)
            current_version = table.version
        except PyDeltaTableError as err:
            # branch to handle if not a delta table
            if "Not a Delta table" not in str(err):
                raise
            table = None
            current_version = -1
        else:
            if mode == 'error':
                raise AssertionError("DeltaTable already exists.")
            elif mode == 'ignore':
                # table already exists
                return
            current_version = table.version()
    else:
        table = table_or_uri
        table_uri = table._table.table_uri()
        current_version = table.version

    if partition_by:
        partitioning = ds.partitioning(field_names=partition_by, flavor="hive")
    else:
        partitioning = None

    add_actions: List[AddAction] = []

    def visitor(written_file):
        # TODO: Get partition values from path
        partition_values = {}
        # TODO: Record column statistics
        # NOTE: will need to aggregate over row groups. Access with
        # written_file.metadata.row_group(i).column(j).statistics
        stats = {"numRecords": written_file.metadata.num_rows}
        add_actions.append(AddAction(
            written_file.path,
            written_file.metadata.serialized_size,
            partition_values,
            int(datetime.now().timestamp()),
            True,
            json.dumps(stats)
        ))

    # TODO: Pass through filesystem? Do we need to transform the URI as well?
    ds.write_dataset(
        data,
        base_dir=table_uri,
        basename_template=f"{current_version + 1}-{uuid.uuid4()}-{{i}}.parquet",
        format="parquet",
        partitioning=partitioning,
        schema=schema,
        file_visitor=visitor,
        existing_data_behavior='overwrite_or_ignore',
    )

    if table is None:
        _write_new_deltalake(
            table_uri,
            schema,
            add_actions,
            mode,
            partition_by or []
        )
    else:
        table._table.write(
            add_actions,
            mode,
            partition_by or [],
        )
