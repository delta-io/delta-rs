from dataclasses import dataclass
from datetime import datetime
import json
from typing import Dict, Iterable, List, Literal, Optional, Union, overload
import uuid
from deltalake import DeltaTable
from .deltalake import _create_empty_table
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
    return DeltaTable._from_raw(_create_empty_table(uri, schema, partition_columns))


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
    data: Union[pa.Table, pa.RecordBatch, pa.RecordBatchReader],
    schema: Optional[pa.Schema],
    partition_by: Optional[Iterable[str]],
    mode: Literal['error', 'append', 'overwrite', 'ignore'] = 'error'
): ...


def write_deltalake(table_or_uri, data, schema, partition_by=None, mode='error'):
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
        try:
            table = DeltaTable(table_or_uri)
        except KeyError:  # TODO update to error we should get if doesn't exist
            # Create it
            if mode == 'error':
                raise AssertionError("DeltaTable already exists.")

            table = create_empty_table(
                table_or_uri, schema, partition_by or [])
        else:
            if mode == 'ignore':
                # table already exists
                return
    else:
        table = table_or_uri

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
        base_dir=table._table.table_uri(),
        basename_template=f"{table.version}-{uuid.uuid4()}-{{i}}.parquet",
        format="parquet",
        partitioning=partitioning,
        schema=schema,
        file_visitor=visitor,
        existing_data_behavior='overwrite_or_ignore',
    )

    table._table.create_write_transaction(
        add_actions,
        mode,
        partition_by
    )
