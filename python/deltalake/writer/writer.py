from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Literal,
    overload,
)

from arro3.core import RecordBatchReader
from arro3.core.types import ArrowArrayExportable, ArrowStreamExportable

from deltalake._internal import write_to_deltalake as write_deltalake_rust
from deltalake.writer._conversion import _convert_arro3_schema_to_delta
from deltalake.writer._utils import try_get_table_and_table_uri

if TYPE_CHECKING:
    from deltalake.table import DeltaTable, WriterProperties
    from deltalake.transaction import (
        CommitProperties,
        PostCommitHookProperties,
    )


@overload
def write_deltalake(
    table_or_uri: str | Path | DeltaTable,
    data: ArrowStreamExportable | ArrowArrayExportable | Sequence[ArrowArrayExportable],
    *,
    partition_by: list[str] | str | None = ...,
    mode: Literal["error", "append", "ignore"] = ...,
    name: str | None = ...,
    description: str | None = ...,
    configuration: Mapping[str, str | None] | None = ...,
    schema_mode: Literal["merge", "overwrite"] | None = ...,
    storage_options: dict[str, str] | None = ...,
    target_file_size: int | None = ...,
    writer_properties: WriterProperties = ...,
    post_commithook_properties: PostCommitHookProperties | None = ...,
    commit_properties: CommitProperties | None = ...,
) -> None: ...


@overload
def write_deltalake(
    table_or_uri: str | Path | DeltaTable,
    data: ArrowStreamExportable | ArrowArrayExportable | Sequence[ArrowArrayExportable],
    *,
    partition_by: list[str] | str | None = ...,
    mode: Literal["overwrite"],
    name: str | None = ...,
    description: str | None = ...,
    configuration: Mapping[str, str | None] | None = ...,
    schema_mode: Literal["merge", "overwrite"] | None = ...,
    storage_options: dict[str, str] | None = ...,
    predicate: str | None = ...,
    target_file_size: int | None = ...,
    writer_properties: WriterProperties = ...,
    post_commithook_properties: PostCommitHookProperties | None = ...,
    commit_properties: CommitProperties | None = ...,
) -> None: ...


def write_deltalake(
    table_or_uri: str | Path | DeltaTable,
    data: ArrowStreamExportable | ArrowArrayExportable | Sequence[ArrowArrayExportable],
    *,
    partition_by: list[str] | str | None = None,
    mode: Literal["error", "append", "overwrite", "ignore"] = "error",
    name: str | None = None,
    description: str | None = None,
    configuration: Mapping[str, str | None] | None = None,
    schema_mode: Literal["merge", "overwrite"] | None = None,
    storage_options: dict[str, str] | None = None,
    predicate: str | None = None,
    target_file_size: int | None = None,
    writer_properties: WriterProperties | None = None,
    post_commithook_properties: PostCommitHookProperties | None = None,
    commit_properties: CommitProperties | None = None,
) -> None:
    """Write to a Delta Lake table

    If the table does not already exist, it will be created.

    Args:
        table_or_uri: URI of a table or a DeltaTable object.
        data: Data to write. If passing iterable, the schema must also be given.
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
        predicate: When using `Overwrite` mode, replace data that matches a predicate.'
        target_file_size: Override for target file size for data files written to the delta table. If not passed, it's taken from `delta.targetFileSize`.
        writer_properties: Pass writer properties to the Rust parquet writer.
        post_commithook_properties: properties for the post commit hook. If None, default values are used.
        commit_properties: properties of the transaction commit. If None, default values are used.
    """
    table, table_uri = try_get_table_and_table_uri(table_or_uri, storage_options)
    if table is not None:
        storage_options = table._storage_options or {}
        storage_options.update(storage_options or {})

    if isinstance(partition_by, str):
        partition_by = [partition_by]

    if table is not None and mode == "ignore":
        return

    if isinstance(data, Sequence):
        data = RecordBatchReader.from_batches(data[0], data)  # type: ignore
    else:
        data = RecordBatchReader.from_arrow(data)

    existing_schema = None
    if table is not None:
        existing_schema = table.schema().to_arrow()

    compatible_delta_schema = _convert_arro3_schema_to_delta(
        data.schema, existing_schema
    )

    if table:
        table._table.write(
            data=data,
            batch_schema=compatible_delta_schema,
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
            batch_schema=compatible_delta_schema,
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
