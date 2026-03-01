from __future__ import annotations

import json
from collections.abc import Generator, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    NamedTuple,
    Union,
)

from arro3.core import RecordBatchReader, Table
from arro3.core.types import (
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)

from deltalake._internal import (
    DeltaError,
    PyMergeBuilder,
    RawDeltaTable,
    TableFeatures,
)
from deltalake._internal import create_deltalake as _create_deltalake
from deltalake._util import encode_partition_value
from deltalake.exceptions import DeltaProtocolError
from deltalake.schema import Field as DeltaField
from deltalake.schema import Schema as DeltaSchema
from deltalake.writer._conversion import _convert_arro3_schema_to_delta

if TYPE_CHECKING:
    import os

    import pandas as pd
    import pyarrow
    import pyarrow.fs as pa_fs
    from pyarrow.dataset import (
        Expression,
        ParquetReadOptions,
    )

    from deltalake.transaction import (
        AddAction,
        CommitProperties,
        PostCommitHookProperties,
    )
    from deltalake.writer.properties import WriterProperties


MAX_SUPPORTED_PYARROW_WRITER_VERSION = 7
NOT_SUPPORTED_PYARROW_WRITER_VERSIONS = [3, 4, 5, 6]
SUPPORTED_WRITER_FEATURES = {"appendOnly", "invariants", "timestampNtz"}

MAX_SUPPORTED_READER_VERSION = 3
NOT_SUPPORTED_READER_VERSION = 2
SUPPORTED_READER_FEATURES = {"timestampNtz"}

FSCK_METRICS_FILES_REMOVED_LABEL = "files_removed"

FilterLiteralType = tuple[str, str, Any]
FilterConjunctionType = list[FilterLiteralType]
FilterDNFType = list[FilterConjunctionType]
FilterType = Union[FilterConjunctionType, FilterDNFType]
PartitionFilterType = list[tuple[str, str, Union[str, list[str]]]]


@dataclass(init=False)
class Metadata:
    """Create a Metadata instance."""

    def __init__(self, table: RawDeltaTable) -> None:
        self._metadata = table.metadata()

    @property
    def id(self) -> int:
        """Return the unique identifier of the DeltaTable."""
        return self._metadata.id

    @property
    def name(self) -> str:
        """Return the user-provided identifier of the DeltaTable."""
        return self._metadata.name

    @property
    def description(self) -> str:
        """Return the user-provided description of the DeltaTable."""
        return self._metadata.description

    @property
    def partition_columns(self) -> list[str]:
        """Return an array containing the names of the partitioned columns of the DeltaTable."""
        return self._metadata.partition_columns

    @property
    def created_time(self) -> int:
        """
        Return The time when this metadata action is created, in milliseconds since the Unix epoch of the DeltaTable.
        """
        return self._metadata.created_time

    @property
    def configuration(self) -> dict[str, str]:
        """Return the DeltaTable properties."""
        return self._metadata.configuration

    def __str__(self) -> str:
        return (
            f"Metadata(id: {self._metadata.id}, name: {self._metadata.name}, "
            f"description: {self._metadata.description}, partition_columns: {self._metadata.partition_columns}, "
            f"created_time: {self.created_time}, configuration: {self._metadata.configuration})"
        )


class DeltaTableConfig(NamedTuple):
    without_files: bool
    log_buffer_size: int


class ProtocolVersions(NamedTuple):
    min_reader_version: int
    min_writer_version: int
    writer_features: list[str] | None
    reader_features: list[str] | None


@dataclass(init=False)
class DeltaTable:
    """Represents a Delta Table"""

    def __init__(
        self,
        table_uri: str | Path | os.PathLike[str],
        version: int | None = None,
        storage_options: dict[str, str] | None = None,
        without_files: bool = False,
        log_buffer_size: int | None = None,
    ) -> None:
        """
        Create the Delta Table from a path with an optional version.
        Multiple StorageBackends are currently supported: AWS S3, Azure Data Lake Storage Gen2, Google Cloud Storage (GCS) and local URI.
        Depending on the storage backend used, you could provide options values using the ``storage_options`` parameter.

        Args:
            table_uri: the path of the DeltaTable
            version: version of the DeltaTable
            storage_options: a dictionary of the options to use for the storage backend
            without_files: If True, will load table without tracking files.
                                Some append-only applications might have no need of tracking any files. So, the
                                DeltaTable will be loaded with a significant memory reduction.
            log_buffer_size: Number of files to buffer when reading the commit log. A positive integer.
                                Setting a value greater than 1 results in concurrent calls to the storage api.
                                This can decrease latency if there are many files in the log since the last checkpoint,
                                but will also increase memory usage. Possible rate limits of the storage backend should
                                also be considered for optimal performance. Defaults to 4 * number of cpus.

        """
        self._storage_options = storage_options
        self._table = RawDeltaTable(
            str(table_uri),
            version=version,
            storage_options=storage_options,
            without_files=without_files,
            log_buffer_size=log_buffer_size,
        )

    @property
    def table_config(self) -> DeltaTableConfig:
        return DeltaTableConfig(*self._table.table_config())

    @staticmethod
    def is_deltatable(
        table_uri: str, storage_options: dict[str, str] | None = None
    ) -> bool:
        """
        Returns True if a Delta Table exists at specified path.
        Returns False otherwise.

        Args:
            table_uri: the path of the DeltaTable
            storage_options: a dictionary of the options to use for the
                storage backend
        """
        return RawDeltaTable.is_deltatable(table_uri, storage_options)

    @classmethod
    def create(
        cls,
        table_uri: str | Path,
        schema: DeltaSchema | ArrowSchemaExportable,
        mode: Literal["error", "append", "overwrite", "ignore"] = "error",
        partition_by: list[str] | str | None = None,
        name: str | None = None,
        description: str | None = None,
        configuration: Mapping[str, str | None] | None = None,
        storage_options: dict[str, str] | None = None,
        commit_properties: CommitProperties | None = None,
        post_commithook_properties: PostCommitHookProperties | None = None,
        raise_if_key_not_exists: bool = True,
    ) -> DeltaTable:
        """`CREATE` or `CREATE_OR_REPLACE` a delta table given a table_uri.

        Args:
            table_uri: URI of a table
            schema: Table schema
            mode: How to handle existing data. Default is to error if table already exists.
                If 'append', returns not support error if table exists.
                If 'overwrite', will `CREATE_OR_REPLACE` table.
                If 'ignore', will not do anything if table already exists. Defaults to "error".
            partition_by:  List of columns to partition the table by.
            name: User-provided identifier for this table.
            description: User-provided description for this table.
            configuration:  A map containing configuration options for the metadata action.
            storage_options: Options passed to the object store crate.
            commit_properties: properties of the transaction commit. If None, default values are used.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            raise_if_key_not_exists: Whether to raise an error if the configuration uses keys that are not Delta keys

        Returns:
            DeltaTable: created delta table

        Example:
            ```python
            import pyarrow as pa

            from deltalake import DeltaTable

            dt = DeltaTable.create(
                table_uri="my_local_table",
                schema=pa.schema(
                    [pa.field("foo", pa.string()), pa.field("bar", pa.string())]
                ),
                mode="error",
                partition_by="bar",
            )
            ```
        """
        if isinstance(partition_by, str):
            partition_by = [partition_by]

        if isinstance(table_uri, Path):
            table_uri = str(table_uri)

        if not isinstance(schema, DeltaSchema):
            schema = DeltaSchema.from_arrow(schema)

        _create_deltalake(
            table_uri,
            schema,
            partition_by or [],
            mode,
            raise_if_key_not_exists,
            name,
            description,
            configuration,
            storage_options,
            commit_properties,
            post_commithook_properties,
        )

        return cls(table_uri=table_uri, storage_options=storage_options)

    def version(self) -> int:
        """
        Get the version of the DeltaTable.

        Returns:
            The current version of the DeltaTable
        """
        return self._table.version()

    def partitions(
        self,
        partition_filters: list[tuple[str, str, Any]] | None = None,
    ) -> list[dict[str, str]]:
        """
        Returns the partitions as a list of dicts. Example: `[{'month': '1', 'year': '2020', 'day': '1'}, ...]`

        Args:
            partition_filters: The partition filters that will be used for getting the matched partitions, defaults to `None` (no filtering).
        """

        partitions: list[dict[str, str]] = []
        for partition in self._table.get_active_partitions(partition_filters):
            if not partition:
                continue
            partitions.append({k: v for (k, v) in partition})
        return partitions

    def file_uris(
        self, partition_filters: FilterConjunctionType | None = None
    ) -> list[str]:
        """
        Get the list of files as absolute URIs, including the scheme (e.g. "s3://").

        Local files will be just plain absolute paths, without a scheme. (That is,
        no 'file://' prefix.)

        Use the partition_filters parameter to retrieve a subset of files that match the
        given filters.

        Args:
            partition_filters: the partition filters that will be used for getting the matched files

        Returns:
            list of the .parquet files with an absolute URI referenced for the current version of the DeltaTable

        Predicates are expressed in disjunctive normal form (DNF), like [("x", "=", "a"), ...].
        DNF allows arbitrary boolean logical combinations of single partition predicates.
        The innermost tuples each describe a single partition predicate. The list of inner
        predicates is interpreted as a conjunction (AND), forming a more selective and
        multiple partition predicates. Each tuple has format: (key, op, value) and compares
        the key with the value. The supported op are: `=`, `!=`, `in`, and `not in`. If
        the op is in or not in, the value must be a collection such as a list, a set or a tuple.
        The supported type for value is str. Use empty string `''` for Null partition value.

        Example:
            ```
            ("x", "=", "a")
            ("x", "!=", "a")
            ("y", "in", ["a", "b", "c"])
            ("z", "not in", ["a","b"])
            ```
        """
        return self._table.file_uris(
            self._stringify_partition_values(partition_filters)
        )

    file_uris.__doc__ = ""

    def load_as_version(self, version: int | str | datetime) -> None:
        """
        Load/time travel a DeltaTable to a specified version number, or a timestamp version of the table. If a
        string is passed then the argument should be an RFC 3339 and ISO 8601 date and time string format.
        If a datetime object without a timezone is passed, the UTC timezone will be assumed.

        Args:
            version: the identifier of the version of the DeltaTable to load

        Example:
            **Use a version number**
            ```
            dt = DeltaTable("test_table")
            dt.load_as_version(1)
            ```

            **Use a datetime object**
            ```
            dt.load_as_version(datetime(2023, 1, 1))
            dt.load_as_version(datetime(2023, 1, 1, tzinfo=timezone.utc))
            ```

            **Use a datetime in string format**
            ```
            dt.load_as_version("2018-01-26T18:30:09Z")
            dt.load_as_version("2018-12-19T16:39:57-08:00")
            dt.load_as_version("2018-01-26T18:30:09.453+00:00")
            ```
        """
        if isinstance(version, int):
            self._table.load_version(version)
        elif isinstance(version, datetime):
            if version.tzinfo is None:
                version = version.astimezone(timezone.utc)
            self._table.load_with_datetime(version.isoformat())
        elif isinstance(version, str):
            self._table.load_with_datetime(version)
        else:
            raise TypeError(
                "Invalid datatype provided for version, only int, str or datetime are accepted."
            )

    def load_cdf(
        self,
        starting_version: int = 0,
        ending_version: int | None = None,
        starting_timestamp: str | None = None,
        ending_timestamp: str | None = None,
        columns: list[str] | None = None,
        predicate: str | None = None,
        allow_out_of_range: bool = False,
    ) -> RecordBatchReader:
        """
        Load the Change Data Feed (CDF) from the Delta table as a stream of record batches.

        Parameters:
            starting_version (int): The version of the Delta table to start reading CDF from.
            ending_version (int | None): The version to stop reading CDF at. If None, reads up to the latest version.
            starting_timestamp (str | None): An ISO 8601 timestamp to start reading CDF from. Ignored if starting_version is provided.
            ending_timestamp (str | None): An ISO 8601 timestamp to stop reading CDF at. Ignored if ending_version is provided.
            columns (list[str] | None): A list of column names to include in the output. If None, all columns are included.
            predicate (str | None): An optional SQL predicate to filter the output rows.
            allow_out_of_range (bool): If True, does not raise an error when specified versions or timestamps are outside the table's history.

        Returns:
            RecordBatchReader: An Arrow RecordBatchReader that streams the resulting change data.

        Raises:
            ValueError: If input parameters are invalid or if the specified range is not found (unless allow_out_of_range is True).
        """
        return self._table.load_cdf(
            columns=columns,
            predicate=predicate,
            starting_version=starting_version,
            ending_version=ending_version,
            starting_timestamp=starting_timestamp,
            ending_timestamp=ending_timestamp,
            allow_out_of_range=allow_out_of_range,
        )

    def deletion_vectors(self) -> RecordBatchReader:
        """
        Return deletion vectors for data files in this table.

        Returns:
            RecordBatchReader: A reader with two columns:
                - filepath (str): fully-qualified file URI.
                - selection_vector (list[bool]): row keep mask where True means keep and False means deleted.

        Notes:
            Only files that have deletion vectors are returned.
            Deletion vectors are materialized in memory before being exposed as record batches.
        """
        return self._table.deletion_vectors()

    @property
    def table_uri(self) -> str:
        return self._table.table_uri()

    def schema(self) -> DeltaSchema:
        """
        Get the current schema of the DeltaTable.

        Returns:
            the current Schema registered in the transaction log
        """
        return self._table.schema

    def metadata(self) -> Metadata:
        """
        Get the current metadata of the DeltaTable.

        Returns:
            the current Metadata registered in the transaction log
        """
        return Metadata(self._table)

    def protocol(self) -> ProtocolVersions:
        """
        Get the reader and writer protocol versions of the DeltaTable.

        Returns:
            the current ProtocolVersions registered in the transaction log
        """
        return ProtocolVersions(*self._table.protocol_versions())

    def generate(self) -> None:
        """
        Generate symlink manifest for engines that cannot read native Delta Lake tables.

        The generate supports the fairly simple "GENERATE" operation which produces a
        [symlink_format_manifest](https://docs.delta.io/delta-utility/#generate-a-manifest-file) file
        when needed for an external engine such as Presto or BigQuery.

        The "symlink_format_manifest" is not something that has been well documented, but for
        enon-partitioned tables this will generate a `_symlink_format_manifest/manifest` file next to
        the `_delta_log`, for example:

        ```
        COVID-19_NYT
        ├── _delta_log
        │   ├── 00000000000000000000.crc
        │   └── 00000000000000000000.json
        ├── part-00000-a496f40c-e091-413a-85f9-b1b69d4b3b4e-c000.snappy.parquet
        ├── part-00001-9d9d980b-c500-4f0b-bb96-771a515fbccc-c000.snappy.parquet
        ├── part-00002-8826af84-73bd-49a6-a4b9-e39ffed9c15a-c000.snappy.parquet
        ├── part-00003-539aff30-2349-4b0d-9726-c18630c6ad90-c000.snappy.parquet
        ├── part-00004-1bb9c3e3-c5b0-4d60-8420-23261f58a5eb-c000.snappy.parquet
        ├── part-00005-4d47f8ff-94db-4d32-806c-781a1cf123d2-c000.snappy.parquet
        ├── part-00006-d0ec7722-b30c-4e1c-92cd-b4fe8d3bb954-c000.snappy.parquet
        ├── part-00007-4582392f-9fc2-41b0-ba97-a74b3afc8239-c000.snappy.parquet
        └── _symlink_format_manifest
            └── manifest
        ```
        """
        return self._table.generate()

    def history(self, limit: int | None = None) -> list[dict[str, Any]]:
        """
        Run the history command on the DeltaTable.
        The operations are returned in reverse chronological order.

        Args:
            limit: the commit info limit to return

        Returns:
            list of the commit infos registered in the transaction log
        """

        def _backwards_enumerate(
            iterable: list[str], start_end: int
        ) -> Generator[tuple[int, str], None, None]:
            n = start_end
            for elem in iterable:
                yield n, elem
                n -= 1

        commits = list(self._table.history(limit))
        history = []
        for version, commit_info_raw in _backwards_enumerate(
            commits, start_end=self._table.get_latest_version()
        ):
            commit = json.loads(commit_info_raw)
            commit["version"] = version
            history.append(commit)
        return history

    def count(self) -> int:
        """
        Get the approximate row count based on file statistics added to the Delta table.

        This requires that add actions have been added to the Delta table with
        per-file statistics enabled. Because this is an optional field this
        "count" will be less than or equal to the true row count of the table.
        In order to get an exact number of rows a full table scan must happen

        Returns:
            The approximate number of rows for this specific table
        """
        total_rows = 0

        for value in self.get_add_actions().column("num_records").to_pylist():
            # Add action file statistics are optional and so while most modern
            # tables are _likely_ to have this information it is not
            # guaranteed.
            if value is not None:
                total_rows += value

        return total_rows

    def vacuum(
        self,
        retention_hours: int | None = None,
        dry_run: bool = True,
        enforce_retention_duration: bool = True,
        post_commithook_properties: PostCommitHookProperties | None = None,
        commit_properties: CommitProperties | None = None,
        full: bool = False,
        keep_versions: list[int] | None = None,
    ) -> list[str]:
        """
        Run the Vacuum command on the Delta Table: list and delete files no longer referenced by the Delta table.
        Here "not referenced" means all removed files (from vacuum/delete/update/merge) older than the retention threshold,
        plus any files not mentioned in the logs (unless they start with underscore).

        Args:
            retention_hours: the retention threshold in hours, if none then the value from `delta.deletedFileRetentionDuration` is used or default of 1 week otherwise.
            dry_run: when activated, list only the files, delete otherwise
            enforce_retention_duration: when disabled, accepts retention hours smaller than the value from `delta.deletedFileRetentionDuration`.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties of the transaction commit. If None, default values are used.
            full: when set to True, will perform a "full" vacuum and remove all files not referenced the transaction log.
                when False, it will only vacuum not referenced files since last log checkpoint (or since genesis if no checkpoint exists).
            keep_versions: An optional list of versions to keep. If provided, files from these versions will not be deleted.
        Returns:
            the list of files no longer referenced by the Delta Table and are older than the retention threshold.
        """
        if retention_hours:
            if retention_hours < 0:
                raise ValueError("The retention periods should be positive.")

        return self._table.vacuum(
            dry_run,
            retention_hours,
            enforce_retention_duration,
            commit_properties,
            post_commithook_properties,
            full,
            keep_versions,
        )

    def update(
        self,
        updates: dict[str, str] | None = None,
        new_values: dict[str, int | float | str | datetime | bool | list[Any]]
        | None = None,
        predicate: str | None = None,
        writer_properties: WriterProperties | None = None,
        error_on_type_mismatch: bool = True,
        post_commithook_properties: PostCommitHookProperties | None = None,
        commit_properties: CommitProperties | None = None,
    ) -> dict[str, Any]:
        """`UPDATE` records in the Delta Table that matches an optional predicate. Either updates or new_values needs
        to be passed for it to execute.

        Args:
            updates: a mapping of column name to update SQL expression.
            new_values: a mapping of column name to python datatype.
            predicate: a logical expression.
            writer_properties: Pass writer properties to the Rust parquet writer.
            error_on_type_mismatch: specify if update will return error if data types are mismatching :default = True
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties of the transaction commit. If None, default values are used.
        Returns:
            the metrics from update

        Example:
            **Update some row values with SQL predicate**

            This is equivalent to `UPDATE table SET deleted = true WHERE id = '3'`
            ```py
            from deltalake import write_deltalake, DeltaTable
            import pandas as pd
            df = pd.DataFrame(
                {"id": ["1", "2", "3"],
                "deleted": [False, False, False],
                "price": [10., 15., 20.]
                })
            write_deltalake("tmp", df)
            dt = DeltaTable("tmp")
            dt.update(predicate="id = '3'", updates = {"deleted": 'True'})

            {'num_added_files': 1, 'num_removed_files': 1, 'num_updated_rows': 1, 'num_copied_rows': 2, 'execution_time_ms': ..., 'scan_time_ms': ...}
            ```

            **Update all row values**

            This is equivalent to ``UPDATE table SET deleted = true, id = concat(id, '_old')``.
            ```py
            dt.update(updates = {"deleted": 'True', "id": "concat(id, '_old')"})

            {'num_added_files': 1, 'num_removed_files': 1, 'num_updated_rows': 3, 'num_copied_rows': 0, 'execution_time_ms': ..., 'scan_time_ms': ...}
            ```

            **Use Python objects instead of SQL strings**

            Use the `new_values` parameter instead of the `updates` parameter. For example,
            this is equivalent to ``UPDATE table SET price = 150.10 WHERE id = '1'``
            ```py
            dt.update(predicate="id = '1_old'", new_values = {"price": 150.10})

            {'num_added_files': 1, 'num_removed_files': 1, 'num_updated_rows': 1, 'num_copied_rows': 2, 'execution_time_ms': ..., 'scan_time_ms': ...}
            ```
        """
        if updates is None and new_values is not None:
            updates = {}
            for key, value in new_values.items():
                if isinstance(value, (int, float, bool, list)):
                    value = str(value)
                elif isinstance(value, str):
                    value = f"'{value}'"
                elif isinstance(value, datetime):
                    value = str(
                        int(value.timestamp() * 1000 * 1000)
                    )  # convert to microseconds
                else:
                    raise TypeError(
                        "Invalid datatype provided in new_values, only int, float, bool, list, str or datetime or accepted."
                    )
                updates[key] = value
        elif updates is not None and new_values is None:
            for key, value in updates.items():
                if not isinstance(value, str) or not isinstance(key, str):
                    raise TypeError(
                        f"The values of the updates parameter must all be SQL strings. Got {updates}. Did you mean to use the new_values parameter?"
                    )

        elif updates is not None and new_values is not None:
            raise ValueError(
                "Passing updates and new_values at same time is not allowed, pick one."
            )
        else:
            raise ValueError(
                "Either updates or new_values need to be passed to update the table."
            )
        metrics = self._table.update(
            updates,
            predicate,
            writer_properties,
            safe_cast=not error_on_type_mismatch,
            commit_properties=commit_properties,
            post_commithook_properties=post_commithook_properties,
        )
        return json.loads(metrics)

    @property
    def optimize(
        self,
    ) -> TableOptimizer:
        """Namespace for all table optimize related methods.

        Returns:
            TableOptimizer: TableOptimizer Object
        """
        return TableOptimizer(self)

    @property
    def alter(
        self,
    ) -> TableAlterer:
        """Namespace for all table alter related methods.

        Returns:
            TableAlterer: TableAlterer Object
        """
        return TableAlterer(self)

    def merge(
        self,
        source: ArrowStreamExportable | ArrowArrayExportable,
        predicate: str,
        source_alias: str | None = None,
        target_alias: str | None = None,
        merge_schema: bool = False,
        error_on_type_mismatch: bool = True,
        writer_properties: WriterProperties | None = None,
        streamed_exec: bool = True,
        max_spill_size: int | None = None,
        max_temp_directory_size: int | None = None,
        post_commithook_properties: PostCommitHookProperties | None = None,
        commit_properties: CommitProperties | None = None,
    ) -> TableMerger:
        """Pass the source data which you want to merge on the target delta table, providing a
        predicate in SQL query like format. You can also specify on what to do when the underlying data types do not
        match the underlying table.

        Args:
            source: source data
            predicate: SQL like predicate on how to merge
            source_alias: Alias for the source table
            target_alias: Alias for the target table
            merge_schema: Enable merge schema evolution for mismatch schema between source and target tables
            error_on_type_mismatch: specify if merge will return error if data types are mismatching :default = True
            writer_properties: Pass writer properties to the Rust parquet writer
            streamed_exec: Will execute MERGE using a LazyMemoryExec plan, this improves memory pressure for large source tables. Enabling streamed_exec
                implicitly disables source table stats to derive an early_pruning_predicate
            max_spill_size: The maximum number of bytes allowed in memory before spilling to disk.
                If not specified, uses DataFusion's default.
                Set this to avoid OOM when merging into large tables with a source table which touches a large number of files.
            max_temp_directory_size: The maximum disk space for temporary spill files. If not specified, uses DataFusion's default.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties for the commit. If None, default values are used.

        Returns:
            TableMerger: TableMerger Object
        """

        source = RecordBatchReader.from_arrow(source)
        compatible_delta_schema = _convert_arro3_schema_to_delta(source.schema)

        py_merge_builder = self._table.create_merge_builder(
            source=source,
            batch_schema=compatible_delta_schema,
            predicate=predicate,
            source_alias=source_alias,
            target_alias=target_alias,
            merge_schema=merge_schema,
            safe_cast=not error_on_type_mismatch,
            streamed_exec=streamed_exec,
            max_spill_size=max_spill_size,
            max_temp_directory_size=max_temp_directory_size,
            writer_properties=writer_properties,
            commit_properties=commit_properties,
            post_commithook_properties=post_commithook_properties,
        )
        return TableMerger(py_merge_builder, self._table)

    def restore(
        self,
        target: int | datetime | str,
        *,
        ignore_missing_files: bool = False,
        protocol_downgrade_allowed: bool = False,
        commit_properties: CommitProperties | None = None,
    ) -> dict[str, Any]:
        """
        Run the Restore command on the Delta Table: restore table to a given version or datetime.

        Args:
            target: the expected version will restore, which represented by int, date str or datetime.
            ignore_missing_files: whether the operation carry on when some data files missing.
            protocol_downgrade_allowed: whether the operation when protocol version upgraded.
            commit_properties: properties of the transaction commit. If None, default values are used.

        Returns:
            the metrics from restore.
        """
        if isinstance(target, datetime):
            metrics = self._table.restore(
                target.isoformat(),
                ignore_missing_files=ignore_missing_files,
                protocol_downgrade_allowed=protocol_downgrade_allowed,
                commit_properties=commit_properties,
            )
        else:
            metrics = self._table.restore(
                target,
                ignore_missing_files=ignore_missing_files,
                protocol_downgrade_allowed=protocol_downgrade_allowed,
                commit_properties=commit_properties,
            )
        return json.loads(metrics)

    def to_pyarrow_dataset(
        self,
        partitions: FilterConjunctionType | None = None,
        filesystem: str | pa_fs.FileSystem | None = None,
        parquet_read_options: ParquetReadOptions | None = None,
        schema: pyarrow.Schema | None = None,
        as_large_types: bool = False,
    ) -> "pyarrow.dataset.Dataset":
        """
        Build a PyArrow Dataset using data from the DeltaTable.

        Args:
            partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
            filesystem: A concrete implementation of the Pyarrow FileSystem or a fsspec-compatible interface. If None, the first file path will be used to determine the right FileSystem
            parquet_read_options: Optional read options for Parquet. Use this to handle INT96 to timestamp conversion for edge cases like 0001-01-01 or 9999-12-31
            schema: The schema to use for the dataset. If None, the schema of the DeltaTable will be used. This can be used to force reading of Parquet/Arrow datatypes
                that DeltaLake can't represent in it's schema (e.g. LargeString).
                If you only need to read the schema with large types (e.g. for compatibility with Polars) you may want to use the `as_large_types` parameter instead.
            as_large_types: get schema with all variable size types (list, binary, string) as large variants (with int64 indices).
                This is for compatibility with systems like Polars that only support the large versions of Arrow types.
                If `schema` is passed it takes precedence over this option.

         More info: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetReadOptions.html

        Example:
            ``deltalake`` will work with any storage compliant with :class:`pyarrow.fs.FileSystem`, however the root of the filesystem has
            to be adjusted to point at the root of the Delta table. We can achieve this by wrapping the custom filesystem into
            a :class:`pyarrow.fs.SubTreeFileSystem`.
            ```
            import pyarrow.fs as fs
            from deltalake import DeltaTable

            table_uri = "s3://<bucket>/<path>"
            raw_fs, normalized_path = fs.FileSystem.from_uri(table_uri)
            filesystem = fs.SubTreeFileSystem(normalized_path, raw_fs)

            dt = DeltaTable(table_uri)
            ds = dt.to_pyarrow_dataset(filesystem=filesystem)
            ```

        Returns:
            the PyArrow dataset in PyArrow
        """
        try:
            from pyarrow.dataset import (
                FileSystemDataset,
                ParquetFileFormat,
                ParquetFragmentScanOptions,
            )
        except ImportError:
            raise ImportError(
                "Pyarrow is required, install deltalake[pyarrow] for pyarrow read functionality."
            )
        if not self._table.has_files():
            raise DeltaError("Table is instantiated without files.")

        table_protocol = self.protocol()
        if (
            table_protocol.min_reader_version > MAX_SUPPORTED_READER_VERSION
            or table_protocol.min_reader_version == NOT_SUPPORTED_READER_VERSION
        ):
            raise DeltaProtocolError(
                f"The table's minimum reader version is {table_protocol.min_reader_version} "
                f"but deltalake only supports version 1 or {MAX_SUPPORTED_READER_VERSION} "
                f"with these reader features: {SUPPORTED_READER_FEATURES}"
            )
        if (
            table_protocol.min_reader_version >= 3
            and table_protocol.reader_features is not None
        ):
            missing_features = {*table_protocol.reader_features}.difference(
                SUPPORTED_READER_FEATURES
            )
            if len(missing_features) > 0:
                raise DeltaProtocolError(
                    f"The table has set these reader features: {missing_features} "
                    "but these are not yet supported by the deltalake reader."
                )

        if (
            table_protocol.reader_features
            and "columnMapping" in table_protocol.reader_features
        ):
            raise DeltaProtocolError(
                "The table requires reader feature 'columnMapping' "
                "but this is not supported using pyarrow Datasets."
            )

        if (
            table_protocol.reader_features
            and "deletionVectors" in table_protocol.reader_features
        ):
            raise DeltaProtocolError(
                "The table requires reader feature 'deletionVectors' "
                "but this is not supported using pyarrow Datasets."
            )

        import pyarrow
        import pyarrow.fs as pa_fs

        from deltalake.fs import DeltaStorageHandler

        if not filesystem:
            filesystem = pa_fs.PyFileSystem(
                DeltaStorageHandler.from_table(
                    self._table,
                    self._storage_options,
                    self._table.get_add_file_sizes(),
                )
            )

        format = ParquetFileFormat(
            read_options=parquet_read_options,
            default_fragment_scan_options=ParquetFragmentScanOptions(pre_buffer=True),
        )

        if schema is None:
            schema = pyarrow.schema(
                self.schema().to_arrow(as_large_types=as_large_types)
            )

        partitions = self._stringify_partition_values(partitions)

        fragments = [
            format.make_fragment(
                file,
                filesystem=filesystem,
                partition_expression=part_expression,
            )
            for file, part_expression in self._table.dataset_partitions(
                schema, partitions
            )
        ]

        dictionary_columns = format.read_options.dictionary_columns or set()
        if dictionary_columns:
            for index, field in enumerate(schema):
                if field.name in dictionary_columns:
                    dict_field = field.with_type(
                        pyarrow.dictionary(pyarrow.int32(), field.type)
                    )
                    schema = schema.set(index, dict_field)

        return FileSystemDataset(fragments, schema, format, filesystem)

    def to_pyarrow_table(
        self,
        partitions: list[tuple[str, str, Any]] | None = None,
        columns: list[str] | None = None,
        filesystem: str | pa_fs.FileSystem | None = None,
        filters: FilterType | Expression | None = None,
    ) -> "pyarrow.Table":
        """
        Build a PyArrow Table using data from the DeltaTable.

        Args:
            partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
            columns: The columns to project. This can be a list of column names to include (order and duplicates will be preserved)
            filesystem: A concrete implementation of the Pyarrow FileSystem or a fsspec-compatible interface. If None, the first file path will be used to determine the right FileSystem
            filters: A disjunctive normal form (DNF) predicate for filtering rows, or directly a pyarrow.dataset.Expression. If you pass a filter you do not need to pass ``partitions``
        """
        try:
            from pyarrow.parquet import filters_to_expression  # pyarrow >= 10.0.0
        except ImportError:
            raise ImportError(
                "Pyarrow is required, install deltalake[pyarrow] for pyarrow read functionality."
            )

        if filters is not None:
            filters = filters_to_expression(filters)
        return self.to_pyarrow_dataset(
            partitions=partitions, filesystem=filesystem
        ).to_table(columns=columns, filter=filters)

    def to_pandas(
        self,
        partitions: list[tuple[str, str, Any]] | None = None,
        columns: list[str] | None = None,
        filesystem: str | pa_fs.FileSystem | None = None,
        filters: FilterType | Expression | None = None,
        types_mapper: Callable[[pyarrow.DataType], Any] | None = None,
    ) -> "pd.DataFrame":
        """
        Build a pandas dataframe using data from the DeltaTable.

        Args:
            partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
            columns: The columns to project. This can be a list of column names to include (order and duplicates will be preserved)
            filesystem: A concrete implementation of the Pyarrow FileSystem or a fsspec-compatible interface. If None, the first file path will be used to determine the right FileSystem
            filters: A disjunctive normal form (DNF) predicate for filtering rows, or directly a pyarrow.dataset.Expression. If you pass a filter you do not need to pass ``partitions``
        """
        return self.to_pyarrow_table(
            partitions=partitions,
            columns=columns,
            filesystem=filesystem,
            filters=filters,
        ).to_pandas(types_mapper=types_mapper)

    def update_incremental(self) -> None:
        """
        Updates the DeltaTable to the latest version by incrementally applying
        newer versions.
        """
        self._table.update_incremental()

    def create_checkpoint(self) -> None:
        """
        Create a checkpoint at the current table version.
        """
        self._table.create_checkpoint()

    def compact_logs(self, starting_version: int, ending_version: int) -> None:
        """
        Create a compaction log for a given version range.
        """
        self._table.compact_logs(starting_version, ending_version)

    def cleanup_metadata(self) -> None:
        """
        Delete expired log files before current version from table. The table log retention is based on
        the `delta.logRetentionDuration` value, 30 days by default.
        """
        self._table.cleanup_metadata()

    def _stringify_partition_values(
        self, partition_filters: FilterConjunctionType | None
    ) -> PartitionFilterType | None:
        if partition_filters is None:
            return partition_filters
        out = []
        for field, op, value in partition_filters:
            str_value: str | list[str]
            if isinstance(value, (list, tuple)):
                str_value = [encode_partition_value(val) for val in value]
            else:
                str_value = encode_partition_value(value)
            out.append((field, op, str_value))
        return out

    def get_add_actions(self, flatten: bool = False) -> Table:
        """Return an Arrow table describing every file currently in the table.

        Each row corresponds to one data file (an *add* action in the
        Delta transaction log).  The returned columns always include:

        - ``path`` relative file path
        - ``size_bytes`` file size in bytes
        - ``modification_time`` last modification timestamp (ms)
        - ``num_records`` row count (when stats are available)

        When ``flatten=False`` (default), partition values and column
        statistics are returned as nested struct columns (``partition``,
        ``null_count``, ``min``, ``max``).

        When ``flatten=True``, those structs are flattened into
        top-level columns with dot-separated prefixes, e.g.
        ``partition.year``, ``null_count.value``, ``min.value``.

        Args:
            flatten: If True, flatten nested partition and statistics
                columns into dot-separated top-level columns.

        Returns:
            An arro3 Table containing one row per data file.

        Example:
            ```python
            from deltalake import DeltaTable, write_deltalake
            import pyarrow as pa

            data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
            write_deltalake("/tmp/my_table", data, partition_by=["x"])
            dt = DeltaTable("/tmp/my_table")

            # Default: partition values in a nested struct column
            actions = dt.get_add_actions()
            actions.column("path")
            actions.column("partition").field("x")

            # Flattened: partition values as top-level columns
            flat = dt.get_add_actions(flatten=True)
            flat.column("partition.x")
            ```
        """
        return self._table.get_add_actions(flatten)

    def delete(
        self,
        predicate: str | None = None,
        writer_properties: WriterProperties | None = None,
        post_commithook_properties: PostCommitHookProperties | None = None,
        commit_properties: CommitProperties | None = None,
    ) -> dict[str, Any]:
        """Delete records from a Delta Table that satisfy a predicate.

        When a predicate is not provided then all records are deleted from the Delta
        Table. Otherwise a scan of the Delta table is performed to mark any files
        that contain records that satisfy the predicate. Once files are determined
        they are rewritten without the records.

        Args:
            predicate: a SQL where clause. If not passed, will delete all rows.
            writer_properties: Pass writer properties to the Rust parquet writer.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties of the transaction commit. If None, default values are used.

        Returns:
            the metrics from delete.
        """
        metrics = self._table.delete(
            predicate,
            writer_properties,
            commit_properties,
            post_commithook_properties,
        )
        return json.loads(metrics)

    def repair(
        self,
        dry_run: bool = False,
        post_commithook_properties: PostCommitHookProperties | None = None,
        commit_properties: CommitProperties | None = None,
    ) -> dict[str, Any]:
        """Repair the Delta Table by auditing active files that do not exist in the underlying
        filesystem and removes them. This can be useful when there are accidental deletions or corrupted files.

        Active files are ones that have an add action in the log, but no corresponding remove action.
        This operation creates a new FSCK transaction containing a remove action for each of the missing
        or corrupted files.

        Args:
            dry_run: when activated, list only the files, otherwise add remove actions to transaction log. Defaults to False.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties of the transaction commit. If None, default values are used.

        Returns:
            The metrics from repair (FSCK) action.

        Example:
            ```python
            from deltalake import DeltaTable
            dt = DeltaTable('TEST')
            dt.repair(dry_run=False)
            ```
            Results in
            ```
            {'dry_run': False, 'files_removed': ['6-0d084325-6885-4847-b008-82c1cf30674c-0.parquet', 5-4fba1d3e-3e20-4de1-933d-a8e13ac59f53-0.parquet']}
            ```
        """
        metrics = self._table.repair(
            dry_run,
            commit_properties,
            post_commithook_properties,
        )
        deserialized_metrics = json.loads(metrics)
        deserialized_metrics[FSCK_METRICS_FILES_REMOVED_LABEL] = json.loads(
            deserialized_metrics[FSCK_METRICS_FILES_REMOVED_LABEL]
        )
        return deserialized_metrics

    def transaction_version(self, app_id: str) -> int | None:
        """
        Retrieve the latest transaction versions for the given application ID.

        Args:
            app_id (str): The application ID for which to retrieve the latest transaction version.

        Returns:
            int | None: The latest transaction version for the given application ID if it exists, otherwise None.
        """
        return self._table.transaction_version(app_id)

    def create_write_transaction(
        self,
        actions: list[AddAction],
        mode: str,
        schema: DeltaSchema | ArrowSchemaExportable,
        partition_by: list[str] | str | None = None,
        partition_filters: FilterType | None = None,
        commit_properties: CommitProperties | None = None,
        post_commithook_properties: PostCommitHookProperties | None = None,
    ) -> None:
        if isinstance(partition_by, str):
            partition_by = [partition_by]

        if not isinstance(schema, DeltaSchema):
            schema = DeltaSchema.from_arrow(schema)

        self._table.create_write_transaction(
            actions,
            mode,
            partition_by or [],
            schema,
            partition_filters,
            commit_properties=commit_properties,
            post_commithook_properties=post_commithook_properties,
        )

    def __datafusion_table_provider__(self, session: Any | None = None) -> Any:
        """Return the DataFusion table provider PyCapsule interface.

        To support DataFusion features such as push down filtering, this function will return a PyCapsule
        interface that conforms to the FFI Table Provider required by DataFusion. From an end user perspective
        you should not need to call this function directly. Instead you can use ``register_table`` in
        the DataFusion SessionContext.

        Returns:
            A PyCapsule DataFusion TableProvider interface.

        Example:
            ```python
            from deltalake import DeltaTable, write_deltalake
            from datafusion import SessionContext
            import pyarrow as pa
            data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
            write_deltalake("tmp", data)
            dt = DeltaTable("tmp")
            ctx = SessionContext()
            ctx.register_table("test", dt)
            ctx.table("test").show()
            ```
            Results in
            ```
            DataFrame()
            +----+----+----+
            | c3 | c1 | c2 |
            +----+----+----+
            | 4  | 6  | a  |
            | 6  | 5  | b  |
            | 5  | 4  | c  |
            +----+----+----+
            ```
        """
        return self._table.__datafusion_table_provider__(session)


class TableMerger:
    """API for various table `MERGE` commands."""

    def __init__(
        self,
        builder: PyMergeBuilder,
        table: RawDeltaTable,
    ) -> None:
        self._builder = builder
        self._table = table

    def when_matched_update(
        self, updates: dict[str, str], predicate: str | None = None
    ) -> TableMerger:
        """Update a matched table row based on the rules defined by ``updates``.
        If a ``predicate`` is specified, then it must evaluate to true for the row to be updated.

        Note:
            Column names with special characters, such as numbers or spaces should be encapsulated
            in backticks: "target.`123column`" or "target.`my column`"

        Args:
            updates: a mapping of column name to update SQL expression.
            predicate:  SQL like predicate on when to update.

        Returns:
            TableMerger: TableMerger Object

        Example:
            ```python
            from deltalake import DeltaTable, write_deltalake
            import pyarrow as pa

            data = pa.table({"x": [1, 2, 3], "1y": [4, 5, 6]})
            write_deltalake("tmp", data)
            dt = DeltaTable("tmp")
            new_data = pa.table({"x": [1], "1y": [7]})

            (
                 dt.merge(
                     source=new_data,
                     predicate="target.x = source.x",
                     source_alias="source",
                     target_alias="target")
                 .when_matched_update(updates={"x": "source.x", "`1y`": "source.`1y`"})
                 .execute()
            )
            {'num_source_rows': 1, 'num_target_rows_inserted': 0, 'num_target_rows_updated': 1, 'num_target_rows_deleted': 0, 'num_target_rows_copied': 2, 'num_output_rows': 3, 'num_target_files_added': 1, 'num_target_files_removed': 1, 'execution_time_ms': ..., 'scan_time_ms': ..., 'rewrite_time_ms': ...}

            dt.to_pandas()
               x  y
            0  1  7
            1  2  5
            2  3  6
            ```
        """
        self._builder.when_matched_update(updates, predicate)
        return self

    def when_matched_update_all(
        self, predicate: str | None = None, except_cols: list[str] | None = None
    ) -> TableMerger:
        """Updating all source fields to target fields, source and target are required to have the same field names.
        If a ``predicate`` is specified, then it must evaluate to true for the row to be updated.
        If ``except_cols`` is specified, then the columns in the exclude list will not be updated.

        Note:
            Column names with special characters, such as numbers or spaces should be encapsulated
            in backticks: "target.`123column`" or "target.`my column`"

        Args:
            predicate: SQL like predicate on when to update all columns.
            except_cols: List of columns to exclude from update.
        Returns:
            TableMerger: TableMerger Object

        Example:
            ** Update all columns **

            ```python
            from deltalake import DeltaTable, write_deltalake
            import pyarrow as pa

            data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
            write_deltalake("tmp", data)
            dt = DeltaTable("tmp")
            new_data = pa.table({"x": [1], "y": [7]})

            (
                dt.merge(
                    source=new_data,
                    predicate="target.x = source.x",
                    source_alias="source",
                    target_alias="target")
                .when_matched_update_all()
                .execute()
            )
            {'num_source_rows': 1, 'num_target_rows_inserted': 0, 'num_target_rows_updated': 1, 'num_target_rows_deleted': 0, 'num_target_rows_copied': 2, 'num_output_rows': 3, 'num_target_files_added': 1, 'num_target_files_removed': 1, 'execution_time_ms': ..., 'scan_time_ms': ..., 'rewrite_time_ms': ...}

            dt.to_pandas()
               x  y
            0  1  7
            1  2  5
            2  3  6
            ```

            ** Update all columns except `bar` **

            ```python
            from deltalake import DeltaTable, write_deltalake
            import pyarrow as pa

            data = pa.table({"foo": [1, 2, 3], "bar": [4, 5, 6]})
            write_deltalake("tmp", data)
            dt = DeltaTable("tmp")
            new_data = pa.table({"foo": [1], "bar": [7]})

            (
                dt.merge(
                    source=new_data,
                    predicate="target.foo = source.foo",
                    source_alias="source",
                    target_alias="target")
                .when_matched_update_all(except_cols=["bar"])
                .execute()
            )
            {'num_source_rows': 1, 'num_target_rows_inserted': 0, 'num_target_rows_updated': 1, 'num_target_rows_deleted': 0, 'num_target_rows_copied': 2, 'num_output_rows': 3, 'num_target_files_added': 1, 'num_target_files_removed': 1, 'execution_time_ms': ..., 'scan_time_ms': ..., 'rewrite_time_ms': ...}

            dt.to_pandas()
               foo  bar
            0  1    4
            1  2    5
            2  3    6
            ```
        """
        maybe_source_alias = self._builder.source_alias
        maybe_target_alias = self._builder.target_alias

        src_alias = (maybe_source_alias + ".") if maybe_source_alias is not None else ""
        trgt_alias = (
            (maybe_target_alias + ".") if maybe_target_alias is not None else ""
        )

        except_columns = except_cols or []

        updates = {
            f"{trgt_alias}`{col.name}`": f"{src_alias}`{col.name}`"
            for col in self._builder.arrow_schema  # type: ignore[attr-defined]
            if col.name not in except_columns
        }

        self._builder.when_matched_update(updates, predicate)
        return self

    def when_matched_delete(self, predicate: str | None = None) -> TableMerger:
        """Delete a matched row from the table only if the given ``predicate`` (if specified) is
        true for the matched row. If not specified it deletes all matches.

        Note:
            Column names with special characters, such as numbers or spaces should be encapsulated
            in backticks: "target.`123column`" or "target.`my column`"

        Args:
           predicate (str | None, Optional):  SQL like predicate on when to delete.

        Returns:
            TableMerger: TableMerger Object

        Example:
            **Delete on a predicate**

            ```python
            from deltalake import DeltaTable, write_deltalake
            import pyarrow as pa

            data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
            write_deltalake("tmp", data)
            dt = DeltaTable("tmp")
            new_data = pa.table({"x": [2, 3], "deleted": [False, True]})

            (
                dt.merge(
                    source=new_data,
                    predicate='target.x = source.x',
                    source_alias='source',
                    target_alias='target')
                .when_matched_delete(
                    predicate="source.deleted = true")
                .execute()
            )
            {'num_source_rows': 2, 'num_target_rows_inserted': 0, 'num_target_rows_updated': 0, 'num_target_rows_deleted': 1, 'num_target_rows_copied': 2, 'num_output_rows': 2, 'num_target_files_added': 1, 'num_target_files_removed': 1, 'execution_time_ms': ..., 'scan_time_ms': ..., 'rewrite_time_ms': ...}

            dt.to_pandas().sort_values("x", ignore_index=True)
               x  y
            0  1  4
            1  2  5
            ```

            **Delete all records that were matched**
            ```python
            dt = DeltaTable("tmp")
            (
                dt.merge(
                    source=new_data,
                    predicate='target.x = source.x',
                    source_alias='source',
                    target_alias='target')
                .when_matched_delete()
                .execute()
            )
            {'num_source_rows': 2, 'num_target_rows_inserted': 0, 'num_target_rows_updated': 0, 'num_target_rows_deleted': 1, 'num_target_rows_copied': 1, 'num_output_rows': 1, 'num_target_files_added': 1, 'num_target_files_removed': 1, 'execution_time_ms': ..., 'scan_time_ms': ..., 'rewrite_time_ms': ...}

            dt.to_pandas()
               x  y
            0  1  4
            ```
        """
        self._builder.when_matched_delete(predicate)
        return self

    def when_not_matched_insert(
        self, updates: dict[str, str], predicate: str | None = None
    ) -> TableMerger:
        """Insert a new row to the target table based on the rules defined by ``updates``. If a
        ``predicate`` is specified, then it must evaluate to true for the new row to be inserted.

        Note:
            Column names with special characters, such as numbers or spaces should be encapsulated
            in backticks: "target.`123column`" or "target.`my column`"

        Args:
            updates (dict):  a mapping of column name to insert SQL expression.
            predicate (str | None, Optional): SQL like predicate on when to insert.

        Returns:
            TableMerger: TableMerger Object

        Example:
            ```python
            from deltalake import DeltaTable, write_deltalake
            import pyarrow as pa

            data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
            write_deltalake("tmp", data)
            dt = DeltaTable("tmp")
            new_data = pa.table({"x": [4], "y": [7]})

            (
                dt.merge(
                    source=new_data,
                    predicate="target.x = source.x",
                    source_alias="source",
                    target_alias="target",)
                .when_not_matched_insert(
                    updates={
                        "x": "source.x",
                        "y": "source.y",
                    })
                .execute()
            )
            {'num_source_rows': 1, 'num_target_rows_inserted': 1, 'num_target_rows_updated': 0, 'num_target_rows_deleted': 0, 'num_target_rows_copied': 3, 'num_output_rows': 4, 'num_target_files_added': 1, 'num_target_files_removed': 1, 'execution_time_ms': ..., 'scan_time_ms': ..., 'rewrite_time_ms': ...}

            dt.to_pandas().sort_values("x", ignore_index=True)
               x  y
            0  1  4
            1  2  5
            2  3  6
            3  4  7
            ```
        """
        self._builder.when_not_matched_insert(updates, predicate)
        return self

    def when_not_matched_insert_all(
        self, predicate: str | None = None, except_cols: list[str] | None = None
    ) -> TableMerger:
        """Insert a new row to the target table, updating all source fields to target fields. Source and target are
        required to have the same field names. If a ``predicate`` is specified, then it must evaluate to true for
        the new row to be inserted. If ``except_cols`` is specified, then the columns in the exclude list will not be inserted.

        Note:
            Column names with special characters, such as numbers or spaces should be encapsulated
            in backticks: "target.`123column`" or "target.`my column`"

        Args:
            predicate: SQL like predicate on when to insert.
            except_cols: List of columns to exclude from insert.

        Returns:
            TableMerger: TableMerger Object

        Example:
            ** Insert all columns **

            ```python
            from deltalake import DeltaTable, write_deltalake
            import pyarrow as pa

            data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
            write_deltalake("tmp", data)
            dt = DeltaTable("tmp")
            new_data = pa.table({"x": [4], "y": [7]})

            (
               dt.merge(
                   source=new_data,
                   predicate='target.x = source.x',
                   source_alias='source',
                   target_alias='target')
               .when_not_matched_insert_all()
               .execute()
            )
            {'num_source_rows': 1, 'num_target_rows_inserted': 1, 'num_target_rows_updated': 0, 'num_target_rows_deleted': 0, 'num_target_rows_copied': 3, 'num_output_rows': 4, 'num_target_files_added': 1, 'num_target_files_removed': 1, 'execution_time_ms': ..., 'scan_time_ms': ..., 'rewrite_time_ms': ...}

            dt.to_pandas().sort_values("x", ignore_index=True)
               x  y
            0  1  4
            1  2  5
            2  3  6
            3  4  7
            ```

            ** Insert all columns except `bar` **

            ```python
            from deltalake import DeltaTable, write_deltalake
            import pyarrow as pa

            data = pa.table({"foo": [1, 2, 3], "bar": [4, 5, 6]})
            write_deltalake("tmp", data)
            dt = DeltaTable("tmp")
            new_data = pa.table({"foo": [4], "bar": [7]})

            (
               dt.merge(
                   source=new_data,
                   predicate='target.foo = source.foo',
                   source_alias='source',
                   target_alias='target')
               .when_not_matched_insert_all(except_cols=["bar"])
               .execute()
            )
            {'num_source_rows': 1, 'num_target_rows_inserted': 1, 'num_target_rows_updated': 0, 'num_target_rows_deleted': 0, 'num_target_rows_copied': 3, 'num_output_rows': 4, 'num_target_files_added': 1, 'num_target_files_removed': 1, 'execution_time_ms': ..., 'scan_time_ms': ..., 'rewrite_time_ms': ...}

            dt.to_pandas().sort_values("foo", ignore_index=True)
               foo  bar
            0  1    4
            1  2    5
            2  3    6
            3  4    NaN
            ```
        """
        maybe_source_alias = self._builder.source_alias
        maybe_target_alias = self._builder.target_alias

        src_alias = (maybe_source_alias + ".") if maybe_source_alias is not None else ""
        trgt_alias = (
            (maybe_target_alias + ".") if maybe_target_alias is not None else ""
        )

        except_columns = except_cols or []

        updates = {
            f"{trgt_alias}`{col.name}`": f"{src_alias}`{col.name}`"
            for col in self._builder.arrow_schema  # type: ignore[attr-defined]
            if col.name not in except_columns
        }

        self._builder.when_not_matched_insert(updates, predicate)
        return self

    def when_not_matched_by_source_update(
        self, updates: dict[str, str], predicate: str | None = None
    ) -> TableMerger:
        """Update a target row that has no matches in the source based on the rules defined by ``updates``.
        If a ``predicate`` is specified, then it must evaluate to true for the row to be updated.

        Note:
            Column names with special characters, such as numbers or spaces should be encapsulated
            in backticks: "target.`123column`" or "target.`my column`"

        Args:
            updates: a mapping of column name to update SQL expression.
            predicate: SQL like predicate on when to update.

        Returns:
            TableMerger: TableMerger Object

        Example:
            ```python
            from deltalake import DeltaTable, write_deltalake
            import pyarrow as pa

            data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
            write_deltalake("tmp", data)
            dt = DeltaTable("tmp")
            new_data = pa.table({"x": [2, 3, 4]})

            (
               dt.merge(
                   source=new_data,
                   predicate='target.x = source.x',
                   source_alias='source',
                   target_alias='target')
               .when_not_matched_by_source_update(
                   predicate = "y > 3",
                   updates = {"y": "0"})
               .execute()
            )
            {'num_source_rows': 3, 'num_target_rows_inserted': 0, 'num_target_rows_updated': 1, 'num_target_rows_deleted': 0, 'num_target_rows_copied': 2, 'num_output_rows': 3, 'num_target_files_added': 1, 'num_target_files_removed': 1, 'execution_time_ms': ..., 'scan_time_ms': ..., 'rewrite_time_ms': ...}

            dt.to_pandas().sort_values("x", ignore_index=True)
               x  y
            0  1  0
            1  2  5
            2  3  6
            ```
        """
        self._builder.when_not_matched_by_source_update(updates, predicate)
        return self

    def when_not_matched_by_source_delete(
        self, predicate: str | None = None
    ) -> TableMerger:
        """Delete a target row that has no matches in the source from the table only if the given
        ``predicate`` (if specified) is true for the target row.

        Note:
            Column names with special characters, such as numbers or spaces should be encapsulated
            in backticks: "target.`123column`" or "target.`my column`"

        Args:
            predicate:  SQL like predicate on when to delete when not matched by source.

        Returns:
            TableMerger: TableMerger Object
        """
        self._builder.when_not_matched_by_source_delete(predicate)
        return self

    def execute(self) -> dict[str, Any]:
        """Executes `MERGE` with the previously provided settings in Rust with Apache Datafusion query engine.

        Returns:
            Dict: metrics
        """
        metrics = self._table.merge_execute(self._builder)
        return json.loads(metrics)


class TableAlterer:
    """API for various table alteration commands."""

    def __init__(self, table: DeltaTable) -> None:
        self.table = table

    def add_feature(
        self,
        feature: TableFeatures | list[TableFeatures],
        allow_protocol_versions_increase: bool = False,
        commit_properties: CommitProperties | None = None,
        post_commithook_properties: PostCommitHookProperties | None = None,
    ) -> None:
        """
        Enable a table feature.

        Args:
            feature: Table Feature e.g. Deletion Vectors, Change Data Feed
            allow_protocol_versions_increase: Allow the protocol to be implicitly bumped to reader 3 or writer 7
            commit_properties: properties of the transaction commit. If None, default values are used.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.

        Example:
            ```python
            from deltalake import DeltaTable
            dt = DeltaTable("test_table")
            dt.alter.add_feature(TableFeatures.AppendOnly)
            ```

            **Check protocol**
            ```
            dt.protocol()
            ProtocolVersions(min_reader_version=1, min_writer_version=7, writer_features=['appendOnly'], reader_features=None)
            ```
        """
        if isinstance(feature, TableFeatures):
            feature = [feature]
        self.table._table.add_feature(
            feature,
            allow_protocol_versions_increase,
            commit_properties,
            post_commithook_properties,
        )

    def add_columns(
        self,
        fields: DeltaField | list[DeltaField],
        commit_properties: CommitProperties | None = None,
        post_commithook_properties: PostCommitHookProperties | None = None,
    ) -> None:
        """Add new columns and/or update the fields of a stuctcolumn

        Args:
            fields: fields to merge into schema
            commit_properties: properties of the transaction commit. If None, default values are used.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.

        Example:
            ```python
            from deltalake.schema import Field, PrimitiveType, StructType
            dt = DeltaTable("test_table")
            new_fields = [
                Field("baz", StructType([Field("bar", PrimitiveType("integer"))])),
                Field("bar", PrimitiveType("integer"))
            ]
            dt.alter.add_columns(
                new_fields
            )
            ```
        """
        if isinstance(fields, DeltaField):
            fields = [fields]

        self.table._table.add_columns(
            fields,
            commit_properties,
            post_commithook_properties,
        )

    def add_constraint(
        self,
        constraints: dict[str, str],
        post_commithook_properties: PostCommitHookProperties | None = None,
        commit_properties: CommitProperties | None = None,
    ) -> None:
        """
        Add constraints to the table. Limited to `single constraint` at once.

        Args:
            constraints: mapping of constraint name to SQL-expression to evaluate on write
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties of the transaction commit. If None, default values are used.

        Example:
            ```python
            from deltalake import DeltaTable
            dt = DeltaTable("test_table_constraints")
            dt.alter.add_constraint({
                "value_gt_5": "value > 5",
            })
            ```

            **Check configuration**
            ```
            dt.metadata().configuration
            {'delta.constraints.value_gt_5': 'value > 5'}
            ```
        """
        self.table._table.add_constraints(
            constraints,
            commit_properties,
            post_commithook_properties,
        )

    def drop_constraint(
        self,
        name: str,
        raise_if_not_exists: bool = True,
        post_commithook_properties: PostCommitHookProperties | None = None,
        commit_properties: CommitProperties | None = None,
    ) -> None:
        """
        Drop constraints from a table. Limited to `single constraint` at once.

        Args:
            name: constraint name which to drop.
            raise_if_not_exists: set if should raise if not exists.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties of the transaction commit. If None, default values are used.

        Example:
            ```python
            from deltalake import DeltaTable
            dt = DeltaTable("test_table_constraints")
            dt.metadata().configuration
            {'delta.constraints.value_gt_5': 'value > 5'}
            ```

            **Drop the constraint**
            ```python
            dt.alter.drop_constraint(name = "value_gt_5")
            ```

            **Configuration after dropping**
            ```python
            dt.metadata().configuration
            {}
            ```
        """
        self.table._table.drop_constraints(
            name,
            raise_if_not_exists,
            commit_properties,
            post_commithook_properties,
        )

    def set_table_properties(
        self,
        properties: dict[str, str],
        raise_if_not_exists: bool = True,
        commit_properties: CommitProperties | None = None,
    ) -> None:
        """
        Set properties from the table.

        Args:
            properties: properties which set
            raise_if_not_exists: set if should raise if not exists.
            commit_properties: properties of the transaction commit. If None, default values are used.

        Example:
            ```python
            from deltalake import write_deltalake, DeltaTable
            import pandas as pd
            df = pd.DataFrame(
                {"id": ["1", "2", "3"],
                "deleted": [False, False, False],
                "price": [10., 15., 20.]
                })
            write_deltalake("tmp", df)

            dt = DeltaTable("tmp")
            dt.alter.set_table_properties({"delta.enableChangeDataFeed": "true"})
            ```
        """
        self.table._table.set_table_properties(
            properties,
            raise_if_not_exists,
            commit_properties,
        )

    def set_table_name(
        self,
        name: str,
        commit_properties: CommitProperties | None = None,
    ) -> None:
        """
        Set the name of the table.

        Args:
            name: the name of the table
            commit_properties: properties of the transaction commit. If None, default values are used.
                              Note: This parameter is not yet implemented and will be ignored.

        Example:
            ```python
            from deltalake import DeltaTable
            dt = DeltaTable("test_table")
            dt.alter.set_table_name("new_table_name")
            ```
        """
        self.table._table.set_table_name(name, commit_properties)

    def set_table_description(
        self,
        description: str,
        commit_properties: CommitProperties | None = None,
    ) -> None:
        """
        Set the description of the table.

        Args:
            description: the description of the table
            commit_properties: properties of the transaction commit. If None, default values are used.
                              Note: This parameter is not yet implemented and will be ignored.

        Example:
            ```python
            from deltalake import DeltaTable
            dt = DeltaTable("test_table")
            dt.alter.set_table_description("new_table_description")
            ```
        """
        self.table._table.set_table_description(description, commit_properties)

    def set_column_metadata(
        self,
        column: str,
        metadata: dict[str, str],
        commit_properties: CommitProperties | None = None,
        post_commithook_properties: PostCommitHookProperties | None = None,
    ) -> None:
        """
        Update a field's metadata in a schema. If the metadata key does not exist, the entry is inserted.

        If the column name doesn't exist in the schema - an error is raised.

        :param column: name of the column to update metadata for.
        :param metadata: the metadata to be added or modified on the column.
        :param commit_properties: properties of the transaction commit. If None, default values are used.
        :param post_commithook_properties: properties for the post commit hook. If None, default values are used.
        :return:
        """
        self.table._table.set_column_metadata(
            column, metadata, commit_properties, post_commithook_properties
        )


class TableOptimizer:
    """API for various table optimization commands."""

    def __init__(self, table: DeltaTable) -> None:
        self.table = table

    def compact(
        self,
        partition_filters: FilterConjunctionType | None = None,
        target_size: int | None = None,
        max_concurrent_tasks: int | None = None,
        max_spill_size: int | None = None,
        max_temp_directory_size: int | None = None,
        min_commit_interval: int | timedelta | None = None,
        writer_properties: WriterProperties | None = None,
        post_commithook_properties: PostCommitHookProperties | None = None,
        commit_properties: CommitProperties | None = None,
    ) -> dict[str, Any]:
        """
        Compacts small files to reduce the total number of files in the table.

        This operation is idempotent; if run twice on the same table (assuming it has
        not been updated) it will do nothing the second time.

        If this operation happens concurrently with any operations other than append,
        it will fail.

        Args:
            partition_filters: the partition filters that will be used for getting the matched files
            target_size: desired file size after bin-packing files, in bytes. If not
                            provided, will attempt to read the table configuration value ``delta.targetFileSize``.
                            If that value isn't set, will use default value of 100MB.
            max_concurrent_tasks: the maximum number of concurrent tasks to use for
                                    file compaction. Defaults to number of CPUs. More concurrent tasks can make compaction
                                    faster, but will also use more memory.
            max_spill_size: the maximum number of bytes allowed in memory before spilling to disk. If not specified, uses DataFusion's default.
            max_temp_directory_size: the maximum disk space for temporary spill files. If not specified, uses DataFusion's default.
            min_commit_interval: minimum interval in seconds or as timedeltas before a new commit is
                                    created. Interval is useful for long running executions. Set to 0 or timedelta(0), if you
                                    want a commit per partition.
            writer_properties: Pass writer properties to the Rust parquet writer.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties of the transaction commit. If None, default values are used.

        Returns:
            the metrics from optimize

        Example:
            Use a timedelta object to specify the seconds, minutes or hours of the interval.
            ```python
            from deltalake import DeltaTable, write_deltalake
            from datetime import timedelta
            import pyarrow as pa

            write_deltalake("tmp", pa.table({"x": [1], "y": [4]}))
            write_deltalake("tmp", pa.table({"x": [2], "y": [5]}), mode="append")

            dt = DeltaTable("tmp")
            time_delta = timedelta(minutes=10)
            dt.optimize.compact(min_commit_interval=time_delta)
            {'numFilesAdded': 1, 'numFilesRemoved': 2, 'filesAdded': ..., 'filesRemoved': ..., 'partitionsOptimized': 1, 'numBatches': 2, 'totalConsideredFiles': 2, 'totalFilesSkipped': 0, 'preserveInsertionOrder': True}
            ```
        """
        if isinstance(min_commit_interval, timedelta):
            min_commit_interval = int(min_commit_interval.total_seconds())

        metrics = self.table._table.compact_optimize(
            self.table._stringify_partition_values(partition_filters),
            target_size,
            max_concurrent_tasks,
            max_spill_size,
            max_temp_directory_size,
            min_commit_interval,
            writer_properties,
            commit_properties,
            post_commithook_properties,
        )
        self.table.update_incremental()
        return json.loads(metrics)

    def z_order(
        self,
        columns: Iterable[str],
        partition_filters: FilterConjunctionType | None = None,
        target_size: int | None = None,
        max_concurrent_tasks: int | None = None,
        max_spill_size: int | None = None,
        max_temp_directory_size: int | None = None,
        min_commit_interval: int | timedelta | None = None,
        writer_properties: WriterProperties | None = None,
        post_commithook_properties: PostCommitHookProperties | None = None,
        commit_properties: CommitProperties | None = None,
    ) -> dict[str, Any]:
        """
        Reorders the data using a Z-order curve to improve data skipping.

        This also performs compaction, so the same parameters as compact() apply.

        Args:
            columns: the columns to use for Z-ordering. There must be at least one column.
            partition_filters: the partition filters that will be used for getting the matched files
            target_size: desired file size after bin-packing files, in bytes. If not
                            provided, will attempt to read the table configuration value ``delta.targetFileSize``.
                            If that value isn't set, will use default value of 100MB.
            max_concurrent_tasks: the maximum number of concurrent tasks to use for
                                    file compaction. Defaults to number of CPUs. More concurrent tasks can make compaction
                                    faster, but will also use more memory.
            max_spill_size: the maximum number of bytes allowed in memory before spilling to disk. If not specified, uses DataFusion's default.
            max_temp_directory_size: the maximum disk space for temporary spill files. If not specified, uses DataFusion's default.
            min_commit_interval: minimum interval in seconds or as timedeltas before a new commit is
                                    created. Interval is useful for long running executions. Set to 0 or timedelta(0), if you
                                    want a commit per partition.
            writer_properties: Pass writer properties to the Rust parquet writer.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties of the transaction commit. If None, default values are used.

        Returns:
            the metrics from optimize

        Example:
            Use a timedelta object to specify the seconds, minutes or hours of the interval.
            ```python
            from deltalake import DeltaTable, write_deltalake
            from datetime import timedelta
            import pyarrow as pa

            write_deltalake("tmp", pa.table({"x": [1], "y": [4]}))
            write_deltalake("tmp", pa.table({"x": [2], "y": [5]}), mode="append")

            dt = DeltaTable("tmp")
            time_delta = timedelta(minutes=10)
            dt.optimize.z_order(["x"], min_commit_interval=time_delta)
            {'numFilesAdded': 1, 'numFilesRemoved': 2, 'filesAdded': ..., 'filesRemoved': ..., 'partitionsOptimized': 0, 'numBatches': 1, 'totalConsideredFiles': 2, 'totalFilesSkipped': 0, 'preserveInsertionOrder': True}
            ```
        """
        if isinstance(min_commit_interval, timedelta):
            min_commit_interval = int(min_commit_interval.total_seconds())

        metrics = self.table._table.z_order_optimize(
            list(columns),
            self.table._stringify_partition_values(partition_filters),
            target_size,
            max_concurrent_tasks,
            max_spill_size,
            max_temp_directory_size,
            min_commit_interval,
            writer_properties,
            commit_properties,
            post_commithook_properties,
        )
        self.table.update_incremental()
        return json.loads(metrics)
