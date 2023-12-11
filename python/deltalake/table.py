import json
import operator
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import reduce
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Tuple,
    Union,
    cast,
)

import pyarrow
import pyarrow.dataset as ds
import pyarrow.fs as pa_fs
from pyarrow.dataset import (
    Expression,
    FileSystemDataset,
    ParquetFileFormat,
    ParquetFragmentScanOptions,
    ParquetReadOptions,
)

if TYPE_CHECKING:
    import os

    import pandas

from deltalake._internal import DeltaDataChecker as _DeltaDataChecker
from deltalake._internal import RawDeltaTable
from deltalake._internal import create_deltalake as _create_deltalake
from deltalake._util import encode_partition_value
from deltalake.data_catalog import DataCatalog
from deltalake.exceptions import DeltaError, DeltaProtocolError
from deltalake.fs import DeltaStorageHandler
from deltalake.schema import Schema as DeltaSchema

MAX_SUPPORTED_READER_VERSION = 1
MAX_SUPPORTED_WRITER_VERSION = 2


@dataclass(init=False)
class Metadata:
    """Create a Metadata instance."""

    def __init__(self, table: RawDeltaTable):
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
    def partition_columns(self) -> List[str]:
        """Return an array containing the names of the partitioned columns of the DeltaTable."""
        return self._metadata.partition_columns

    @property
    def created_time(self) -> int:
        """
        Return The time when this metadata action is created, in milliseconds since the Unix epoch of the DeltaTable.
        """
        return self._metadata.created_time

    @property
    def configuration(self) -> Dict[str, str]:
        """Return the DeltaTable properties."""
        return self._metadata.configuration

    def __str__(self) -> str:
        return (
            f"Metadata(id: {self._metadata.id}, name: {self._metadata.name}, "
            f"description: {self._metadata.description}, partition_columns: {self._metadata.partition_columns}, "
            f"created_time: {self.created_time}, configuration: {self._metadata.configuration})"
        )


class ProtocolVersions(NamedTuple):
    min_reader_version: int
    min_writer_version: int


FilterLiteralType = Tuple[str, str, Any]


FilterConjunctionType = List[FilterLiteralType]


FilterDNFType = List[FilterConjunctionType]


FilterType = Union[FilterConjunctionType, FilterDNFType]


def _check_contains_null(value: Any) -> bool:
    """
    Check if target contains nullish value.
    """
    if isinstance(value, bytes):
        for byte in value:
            if isinstance(byte, bytes):
                compare_to = chr(0)
            else:
                compare_to = 0
            if byte == compare_to:
                return True
    elif isinstance(value, str):
        return "\x00" in value
    return False


def _check_dnf(
    dnf: FilterDNFType,
    check_null_strings: bool = True,
) -> FilterDNFType:
    """
    Check if DNF are well-formed.
    """
    if len(dnf) == 0 or any(len(c) == 0 for c in dnf):
        raise ValueError("Malformed DNF")
    if check_null_strings:
        for conjunction in dnf:
            for col, op, val in conjunction:
                if (
                    isinstance(val, list)
                    and all(_check_contains_null(v) for v in val)
                    or _check_contains_null(val)
                ):
                    raise NotImplementedError(
                        "Null-terminated binary strings are not supported "
                        "as filter values."
                    )
    return dnf


def _convert_single_predicate(column: str, op: str, value: Any) -> Expression:
    """
    Convert given `tuple` to [pyarrow.dataset.Expression].
    """
    import pyarrow.dataset as ds

    field = ds.field(column)
    if op == "=" or op == "==":
        return field == value
    elif op == "!=":
        return field != value
    elif op == "<":
        return field < value
    elif op == ">":
        return field > value
    elif op == "<=":
        return field <= value
    elif op == ">=":
        return field >= value
    elif op == "in":
        return field.isin(value)
    elif op == "not in":
        return ~field.isin(value)
    else:
        raise ValueError(
            f'"{(column, op, value)}" is not a valid operator in predicates.'
        )


def _filters_to_expression(filters: FilterType) -> Expression:
    """
    Check if filters are well-formed and convert to an [pyarrow.dataset.Expression].
    """
    if isinstance(filters[0][0], str):
        # We have encountered the situation where we have one nesting level too few:
        #   We have [(,,), ..] instead of [[(,,), ..]]
        dnf = cast(FilterDNFType, [filters])
    else:
        dnf = cast(FilterDNFType, filters)
    dnf = _check_dnf(dnf, check_null_strings=False)
    disjunction_members = []
    for conjunction in dnf:
        conjunction_members = [
            _convert_single_predicate(col, op, val) for col, op, val in conjunction
        ]
        disjunction_members.append(reduce(operator.and_, conjunction_members))
    return reduce(operator.or_, disjunction_members)


_DNF_filter_doc = """
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


@dataclass(init=False)
class DeltaTable:
    """Represents a Delta Table"""

    def __init__(
        self,
        table_uri: Union[str, Path, "os.PathLike[str]"],
        version: Optional[int] = None,
        storage_options: Optional[Dict[str, str]] = None,
        without_files: bool = False,
        log_buffer_size: Optional[int] = None,
    ):
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
        self._latest_version = -1
        self._table = RawDeltaTable(
            str(table_uri),
            version=version,
            storage_options=storage_options,
            without_files=without_files,
            log_buffer_size=log_buffer_size,
        )
        self._metadata = Metadata(self._table)

    @classmethod
    def from_data_catalog(
        cls,
        data_catalog: DataCatalog,
        database_name: str,
        table_name: str,
        data_catalog_id: Optional[str] = None,
        version: Optional[int] = None,
        log_buffer_size: Optional[int] = None,
    ) -> "DeltaTable":
        """
        Create the Delta Table from a Data Catalog.

        Args:
            data_catalog: the Catalog to use for getting the storage location of the Delta Table
            database_name: the database name inside the Data Catalog
            table_name: the table name inside the Data Catalog
            data_catalog_id: the identifier of the Data Catalog
            version: version of the DeltaTable
            log_buffer_size: Number of files to buffer when reading the commit log. A positive integer.
                                Setting a value greater than 1 results in concurrent calls to the storage api.
                                This can decrease latency if there are many files in the log since the last checkpoint,
                                but will also increase memory usage. Possible rate limits of the storage backend should
                                also be considered for optimal performance. Defaults to 4 * number of cpus.
        """
        table_uri = RawDeltaTable.get_table_uri_from_data_catalog(
            data_catalog=data_catalog.value,
            data_catalog_id=data_catalog_id,
            database_name=database_name,
            table_name=table_name,
        )
        return cls(
            table_uri=table_uri, version=version, log_buffer_size=log_buffer_size
        )

    @classmethod
    def create(
        cls,
        table_uri: Union[str, Path],
        schema: Union[pyarrow.Schema, DeltaSchema],
        mode: Literal["error", "append", "overwrite", "ignore"] = "error",
        partition_by: Optional[Union[List[str], str]] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        configuration: Optional[Mapping[str, Optional[str]]] = None,
        storage_options: Optional[Dict[str, str]] = None,
    ) -> "DeltaTable":
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
            storage_options: options passed to the object store crate.

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
        if isinstance(schema, DeltaSchema):
            schema = schema.to_pyarrow()
        if isinstance(partition_by, str):
            partition_by = [partition_by]

        if isinstance(table_uri, Path):
            table_uri = str(table_uri)

        _create_deltalake(
            table_uri,
            schema,
            partition_by or [],
            mode,
            name,
            description,
            configuration,
            storage_options,
        )

        return cls(table_uri=table_uri, storage_options=storage_options)

    def version(self) -> int:
        """
        Get the version of the DeltaTable.

        Returns:
            The current version of the DeltaTable
        """
        return self._table.version()

    def files(
        self, partition_filters: Optional[List[Tuple[str, str, Any]]] = None
    ) -> List[str]:
        """
        Get the .parquet files of the DeltaTable.

        The paths are as they are saved in the delta log, which may either be
        relative to the table root or absolute URIs.

        Args:
            partition_filters: the partition filters that will be used for
                                getting the matched files

        Returns:
            list of the .parquet files referenced for the current version of the DeltaTable

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
        return self._table.files(self.__stringify_partition_values(partition_filters))

    def file_uris(
        self, partition_filters: Optional[List[Tuple[str, str, Any]]] = None
    ) -> List[str]:
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
            self.__stringify_partition_values(partition_filters)
        )

    file_uris.__doc__ = ""

    def load_version(self, version: int) -> None:
        """
        Load a DeltaTable with a specified version.

        Args:
            version: the identifier of the version of the DeltaTable to load
        """
        self._table.load_version(version)

    def load_with_datetime(self, datetime_string: str) -> None:
        """
        Time travel Delta table to the latest version that's created at or before provided `datetime_string` argument.
        The `datetime_string` argument should be an RFC 3339 and ISO 8601 date and time string.

        Args:
            datetime_string: the identifier of the datetime point of the DeltaTable to load

        Example:
            ```
            "2018-01-26T18:30:09Z"
            "2018-12-19T16:39:57-08:00"
            "2018-01-26T18:30:09.453+00:00"
            ```
        """
        self._table.load_with_datetime(datetime_string)

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
        return self._metadata

    def protocol(self) -> ProtocolVersions:
        """
        Get the reader and writer protocol versions of the DeltaTable.

        Returns:
            the current ProtocolVersions registered in the transaction log
        """
        return ProtocolVersions(*self._table.protocol_versions())

    def history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Run the history command on the DeltaTable.
        The operations are returned in reverse chronological order.

        Args:
            limit: the commit info limit to return

        Returns:
            list of the commit infos registered in the transaction log
        """

        def _backwards_enumerate(
            iterable: List[str], start_end: int
        ) -> Generator[Tuple[int, str], None, None]:
            n = start_end
            for elem in iterable:
                yield n, elem
                n -= 1

        commits = list(reversed(self._table.history(limit)))

        history = []
        for version, commit_info_raw in _backwards_enumerate(
            commits, start_end=self._table.get_latest_version()
        ):
            commit = json.loads(commit_info_raw)
            commit["version"] = version
            history.append(commit)
        return history

    def vacuum(
        self,
        retention_hours: Optional[int] = None,
        dry_run: bool = True,
        enforce_retention_duration: bool = True,
    ) -> List[str]:
        """
        Run the Vacuum command on the Delta Table: list and delete files no longer referenced by the Delta table and are older than the retention threshold.

        Args:
            retention_hours: the retention threshold in hours, if none then the value from `configuration.deletedFileRetentionDuration` is used or default of 1 week otherwise.
            dry_run: when activated, list only the files, delete otherwise
            enforce_retention_duration: when disabled, accepts retention hours smaller than the value from `configuration.deletedFileRetentionDuration`.

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
        )

    def update(
        self,
        updates: Optional[Dict[str, str]] = None,
        new_values: Optional[
            Dict[str, Union[int, float, str, datetime, bool, List[Any]]]
        ] = None,
        predicate: Optional[str] = None,
        writer_properties: Optional[Dict[str, int]] = None,
        error_on_type_mismatch: bool = True,
    ) -> Dict[str, Any]:
        """`UPDATE` records in the Delta Table that matches an optional predicate. Either updates or new_values needs
        to be passed for it to execute.

        Args:
            updates: a mapping of column name to update SQL expression.
            new_values: a mapping of column name to python datatype.
            predicate: a logical expression.
            writer_properties: Pass writer properties to the Rust parquet writer, see options https://arrow.apache.org/rust/parquet/file/properties/struct.WriterProperties.html,
                only the following fields are supported: `data_page_size_limit`, `dictionary_page_size_limit`,
                `data_page_row_count_limit`, `write_batch_size`, `max_row_group_size`.
            error_on_type_mismatch: specify if update will return error if data types are mismatching :default = True

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
                print(type(key), type(value))
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
        )
        return json.loads(metrics)

    @property
    def optimize(
        self,
    ) -> "TableOptimizer":
        return TableOptimizer(self)

    def merge(
        self,
        source: Union[
            pyarrow.Table,
            pyarrow.RecordBatch,
            pyarrow.RecordBatchReader,
            ds.Dataset,
            "pandas.DataFrame",
        ],
        predicate: str,
        source_alias: Optional[str] = None,
        target_alias: Optional[str] = None,
        error_on_type_mismatch: bool = True,
    ) -> "TableMerger":
        """Pass the source data which you want to merge on the target delta table, providing a
        predicate in SQL query like format. You can also specify on what to do when the underlying data types do not
        match the underlying table.

        Args:
            source: source data
            predicate: SQL like predicate on how to merge
            source_alias: Alias for the source table
            target_alias: Alias for the target table
            error_on_type_mismatch: specify if merge will return error if data types are mismatching :default = True

        Returns:
            TableMerger: TableMerger Object
        """
        invariants = self.schema().invariants
        checker = _DeltaDataChecker(invariants)

        from .schema import (
            convert_pyarrow_dataset,
            convert_pyarrow_recordbatch,
            convert_pyarrow_recordbatchreader,
            convert_pyarrow_table,
        )

        if isinstance(source, pyarrow.RecordBatchReader):
            source = convert_pyarrow_recordbatchreader(source, large_dtypes=True)
        elif isinstance(source, pyarrow.RecordBatch):
            source = convert_pyarrow_recordbatch(source, large_dtypes=True)
        elif isinstance(source, pyarrow.Table):
            source = convert_pyarrow_table(source, large_dtypes=True)
        elif isinstance(source, ds.Dataset):
            source = convert_pyarrow_dataset(source, large_dtypes=True)
        elif isinstance(source, pandas.DataFrame):
            source = convert_pyarrow_table(
                pyarrow.Table.from_pandas(source), large_dtypes=True
            )
        else:
            raise TypeError(
                f"{type(source).__name__} is not a valid input. Only PyArrow RecordBatchReader, RecordBatch, Table or Pandas DataFrame are valid inputs for source."
            )

        def validate_batch(batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
            checker.check_batch(batch)
            return batch

        source = pyarrow.RecordBatchReader.from_batches(
            source.schema, (validate_batch(batch) for batch in source)
        )

        return TableMerger(
            self,
            source=source,
            predicate=predicate,
            source_alias=source_alias,
            target_alias=target_alias,
            safe_cast=not error_on_type_mismatch,
        )

    def restore(
        self,
        target: Union[int, datetime, str],
        *,
        ignore_missing_files: bool = False,
        protocol_downgrade_allowed: bool = False,
    ) -> Dict[str, Any]:
        """
        Run the Restore command on the Delta Table: restore table to a given version or datetime.

        Args:
            target: the expected version will restore, which represented by int, date str or datetime.
            ignore_missing_files: whether the operation carry on when some data files missing.
            protocol_downgrade_allowed: whether the operation when protocol version upgraded.

        Returns:
            the metrics from restore.
        """
        if isinstance(target, datetime):
            metrics = self._table.restore(
                target.isoformat(),
                ignore_missing_files=ignore_missing_files,
                protocol_downgrade_allowed=protocol_downgrade_allowed,
            )
        else:
            metrics = self._table.restore(
                target,
                ignore_missing_files=ignore_missing_files,
                protocol_downgrade_allowed=protocol_downgrade_allowed,
            )
        return json.loads(metrics)

    def to_pyarrow_dataset(
        self,
        partitions: Optional[List[Tuple[str, str, Any]]] = None,
        filesystem: Optional[Union[str, pa_fs.FileSystem]] = None,
        parquet_read_options: Optional[ParquetReadOptions] = None,
    ) -> pyarrow.dataset.Dataset:
        """
        Build a PyArrow Dataset using data from the DeltaTable.

        Args:
            partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
            filesystem: A concrete implementation of the Pyarrow FileSystem or a fsspec-compatible interface. If None, the first file path will be used to determine the right FileSystem
            parquet_read_options: Optional read options for Parquet. Use this to handle INT96 to timestamp conversion for edge cases like 0001-01-01 or 9999-12-31

         More info: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetReadOptions.html

        Returns:
            the PyArrow dataset in PyArrow
        """
        if self.protocol().min_reader_version > MAX_SUPPORTED_READER_VERSION:
            raise DeltaProtocolError(
                f"The table's minimum reader version is {self.protocol().min_reader_version} "
                f"but deltalake only supports up to version {MAX_SUPPORTED_READER_VERSION}."
            )

        if not filesystem:
            file_sizes = self.get_add_actions().to_pydict()
            file_sizes = {
                x: y for x, y in zip(file_sizes["path"], file_sizes["size_bytes"])
            }
            filesystem = pa_fs.PyFileSystem(
                DeltaStorageHandler(
                    self._table.table_uri(), self._storage_options, file_sizes
                )
            )

        format = ParquetFileFormat(
            read_options=parquet_read_options,
            default_fragment_scan_options=ParquetFragmentScanOptions(pre_buffer=True),
        )

        fragments = [
            format.make_fragment(
                file,
                filesystem=filesystem,
                partition_expression=part_expression,
            )
            for file, part_expression in self._table.dataset_partitions(
                self.schema().to_pyarrow(), partitions
            )
        ]

        schema = self.schema().to_pyarrow()

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
        partitions: Optional[List[Tuple[str, str, Any]]] = None,
        columns: Optional[List[str]] = None,
        filesystem: Optional[Union[str, pa_fs.FileSystem]] = None,
        filters: Optional[FilterType] = None,
    ) -> pyarrow.Table:
        """
        Build a PyArrow Table using data from the DeltaTable.

        Args:
            partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
            columns: The columns to project. This can be a list of column names to include (order and duplicates will be preserved)
            filesystem: A concrete implementation of the Pyarrow FileSystem or a fsspec-compatible interface. If None, the first file path will be used to determine the right FileSystem
            filters: A disjunctive normal form (DNF) predicate for filtering rows. If you pass a filter you do not need to pass ``partitions``
        """
        if filters is not None:
            filters = _filters_to_expression(filters)
        return self.to_pyarrow_dataset(
            partitions=partitions, filesystem=filesystem
        ).to_table(columns=columns, filter=filters)

    def to_pandas(
        self,
        partitions: Optional[List[Tuple[str, str, Any]]] = None,
        columns: Optional[List[str]] = None,
        filesystem: Optional[Union[str, pa_fs.FileSystem]] = None,
        filters: Optional[FilterType] = None,
    ) -> "pandas.DataFrame":
        """
        Build a pandas dataframe using data from the DeltaTable.

        Args:
            partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
            columns: The columns to project. This can be a list of column names to include (order and duplicates will be preserved)
            filesystem: A concrete implementation of the Pyarrow FileSystem or a fsspec-compatible interface. If None, the first file path will be used to determine the right FileSystem
            filters: A disjunctive normal form (DNF) predicate for filtering rows. If you pass a filter you do not need to pass ``partitions``
        """
        return self.to_pyarrow_table(
            partitions=partitions,
            columns=columns,
            filesystem=filesystem,
            filters=filters,
        ).to_pandas()

    def update_incremental(self) -> None:
        """
        Updates the DeltaTable to the latest version by incrementally applying
        newer versions.
        """
        self._table.update_incremental()

    def get_latest_version(self) -> int:
        """
        Get latest version of commit.
        """
        return self._table.get_latest_version()

    def peek_next_commit(
        self, version: int
    ) -> Tuple[Optional[List[Dict[Any, Any]]], int]:
        """
        Peek next commit of the input version.
        """
        actions = []
        next_version = version + 1
        if next_version > self._latest_version:
            self._latest_version = self.get_latest_version()
        while next_version <= self._latest_version:
            try:
                commit_log_bytes = self._table.get_obj(next_version)
                for commit_action in commit_log_bytes.split(b"\n"):
                    if commit_action:
                        actions.append(json.loads(commit_action))
                return actions, next_version
            except DeltaError as e:
                if str(e) == f"Delta log not found for table version: {next_version}":
                    next_version += 1
                else:
                    raise
        return None, version

    def create_checkpoint(self) -> None:
        self._table.create_checkpoint()

    def cleanup_metadata(self) -> None:
        """
        Delete expired log files before current version from table. The table log retention is based on
        the `configuration.logRetentionDuration` value, 30 days by default.
        """
        self._table.cleanup_metadata()

    def __stringify_partition_values(
        self, partition_filters: Optional[List[Tuple[str, str, Any]]]
    ) -> Optional[List[Tuple[str, str, Union[str, List[str]]]]]:
        if partition_filters is None:
            return partition_filters
        out = []
        for field, op, value in partition_filters:
            str_value: Union[str, List[str]]
            if isinstance(value, (list, tuple)):
                str_value = [encode_partition_value(val) for val in value]
            else:
                str_value = encode_partition_value(value)
            out.append((field, op, str_value))
        return out

    def get_add_actions(self, flatten: bool = False) -> pyarrow.RecordBatch:
        """
        Return a dataframe with all current add actions.

        Add actions represent the files that currently make up the table. This
        data is a low-level representation parsed from the transaction log.

        Args:
            flatten: whether to flatten the schema. Partition values columns are
                        given the prefix `partition.`, statistics (null_count, min, and max) are
                        given the prefix `null_count.`, `min.`, and `max.`, and tags the
                        prefix `tags.`. Nested field names are concatenated with `.`.

        Returns:
            a PyArrow RecordBatch containing the add action data.

        Example:
            ```python
            from pprint import pprint
            from deltalake import DeltaTable, write_deltalake
            import pyarrow as pa
            data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
            write_deltalake("tmp", data, partition_by=["x"])
            dt = DeltaTable("tmp")
            df = dt.get_add_actions().to_pandas()
            df["path"].sort_values(ignore_index=True)
            0    x=1/0
            1    x=2/0
            2    x=3/0
            ```

            ```python
            df = dt.get_add_actions(flatten=True).to_pandas()
            df["partition.x"].sort_values(ignore_index=True)
            0    1
            1    2
            2    3
            ```
        """
        return self._table.get_add_actions(flatten)

    def delete(self, predicate: Optional[str] = None) -> Dict[str, Any]:
        """Delete records from a Delta Table that statisfy a predicate.

        When a predicate is not provided then all records are deleted from the Delta
        Table. Otherwise a scan of the Delta table is performed to mark any files
        that contain records that satisfy the predicate. Once files are determined
        they are rewritten without the records.

        Args:
            predicate: a SQL where clause. If not passed, will delete all rows.

        Returns:
            the metrics from delete.
        """
        metrics = self._table.delete(predicate)
        return json.loads(metrics)

    def repair(self, dry_run: bool = False) -> Dict[str, Any]:
        """Repair the Delta Table by auditing active files that do not exist in the underlying
        filesystem and removes them. This can be useful when there are accidental deletions or corrupted files.

        Active files are ones that have an add action in the log, but no corresponding remove action.
        This operation creates a new FSCK transaction containing a remove action for each of the missing
        or corrupted files.

        Args:
            dry_run: when activated, list only the files, otherwise add remove actions to transaction log. Defaults to False.
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
        metrics = self._table.repair(dry_run)
        return json.loads(metrics)


class TableMerger:
    """API for various table `MERGE` commands."""

    def __init__(
        self,
        table: DeltaTable,
        source: pyarrow.RecordBatchReader,
        predicate: str,
        source_alias: Optional[str] = None,
        target_alias: Optional[str] = None,
        safe_cast: bool = True,
    ):
        self.table = table
        self.source = source
        self.predicate = predicate
        self.source_alias = source_alias
        self.target_alias = target_alias
        self.safe_cast = safe_cast
        self.writer_properties: Optional[Dict[str, Optional[int]]] = None
        self.matched_update_updates: Optional[List[Dict[str, str]]] = None
        self.matched_update_predicate: Optional[List[Optional[str]]] = None
        self.matched_delete_predicate: Optional[List[str]] = None
        self.matched_delete_all: Optional[bool] = None
        self.not_matched_insert_updates: Optional[List[Dict[str, str]]] = None
        self.not_matched_insert_predicate: Optional[List[Optional[str]]] = None
        self.not_matched_by_source_update_updates: Optional[List[Dict[str, str]]] = None
        self.not_matched_by_source_update_predicate: Optional[
            List[Optional[str]]
        ] = None
        self.not_matched_by_source_delete_predicate: Optional[List[str]] = None
        self.not_matched_by_source_delete_all: Optional[bool] = None

    def with_writer_properties(
        self,
        data_page_size_limit: Optional[int] = None,
        dictionary_page_size_limit: Optional[int] = None,
        data_page_row_count_limit: Optional[int] = None,
        write_batch_size: Optional[int] = None,
        max_row_group_size: Optional[int] = None,
    ) -> "TableMerger":
        """Pass writer properties to the Rust parquet writer, see options https://arrow.apache.org/rust/parquet/file/properties/struct.WriterProperties.html:

        Args:
            data_page_size_limit: Limit DataPage size to this in bytes.
            dictionary_page_size_limit: Limit the size of each DataPage to store dicts to this amount in bytes.
            data_page_row_count_limit: Limit the number of rows in each DataPage.
            write_batch_size: Splits internally to smaller batch size.
            max_row_group_size: Max number of rows in row group.

        Returns:
            TableMerger: TableMerger Object
        """
        writer_properties = {
            "data_page_size_limit": data_page_size_limit,
            "dictionary_page_size_limit": dictionary_page_size_limit,
            "data_page_row_count_limit": data_page_row_count_limit,
            "write_batch_size": write_batch_size,
            "max_row_group_size": max_row_group_size,
        }
        self.writer_properties = writer_properties
        return self

    def when_matched_update(
        self, updates: Dict[str, str], predicate: Optional[str] = None
    ) -> "TableMerger":
        """Update a matched table row based on the rules defined by ``updates``.
        If a ``predicate`` is specified, then it must evaluate to true for the row to be updated.

        Args:
            updates: a mapping of column name to update SQL expression.
            predicate:  SQL like predicate on when to update.

        Returns:
            TableMerger: TableMerger Object

        Example:
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
                 .when_matched_update(updates={"x": "source.x", "y": "source.y"})
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
        if isinstance(self.matched_update_updates, list) and isinstance(
            self.matched_update_predicate, list
        ):
            self.matched_update_updates.append(updates)
            self.matched_update_predicate.append(predicate)
        else:
            self.matched_update_updates = [updates]
            self.matched_update_predicate = [predicate]
        return self

    def when_matched_update_all(self, predicate: Optional[str] = None) -> "TableMerger":
        """Updating all source fields to target fields, source and target are required to have the same field names.
        If a ``predicate`` is specified, then it must evaluate to true for the row to be updated.

        Args:
            predicate: SQL like predicate on when to update all columns.

        Returns:
            TableMerger: TableMerger Object

        Example:
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
        """

        src_alias = (self.source_alias + ".") if self.source_alias is not None else ""
        trgt_alias = (self.target_alias + ".") if self.target_alias is not None else ""

        updates = {
            f"{trgt_alias}{col.name}": f"{src_alias}{col.name}"
            for col in self.source.schema
        }

        if isinstance(self.matched_update_updates, list) and isinstance(
            self.matched_update_predicate, list
        ):
            self.matched_update_updates.append(updates)
            self.matched_update_predicate.append(predicate)
        else:
            self.matched_update_updates = [updates]
            self.matched_update_predicate = [predicate]

        return self

    def when_matched_delete(self, predicate: Optional[str] = None) -> "TableMerger":
        """Delete a matched row from the table only if the given ``predicate`` (if specified) is
        true for the matched row. If not specified it deletes all matches.

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
        if self.matched_delete_all is not None:
            raise ValueError(
                """when_matched_delete without a predicate has already been set, which means
                             it will delete all, any subsequent when_matched_delete, won't make sense."""
            )

        if predicate is None:
            self.matched_delete_all = True
        else:
            if isinstance(self.matched_delete_predicate, list):
                self.matched_delete_predicate.append(predicate)
            else:
                self.matched_delete_predicate = [predicate]
        return self

    def when_not_matched_insert(
        self, updates: Dict[str, str], predicate: Optional[str] = None
    ) -> "TableMerger":
        """Insert a new row to the target table based on the rules defined by ``updates``. If a
        ``predicate`` is specified, then it must evaluate to true for the new row to be inserted.

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

        if isinstance(self.not_matched_insert_updates, list) and isinstance(
            self.not_matched_insert_predicate, list
        ):
            self.not_matched_insert_updates.append(updates)
            self.not_matched_insert_predicate.append(predicate)
        else:
            self.not_matched_insert_updates = [updates]
            self.not_matched_insert_predicate = [predicate]

        return self

    def when_not_matched_insert_all(
        self, predicate: Optional[str] = None
    ) -> "TableMerger":
        """Insert a new row to the target table, updating all source fields to target fields. Source and target are
        required to have the same field names. If a ``predicate`` is specified, then it must evaluate to true for
        the new row to be inserted.

        Args:
            predicate: SQL like predicate on when to insert.

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
        """

        src_alias = (self.source_alias + ".") if self.source_alias is not None else ""
        trgt_alias = (self.target_alias + ".") if self.target_alias is not None else ""
        updates = {
            f"{trgt_alias}{col.name}": f"{src_alias}{col.name}"
            for col in self.source.schema
        }
        if isinstance(self.not_matched_insert_updates, list) and isinstance(
            self.not_matched_insert_predicate, list
        ):
            self.not_matched_insert_updates.append(updates)
            self.not_matched_insert_predicate.append(predicate)
        else:
            self.not_matched_insert_updates = [updates]
            self.not_matched_insert_predicate = [predicate]

        return self

    def when_not_matched_by_source_update(
        self, updates: Dict[str, str], predicate: Optional[str] = None
    ) -> "TableMerger":
        """Update a target row that has no matches in the source based on the rules defined by ``updates``.
        If a ``predicate`` is specified, then it must evaluate to true for the row to be updated.

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

        if isinstance(self.not_matched_by_source_update_updates, list) and isinstance(
            self.not_matched_by_source_update_predicate, list
        ):
            self.not_matched_by_source_update_updates.append(updates)
            self.not_matched_by_source_update_predicate.append(predicate)
        else:
            self.not_matched_by_source_update_updates = [updates]
            self.not_matched_by_source_update_predicate = [predicate]
        return self

    def when_not_matched_by_source_delete(
        self, predicate: Optional[str] = None
    ) -> "TableMerger":
        """Delete a target row that has no matches in the source from the table only if the given
        ``predicate`` (if specified) is true for the target row.

        Args:
            predicate:  SQL like predicate on when to delete when not matched by source.

        Returns:
            TableMerger: TableMerger Object
        """
        if self.not_matched_by_source_delete_all is not None:
            raise ValueError(
                """when_not_matched_by_source_delete without a predicate has already been set, which means
                             it will delete all, any subsequent when_not_matched_by_source_delete, won't make sense."""
            )

        if predicate is None:
            self.not_matched_by_source_delete_all = True
        else:
            if isinstance(self.not_matched_by_source_delete_predicate, list):
                self.not_matched_by_source_delete_predicate.append(predicate)
            else:
                self.not_matched_by_source_delete_predicate = [predicate]
        return self

    def execute(self) -> Dict[str, Any]:
        """Executes `MERGE` with the previously provided settings in Rust with Apache Datafusion query engine.

        Returns:
            Dict: metrics
        """
        metrics = self.table._table.merge_execute(
            source=self.source,
            predicate=self.predicate,
            source_alias=self.source_alias,
            target_alias=self.target_alias,
            safe_cast=self.safe_cast,
            writer_properties=self.writer_properties,
            matched_update_updates=self.matched_update_updates,
            matched_update_predicate=self.matched_update_predicate,
            matched_delete_predicate=self.matched_delete_predicate,
            matched_delete_all=self.matched_delete_all,
            not_matched_insert_updates=self.not_matched_insert_updates,
            not_matched_insert_predicate=self.not_matched_insert_predicate,
            not_matched_by_source_update_updates=self.not_matched_by_source_update_updates,
            not_matched_by_source_update_predicate=self.not_matched_by_source_update_predicate,
            not_matched_by_source_delete_predicate=self.not_matched_by_source_delete_predicate,
            not_matched_by_source_delete_all=self.not_matched_by_source_delete_all,
        )
        self.table.update_incremental()
        return json.loads(metrics)


class TableOptimizer:
    """API for various table optimization commands."""

    def __init__(self, table: DeltaTable):
        self.table = table

    def __call__(
        self,
        partition_filters: Optional[FilterType] = None,
        target_size: Optional[int] = None,
        max_concurrent_tasks: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        !!! warning "DEPRECATED 0.10.0"
            Use [compact][deltalake.table.DeltaTable.compact] instead, which has the same signature.
        """

        warnings.warn(
            "Call to deprecated method DeltaTable.optimize. Use DeltaTable.optimize.compact() instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )

        return self.compact(partition_filters, target_size, max_concurrent_tasks)

    def compact(
        self,
        partition_filters: Optional[FilterType] = None,
        target_size: Optional[int] = None,
        max_concurrent_tasks: Optional[int] = None,
        min_commit_interval: Optional[Union[int, timedelta]] = None,
    ) -> Dict[str, Any]:
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
                            If that value isn't set, will use default value of 256MB.
            max_concurrent_tasks: the maximum number of concurrent tasks to use for
                                    file compaction. Defaults to number of CPUs. More concurrent tasks can make compaction
                                    faster, but will also use more memory.
            min_commit_interval: minimum interval in seconds or as timedeltas before a new commit is
                                    created. Interval is useful for long running executions. Set to 0 or timedelta(0), if you
                                    want a commit per partition.

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
            partition_filters, target_size, max_concurrent_tasks, min_commit_interval
        )
        self.table.update_incremental()
        return json.loads(metrics)

    def z_order(
        self,
        columns: Iterable[str],
        partition_filters: Optional[FilterType] = None,
        target_size: Optional[int] = None,
        max_concurrent_tasks: Optional[int] = None,
        max_spill_size: int = 20 * 1024 * 1024 * 1024,
        min_commit_interval: Optional[Union[int, timedelta]] = None,
    ) -> Dict[str, Any]:
        """
        Reorders the data using a Z-order curve to improve data skipping.

        This also performs compaction, so the same parameters as compact() apply.

        Args:
            columns: the columns to use for Z-ordering. There must be at least one column.
                        partition_filters: the partition filters that will be used for getting the matched files
            target_size: desired file size after bin-packing files, in bytes. If not
                            provided, will attempt to read the table configuration value ``delta.targetFileSize``.
                            If that value isn't set, will use default value of 256MB.
            max_concurrent_tasks: the maximum number of concurrent tasks to use for
                                    file compaction. Defaults to number of CPUs. More concurrent tasks can make compaction
                                    faster, but will also use more memory.
            max_spill_size: the maximum number of bytes to spill to disk. Defaults to 20GB.
            min_commit_interval: minimum interval in seconds or as timedeltas before a new commit is
                                    created. Interval is useful for long running executions. Set to 0 or timedelta(0), if you
                                    want a commit per partition.

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
            partition_filters,
            target_size,
            max_concurrent_tasks,
            max_spill_size,
            min_commit_interval,
        )
        self.table.update_incremental()
        return json.loads(metrics)
