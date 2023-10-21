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
    NamedTuple,
    Optional,
    Tuple,
    Union,
    cast,
)

import pyarrow
import pyarrow.fs as pa_fs
from pyarrow.dataset import (
    Expression,
    FileSystemDataset,
    ParquetFileFormat,
    ParquetFragmentScanOptions,
    ParquetReadOptions,
)

if TYPE_CHECKING:
    import pandas

from ._internal import DeltaDataChecker as _DeltaDataChecker
from ._internal import RawDeltaTable
from ._util import encode_partition_value
from .data_catalog import DataCatalog
from .exceptions import DeltaProtocolError
from .fs import DeltaStorageHandler
from .schema import Schema

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
    Convert given `tuple` to `pyarrow.dataset.Expression`.
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
    Check if filters are well-formed and convert to an ``pyarrow.dataset.Expression``.
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

Examples:
("x", "=", "a")
("x", "!=", "a")
("y", "in", ["a", "b", "c"])
("z", "not in", ["a","b"])
"""


@dataclass(init=False)
class DeltaTable:
    """Create a DeltaTable instance."""

    def __init__(
        self,
        table_uri: Union[str, Path],
        version: Optional[int] = None,
        storage_options: Optional[Dict[str, str]] = None,
        without_files: bool = False,
        log_buffer_size: Optional[int] = None,
    ):
        """
        Create the Delta Table from a path with an optional version.
        Multiple StorageBackends are currently supported: AWS S3, Azure Data Lake Storage Gen2, Google Cloud Storage (GCS) and local URI.
        Depending on the storage backend used, you could provide options values using the ``storage_options`` parameter.

        :param table_uri: the path of the DeltaTable
        :param version: version of the DeltaTable
        :param storage_options: a dictionary of the options to use for the storage backend
        :param without_files: If True, will load table without tracking files.
                              Some append-only applications might have no need of tracking any files. So, the
                              DeltaTable will be loaded with a significant memory reduction.
        :param log_buffer_size: Number of files to buffer when reading the commit log. A positive integer.
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

        :param data_catalog: the Catalog to use for getting the storage location of the Delta Table
        :param database_name: the database name inside the Data Catalog
        :param table_name: the table name inside the Data Catalog
        :param data_catalog_id: the identifier of the Data Catalog
        :param version: version of the DeltaTable
        :param log_buffer_size: Number of files to buffer when reading the commit log. A positive integer.
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

    def version(self) -> int:
        """
        Get the version of the DeltaTable.

        :return: The current version of the DeltaTable
        """
        return self._table.version()

    def files(
        self, partition_filters: Optional[List[Tuple[str, str, Any]]] = None
    ) -> List[str]:
        return self._table.files(self.__stringify_partition_values(partition_filters))

    files.__doc__ = f"""
Get the .parquet files of the DeltaTable.

The paths are as they are saved in the delta log, which may either be
relative to the table root or absolute URIs.

:param partition_filters: the partition filters that will be used for
    getting the matched files
:return: list of the .parquet files referenced for the current version
    of the DeltaTable
{_DNF_filter_doc}
    """

    def file_uris(
        self, partition_filters: Optional[List[Tuple[str, str, Any]]] = None
    ) -> List[str]:
        return self._table.file_uris(
            self.__stringify_partition_values(partition_filters)
        )

    file_uris.__doc__ = f"""
Get the list of files as absolute URIs, including the scheme (e.g. "s3://").

Local files will be just plain absolute paths, without a scheme. (That is,
no 'file://' prefix.)

Use the partition_filters parameter to retrieve a subset of files that match the
given filters.

:param partition_filters: the partition filters that will be used for getting the matched files
:return: list of the .parquet files with an absolute URI referenced for the current version of the DeltaTable
{_DNF_filter_doc}
    """

    def load_version(self, version: int) -> None:
        """
        Load a DeltaTable with a specified version.

        :param version: the identifier of the version of the DeltaTable to load
        """
        self._table.load_version(version)

    def load_with_datetime(self, datetime_string: str) -> None:
        """
        Time travel Delta table to the latest version that's created at or before provided `datetime_string` argument.
        The `datetime_string` argument should be an RFC 3339 and ISO 8601 date and time string.

        Examples:
        `2018-01-26T18:30:09Z`
        `2018-12-19T16:39:57-08:00`
        `2018-01-26T18:30:09.453+00:00`

        :param datetime_string: the identifier of the datetime point of the DeltaTable to load
        """
        self._table.load_with_datetime(datetime_string)

    @property
    def table_uri(self) -> str:
        return self._table.table_uri()

    def schema(self) -> Schema:
        """
        Get the current schema of the DeltaTable.

        :return: the current Schema registered in the transaction log
        """
        return self._table.schema

    def metadata(self) -> Metadata:
        """
        Get the current metadata of the DeltaTable.

        :return: the current Metadata registered in the transaction log
        """
        return self._metadata

    def protocol(self) -> ProtocolVersions:
        """
        Get the reader and writer protocol versions of the DeltaTable.

        :return: the current ProtocolVersions registered in the transaction log
        """
        return ProtocolVersions(*self._table.protocol_versions())

    def history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Run the history command on the DeltaTable.
        The operations are returned in reverse chronological order.

        :param limit: the commit info limit to return
        :return: list of the commit infos registered in the transaction log
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

        :param retention_hours: the retention threshold in hours, if none then the value from `configuration.deletedFileRetentionDuration` is used or default of 1 week otherwise.
        :param dry_run: when activated, list only the files, delete otherwise
        :param enforce_retention_duration: when disabled, accepts retention hours smaller than the value from `configuration.deletedFileRetentionDuration`.
        :return: the list of files no longer referenced by the Delta Table and are older than the retention threshold.
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
        updates: Dict[str, str],
        predicate: Optional[str] = None,
        writer_properties: Optional[Dict[str, int]] = None,
        error_on_type_mismatch: bool = True,
    ) -> Dict[str, Any]:
        """UPDATE records in the Delta Table that matches an optional predicate.

        :param updates: a mapping of column name to update SQL expression.
        :param predicate: a logical expression, defaults to None
        :writer_properties: Pass writer properties to the Rust parquet writer, see options https://arrow.apache.org/rust/parquet/file/properties/struct.WriterProperties.html,
            only the fields: data_page_size_limit, dictionary_page_size_limit, data_page_row_count_limit, write_batch_size, max_row_group_size are supported.
        :error_on_type_mismatch: specify if merge will return error if data types are mismatching :default = True
        :return: the metrics from delete

        Examples:

        Update some row values with SQL predicate. This is equivalent to
        ``UPDATE table SET deleted = true WHERE id = '5'``

        >>> from deltalake import DeltaTable
        >>> dt = DeltaTable("tmp")
        >>> dt.update(predicate="id = '5'",
        ...           updates = {
        ...             "deleted": True,
        ...             }
        ...         )

        Update all row values. This is equivalent to
        ``UPDATE table SET id = concat(id, '_old')``.
        >>> from deltalake import DeltaTable
        >>> dt = DeltaTable("tmp")
        >>> dt.update(updates = {
        ...             "deleted": True,
        ...             "id": "concat(id, '_old')"
        ...             }
        ...         )

        """

        metrics = self._table.update(
            updates, predicate, writer_properties, safe_cast=not error_on_type_mismatch
        )
        return json.loads(metrics)

    @property
    def optimize(
        self,
    ) -> "TableOptimizer":
        return TableOptimizer(self)

    def merge(
        self,
        source: Union[pyarrow.Table, pyarrow.RecordBatch, pyarrow.RecordBatchReader],
        predicate: str,
        source_alias: Optional[str] = None,
        target_alias: Optional[str] = None,
        error_on_type_mismatch: bool = True,
    ) -> "TableMerger":
        """Pass the source data which you want to merge on the target delta table, providing a
        predicate in SQL query like format. You can also specify on what to do when the underlying data types do not
        match the underlying table.

        Args:
            source (pyarrow.Table | pyarrow.RecordBatch | pyarrow.RecordBatchReader ): source data
            predicate (str): SQL like predicate on how to merge
            source_alias (str): Alias for the source table
            target_alias (str): Alias for the target table
            error_on_type_mismatch (bool): specify if merge will return error if data types are mismatching :default = True

        Returns:
            TableMerger: TableMerger Object
        """
        invariants = self.schema().invariants
        checker = _DeltaDataChecker(invariants)

        if isinstance(source, pyarrow.RecordBatchReader):
            schema = source.schema
        elif isinstance(source, pyarrow.RecordBatch):
            schema = source.schema
            source = [source]
        elif isinstance(source, pyarrow.Table):
            schema = source.schema
            source = source.to_reader()
        else:
            raise TypeError(
                f"{type(source).__name__} is not a valid input. Only PyArrow RecordBatchReader, RecordBatch or Table are valid inputs for source."
            )

        def validate_batch(batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
            checker.check_batch(batch)
            return batch

        source = pyarrow.RecordBatchReader.from_batches(
            schema, (validate_batch(batch) for batch in source)
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

        :param target: the expected version will restore, which represented by int, date str or datetime.
        :param ignore_missing_files: whether the operation carry on when some data files missing.
        :param protocol_downgrade_allowed: whether the operation when protocol version upgraded.
        :return: the metrics from restore.
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

        :param partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
        :param filesystem: A concrete implementation of the Pyarrow FileSystem or a fsspec-compatible interface. If None, the first file path will be used to determine the right FileSystem
        :param parquet_read_options: Optional read options for Parquet. Use this to handle INT96 to timestamp conversion for edge cases like 0001-01-01 or 9999-12-31
         More info: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetReadOptions.html
        :return: the PyArrow dataset in PyArrow
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

        :param partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
        :param columns: The columns to project. This can be a list of column names to include (order and duplicates will be preserved)
        :param filesystem: A concrete implementation of the Pyarrow FileSystem or a fsspec-compatible interface. If None, the first file path will be used to determine the right FileSystem
        :param filters: A disjunctive normal form (DNF) predicate for filtering rows. If you pass a filter you do not need to pass ``partitions``
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

        :param partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
        :param columns: The columns to project. This can be a list of column names to include (order and duplicates will be preserved)
        :param filesystem: A concrete implementation of the Pyarrow FileSystem or a fsspec-compatible interface. If None, the first file path will be used to determine the right FileSystem
        :param filters: A disjunctive normal form (DNF) predicate for filtering rows. If you pass a filter you do not need to pass ``partitions``
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

    def create_checkpoint(self) -> None:
        self._table.create_checkpoint()

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

        :param flatten: whether to flatten the schema. Partition values columns are
          given the prefix `partition.`, statistics (null_count, min, and max) are
          given the prefix `null_count.`, `min.`, and `max.`, and tags the
          prefix `tags.`. Nested field names are concatenated with `.`.

        :returns: a PyArrow RecordBatch containing the add action data.

        Examples:

        >>> from deltalake import DeltaTable, write_deltalake
        >>> import pyarrow as pa
        >>> data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> write_deltalake("tmp", data, partition_by=["x"])
        >>> dt = DeltaTable("tmp")
        >>> dt.get_add_actions().to_pandas()
                                                                path  size_bytes       modification_time  data_change partition_values  num_records null_count       min       max
        0  x=2/0-91820cbf-f698-45fb-886d-5d5f5669530b-0.p...         565 1970-01-20 08:40:08.071         True         {'x': 2}            1   {'y': 0}  {'y': 5}  {'y': 5}
        1  x=3/0-91820cbf-f698-45fb-886d-5d5f5669530b-0.p...         565 1970-01-20 08:40:08.071         True         {'x': 3}            1   {'y': 0}  {'y': 6}  {'y': 6}
        2  x=1/0-91820cbf-f698-45fb-886d-5d5f5669530b-0.p...         565 1970-01-20 08:40:08.071         True         {'x': 1}            1   {'y': 0}  {'y': 4}  {'y': 4}
        >>> dt.get_add_actions(flatten=True).to_pandas()
                                                                path  size_bytes       modification_time  data_change  partition.x  num_records  null_count.y  min.y  max.y
        0  x=2/0-91820cbf-f698-45fb-886d-5d5f5669530b-0.p...         565 1970-01-20 08:40:08.071         True            2            1             0      5      5
        1  x=3/0-91820cbf-f698-45fb-886d-5d5f5669530b-0.p...         565 1970-01-20 08:40:08.071         True            3            1             0      6      6
        2  x=1/0-91820cbf-f698-45fb-886d-5d5f5669530b-0.p...         565 1970-01-20 08:40:08.071         True            1            1             0      4      4
        """
        return self._table.get_add_actions(flatten)

    def delete(self, predicate: Optional[str] = None) -> Dict[str, Any]:
        """Delete records from a Delta Table that statisfy a predicate.

        When a predicate is not provided then all records are deleted from the Delta
        Table. Otherwise a scan of the Delta table is performed to mark any files
        that contain records that satisfy the predicate. Once files are determined
        they are rewritten without the records.

        :param predicate: a SQL where clause. If not passed, will delete all rows.
        :return: the metrics from delete.
        """
        metrics = self._table.delete(predicate)
        return json.loads(metrics)


class TableMerger:
    """API for various table MERGE commands."""

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
        self.matched_update_updates: Optional[Dict[str, str]] = None
        self.matched_update_predicate: Optional[str] = None
        self.matched_delete_predicate: Optional[str] = None
        self.matched_delete_all: Optional[bool] = None
        self.not_matched_insert_updates: Optional[Dict[str, str]] = None
        self.not_matched_insert_predicate: Optional[str] = None
        self.not_matched_by_source_update_updates: Optional[Dict[str, str]] = None
        self.not_matched_by_source_update_predicate: Optional[str] = None
        self.not_matched_by_source_delete_predicate: Optional[str] = None
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
            data_page_size_limit (int|None, optional): Limit DataPage size to this in bytes. Defaults to None.
            dictionary_page_size_limit (int|None, optional): Limit the size of each DataPage to store dicts to this amount in bytes. Defaults to None.
            data_page_row_count_limit (int|None, optional): Limit the number of rows in each DataPage. Defaults to None.
            write_batch_size (int|None, optional): Splits internally to smaller batch size. Defaults to None.
            max_row_group_size (int|None, optional): Max number of rows in row group. Defaults to None.

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
            updates (dict): a mapping of column name to update SQL expression.
            predicate (str | None, optional):  SQL like predicate on when to update. Defaults to None.

        Returns:
            TableMerger: TableMerger Object
            
        Examples:

        >>> from deltalake import DeltaTable
        >>> import pyarrow as pa
        >>> data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> dt = DeltaTable("tmp")
        >>> dt.merge(source=data, predicate='target.x = source.x', source_alias='source', target_alias='target') \
        ...     .when_matched_update(
        ...         updates = {
        ...             "x": "source.x",
        ...             "y": "source.y"
        ...             }
        ...         ).execute()
        """
        self.matched_update_updates = updates
        self.matched_update_predicate = predicate
        return self

    def when_matched_update_all(self, predicate: Optional[str] = None) -> "TableMerger":
        """Updating all source fields to target fields, source and target are required to have the same field names. 
        If a ``predicate`` is specified, then it must evaluate to true for the row to be updated.

        Args:
            predicate (str | None, optional): SQL like predicate on when to update all columns. Defaults to None.

        Returns:
            TableMerger: TableMerger Object
            
        Examples:

        >>> from deltalake import DeltaTable
        >>> import pyarrow as pa
        >>> data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> dt = DeltaTable("tmp")
        >>> dt.merge(source=data, predicate='target.x = source.x', source_alias='source', target_alias='target')  \
        ...     .when_matched_update_all().execute()
        """

        src_alias = (self.source_alias + ".") if self.source_alias is not None else ""
        trgt_alias = (self.target_alias + ".") if self.target_alias is not None else ""

        self.matched_update_updates = {
            f"{trgt_alias}{col.name}": f"{src_alias}{col.name}"
            for col in self.source.schema
        }
        self.matched_update_predicate = predicate
        return self

    def when_matched_delete(self, predicate: Optional[str] = None) -> "TableMerger":
        """Delete a matched row from the table only if the given ``predicate`` (if specified) is
        true for the matched row. If not specified it deletes all matches.

        Args:
           predicate (str | None, optional):  SQL like predicate on when to delete. Defaults to None.

        Returns:
            TableMerger: TableMerger Object
            
        Examples:

        Delete on a predicate
        
        >>> from deltalake import DeltaTable
        >>> import pyarrow as pa
        >>> data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> dt = DeltaTable("tmp")
        >>> dt.merge(source=data, predicate='target.x = source.x', source_alias='source', target_alias='target') \
        ...     .when_matched_delete(predicate = "source.deleted = true")
        ...     .execute()
        
        Delete all records that were matched
        
        >>> from deltalake import DeltaTable
        >>> import pyarrow as pa
        >>> data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> dt = DeltaTable("tmp")
        >>> dt.merge(source=data, predicate='target.x = source.x', source_alias='source', target_alias='target')  \
        ...     .when_matched_delete()
        ...     .execute()
        """

        if predicate is None:
            self.matched_delete_all = True
        else:
            self.matched_delete_predicate = predicate
        return self

    def when_not_matched_insert(
        self, updates: Dict[str, str], predicate: Optional[str] = None
    ) -> "TableMerger":
        """Insert a new row to the target table based on the rules defined by ``updates``. If a
        ``predicate`` is specified, then it must evaluate to true for the new row to be inserted.

        Args:
            updates (dict):  a mapping of column name to insert SQL expression.
            predicate (str | None, optional): SQL like predicate on when to insert. Defaults to None.

        Returns:
            TableMerger: TableMerger Object
            
        Examples:

        >>> from deltalake import DeltaTable
        >>> import pyarrow as pa
        >>> data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> dt = DeltaTable("tmp")
        >>> dt.merge(source=data, predicate='target.x = source.x', source_alias='source', target_alias='target')  \
        ...     .when_not_matched_insert(
        ...         updates = {
        ...             "x": "source.x",
        ...             "y": "source.y"
        ...             }
        ...         ).execute()
        """

        self.not_matched_insert_updates = updates
        self.not_matched_insert_predicate = predicate

        return self

    def when_not_matched_insert_all(
        self, predicate: Optional[str] = None
    ) -> "TableMerger":
        """Insert a new row to the target table, updating all source fields to target fields. Source and target are 
        required to have the same field names. If a ``predicate`` is specified, then it must evaluate to true for 
        the new row to be inserted.

        Args:
            predicate (str | None, optional): SQL like predicate on when to insert. Defaults to None.

        Returns:
            TableMerger: TableMerger Object
            
        Examples:

        >>> from deltalake import DeltaTable
        >>> import pyarrow as pa
        >>> data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> dt = DeltaTable("tmp")
        >>> dt.merge(source=data, predicate='target.x = source.x', source_alias='source', target_alias='target')  \
        ...     .when_not_matched_insert_all().execute()
        """

        src_alias = (self.source_alias + ".") if self.source_alias is not None else ""
        trgt_alias = (self.target_alias + ".") if self.target_alias is not None else ""
        self.not_matched_insert_updates = {
            f"{trgt_alias}{col.name}": f"{src_alias}{col.name}"
            for col in self.source.schema
        }
        self.not_matched_insert_predicate = predicate
        return self

    def when_not_matched_by_source_update(
        self, updates: Dict[str, str], predicate: Optional[str] = None
    ) -> "TableMerger":
        """Update a target row that has no matches in the source based on the rules defined by ``updates``.
        If a ``predicate`` is specified, then it must evaluate to true for the row to be updated.

        Args:
            updates (dict): a mapping of column name to update SQL expression.
            predicate (str | None, optional): SQL like predicate on when to update. Defaults to None.

        Returns:
            TableMerger: TableMerger Object
            
        >>> from deltalake import DeltaTable
        >>> import pyarrow as pa
        >>> data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> dt = DeltaTable("tmp")
        >>> dt.merge(source=data, predicate='target.x = source.x', source_alias='source', target_alias='target') \
        ...     .when_not_matched_by_source_update(
        ...         predicate = "y > 3"
        ...         updates = {
        ...             "y": "0",
        ...             }
        ...         ).execute()    
        """
        self.not_matched_by_source_update_updates = updates
        self.not_matched_by_source_update_predicate = predicate
        return self

    def when_not_matched_by_source_delete(
        self, predicate: Optional[str] = None
    ) -> "TableMerger":
        """Delete a target row that has no matches in the source from the table only if the given
        ``predicate`` (if specified) is true for the target row.

        Args:
            updates (dict): a mapping of column name to update SQL expression.
            predicate (str | None, optional):  SQL like predicate on when to delete when not matched by source. Defaults to None.

        Returns:
            TableMerger: TableMerger Object
        """

        if predicate is None:
            self.not_matched_by_source_delete_all = True
        else:
            self.not_matched_by_source_delete_predicate = predicate
        return self

    def execute(self) -> Dict[str, Any]:
        """Executes MERGE with the previously provided settings in Rust with Apache Datafusion query engine.

        Returns:
            Dict[str, any]: metrics
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
        .. deprecated:: 0.10.0
            Use :meth:`compact` instead, which has the same signature.
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

        :param partition_filters: the partition filters that will be used for getting the matched files
        :param target_size: desired file size after bin-packing files, in bytes. If not
          provided, will attempt to read the table configuration value ``delta.targetFileSize``.
          If that value isn't set, will use default value of 256MB.
        :param max_concurrent_tasks: the maximum number of concurrent tasks to use for
            file compaction. Defaults to number of CPUs. More concurrent tasks can make compaction
            faster, but will also use more memory.
        :param min_commit_interval: minimum interval in seconds or as timedeltas before a new commit is
            created. Interval is useful for long running executions. Set to 0 or timedelta(0), if you
            want a commit per partition.
        :return: the metrics from optimize

        Examples:

        Use a timedelta object to specify the seconds, minutes or hours of the interval.
        >>> from deltalake import DeltaTable
        >>> from datetime import timedelta
        >>> dt = DeltaTable("tmp")
        >>> time_delta = timedelta(minutes=10)
        >>> dt.optimize.z_order(["timestamp"], min_commit_interval=time_delta)
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

        :param columns: the columns to use for Z-ordering. There must be at least one column.
        :param partition_filters: the partition filters that will be used for getting the matched files
        :param target_size: desired file size after bin-packing files, in bytes. If not
          provided, will attempt to read the table configuration value ``delta.targetFileSize``.
          If that value isn't set, will use default value of 256MB.
        :param max_concurrent_tasks: the maximum number of concurrent tasks to use for
            file compaction. Defaults to number of CPUs. More concurrent tasks can make compaction
            faster, but will also use more memory.
        :param max_spill_size: the maximum number of bytes to spill to disk. Defaults to 20GB.
        :param min_commit_interval: minimum interval in seconds or as timedeltas before a new commit is
            created. Interval is useful for long running executions. Set to 0 or timedelta(0), if you
            want a commit per partition.
        :return: the metrics from optimize

        Examples:

        Use a timedelta object to specify the seconds, minutes or hours of the interval.
        >>> from deltalake import DeltaTable
        >>> from datetime import timedelta
        >>> dt = DeltaTable("tmp")
        >>> time_delta = timedelta(minutes=10)
        >>> dt.optimize.compact(min_commit_interval=time_delta)
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
