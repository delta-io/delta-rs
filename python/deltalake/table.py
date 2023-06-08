import json
import operator
import warnings
from dataclasses import dataclass
from functools import reduce
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
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
    ParquetReadOptions,
)

if TYPE_CHECKING:
    import pandas

from ._internal import RawDeltaTable
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
        """
        self._storage_options = storage_options
        self._table = RawDeltaTable(
            str(table_uri),
            version=version,
            storage_options=storage_options,
            without_files=without_files,
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
    ) -> "DeltaTable":
        """
        Create the Delta Table from a Data Catalog.

        :param data_catalog: the Catalog to use for getting the storage location of the Delta Table
        :param database_name: the database name inside the Data Catalog
        :param table_name: the table name inside the Data Catalog
        :param data_catalog_id: the identifier of the Data Catalog
        :param version: version of the DeltaTable
        """
        table_uri = RawDeltaTable.get_table_uri_from_data_catalog(
            data_catalog=data_catalog.value,
            data_catalog_id=data_catalog_id,
            database_name=database_name,
            table_name=table_name,
        )
        return cls(table_uri=table_uri, version=version)

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

    def files_by_partitions(
        self, partition_filters: List[Tuple[str, str, Any]]
    ) -> List[str]:
        """
        Get the files that match a given list of partitions filters.

        .. deprecated:: 0.7.0
            Use :meth:`file_uris` instead.

        Partitions which do not match the filter predicate will be removed from scanned data.
        Predicates are expressed in disjunctive normal form (DNF), like [("x", "=", "a"), ...].
        DNF allows arbitrary boolean logical combinations of single partition predicates.
        The innermost tuples each describe a single partition predicate.
        The list of inner predicates is interpreted as a conjunction (AND), forming a more selective and multiple partition predicates.
        Each tuple has format: (key, op, value) and compares the key with the value.
        The supported op are: `=`, `!=`, `in`, and `not in`.
        If the op is in or not in, the value must be a collection such as a list, a set or a tuple.
        The supported type for value is str. Use empty string `''` for Null partition value.

        Examples:
        ("x", "=", "a")
        ("x", "!=", "a")
        ("y", "in", ["a", "b", "c"])
        ("z", "not in", ["a","b"])

        :param partition_filters: the partition filters that will be used for getting the matched files
        :return: list of the .parquet files after applying the partition filters referenced for the current version of the DeltaTable.
        """
        warnings.warn(
            "Call to deprecated method files_by_partitions. Please use file_uris instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return self.file_uris(partition_filters)

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
        return ProtocolVersions(*self._table.protocol_versions())

    def history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Run the history command on the DeltaTable.
        The operations are returned in reverse chronological order.

        :param limit: the commit info limit to return
        :return: list of the commit infos registered in the transaction log
        """
        return [
            json.loads(commit_info_raw)
            for commit_info_raw in self._table.history(limit)
        ]

    def vacuum(
        self,
        retention_hours: Optional[int] = None,
        dry_run: bool = True,
        enforce_retention_duration: bool = True,
        max_concurrent_requests: int = 10,
    ) -> List[str]:
        """
        Run the Vacuum command on the Delta Table: list and delete files no longer referenced by the Delta table and are older than the retention threshold.

        :param retention_hours: the retention threshold in hours, if none then the value from `configuration.deletedFileRetentionDuration` is used or default of 1 week otherwise.
        :param dry_run: when activated, list only the files, delete otherwise
        :param enforce_retention_duration: when disabled, accepts retention hours smaller than the value from `configuration.deletedFileRetentionDuration`.
        :param max_concurrent_requests: the maximum number of concurrent requests to send to the backend.
          Increasing this number may improve performance of vacuuming large tables, however it might also
          increase the risk of hitting rate limits.
        :return: the list of files no longer referenced by the Delta Table and are older than the retention threshold.
        """
        if retention_hours:
            if retention_hours < 0:
                raise ValueError("The retention periods should be positive.")

        return self._table.vacuum(
            dry_run,
            retention_hours,
            enforce_retention_duration,
            max_concurrent_requests,
        )

    @property
    def optimize(
        self,
    ) -> "TableOptimizer":
        return TableOptimizer(self)

    def pyarrow_schema(self) -> pyarrow.Schema:
        """
        Get the current schema of the DeltaTable with the Parquet PyArrow format.

        DEPRECATED: use DeltaTable.schema().to_pyarrow() instead.

        :return: the current Schema with the Parquet PyArrow format
        """
        warnings.warn(
            "DeltaTable.pyarrow_schema() is deprecated. Use DeltaTable.schema().to_pyarrow() instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return self.schema().to_pyarrow()

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
                f"The table's minimum reader version is {self.protocol().min_reader_version}"
                f"but deltalake only supports up to version {MAX_SUPPORTED_READER_VERSION}."
            )

        if not filesystem:
            filesystem = pa_fs.PyFileSystem(
                DeltaStorageHandler(
                    self._table.table_uri(),
                    self._storage_options,
                )
            )

        format = ParquetFileFormat(read_options=parquet_read_options)

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
                str_value = [str(val) for val in value]
            else:
                str_value = str(value)
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
        :return: the metrics from optimize
        """
        metrics = self.table._table.compact_optimize(
            partition_filters, target_size, max_concurrent_tasks
        )
        self.table.update_incremental()
        return json.loads(metrics)

    def z_order(
        self,
        columns: Iterable[str],
        partition_filters: Optional[FilterType] = None,
        target_size: Optional[int] = None,
        max_concurrent_tasks: Optional[int] = None,
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
        :return: the metrics from optimize
        """
        metrics = self.table._table.z_order_optimize(
            list(columns), partition_filters, target_size, max_concurrent_tasks
        )
        self.table.update_incremental()
        return json.loads(metrics)
