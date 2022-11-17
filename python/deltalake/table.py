import json
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Optional, Tuple, Union

import pyarrow
import pyarrow.fs as pa_fs
from pyarrow.dataset import FileSystemDataset, ParquetFileFormat, ParquetReadOptions

if TYPE_CHECKING:
    import pandas

from ._internal import PyDeltaTableError, RawDeltaTable
from .data_catalog import DataCatalog
from .fs import DeltaStorageHandler
from .schema import Schema


class DeltaTableProtocolError(PyDeltaTableError):
    pass


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


@dataclass(init=False)
class DeltaTable:
    """Create a DeltaTable instance."""

    def __init__(
        self,
        table_uri: str,
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
            table_uri,
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

    def files(self) -> List[str]:
        """
        Get the .parquet files of the DeltaTable.

        :return: list of the .parquet files referenced for the current version of the DeltaTable
        """
        return self._table.files()

    def files_by_partitions(
        self, partition_filters: List[Tuple[str, str, Any]]
    ) -> List[str]:
        """
        Get the files that match a given list of partitions filters.
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
        try:
            return self._table.files_by_partitions(partition_filters)
        except TypeError:
            raise ValueError(
                "Only the type String is currently allowed inside the partition filters."
            )

    def file_paths(self) -> List[str]:
        """
        Get the list of files with an absolute path.

        :return: list of the .parquet files with an absolute URI referenced for the current version of the DeltaTable
        """
        warnings.warn(
            "Call to deprecated method file_paths. Please use file_uris instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return self.file_uris()

    def file_uris(self) -> List[str]:
        """
        Get the list of files with an absolute path.

        :return: list of the .parquet files with an absolute URI referenced for the current version of the DeltaTable
        """
        return self._table.file_uris()

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

        return self._table.vacuum(dry_run, retention_hours, enforce_retention_duration)

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
            raise DeltaTableProtocolError(
                f"The table's minimum reader version is {self.protocol().min_reader_version}"
                f"but deltalake only supports up to version {MAX_SUPPORTED_READER_VERSION}."
            )

        if not filesystem:
            filesystem = pa_fs.PyFileSystem(
                DeltaStorageHandler(
                    self._table.table_uri(),
                    self._storage_options,
                    self._table.get_py_storage_backend(),
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
                partitions, self.schema().to_pyarrow()
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
    ) -> pyarrow.Table:
        """
        Build a PyArrow Table using data from the DeltaTable.

        :param partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
        :param columns: The columns to project. This can be a list of column names to include (order and duplicates will be preserved)
        :param filesystem: A concrete implementation of the Pyarrow FileSystem or a fsspec-compatible interface. If None, the first file path will be used to determine the right FileSystem
        :return: the PyArrow table
        """
        return self.to_pyarrow_dataset(
            partitions=partitions, filesystem=filesystem
        ).to_table(columns=columns)

    def to_pandas(
        self,
        partitions: Optional[List[Tuple[str, str, Any]]] = None,
        columns: Optional[List[str]] = None,
        filesystem: Optional[Union[str, pa_fs.FileSystem]] = None,
    ) -> "pandas.DataFrame":
        """
        Build a pandas dataframe using data from the DeltaTable.

        :param partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
        :param columns: The columns to project. This can be a list of column names to include (order and duplicates will be preserved)
        :param filesystem: A concrete implementation of the Pyarrow FileSystem or a fsspec-compatible interface. If None, the first file path will be used to determine the right FileSystem
        :return: a pandas dataframe
        """
        return self.to_pyarrow_table(
            partitions=partitions, columns=columns, filesystem=filesystem
        ).to_pandas()

    def update_incremental(self) -> None:
        """
        Updates the DeltaTable to the latest version by incrementally applying
        newer versions.
        """
        self._table.update_incremental()
