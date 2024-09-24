import json
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
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

try:
    from pyarrow.parquet import filters_to_expression  # pyarrow >= 10.0.0
except ImportError:
    from pyarrow.parquet import _filters_to_expression as filters_to_expression

if TYPE_CHECKING:
    import os

from deltalake._internal import (
    DeltaError,
    PyMergeBuilder,
    RawDeltaTable,
    TableFeatures,
)
from deltalake._internal import create_deltalake as _create_deltalake
from deltalake._util import encode_partition_value
from deltalake.data_catalog import DataCatalog
from deltalake.exceptions import DeltaProtocolError
from deltalake.fs import DeltaStorageHandler
from deltalake.schema import ArrowSchemaConversionMode
from deltalake.schema import Field as DeltaField
from deltalake.schema import Schema as DeltaSchema

try:
    import pandas as pd
except ModuleNotFoundError:
    _has_pandas = False
else:
    _has_pandas = True

MAX_SUPPORTED_PYARROW_WRITER_VERSION = 7
NOT_SUPPORTED_PYARROW_WRITER_VERSIONS = [3, 4, 5, 6]
SUPPORTED_WRITER_FEATURES = {"appendOnly", "invariants", "timestampNtz"}

MAX_SUPPORTED_READER_VERSION = 3
NOT_SUPPORTED_READER_VERSION = 2
SUPPORTED_READER_FEATURES = {"timestampNtz"}

FilterLiteralType = Tuple[str, str, Any]
FilterConjunctionType = List[FilterLiteralType]
FilterDNFType = List[FilterConjunctionType]
FilterType = Union[FilterConjunctionType, FilterDNFType]
PartitionFilterType = List[Tuple[str, str, Union[str, List[str]]]]


class Compression(Enum):
    UNCOMPRESSED = "UNCOMPRESSED"
    SNAPPY = "SNAPPY"
    GZIP = "GZIP"
    BROTLI = "BROTLI"
    LZ4 = "LZ4"
    ZSTD = "ZSTD"
    LZ4_RAW = "LZ4_RAW"

    @classmethod
    def from_str(cls, value: str) -> "Compression":
        try:
            return cls(value.upper())
        except ValueError:
            raise ValueError(
                f"{value} is not a valid Compression. Valid values are: {[item.value for item in Compression]}"
            )

    def get_level_range(self) -> Tuple[int, int]:
        if self == Compression.GZIP:
            MIN_LEVEL = 0
            MAX_LEVEL = 10
        elif self == Compression.BROTLI:
            MIN_LEVEL = 0
            MAX_LEVEL = 11
        elif self == Compression.ZSTD:
            MIN_LEVEL = 1
            MAX_LEVEL = 22
        else:
            raise KeyError(f"{self.value} does not have a compression level.")
        return MIN_LEVEL, MAX_LEVEL

    def get_default_level(self) -> int:
        if self == Compression.GZIP:
            DEFAULT = 6
        elif self == Compression.BROTLI:
            DEFAULT = 1
        elif self == Compression.ZSTD:
            DEFAULT = 1
        else:
            raise KeyError(f"{self.value} does not have a compression level.")
        return DEFAULT

    def check_valid_level(self, level: int) -> bool:
        MIN_LEVEL, MAX_LEVEL = self.get_level_range()
        if level < MIN_LEVEL or level > MAX_LEVEL:
            raise ValueError(
                f"Compression level for {self.value} should fall between {MIN_LEVEL}-{MAX_LEVEL}"
            )
        else:
            return True


@dataclass(init=True)
class PostCommitHookProperties:
    """The post commit hook properties, only required for advanced usecases where you need to control this."""

    def __init__(
        self,
        create_checkpoint: bool = True,
        cleanup_expired_logs: Optional[bool] = None,
    ):
        """Checkpoints are by default created based on the delta.checkpointInterval config setting.
        cleanup_expired_logs can be set to override the delta.enableExpiredLogCleanup, otherwise the
        config setting will be used to decide whether to clean up logs automatically by taking also
        the delta.logRetentionDuration into account.

        Args:
            create_checkpoint (bool, optional): to create checkpoints based on checkpoint interval. Defaults to True.
            cleanup_expired_logs (Optional[bool], optional): to clean up logs based on interval. Defaults to None.
        """
        self.create_checkpoint = create_checkpoint
        self.cleanup_expired_logs = cleanup_expired_logs


@dataclass(init=True)
class CommitProperties:
    """The commit properties. Controls the behaviour of the commit."""

    def __init__(
        self,
        custom_metadata: Optional[Dict[str, str]] = None,
        max_commit_retries: Optional[int] = None,
    ):
        """Custom metadata to be stored in the commit. Controls the number of retries for the commit.

        Args:
            custom_metadata: custom metadata that will be added to the transaction commit.
            max_commit_retries: maximum number of times to retry the transaction commit.
        """
        self.custom_metadata = custom_metadata
        self.max_commit_retries = max_commit_retries


def _commit_properties_from_custom_metadata(
    maybe_properties: Optional[CommitProperties], custom_metadata: Dict[str, str]
) -> CommitProperties:
    if maybe_properties is not None:
        if maybe_properties.custom_metadata is None:
            maybe_properties.custom_metadata = custom_metadata
            return maybe_properties
        return maybe_properties
    return CommitProperties(custom_metadata=custom_metadata)


@dataclass(init=True)
class BloomFilterProperties:
    """The Bloom Filter Properties instance for the Rust parquet writer."""

    def __init__(
        self,
        set_bloom_filter_enabled: Optional[bool],
        fpp: Optional[float] = None,
        ndv: Optional[int] = None,
    ):
        """Create a Bloom Filter Properties instance for the Rust parquet writer:

        Args:
            set_bloom_filter_enabled: If True and no fpp or ndv are provided, the default values will be used.
            fpp: The false positive probability for the bloom filter. Must be between 0 and 1 exclusive.
            ndv: The number of distinct values for the bloom filter.
        """
        if fpp is not None and (fpp <= 0 or fpp >= 1):
            raise ValueError("fpp must be between 0 and 1 exclusive")
        self.set_bloom_filter_enabled = set_bloom_filter_enabled
        self.fpp = fpp
        self.ndv = ndv

    def __str__(self) -> str:
        return f"set_bloom_filter_enabled: {self.set_bloom_filter_enabled}, fpp: {self.fpp}, ndv: {self.ndv}"


@dataclass(init=True)
class ColumnProperties:
    """The Column Properties instance for the Rust parquet writer."""

    def __init__(
        self,
        dictionary_enabled: Optional[bool] = None,
        max_statistics_size: Optional[int] = None,
        bloom_filter_properties: Optional[BloomFilterProperties] = None,
    ):
        """Create a Column Properties instance for the Rust parquet writer:

        Args:
            dictionary_enabled: Enable dictionary encoding for the column.
            max_statistics_size: Maximum size of statistics for the column.
            bloom_filter_properties: Bloom Filter Properties for the column.
        """
        self.dictionary_enabled = dictionary_enabled
        self.max_statistics_size = max_statistics_size
        self.bloom_filter_properties = bloom_filter_properties

    def __str__(self) -> str:
        return f"dictionary_enabled: {self.dictionary_enabled}, max_statistics_size: {self.max_statistics_size}, bloom_filter_properties: {self.bloom_filter_properties}"


@dataclass(init=True)
class WriterProperties:
    """A Writer Properties instance for the Rust parquet writer."""

    def __init__(
        self,
        data_page_size_limit: Optional[int] = None,
        dictionary_page_size_limit: Optional[int] = None,
        data_page_row_count_limit: Optional[int] = None,
        write_batch_size: Optional[int] = None,
        max_row_group_size: Optional[int] = None,
        compression: Optional[
            Literal[
                "UNCOMPRESSED",
                "SNAPPY",
                "GZIP",
                "BROTLI",
                "LZ4",
                "ZSTD",
                "LZ4_RAW",
            ]
        ] = None,
        compression_level: Optional[int] = None,
        statistics_truncate_length: Optional[int] = None,
        default_column_properties: Optional[ColumnProperties] = None,
        column_properties: Optional[Dict[str, ColumnProperties]] = None,
    ):
        """Create a Writer Properties instance for the Rust parquet writer:

        Args:
            data_page_size_limit: Limit DataPage size to this in bytes.
            dictionary_page_size_limit: Limit the size of each DataPage to store dicts to this amount in bytes.
            data_page_row_count_limit: Limit the number of rows in each DataPage.
            write_batch_size: Splits internally to smaller batch size.
            max_row_group_size: Max number of rows in row group.
            compression: compression type.
            compression_level: If none and compression has a level, the default level will be used, only relevant for
                GZIP: levels (1-9),
                BROTLI: levels (1-11),
                ZSTD: levels (1-22),
            statistics_truncate_length: maximum length of truncated min/max values in statistics.
            default_column_properties: Default Column Properties for the Rust parquet writer.
            column_properties: Column Properties for the Rust parquet writer.
        """
        self.data_page_size_limit = data_page_size_limit
        self.dictionary_page_size_limit = dictionary_page_size_limit
        self.data_page_row_count_limit = data_page_row_count_limit
        self.write_batch_size = write_batch_size
        self.max_row_group_size = max_row_group_size
        self.compression = None
        self.statistics_truncate_length = statistics_truncate_length
        self.default_column_properties = default_column_properties
        self.column_properties = column_properties

        if compression_level is not None and compression is None:
            raise ValueError(
                """Providing a compression level without the compression type is not possible, 
                             please provide the compression as well."""
            )
        if isinstance(compression, str):
            compression_enum = Compression.from_str(compression)
            if compression_enum in [
                Compression.GZIP,
                Compression.BROTLI,
                Compression.ZSTD,
            ]:
                if compression_level is not None:
                    if compression_enum.check_valid_level(compression_level):
                        parquet_compression = (
                            f"{compression_enum.value}({compression_level})"
                        )
                else:
                    parquet_compression = f"{compression_enum.value}({compression_enum.get_default_level()})"
            else:
                parquet_compression = compression_enum.value
            self.compression = parquet_compression

    def __str__(self) -> str:
        column_properties_str = (
            ", ".join([f"column '{k}': {v}" for k, v in self.column_properties.items()])
            if self.column_properties
            else None
        )
        return (
            f"WriterProperties(data_page_size_limit: {self.data_page_size_limit}, dictionary_page_size_limit: {self.dictionary_page_size_limit}, "
            f"data_page_row_count_limit: {self.data_page_row_count_limit}, write_batch_size: {self.write_batch_size}, "
            f"max_row_group_size: {self.max_row_group_size}, compression: {self.compression}, statistics_truncate_length: {self.statistics_truncate_length},"
            f"default_column_properties: {self.default_column_properties}, column_properties: {column_properties_str})"
        )


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
    writer_features: Optional[List[str]]
    reader_features: Optional[List[str]]


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
        self._table = RawDeltaTable(
            str(table_uri),
            version=version,
            storage_options=storage_options,
            without_files=without_files,
            log_buffer_size=log_buffer_size,
        )

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
        raise NotImplementedError(
            "Reading from data catalog is not supported at this point in time."
        )

    @staticmethod
    def is_deltatable(
        table_uri: str, storage_options: Optional[Dict[str, str]] = None
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
        table_uri: Union[str, Path],
        schema: Union[pyarrow.Schema, DeltaSchema],
        mode: Literal["error", "append", "overwrite", "ignore"] = "error",
        partition_by: Optional[Union[List[str], str]] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        configuration: Optional[Mapping[str, Optional[str]]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        custom_metadata: Optional[Dict[str, str]] = None,
        raise_if_key_not_exists: bool = True,
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
            storage_options: Options passed to the object store crate.
            custom_metadata: Custom metadata that will be added to the transaction commit.
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
            raise_if_key_not_exists,
            name,
            description,
            configuration,
            storage_options,
            custom_metadata,
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
        partition_filters: Optional[List[Tuple[str, str, Any]]] = None,
    ) -> List[Dict[str, str]]:
        """
        Returns the partitions as a list of dicts. Example: `[{'month': '1', 'year': '2020', 'day': '1'}, ...]`

        Args:
            partition_filters: The partition filters that will be used for getting the matched partitions, defaults to `None` (no filtering).
        """

        partitions: List[Dict[str, str]] = []
        for partition in self._table.get_active_partitions(partition_filters):
            if not partition:
                continue
            partitions.append({k: v for (k, v) in partition})
        return partitions

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
        return self._table.files(self._stringify_partition_values(partition_filters))

    def file_uris(
        self, partition_filters: Optional[FilterConjunctionType] = None
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
            self._stringify_partition_values(partition_filters)
        )

    file_uris.__doc__ = ""

    def load_as_version(self, version: Union[int, str, datetime]) -> None:
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
        ending_version: Optional[int] = None,
        starting_timestamp: Optional[str] = None,
        ending_timestamp: Optional[str] = None,
        columns: Optional[List[str]] = None,
    ) -> pyarrow.RecordBatchReader:
        return self._table.load_cdf(
            columns=columns,
            starting_version=starting_version,
            ending_version=ending_version,
            starting_timestamp=starting_timestamp,
            ending_timestamp=ending_timestamp,
        )

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

    def files_by_partitions(self, partition_filters: PartitionFilterType) -> List[str]:
        """
        Get the files for each partition

        """
        warnings.warn(
            "files_by_partitions is deprecated, please use DeltaTable.files() instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )

        return self.files(partition_filters)

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

        commits = list(self._table.history(limit))
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
        custom_metadata: Optional[Dict[str, str]] = None,
        post_commithook_properties: Optional[PostCommitHookProperties] = None,
        commit_properties: Optional[CommitProperties] = None,
    ) -> List[str]:
        """
        Run the Vacuum command on the Delta Table: list and delete files no longer referenced by the Delta table and are older than the retention threshold.

        Args:
            retention_hours: the retention threshold in hours, if none then the value from `delta.deletedFileRetentionDuration` is used or default of 1 week otherwise.
            dry_run: when activated, list only the files, delete otherwise
            enforce_retention_duration: when disabled, accepts retention hours smaller than the value from `delta.deletedFileRetentionDuration`.
            custom_metadata: Deprecated and will be removed in future versions. Use commit_properties instead.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties of the transaction commit. If None, default values are used.
        Returns:
            the list of files no longer referenced by the Delta Table and are older than the retention threshold.
        """
        if custom_metadata:
            warnings.warn(
                "custom_metadata is deprecated, please use commit_properties instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            commit_properties = _commit_properties_from_custom_metadata(
                commit_properties, custom_metadata
            )

        if retention_hours:
            if retention_hours < 0:
                raise ValueError("The retention periods should be positive.")

        return self._table.vacuum(
            dry_run,
            retention_hours,
            enforce_retention_duration,
            commit_properties,
            post_commithook_properties,
        )

    def update(
        self,
        updates: Optional[Dict[str, str]] = None,
        new_values: Optional[
            Dict[str, Union[int, float, str, datetime, bool, List[Any]]]
        ] = None,
        predicate: Optional[str] = None,
        writer_properties: Optional[WriterProperties] = None,
        error_on_type_mismatch: bool = True,
        custom_metadata: Optional[Dict[str, str]] = None,
        post_commithook_properties: Optional[PostCommitHookProperties] = None,
        commit_properties: Optional[CommitProperties] = None,
    ) -> Dict[str, Any]:
        """`UPDATE` records in the Delta Table that matches an optional predicate. Either updates or new_values needs
        to be passed for it to execute.

        Args:
            updates: a mapping of column name to update SQL expression.
            new_values: a mapping of column name to python datatype.
            predicate: a logical expression.
            writer_properties: Pass writer properties to the Rust parquet writer.
            error_on_type_mismatch: specify if update will return error if data types are mismatching :default = True
            custom_metadata: Deprecated and will be removed in future versions. Use commit_properties instead.
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
        if custom_metadata:
            warnings.warn(
                "custom_metadata is deprecated, please use commit_properties instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            commit_properties = _commit_properties_from_custom_metadata(
                commit_properties, custom_metadata
            )

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
    ) -> "TableOptimizer":
        """Namespace for all table optimize related methods.

        Returns:
            TableOptimizer: TableOptimizer Object
        """
        return TableOptimizer(self)

    @property
    def alter(
        self,
    ) -> "TableAlterer":
        """Namespace for all table alter related methods.

        Returns:
            TableAlterer: TableAlterer Object
        """
        return TableAlterer(self)

    def merge(
        self,
        source: Union[
            pyarrow.Table,
            pyarrow.RecordBatch,
            pyarrow.RecordBatchReader,
            ds.Dataset,
            "pd.DataFrame",
        ],
        predicate: str,
        source_alias: Optional[str] = None,
        target_alias: Optional[str] = None,
        error_on_type_mismatch: bool = True,
        writer_properties: Optional[WriterProperties] = None,
        large_dtypes: Optional[bool] = None,
        custom_metadata: Optional[Dict[str, str]] = None,
        post_commithook_properties: Optional[PostCommitHookProperties] = None,
        commit_properties: Optional[CommitProperties] = None,
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
            writer_properties: Pass writer properties to the Rust parquet writer
            large_dtypes: Deprecated, will be removed in 1.0
            arrow_schema_conversion_mode: Large converts all types of data schema into Large Arrow types, passthrough keeps string/binary/list types untouched
            custom_metadata: Deprecated and will be removed in future versions. Use commit_properties instead.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties for the commit. If None, default values are used.

        Returns:
            TableMerger: TableMerger Object
        """
        if custom_metadata:
            warnings.warn(
                "custom_metadata is deprecated, please use commit_properties instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            commit_properties = _commit_properties_from_custom_metadata(
                commit_properties, custom_metadata
            )

        if large_dtypes is not None:
            warnings.warn(
                "large_dtypes is deprecated",
                category=DeprecationWarning,
                stacklevel=2,
            )
            if large_dtypes:
                conversion_mode = ArrowSchemaConversionMode.LARGE
            else:
                conversion_mode = ArrowSchemaConversionMode.NORMAL
        else:
            conversion_mode = ArrowSchemaConversionMode.PASSTHROUGH

        from .schema import (
            convert_pyarrow_dataset,
            convert_pyarrow_recordbatch,
            convert_pyarrow_recordbatchreader,
            convert_pyarrow_table,
        )

        if isinstance(source, pyarrow.RecordBatchReader):
            source = convert_pyarrow_recordbatchreader(source, conversion_mode)
        elif isinstance(source, pyarrow.RecordBatch):
            source = convert_pyarrow_recordbatch(source, conversion_mode)
        elif isinstance(source, pyarrow.Table):
            source = convert_pyarrow_table(source, conversion_mode)
        elif isinstance(source, ds.Dataset):
            source = convert_pyarrow_dataset(source, conversion_mode)
        elif _has_pandas and isinstance(source, pd.DataFrame):
            source = convert_pyarrow_table(
                pyarrow.Table.from_pandas(source), conversion_mode
            )
        else:
            raise TypeError(
                f"{type(source).__name__} is not a valid input. Only PyArrow RecordBatchReader, RecordBatch, Table or Pandas DataFrame are valid inputs for source."
            )

        source = pyarrow.RecordBatchReader.from_batches(
            source.schema, (batch for batch in source)
        )

        py_merge_builder = self._table.create_merge_builder(
            source=source,
            predicate=predicate,
            source_alias=source_alias,
            target_alias=target_alias,
            safe_cast=not error_on_type_mismatch,
            writer_properties=writer_properties,
            commit_properties=commit_properties,
            post_commithook_properties=post_commithook_properties,
        )
        return TableMerger(py_merge_builder, self._table)

    def restore(
        self,
        target: Union[int, datetime, str],
        *,
        ignore_missing_files: bool = False,
        protocol_downgrade_allowed: bool = False,
        custom_metadata: Optional[Dict[str, str]] = None,
        commit_properties: Optional[CommitProperties] = None,
    ) -> Dict[str, Any]:
        """
        Run the Restore command on the Delta Table: restore table to a given version or datetime.

        Args:
            target: the expected version will restore, which represented by int, date str or datetime.
            ignore_missing_files: whether the operation carry on when some data files missing.
            protocol_downgrade_allowed: whether the operation when protocol version upgraded.
            custom_metadata: Deprecated and will be removed in future versions. Use commit_properties instead.
            commit_properties: properties of the transaction commit. If None, default values are used.

        Returns:
            the metrics from restore.
        """
        if custom_metadata:
            warnings.warn(
                "custom_metadata is deprecated, please use commit_properties instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            commit_properties = _commit_properties_from_custom_metadata(
                commit_properties, custom_metadata
            )

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
        partitions: Optional[FilterConjunctionType] = None,
        filesystem: Optional[Union[str, pa_fs.FileSystem]] = None,
        parquet_read_options: Optional[ParquetReadOptions] = None,
        schema: Optional[pyarrow.Schema] = None,
        as_large_types: bool = False,
    ) -> pyarrow.dataset.Dataset:
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
        if not self._table.has_files():
            raise DeltaError("Table is instantiated without files.")

        table_protocol = self.protocol()
        if (
            table_protocol.min_reader_version > MAX_SUPPORTED_READER_VERSION
            or table_protocol.min_reader_version == NOT_SUPPORTED_READER_VERSION
        ):
            raise DeltaProtocolError(
                f"The table's minimum reader version is {table_protocol.min_reader_version} "
                f"but deltalake only supports version 1 or {MAX_SUPPORTED_READER_VERSION} with these reader features: {SUPPORTED_READER_FEATURES}"
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
                    f"The table has set these reader features: {missing_features} but these are not yet supported by the deltalake reader."
                )
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

        schema = schema or self.schema().to_pyarrow(as_large_types=as_large_types)

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
        partitions: Optional[List[Tuple[str, str, Any]]] = None,
        columns: Optional[List[str]] = None,
        filesystem: Optional[Union[str, pa_fs.FileSystem]] = None,
        filters: Optional[Union[FilterType, Expression]] = None,
    ) -> pyarrow.Table:
        """
        Build a PyArrow Table using data from the DeltaTable.

        Args:
            partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
            columns: The columns to project. This can be a list of column names to include (order and duplicates will be preserved)
            filesystem: A concrete implementation of the Pyarrow FileSystem or a fsspec-compatible interface. If None, the first file path will be used to determine the right FileSystem
            filters: A disjunctive normal form (DNF) predicate for filtering rows, or directly a pyarrow.dataset.Expression. If you pass a filter you do not need to pass ``partitions``
        """
        if filters is not None:
            filters = filters_to_expression(filters)
        return self.to_pyarrow_dataset(
            partitions=partitions, filesystem=filesystem
        ).to_table(columns=columns, filter=filters)

    def to_pandas(
        self,
        partitions: Optional[List[Tuple[str, str, Any]]] = None,
        columns: Optional[List[str]] = None,
        filesystem: Optional[Union[str, pa_fs.FileSystem]] = None,
        filters: Optional[Union[FilterType, Expression]] = None,
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
        ).to_pandas()

    def update_incremental(self) -> None:
        """
        Updates the DeltaTable to the latest version by incrementally applying
        newer versions.
        """
        self._table.update_incremental()

    def create_checkpoint(self) -> None:
        self._table.create_checkpoint()

    def cleanup_metadata(self) -> None:
        """
        Delete expired log files before current version from table. The table log retention is based on
        the `delta.logRetentionDuration` value, 30 days by default.
        """
        self._table.cleanup_metadata()

    def _stringify_partition_values(
        self, partition_filters: Optional[FilterConjunctionType]
    ) -> Optional[PartitionFilterType]:
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

    def delete(
        self,
        predicate: Optional[str] = None,
        writer_properties: Optional[WriterProperties] = None,
        custom_metadata: Optional[Dict[str, str]] = None,
        post_commithook_properties: Optional[PostCommitHookProperties] = None,
        commit_properties: Optional[CommitProperties] = None,
    ) -> Dict[str, Any]:
        """Delete records from a Delta Table that statisfy a predicate.

        When a predicate is not provided then all records are deleted from the Delta
        Table. Otherwise a scan of the Delta table is performed to mark any files
        that contain records that satisfy the predicate. Once files are determined
        they are rewritten without the records.

        Args:
            predicate: a SQL where clause. If not passed, will delete all rows.
            writer_properties: Pass writer properties to the Rust parquet writer.
            custom_metadata: Deprecated and will be removed in future versions. Use commit_properties instead.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.
            commit_properties: properties of the transaction commit. If None, default values are used.

        Returns:
            the metrics from delete.
        """
        if custom_metadata:
            warnings.warn(
                "custom_metadata is deprecated, please use commit_properties instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            commit_properties = _commit_properties_from_custom_metadata(
                commit_properties, custom_metadata
            )

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
        custom_metadata: Optional[Dict[str, str]] = None,
        post_commithook_properties: Optional[PostCommitHookProperties] = None,
        commit_properties: Optional[CommitProperties] = None,
    ) -> Dict[str, Any]:
        """Repair the Delta Table by auditing active files that do not exist in the underlying
        filesystem and removes them. This can be useful when there are accidental deletions or corrupted files.

        Active files are ones that have an add action in the log, but no corresponding remove action.
        This operation creates a new FSCK transaction containing a remove action for each of the missing
        or corrupted files.

        Args:
            dry_run: when activated, list only the files, otherwise add remove actions to transaction log. Defaults to False.
            custom_metadata: Deprecated and will be removed in future versions. Use commit_properties instead.
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
        if custom_metadata:
            warnings.warn(
                "custom_metadata is deprecated, please use commit_properties instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            commit_properties = _commit_properties_from_custom_metadata(
                commit_properties, custom_metadata
            )

        metrics = self._table.repair(
            dry_run,
            commit_properties,
            post_commithook_properties,
        )
        return json.loads(metrics)


class TableMerger:
    """API for various table `MERGE` commands."""

    def __init__(
        self,
        builder: PyMergeBuilder,
        table: RawDeltaTable,
    ):
        self._builder = builder
        self._table = table

    def when_matched_update(
        self, updates: Dict[str, str], predicate: Optional[str] = None
    ) -> "TableMerger":
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

    def when_matched_update_all(self, predicate: Optional[str] = None) -> "TableMerger":
        """Updating all source fields to target fields, source and target are required to have the same field names.
        If a ``predicate`` is specified, then it must evaluate to true for the row to be updated.

        Note:
            Column names with special characters, such as numbers or spaces should be encapsulated
            in backticks: "target.`123column`" or "target.`my column`"

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
        maybe_source_alias = self._builder.source_alias
        maybe_target_alias = self._builder.target_alias

        src_alias = (maybe_source_alias + ".") if maybe_source_alias is not None else ""
        trgt_alias = (
            (maybe_target_alias + ".") if maybe_target_alias is not None else ""
        )

        updates = {
            f"{trgt_alias}`{col.name}`": f"{src_alias}`{col.name}`"
            for col in self._builder.arrow_schema
        }

        self._builder.when_matched_update(updates, predicate)
        return self

    def when_matched_delete(self, predicate: Optional[str] = None) -> "TableMerger":
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
        self, updates: Dict[str, str], predicate: Optional[str] = None
    ) -> "TableMerger":
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
        self, predicate: Optional[str] = None
    ) -> "TableMerger":
        """Insert a new row to the target table, updating all source fields to target fields. Source and target are
        required to have the same field names. If a ``predicate`` is specified, then it must evaluate to true for
        the new row to be inserted.

        Note:
            Column names with special characters, such as numbers or spaces should be encapsulated
            in backticks: "target.`123column`" or "target.`my column`"

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
        maybe_source_alias = self._builder.source_alias
        maybe_target_alias = self._builder.target_alias

        src_alias = (maybe_source_alias + ".") if maybe_source_alias is not None else ""
        trgt_alias = (
            (maybe_target_alias + ".") if maybe_target_alias is not None else ""
        )
        updates = {
            f"{trgt_alias}`{col.name}`": f"{src_alias}`{col.name}`"
            for col in self._builder.arrow_schema
        }

        self._builder.when_not_matched_insert(updates, predicate)
        return self

    def when_not_matched_by_source_update(
        self, updates: Dict[str, str], predicate: Optional[str] = None
    ) -> "TableMerger":
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
        self, predicate: Optional[str] = None
    ) -> "TableMerger":
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

    def execute(self) -> Dict[str, Any]:
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
        feature: Union[TableFeatures, List[TableFeatures]],
        allow_protocol_versions_increase: bool = False,
        commit_properties: Optional[CommitProperties] = None,
        post_commithook_properties: Optional[PostCommitHookProperties] = None,
    ) -> None:
        """
        Enable a table feature.

        Args:
            feature: Table Feature e.g. Deletion Vectors, Change Data Feed
            allow_protocol_versions_increase: Allow the protocol to be implicitily bumped to reader 3 or writer 7
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
        fields: Union[DeltaField, List[DeltaField]],
        custom_metadata: Optional[Dict[str, str]] = None,
        commit_properties: Optional[CommitProperties] = None,
        post_commithook_properties: Optional[PostCommitHookProperties] = None,
    ) -> None:
        """Add new columns and/or update the fields of a stuctcolumn

        Args:
            fields: fields to merge into schema
            commit_properties: properties of the transaction commit. If None, default values are used.
            post_commithook_properties: properties for the post commit hook. If None, default values are used.

        Example:
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
        if custom_metadata:
            warnings.warn(
                "custom_metadata is deprecated, please use commit_properties instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            commit_properties = _commit_properties_from_custom_metadata(
                commit_properties, custom_metadata
            )

        if isinstance(fields, DeltaField):
            fields = [fields]

        self.table._table.add_columns(
            fields,
            commit_properties,
            post_commithook_properties,
        )

    def add_constraint(
        self,
        constraints: Dict[str, str],
        custom_metadata: Optional[Dict[str, str]] = None,
        post_commithook_properties: Optional[PostCommitHookProperties] = None,
        commit_properties: Optional[CommitProperties] = None,
    ) -> None:
        """
        Add constraints to the table. Limited to `single constraint` at once.

        Args:
            constraints: mapping of constraint name to SQL-expression to evaluate on write
            custom_metadata: Deprecated and will be removed in future versions. Use commit_properties instead.
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
        if custom_metadata:
            warnings.warn(
                "custom_metadata is deprecated, please use commit_properties instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            commit_properties = _commit_properties_from_custom_metadata(
                commit_properties, custom_metadata
            )

        if len(constraints.keys()) > 1:
            raise ValueError(
                """add_constraints is limited to a single constraint addition at once for now. 
                Please execute add_constraints multiple times with each time a different constraint."""
            )

        self.table._table.add_constraints(
            constraints,
            commit_properties,
            post_commithook_properties,
        )

    def drop_constraint(
        self,
        name: str,
        raise_if_not_exists: bool = True,
        custom_metadata: Optional[Dict[str, str]] = None,
        post_commithook_properties: Optional[PostCommitHookProperties] = None,
        commit_properties: Optional[CommitProperties] = None,
    ) -> None:
        """
        Drop constraints from a table. Limited to `single constraint` at once.

        Args:
            name: constraint name which to drop.
            raise_if_not_exists: set if should raise if not exists.
            custom_metadata: Deprecated and will be removed in future versions. Use commit_properties instead.
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
        if custom_metadata:
            warnings.warn(
                "custom_metadata is deprecated, please use commit_properties instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            commit_properties = _commit_properties_from_custom_metadata(
                commit_properties, custom_metadata
            )

        self.table._table.drop_constraints(
            name,
            raise_if_not_exists,
            commit_properties,
            post_commithook_properties,
        )

    def set_table_properties(
        self,
        properties: Dict[str, str],
        raise_if_not_exists: bool = True,
        custom_metadata: Optional[Dict[str, str]] = None,
        commit_properties: Optional[CommitProperties] = None,
    ) -> None:
        """
        Set properties from the table.

        Args:
            properties: properties which set
            raise_if_not_exists: set if should raise if not exists.
            custom_metadata: Deprecated and will be removed in future versions. Use commit_properties instead.
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
        if custom_metadata:
            warnings.warn(
                "custom_metadata is deprecated, please use commit_properties instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            commit_properties = _commit_properties_from_custom_metadata(
                commit_properties, custom_metadata
            )

        self.table._table.set_table_properties(
            properties,
            raise_if_not_exists,
            commit_properties,
        )


class TableOptimizer:
    """API for various table optimization commands."""

    def __init__(self, table: DeltaTable):
        self.table = table

    def compact(
        self,
        partition_filters: Optional[FilterConjunctionType] = None,
        target_size: Optional[int] = None,
        max_concurrent_tasks: Optional[int] = None,
        min_commit_interval: Optional[Union[int, timedelta]] = None,
        writer_properties: Optional[WriterProperties] = None,
        custom_metadata: Optional[Dict[str, str]] = None,
        post_commithook_properties: Optional[PostCommitHookProperties] = None,
        commit_properties: Optional[CommitProperties] = None,
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
            writer_properties: Pass writer properties to the Rust parquet writer.
            custom_metadata: Deprecated and will be removed in future versions. Use commit_properties instead.
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
        if custom_metadata:
            warnings.warn(
                "custom_metadata is deprecated, please use commit_properties instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            commit_properties = _commit_properties_from_custom_metadata(
                commit_properties, custom_metadata
            )

        if isinstance(min_commit_interval, timedelta):
            min_commit_interval = int(min_commit_interval.total_seconds())

        metrics = self.table._table.compact_optimize(
            self.table._stringify_partition_values(partition_filters),
            target_size,
            max_concurrent_tasks,
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
        partition_filters: Optional[FilterConjunctionType] = None,
        target_size: Optional[int] = None,
        max_concurrent_tasks: Optional[int] = None,
        max_spill_size: int = 20 * 1024 * 1024 * 1024,
        min_commit_interval: Optional[Union[int, timedelta]] = None,
        writer_properties: Optional[WriterProperties] = None,
        custom_metadata: Optional[Dict[str, str]] = None,
        post_commithook_properties: Optional[PostCommitHookProperties] = None,
        commit_properties: Optional[CommitProperties] = None,
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
            max_spill_size: the maximum number of bytes allowed in memory before spilling to disk. Defaults to 20GB.
            min_commit_interval: minimum interval in seconds or as timedeltas before a new commit is
                                    created. Interval is useful for long running executions. Set to 0 or timedelta(0), if you
                                    want a commit per partition.
            writer_properties: Pass writer properties to the Rust parquet writer.
            custom_metadata: Deprecated and will be removed in future versions. Use commit_properties instead.
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
        if custom_metadata:
            warnings.warn(
                "custom_metadata is deprecated, please use commit_properties instead.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            commit_properties = _commit_properties_from_custom_metadata(
                commit_properties, custom_metadata
            )

        if isinstance(min_commit_interval, timedelta):
            min_commit_interval = int(min_commit_interval.total_seconds())

        metrics = self.table._table.z_order_optimize(
            list(columns),
            self.table._stringify_partition_values(partition_filters),
            target_size,
            max_concurrent_tasks,
            max_spill_size,
            min_commit_interval,
            writer_properties,
            commit_properties,
            post_commithook_properties,
        )
        self.table.update_incremental()
        return json.loads(metrics)
