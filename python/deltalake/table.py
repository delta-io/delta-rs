import os
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional, Tuple
from urllib.parse import urlparse

import pyarrow
from pyarrow.dataset import dataset, partitioning

if TYPE_CHECKING:
    import pandas

from .deltalake import RawDeltaTable, RawDeltaTableMetaData
from .schema import Schema, pyarrow_schema_from_json


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
    def configuration(self) -> List[str]:
        """Return the DeltaTable properties."""
        return self._metadata.configuration

    def __str__(self) -> str:
        return (
            f"Metadata(id: {self._metadata.id}, name: {self._metadata.name}, "
            f"description: {self._metadata.description}, partitionColumns: {self._metadata.partition_columns}, "
            f"created_time: {self.created_time}, configuration={self._metadata.configuration})"
        )


@dataclass(init=False)
class DeltaTable:
    """Create a DeltaTable instance."""

    def __init__(self, table_uri: str, version: Optional[int] = None):
        """
        Create the Delta Table from a path with an optional version.
        Multiple StorageBackends are currently supported: AWS S3, Azure Data Lake Storage Gen2 and local URI.

        :param table_uri: the path of the DeltaTable
        :param version: version of the DeltaTable
        """
        self._table = RawDeltaTable(table_uri, version=version)
        self._metadata = Metadata(self._table)

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
        The supported type for value is str.

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

    def schema(self) -> Schema:
        """
        Get the current schema of the DeltaTable.

        :return: the current Schema registered in the transaction log
        """
        return Schema.from_json(self._table.schema_json())

    def metadata(self) -> Metadata:
        """
        Get the current metadata of the DeltaTable.

        :return: the current Metadata registered in the transaction log
        """
        return self._metadata

    def vacuum(self, retention_hours: int, dry_run: bool = True) -> List[str]:
        """
        Run the Vacuum command on the Delta Table: list and delete files no longer referenced by the Delta table and are older than the retention threshold.

        :param retention_hours: the retention threshold in hours
        :param dry_run: when activated, list only the files, delete otherwise
        :return: the list of files no longer referenced by the Delta Table and are older than the retention threshold.
        """
        if retention_hours < 0:
            raise ValueError("The retention periods should be positive.")

        return self._table.vacuum(dry_run, retention_hours)

    def pyarrow_schema(self) -> pyarrow.Schema:
        """
        Get the current schema of the DeltaTable with the Parquet PyArrow format.

        :return: the current Schema with the Parquet PyArrow format
        """
        return pyarrow_schema_from_json(self._table.arrow_schema_json())

    def to_pyarrow_dataset(
        self, partitions: Optional[List[Tuple[str, str, Any]]] = None
    ) -> pyarrow.dataset.Dataset:
        """
        Build a PyArrow Dataset using data from the DeltaTable.

        :param partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
        :return: the PyArrow dataset in PyArrow
        """
        if partitions is None:
            file_paths = self._table.file_uris()
        else:
            file_paths = self._table.files_by_partitions(partitions)
        paths = [urlparse(curr_file) for curr_file in file_paths]

        empty_delta_table = len(paths) == 0
        if empty_delta_table:
            return dataset(
                [],
                schema=self.pyarrow_schema(),
                partitioning=partitioning(flavor="hive"),
            )

        # Decide based on the first file, if the file is on cloud storage or local
        if paths[0].netloc:
            query_str = ""
            # pyarrow doesn't properly support the AWS_ENDPOINT_URL environment variable
            # for non-AWS S3 like resources. This is a slight hack until such a
            # point when pyarrow learns about AWS_ENDPOINT_URL
            endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
            if endpoint_url is not None:
                endpoint = urlparse(endpoint_url)
                # This format specific to the URL schema inference done inside
                # of pyarrow, consult their tests/dataset.py for examples
                query_str += (
                    f"?scheme={endpoint.scheme}&endpoint_override={endpoint.netloc}"
                )

            keys = [curr_file.path for curr_file in paths]
            return dataset(
                keys,
                schema=self.pyarrow_schema(),
                filesystem=f"{paths[0].scheme}://{paths[0].netloc}{query_str}",
                partitioning=partitioning(flavor="hive"),
            )
        else:
            return dataset(
                file_paths,
                schema=self.pyarrow_schema(),
                format="parquet",
                partitioning=partitioning(flavor="hive"),
            )

    def to_pyarrow_table(
        self,
        partitions: Optional[List[Tuple[str, str, Any]]] = None,
        columns: Optional[List[str]] = None,
    ) -> pyarrow.Table:
        """
        Build a PyArrow Table using data from the DeltaTable.

        :param partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
        :param columns: The columns to project. This can be a list of column names to include (order and duplicates will be preserved)
        :return: the PyArrow table
        """
        return self.to_pyarrow_dataset(partitions).to_table(columns=columns)

    def to_pandas(
        self,
        partitions: Optional[List[Tuple[str, str, Any]]] = None,
        columns: Optional[List[str]] = None,
    ) -> "pandas.DataFrame":
        """
        Build a pandas dataframe using data from the DeltaTable.

        :param partitions: A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax
        :param columns: The columns to project. This can be a list of column names to include (order and duplicates will be preserved)
        :return: a pandas dataframe
        """
        return self.to_pyarrow_table(partitions, columns).to_pandas()

    def update_incremental(self) -> None:
        """
        Updates the DeltaTable to the latest version by incrementally applying
        newer versions.
        """
        self._table.update_incremental()
