from typing import List, Optional, Tuple
from urllib.parse import urlparse

import pyarrow
from pyarrow.dataset import dataset

from .deltalake import RawDeltaTable, RawDeltaTableMetaData
from .schema import Schema, pyarrow_schema_from_json


class Metadata:
    """ Create a Metadata instance. """

    def __init__(self, table: RawDeltaTable):
        self._metadata = table.metadata()

    @property
    def id(self):
        """ Return the unique identifier of the DeltaTable. """
        return self._metadata.id

    @property
    def name(self):
        """ Return the user-provided identifier of the DeltaTable. """
        return self._metadata.name

    @property
    def description(self):
        """ Return the user-provided description of the DeltaTable. """
        return self._metadata.description

    @property
    def configuration(self):
        """ Return the DeltaTable properties. """
        return self._metadata.configuration

    @property
    def partition_columns(self):
        """ Return an array containing the names of the partitioned columns of the DeltaTable. """
        return self._metadata.partition_columns

    def __str__(self) -> str:
        return (
            f"Metadata(id: {self._metadata.id}, name: {self._metadata.name}, "
            f"description: {self._metadata.description}, partitionColumns: {self._metadata.partition_columns}, "
            f"configuration={self._metadata.configuration})"
        )

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other: "Metadata") -> bool:
        return (
            isinstance(other, Metadata)
            and self._metadata.id == other._metadata.id
            and self._metadata.name == other._metadata.name
            and self._metadata.description == other._metadata.description
            and self._metadata.configuration == other._metadata.configuration
            and self._metadata.partition_columns == other._metadata.partition_columns
        )


class DeltaTable:
    """ Create a DeltaTable instance."""

    def __init__(self, table_path: str, version: Optional[int] = None):
        """
        Create the Delta Table from a path with an optional version.
        Multiple StorageBackends are currently supported: AWS S3, Azure Data Lake Storage Gen2 and local URI.

        :param table_path: the path of the DeltaTable
        :param version: version of the DeltaTable
        """
        self._table = RawDeltaTable(table_path, version=version)
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

    def files_by_partitions(self, partition_filters: List[Tuple]) -> List[str]:
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

        :return: list of the .parquet files with an absolute path referenced for the current version of the DeltaTable
        """
        return self._table.file_paths()

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

    def pyarrow_schema(self) -> pyarrow.Schema:
        """
        Get the current schema of the DeltaTable with the Parquet PyArrow format.

        :return: the current Schema with the Parquet PyArrow format
        """
        return pyarrow_schema_from_json(self._table.arrow_schema_json())

    def to_pyarrow_dataset(self) -> pyarrow.dataset.Dataset:
        """
        Build a PyArrow Dataset using data from the DeltaTable.

        :return: the PyArrow dataset in PyArrow
        """
        file_paths = self._table.file_paths()
        paths = [urlparse(curr_file) for curr_file in file_paths]

        # Decide based on the first file, if the file is on cloud storage or local
        if paths[0].netloc:
            keys = [curr_file.path for curr_file in paths]
            return dataset(
                keys,
                schema=self.pyarrow_schema(),
                filesystem=f"{paths[0].scheme}://{paths[0].netloc}",
            )
        else:
            return dataset(file_paths, schema=self.pyarrow_schema(), format="parquet")

    def to_pyarrow_table(self) -> pyarrow.Table:
        """
        Build a PyArrow Table using data from the DeltaTable.

        :return: the PyArrow table
        """
        return self.to_pyarrow_dataset().to_table()
