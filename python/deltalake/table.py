from typing import List, Optional, Tuple
from urllib.parse import urlparse

import pyarrow
from pyarrow.dataset import dataset

from .deltalake import RawDeltaTable
from .schema import Schema, pyarrow_schema_from_json


class DeltaTable:
    def __init__(self, table_path: str, version: Optional[int] = None):
        self._table = RawDeltaTable(table_path, version=version)

    def version(self) -> int:
        return self._table.version()

    def files(self) -> List[str]:
        return self._table.files()

    def files_by_partitions(self, partition_filters: List[Tuple]) -> List[str]:
        """
        Partitions which do not match the filter predicate will be removed from scanned data.
        Predicates are expressed in disjunctive normal form (DNF), like [("x", "=", "a"), ...].
        DNF allows arbitrary boolean logical combinations of single partition predicates.
        The innermost tuples each describe a single partition predicate.
        The list of inner predicates is interpreted as a conjunction (AND), forming a more selective and multiple
        partition predicates.
        Each tuple has format: (key, op, value) and compares the key with the value.
        The supported op are: =, !=, in and not in.
        If the op is in or not in, the value must be a collection such as a list, a set or a tuple.
        The supported type for value is str.

        Examples:
        ("x", "=", "a")
        ("x", "!=", "a")
        ("y", "in", ["a", "b", "c"])
        ("z", "not in", ["a","b"])
        """
        return self._table.files(partition_filters)

    def file_paths(self) -> List[str]:
        return self._table.file_paths()

    def load_version(self, version: int) -> None:
        self._table.load_version(version)

    def schema(self) -> Schema:
        return Schema.from_json(self._table.schema_json())

    def pyarrow_schema(self) -> pyarrow.Schema:
        return pyarrow_schema_from_json(self._table.arrow_schema_json())

    def to_pyarrow_dataset(self) -> pyarrow.dataset.Dataset:
        file_paths = self._table.file_paths()
        paths = [urlparse(curr_file) for curr_file in file_paths]

        # Decide based on the first file, if the file is on cloud storage or local
        if paths[0].netloc:
            keys = [curr_file.path for curr_file in paths]
            return dataset(keys, filesystem=f"{paths[0].scheme}://{paths[0].netloc}")
        else:
            return dataset(file_paths, format="parquet")

    def to_pyarrow_table(self) -> pyarrow.Table:
        return self.to_pyarrow_dataset().to_table()
