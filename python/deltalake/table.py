from typing import List
from urllib.parse import urlparse

import pyarrow
from pyarrow.dataset import dataset

from .deltalake import RawDeltaTable
from .schema import Schema, pyarrow_schema_from_json, schema_from_json


class DeltaTable:
    def __init__(self, table_path: str):
        self._table = RawDeltaTable(table_path)

    def version(self) -> int:
        return self._table.version()

    def files(self) -> List[str]:
        return self._table.files()

    def file_paths(self) -> List[str]:
        return self._table.file_paths()

    def load_version(self, version: int) -> None:
        self._table.load_version(version)

    def schema(self) -> Schema:
        return schema_from_json(self._table.schema_json())

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
