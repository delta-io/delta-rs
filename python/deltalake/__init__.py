from typing import List

import pyarrow
from pyarrow.dataset import dataset
from urllib.parse import urlparse

from .deltalake import RawDeltaTable, rust_core_version


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

    def to_pyarrow_dataset(self) -> pyarrow.dataset.Dataset:
        paths = [urlparse(curr_file) for curr_file in self._table.file_paths()]

        # Decide based on the first file, if the file is on cloud storage or local
        if paths[0].netloc:
            keys = [curr_file.path for curr_file in paths]
            return dataset.dataset(
                keys, filesystem=paths[0].scheme + "://" + paths[0].netloc
            )
        else:
            return dataset.dataset(self._table.file_paths(), format="parquet")

    def to_pyarrow_table(self) -> pyarrow.Table:
        return self.to_pyarrow_dataset().to_table()
