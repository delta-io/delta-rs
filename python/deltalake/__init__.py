from typing import List

import pyarrow
from pyarrow.dataset import dataset

from .deltalake import RawDeltaTable


class DeltaTable():
    def __init__(self, table_path: str):
        self._table = RawDeltaTable(table_path)

    def files(self, full_path: bool = False) -> List[str]:
        return self._table.files(full_path=full_path)

    def to_pyarrow_dataset(self) -> pyarrow.dataset.Dataset:
        return dataset(self._table.files(full_path=True), format="parquet")

    def to_pyarrow_table(self) -> pyarrow.Table:
        return self.to_pyarrow_dataset().to_table()
