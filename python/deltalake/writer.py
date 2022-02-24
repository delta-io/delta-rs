from typing import Iterator, Union
from deltalake import DeltaTable
import pyarrow as pa


class DeltaTableWriter:
    def __init__(self, table: DeltaTable):
        self.table = table

    def append(self, batches: Iterator[pa.RecordBatch]):
        pass

    def overwrite(self, batches: Iterator[pa.RecordBatch]):
        pass

    def delete(self, where):
        pass

    def update(self, on, batches: Iterator[pa.RecordBatch]):
        pass

    # TODO: merge


def create_empty_table(uri: str, schema: pa.Schema, backend: str = 'pyarrow'):
    pass


def write_deltalake(
    table: Union[str, DeltaTable],
    data,
    mode: Union['append', 'overwrite'] = 'append',
    backend: str = 'pyarrow'
):
    pass
