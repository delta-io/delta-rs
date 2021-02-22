import pyarrow

from typing import List
from pyarrow.dataset import dataset
from urllib.parse import urlparse

from .deltalake import RawDeltaTable, rust_core_version
from deltalake.schema import (
    DataType,
    DeltaTableSchema,
    DeltaTableField,
    DeltaTableSchemaFormat,
)


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

    def schema(
        self, format: DeltaTableSchemaFormat = DeltaTableSchemaFormat.DELTA
    ) -> DeltaTableSchema:
        fields = [
            DeltaTableField(
                name=field.name,
                type=field.rtype,
                metadata=field.metadata,
                nullable=field.nullable,
                format=format,
            )
            for field in self._table.schema(format=format.value).schema
        ]
        return DeltaTableSchema(fields=fields)

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

    def to_pyarrow_schema(self) -> pyarrow.Schema:
        format = DeltaTableSchemaFormat.ARROW
        delta_fields = [
            DeltaTableField(
                name=field.name,
                type=field.rtype,
                metadata=field.metadata,
                nullable=field.nullable,
                format=format,
            )
            for field in self._table.schema(format=format.value).schema
        ]
        arrow_fields = [pyarrow.field(field.name, field.type) for field in delta_fields]
        return pyarrow.schema(arrow_fields)
