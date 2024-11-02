from __future__ import annotations

import warnings
from typing import List

import pyarrow

from deltalake._internal import PyQueryBuilder
from deltalake.table import DeltaTable
from deltalake.warnings import ExperimentalWarning


class QueryBuilder:
    def __init__(self) -> None:
        warnings.warn(
            "QueryBuilder is experimental and subject to change",
            category=ExperimentalWarning,
        )
        self._query_builder = PyQueryBuilder()

    def register(self, table_name: str, delta_table: DeltaTable) -> QueryBuilder:
        """Add a table to the query builder."""
        self._query_builder.register(
            table_name=table_name,
            delta_table=delta_table._table,
        )
        return self

    def execute(self, sql: str) -> List[pyarrow.RecordBatch]:
        """Execute the query and return a list of record batches."""
        return self._query_builder.execute(sql)
