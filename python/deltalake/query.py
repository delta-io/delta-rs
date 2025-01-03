from __future__ import annotations

import logging
import warnings
from typing import List

import pyarrow

from deltalake._internal import PyQueryBuilder
from deltalake.table import DeltaTable
from deltalake.warnings import ExperimentalWarning

logger = logging.getLogger(__name__)


class QueryBuilder:
    """
    QueryBuilder is an experimental API which exposes Apache DataFusion SQL to Python users of the deltalake library.

    This API is subject to change.

    >>> qb = QueryBuilder()
    """

    def __init__(self) -> None:
        warnings.warn(
            "QueryBuilder is experimental and subject to change",
            category=ExperimentalWarning,
        )
        self._query_builder = PyQueryBuilder()
        self._print_output = False

    def register(self, table_name: str, delta_table: DeltaTable) -> QueryBuilder:
        """
        Add a table to the query builder instance by name. The `table_name`
        will be how the referenced `DeltaTable` can be referenced in SQL
        queries.

        For example:

        >>> tmp = getfixture('tmp_path')
        >>> import pyarrow as pa
        >>> from deltalake import DeltaTable, QueryBuilder
        >>> dt = DeltaTable.create(table_uri=tmp, schema=pa.schema([pa.field('name', pa.string())]))
        >>> qb = QueryBuilder().register('test', dt)
        >>> assert qb is not None
        """
        self._query_builder.register(
            table_name=table_name,
            delta_table=delta_table._table,
        )
        return self

    def execute(self, sql: str) -> List[pyarrow.RecordBatch]:
        """
        Execute the query and return a list of record batches

        For example:

        >>> tmp = getfixture('tmp_path')
        >>> import pyarrow as pa
        >>> from deltalake import DeltaTable, QueryBuilder
        >>> dt = DeltaTable.create(table_uri=tmp, schema=pa.schema([pa.field('name', pa.string())]))
        >>> qb = QueryBuilder().register('test', dt)
        >>> results = qb.execute('SELECT * FROM test')
        >>> assert results is not None
        """
        records = self._query_builder.execute(sql)
        if self._print_output:
            if len(records) > 0:
                print(pyarrow.Table.from_batches(records))
            else:
                logger.info("The executed query contains no records.")

        return records

    def show(self, print_output: bool = True) -> QueryBuilder:
        """
        Controls whether succeeding query outputs would be printed in the console.

        For example:

        >>> tmp = getfixture('tmp_path')
        >>> import pyarrow as pa
        >>> from deltalake import DeltaTable, QueryBuilder
        >>> dt = DeltaTable.create(table_uri=tmp, schema=pa.schema([pa.field('name', pa.string())]))
        >>> qb = QueryBuilder().register('test', dt)
        >>> results = qb.show().execute('SELECT * FROM test')

        """
        self._print_output = print_output
        return self

    def sql(self, sql: str) -> List[pyarrow.RecordBatch]:
        """
        Convenience method for `execute()` method

        For example:

        >>> tmp = getfixture('tmp_path')
        >>> import pyarrow as pa
        >>> from deltalake import DeltaTable, QueryBuilder
        >>> dt = DeltaTable.create(table_uri=tmp, schema=pa.schema([pa.field('name', pa.string())]))
        >>> qb = QueryBuilder().register('test', dt)
        >>> query = 'SELECT * FROM test'
        >>> assert qb.execute(query) == qb.sql(query)

        """
        return self.execute(sql)
