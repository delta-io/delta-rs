from __future__ import annotations

import logging
import warnings

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

    def execute(self, sql: str) -> QueryResult:
        """
        Prepares the sql query to be executed.

        For example:

        >>> tmp = getfixture('tmp_path')
        >>> import pyarrow as pa
        >>> from deltalake import DeltaTable, QueryBuilder
        >>> dt = DeltaTable.create(table_uri=tmp, schema=pa.schema([pa.field('name', pa.string())]))
        >>> qb = QueryBuilder().register('test', dt)
        >>> results = qb.execute('SELECT * FROM test')
        >>> assert isinstance(results, QueryResult)
        """
        return QueryResult(self._query_builder, sql)

    def sql(self, sql: str) -> QueryResult:
        """
        Convenience method for `execute()` method.

        For example:

        >>> tmp = getfixture('tmp_path')
        >>> import pyarrow as pa
        >>> from deltalake import DeltaTable, QueryBuilder
        >>> dt = DeltaTable.create(table_uri=tmp, schema=pa.schema([pa.field('name', pa.string())]))
        >>> qb = QueryBuilder().register('test', dt)
        >>> query = 'SELECT * FROM test'
        >>> assert qb.execute(query).fetchall() == qb.sql(query).fetchall()
        """
        return self.execute(sql)


class QueryResult:
    def __init__(self, query_builder: PyQueryBuilder, sql: str) -> None:
        self._query_builder = query_builder
        self._sql_query = sql

    def show(self) -> None:
        """
        Execute the query and prints the output in the console.

        For example:

        >>> tmp = getfixture('tmp_path')
        >>> import pyarrow as pa
        >>> from deltalake import DeltaTable, QueryBuilder
        >>> dt = DeltaTable.create(table_uri=tmp, schema=pa.schema([pa.field('name', pa.string())]))
        >>> qb = QueryBuilder().register('test', dt)
        >>> results = qb.execute('SELECT * FROM test').show()
        """
        records = self.fetchall()
        if len(records) > 0:
            print(pyarrow.Table.from_batches(records))
        else:
            logger.info("The executed query contains no records.")

    def fetchall(self) -> list[pyarrow.RecordBatch]:
        """
        Execute the query and return a list of record batches.

        >>> tmp = getfixture('tmp_path')
        >>> import pyarrow as pa
        >>> from deltalake import DeltaTable, QueryBuilder
        >>> dt = DeltaTable.create(table_uri=tmp, schema=pa.schema([pa.field('name', pa.string())]))
        >>> qb = QueryBuilder().register('test', dt)
        >>> results = qb.execute('SELECT * FROM test').fetchall()
        >>> assert results is not None
        """
        return self._query_builder.execute(self._sql_query)
