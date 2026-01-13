from __future__ import annotations

import logging

from arro3.core import RecordBatchReader

from deltalake._internal import PyQueryBuilder
from deltalake.table import DeltaTable

logger = logging.getLogger(__name__)


class QueryBuilder:
    """
    QueryBuilder is an API which exposes Apache DataFusion SQL to Python users of the deltalake library.

    >>> qb = QueryBuilder()
    """

    def __init__(self) -> None:
        self._query_builder = PyQueryBuilder()

    def register(self, table_name: str, delta_table: DeltaTable) -> QueryBuilder:
        """
        Add a table to the query builder instance by name. The `table_name`
        will be how the referenced `DeltaTable` can be referenced in SQL
        queries.

        For example:

        ```python
        from deltalake import DeltaTable, QueryBuilder
        dt = DeltaTable("my_table")
        qb = QueryBuilder().register('test', dt)
        ```
        """
        self._query_builder.register(
            table_name=table_name,
            delta_table=delta_table._table,
        )
        return self

    def execute(self, sql: str) -> RecordBatchReader:
        """
        Prepares the sql query to be executed.

        For example:
        ```python
        from deltalake import DeltaTable, QueryBuilder
        dt = DeltaTable("my_table")
        data = QueryBuilder().register('test', dt).execute("select * from test").read_all()
        ```
        """
        return self._query_builder.execute(sql)
