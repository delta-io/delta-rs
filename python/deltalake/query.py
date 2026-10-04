from __future__ import annotations

import logging

from arro3.core import RecordBatchReader

from deltalake._internal import PyQueryBuilder
from deltalake.table import DeltaTable

logger = logging.getLogger(__name__)


class QueryBuilder:
    """
    QueryBuilder is an API which exposes Apache DataFusion SQL to Python users of the deltalake library.

    ```py
    qb = QueryBuilder()
    ```
    """

    def __init__(self, session_config: dict[str, str] | None = None) -> None:
        """Create a QueryBuilder, optionally overriding the DataFusion session config.

        Example:
            ```py
            from deltalake import QueryBuilder
            qb = QueryBuilder({"datafusion.execution.batch_size": "1024"})
            ```

        Args:
            session_config: [DataFusion configuration settings](https://datafusion.apache.org/user-guide/configs.html)
                applied on top of the defaults deltalake tunes for Delta tables.
        """
        self._query_builder = PyQueryBuilder(session_config)

    def register(self, table_name: str, delta_table: DeltaTable) -> QueryBuilder:
        """Add a table to the query builder instance by name. Table `DeltaTable`
        is available in SQL queries as `table_name`.

        Example:
            ```py
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

        Example:
            ```py
            from deltalake import DeltaTable, QueryBuilder
            dt = DeltaTable("my_table")
            data = QueryBuilder().register('test', dt).execute("select * from test").read_all()
            ```
        """
        return self._query_builder.execute(sql)
