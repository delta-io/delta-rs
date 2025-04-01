from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from deltalake.exceptions import TableNotFoundError

if TYPE_CHECKING:
    from deltalake.table import DeltaTable


def try_get_table_and_table_uri(
    table_or_uri: str | Path | DeltaTable,
    storage_options: dict[str, str] | None = None,
) -> tuple[DeltaTable | None, str]:
    """Parses the `table_or_uri`.
    Raises [ValueError] If `table_or_uri` is not of type str, Path or DeltaTable.

    Args:
        table_or_uri: URI of a table or a DeltaTable object.
        storage_options: Options passed to the native delta filesystem.

    Returns:
        (DeltaTable object, URI of the table)
    """
    from deltalake.table import DeltaTable

    if not isinstance(table_or_uri, (str, Path, DeltaTable)):
        raise ValueError("table_or_uri must be a str, Path or DeltaTable")

    if isinstance(table_or_uri, (str, Path)):
        table = try_get_deltatable(table_or_uri, storage_options)
        table_uri = str(table_or_uri)
    else:
        table = table_or_uri
        table_uri = table._table.table_uri()

    return (table, table_uri)


def try_get_deltatable(
    table_uri: str | Path, storage_options: dict[str, str] | None
) -> DeltaTable | None:
    from deltalake.table import DeltaTable

    try:
        return DeltaTable(table_uri, storage_options=storage_options)
    except TableNotFoundError:
        return None
