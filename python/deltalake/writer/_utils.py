from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional, Tuple, Union

from deltalake.exceptions import TableNotFoundError

if TYPE_CHECKING:
    from deltalake.table import DeltaTable


def try_get_table_and_table_uri(
    table_or_uri: Union[str, Path, "DeltaTable"],
    storage_options: Optional[Dict[str, str]] = None,
) -> Tuple[Optional["DeltaTable"], str]:
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
    table_uri: Union[str, Path], storage_options: Optional[Dict[str, str]]
) -> Optional["DeltaTable"]:
    from deltalake.table import DeltaTable

    try:
        return DeltaTable(table_uri, storage_options=storage_options)
    except TableNotFoundError:
        return None
