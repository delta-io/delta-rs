import atexit

from deltalake._internal import (
    TableFeatures,
    Transaction,
    __version__,
    rust_core_version,
)
from deltalake._internal import (
    init_tracing as _init_tracing,
)
from deltalake._internal import (
    shutdown_tracing as _shutdown_tracing,
)
from deltalake.query import QueryBuilder
from deltalake.schema import DataType, Field, Schema
from deltalake.table import DeltaTable, Metadata
from deltalake.transaction import CommitProperties, PostCommitHookProperties
from deltalake.writer import (
    BloomFilterProperties,
    ColumnProperties,
    WriterProperties,
    convert_to_deltalake,
    write_deltalake,
)


def init_tracing(otel_endpoint: str = "http://localhost:4317") -> None:
    """
    Initialize OpenTelemetry tracing for delta-rs operations.

    Args:
        otel_endpoint: The OTLP gRPC endpoint URL (default: http://localhost:4317)

    Raises:
        RuntimeError: If tracing initialization fails

    Example:
        ```python
        import os
        import deltalake

        os.environ["RUST_LOG"] = "deltalake=debug"

        deltalake.init_tracing("http://localhost:4317")

        deltalake.write_deltalake("my_table", data)
        dt = deltalake.DeltaTable("my_table")

        deltalake.shutdown_tracing()
        ```
    """
    _init_tracing(otel_endpoint)
    atexit.register(_shutdown_tracing)


def shutdown_tracing() -> None:
    """Shutdown OpenTelemetry tracing and flush remaining spans."""
    _shutdown_tracing()


__all__ = [
    "BloomFilterProperties",
    "ColumnProperties",
    "CommitProperties",
    "DataType",
    "DeltaTable",
    "Field",
    "Metadata",
    "PostCommitHookProperties",
    "QueryBuilder",
    "Schema",
    "TableFeatures",
    "Transaction",
    "WriterProperties",
    "__version__",
    "convert_to_deltalake",
    "init_tracing",
    "rust_core_version",
    "shutdown_tracing",
    "write_deltalake",
]
