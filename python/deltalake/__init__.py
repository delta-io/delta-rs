import atexit
from typing import Optional

from deltalake._internal import (
    TableFeatures,
    Transaction,
    __version__,
    rust_core_version,
    _NANOSECOND_TIMESTAMPS as _INTERNAL_NANOSECOND_TIMESTAMPS,
)
from deltalake import table
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


def init_tracing(endpoint: Optional[str] = None) -> None:
    """
    Initialize OpenTelemetry tracing for delta-rs operations.

    Args:
        endpoint: The OTLP HTTP endpoint URL. If not provided, uses the
            OTEL_EXPORTER_OTLP_ENDPOINT environment variable or defaults to
            "http://localhost:4318/v1/traces"

    Raises:
        RuntimeError: If tracing initialization fails

    Note:
        - Tracing will be automatically shut down when the Python process exits
        - Use OTEL_EXPORTER_OTLP_HEADERS environment variable for authentication
          (format: "key1=value1,key2=value2")
        - Use RUST_LOG environment variable to control log level filtering
          (e.g., "info", "debug", "deltalake=debug")

    Example:
        Basic usage with defaults:
        ```python
        import deltalake

        deltalake.init_tracing()
        deltalake.write_deltalake("my_table", data)
        dt = deltalake.DeltaTable("my_table")
        ```

        Using custom endpoint:
        ```python
        import deltalake

        deltalake.init_tracing(endpoint="http://localhost:4318/v1/traces")
        ```

        Using environment variables for authentication:
        ```python
        import os
        import deltalake

        os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "https://api.honeycomb.io/v1/traces"
        os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = "x-honeycomb-team=your-api-key"
        os.environ["RUST_LOG"] = "deltalake=debug"
        deltalake.init_tracing()
        ```
    """
    _init_tracing(endpoint)
    atexit.register(_shutdown_tracing)


# Support nanosecond timestamps:
_NANOSECOND_TIMESTAMPS: bool = False


def enable_nanosecond_timestamps() -> None:
    """
    Enable experimental support for nanosecond timestamp primitive data types.
    """
    if not _INTERNAL_NANOSECOND_TIMESTAMPS:
        raise RuntimeError(
            "Extension wasn't compiled with nanosecond-timestamps Cargo feature"
        )

    if _nanosecond_timestamps_enabled():
        # Already called, no work necessary.
        return

    global _NANOSECOND_TIMESTAMPS
    _NANOSECOND_TIMESTAMPS = True
    table.SUPPORTED_WRITER_FEATURES.add("timestampNanos")
    table.SUPPORTED_READER_FEATURES.add("timestampNanos")

    from deltalake._internal import _set_cast_nanos_timestamps_to_micros

    _set_cast_nanos_timestamps_to_micros(False)


def _disable_nanosecond_timestamps() -> None:
    """
    Disable nanosecond timestamps, for unit tests.
    """
    if not _nanosecond_timestamps_enabled():
        return

    global _NANOSECOND_TIMESTAMPS
    _NANOSECOND_TIMESTAMPS = False
    table.SUPPORTED_WRITER_FEATURES.remove("timestampNanos")
    table.SUPPORTED_READER_FEATURES.remove("timestampNanos")

    from deltalake._internal import _set_cast_nanos_timestamps_to_micros

    _set_cast_nanos_timestamps_to_micros(True)


def _nanosecond_timestamps_enabled() -> bool:
    """
    Return whether nanosecond timestamps are enabled.
    """
    return _NANOSECOND_TIMESTAMPS


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
    "enable_nanosecond_timestamps",
    "init_tracing",
    "rust_core_version",
    "write_deltalake",
]
