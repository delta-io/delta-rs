from __future__ import annotations

from collections.abc import Mapping

from deltalake._internal import RawDeltaTable
from deltalake.fs._base_handler import BaseDeltaStorageHandler

try:
    import pyarrow as pa
    from pyarrow.fs import FileSystemHandler

    PYARROW_AVAILABLE = True
except ImportError as e:
    if "pyarrow" in str(e):
        PYARROW_AVAILABLE = False
    else:
        raise


# NOTE  we need to inherit form FileSystemHandler to pass pyarrow's internal type checks.
if PYARROW_AVAILABLE:

    class DeltaStorageHandler(BaseDeltaStorageHandler, FileSystemHandler):
        """
        DeltaStorageHandler is a concrete implementations of a PyArrow FileSystemHandler.
        """

        def open_input_file(self, path: str) -> pa.PythonFile:
            """
            Open an input file for random access reading.

            Args:
                path: The source to open for reading.

            Returns:
                NativeFile
            """
            return pa.PythonFile(self._handler.open_input_file(path))

        def open_input_stream(self, path: str) -> pa.PythonFile:
            """
            Open an input stream for sequential reading.

            Args:
                path: The source to open for reading.

            Returns:
                NativeFile
            """
            return pa.PythonFile(self._handler.open_input_file(path))

        def open_output_stream(
            self, path: str, metadata: dict[str, str] | None = None
        ) -> pa.PythonFile:
            """
            Open an output stream for sequential writing.

            If the target already exists, existing data is truncated.

            Args:
                path: The source to open for writing.
                metadata: If not None, a mapping of string keys to string values.

            Returns:
                NativeFile
            """
            return pa.PythonFile(self._handler.open_output_stream(path, metadata))

        def open_append_stream(self, path: str, metadata: Mapping[str, str]) -> None:
            raise NotImplementedError
else:

    class DeltaStorageHandler(BaseDeltaStorageHandler):  # type: ignore[no-redef]
        """
        DeltaStorageHandler is a concrete implementations of a PyArrow FileSystemHandler.
        """

        def __init__(
            self,
            table_uri: str,
            options: dict[str, str] | None = None,
            known_sizes: dict[str, int] | None = None,
        ) -> None:
            raise ImportError(
                "DeltaStorageHandler requires pyarrow. Please install deltalake[pyarrow] to use this class."
            )

        @classmethod
        def from_table(
            cls,
            table: RawDeltaTable,
            options: dict[str, str] | None = None,
            known_sizes: dict[str, int] | None = None,
        ) -> "DeltaStorageHandler":
            raise ImportError(
                "DeltaStorageHandler requires pyarrow. Please install deltalake[pyarrow] to use this class."
            )
