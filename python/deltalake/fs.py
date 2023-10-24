from typing import Dict, List, Optional

import pyarrow as pa
from pyarrow.fs import FileInfo, FileSelector, FileSystemHandler

from ._internal import DeltaFileSystemHandler


# NOTE  we need to inherit form FileSystemHandler to pass pyarrow's internal type checks.
class DeltaStorageHandler(DeltaFileSystemHandler, FileSystemHandler):
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
        return pa.PythonFile(DeltaFileSystemHandler.open_input_file(self, path))

    def open_input_stream(self, path: str) -> pa.PythonFile:
        """
        Open an input stream for sequential reading.

        Args:
            path: The source to open for reading.

        Returns:
            NativeFile
        """
        return pa.PythonFile(DeltaFileSystemHandler.open_input_file(self, path))

    def open_output_stream(
        self, path: str, metadata: Optional[Dict[str, str]] = None
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
        return pa.PythonFile(
            DeltaFileSystemHandler.open_output_stream(self, path, metadata)
        )

    def get_file_info_selector(self, selector: FileSelector) -> List[FileInfo]:  # type: ignore
        """
        Get info for the files defined by FileSelector.

        Args:
            selector: FileSelector object

        Returns:
            list of file info objects
        """
        return DeltaFileSystemHandler.get_file_info_selector(
            self, selector.base_dir, selector.allow_not_found, selector.recursive
        )
