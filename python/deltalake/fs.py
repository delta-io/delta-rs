from typing import Dict, List, Optional

import pyarrow as pa
from pyarrow.fs import FileInfo, FileSelector, FileSystemHandler

from ._internal import DeltaFileSystemHandler


# NOTE  we need to inherit form FileSystemHandler to pass pyarrow's internal type checks.
class DeltaStorageHandler(DeltaFileSystemHandler, FileSystemHandler):
    """
    DeltaStorageHandler is a concrete implementations of a PyArrow FileSystemHandler.
    """

    known_sizes: Dict[str, int] = {}

    def __new__(  # type:ignore
        cls,
        table_uri: str,
        storage_options: Optional[Dict[str, str]] = None,
        known_sizes: Optional[Dict[str, int]] = None,
    ):
        return super().__new__(
            cls, table_uri=table_uri, options=storage_options  # type:ignore
        )

    def __init__(
        self,
        table_uri: str,
        storage_options: Optional[Dict[str, str]] = None,
        known_sizes: Optional[Dict[str, int]] = None,
    ):
        if known_sizes:
            self.known_sizes = known_sizes
        return

    def open_input_file(self, path: str, size: Optional[int] = None) -> pa.PythonFile:
        """
        Open an input file for random access reading.

        :param source: The source to open for reading.
        :return:  NativeFile
        """
        size = self.known_sizes.get(path)
        return pa.PythonFile(DeltaFileSystemHandler.open_input_file(self, path, size))

    def open_input_stream(self, path: str, size: Optional[int] = None) -> pa.PythonFile:
        """
        Open an input stream for sequential reading.

        :param source: The source to open for reading.
        :return:  NativeFile
        """
        size = self.known_sizes.get(path)
        return pa.PythonFile(DeltaFileSystemHandler.open_input_file(self, path, size))

    def open_output_stream(
        self, path: str, metadata: Optional[Dict[str, str]] = None
    ) -> pa.PythonFile:
        """
        Open an output stream for sequential writing.

        If the target already exists, existing data is truncated.

        :param path: The source to open for writing.
        :param metadata: If not None, a mapping of string keys to string values.
        :return:  NativeFile
        """
        return pa.PythonFile(
            DeltaFileSystemHandler.open_output_stream(self, path, metadata)
        )

    def get_file_info_selector(self, selector: FileSelector) -> List[FileInfo]:  # type: ignore
        """
        Get info for the files defined by FileSelector.

        :param selector: FileSelector object
        :return: list of file info objects
        """
        return DeltaFileSystemHandler.get_file_info_selector(
            self, selector.base_dir, selector.allow_not_found, selector.recursive
        )
