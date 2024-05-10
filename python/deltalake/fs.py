from typing import Any, Dict, List, Mapping, Optional

import pyarrow as pa
from pyarrow.fs import FileInfo, FileSelector, FileSystemHandler

from ._internal import DeltaFileSystemHandler, RawDeltaTable


# NOTE  we need to inherit form FileSystemHandler to pass pyarrow's internal type checks.
class DeltaStorageHandler(FileSystemHandler):
    """
    DeltaStorageHandler is a concrete implementations of a PyArrow FileSystemHandler.
    """

    def __init__(
        self,
        table_uri: str,
        options: Optional[Dict[str, str]] = None,
        known_sizes: Optional[Dict[str, int]] = None,
    ):
        self._handler = DeltaFileSystemHandler(
            table_uri=table_uri, options=options, known_sizes=known_sizes
        )

    @classmethod
    def from_table(
        cls,
        table: RawDeltaTable,
        options: Optional[Dict[str, str]] = None,
        known_sizes: Optional[Dict[str, int]] = None,
    ) -> "DeltaStorageHandler":
        self = cls.__new__(cls)
        self._handler = DeltaFileSystemHandler.from_table(table, options, known_sizes)
        return self

    def get_type_name(self) -> str:
        return self._handler.get_type_name()

    def copy_file(self, src: str, dst: str) -> None:
        """Copy a file.

        If the destination exists and is a directory, an error is returned. Otherwise, it is replaced.
        """
        return self._handler.copy_file(src=src, dst=dst)

    def create_dir(self, path: str, recursive: bool = True) -> None:
        """Create a directory and subdirectories.

        This function succeeds if the directory already exists.
        """
        return self._handler.create_dir(path, recursive)

    def delete_dir(self, path: str) -> None:
        """Delete a directory and its contents, recursively."""
        return self._handler.delete_dir(path)

    def delete_file(self, path: str) -> None:
        """Delete a file."""
        return self._handler.delete_file(path)

    def equals(self, other: Any) -> bool:
        return self._handler.equals(other)

    def delete_dir_contents(
        self, path: str, *, accept_root_dir: bool = False, missing_dir_ok: bool = False
    ) -> None:
        """Delete a directory's contents, recursively.

        Like delete_dir, but doesn't delete the directory itself.
        """
        return self._handler.delete_dir_contents(
            path=path, accept_root_dir=accept_root_dir, missing_dir_ok=missing_dir_ok
        )

    def delete_root_dir_contents(self) -> None:
        """Delete the root directory contents, recursively."""
        return self._handler.delete_root_dir_contents()

    def get_file_info(self, paths: List[str]) -> List[FileInfo]:
        """Get info for the given files.

        A non-existing or unreachable file returns a FileStat object and has a FileType of value NotFound.
        An exception indicates a truly exceptional condition (low-level I/O error, etc.).
        """
        return self._handler.get_file_info(paths)

    def move(self, src: str, dest: str) -> None:
        """Move / rename a file or directory.

        If the destination exists: - if it is a non-empty directory, an error is returned - otherwise,
        if it has the same type as the source, it is replaced - otherwise, behavior is
        unspecified (implementation-dependent).
        """
        self._handler.move_file(src=src, dest=dest)

    def normalize_path(self, path: str) -> str:
        """Normalize filesystem path."""
        return self._handler.normalize_path(path)

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
        return pa.PythonFile(self._handler.open_output_stream(path, metadata))

    def get_file_info_selector(self, selector: FileSelector) -> List[FileInfo]:
        """
        Get info for the files defined by FileSelector.

        Args:
            selector: FileSelector object

        Returns:
            list of file info objects
        """
        return self._handler.get_file_info_selector(
            selector.base_dir, selector.allow_not_found, selector.recursive
        )

    def open_append_stream(self, path: str, metadata: Mapping[str, str]) -> None:
        raise NotImplementedError
