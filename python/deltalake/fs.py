from typing import Any, Dict, List, Optional

import pyarrow as pa
from pyarrow.fs import FileInfo, FileSelector, FileSystemHandler, FileType

from .deltalake import DeltaStorageFsBackend


class DeltaStorageHandler(FileSystemHandler):
    """
    DeltaStorageHander is a concrete implementations of a PyArrow FileSystemHandler.
    """

    def __init__(self, table_uri: str) -> None:
        self._storage = DeltaStorageFsBackend(table_uri)

    def __eq__(self, other: Any) -> bool:
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return NotImplemented

    def get_type_name(self) -> str:
        """
        The filesystem’s type name.

        :return: The filesystem’s type name.
        """
        return NotImplemented

    def normalize_path(self, path: str) -> str:
        """
        Normalize filesystem path.

        :param path: the path to normalize
        :return: the normalized path
        """
        return self._storage.normalize_path(path)

    def get_file_info(self, paths: List[str]) -> List[FileInfo]:
        """
        Get info for the given files.

        :param paths: List of file paths
        :return: list of file info objects
        """
        infos = []
        for path in paths:
            path, secs = self._storage.head_obj(path)
            infos.append(FileInfo(path, type=FileType.File, mtime=float(secs)))
        return infos

    def get_file_info_selector(self, selector: FileSelector) -> List[FileInfo]:
        """
        Get info for the files defined by FileSelector.

        :param selector: FileSelector object
        :return: list of file info objects
        """
        raise NotImplementedError

    def create_dir(self, path: str, *, recursive: bool = True) -> None:
        """
        Create a directory and subdirectories.

        This function succeeds if the directory already exists.

        :param path: The path of the new directory.
        :param recursive: Create nested directories as well.
        """
        raise NotImplementedError

    def delete_dir(self, path: str) -> None:
        """
        Delete a directory and its contents, recursively.

        :param path: The path of the directory to be deleted.
        """
        raise NotImplementedError

    def delete_dir_contents(self, path: str) -> None:
        """
        Delete a directory’s contents, recursively.

        Like delete_dir, but doesn’t delete the directory itself.

        :param path: The path of the directory to be deleted.
        """
        raise NotImplementedError

    def delete_root_dir_contents(self) -> None:
        """
        Delete a directory’s contents, recursively.

        Like delete_dir_contents, but for the root directory (path is empty or “/”)
        """
        raise NotImplementedError

    def delete_file(self, path: str) -> None:
        """
        Delete a file.

        :param path: The path of the file to be deleted.
        """
        raise NotImplementedError

    def move(self, src: str, dest: str) -> None:
        """
        Move / rename a file or directory.

        If the destination exists: - if it is a non-empty directory, an error is returned - otherwise,
        if it has the same type as the source, it is replaced - otherwise,
        behavior is unspecified (implementation-dependent).

        :param src: The path of the file or the directory to be moved.
        :param dest: The destination path where the file or directory is moved to.
        """
        raise NotImplementedError

    def copy_file(self, src: str, dest: str) -> None:
        """
        Copy a file.

        If the destination exists and is a directory, an error is returned.
        Otherwise, it is replaced.

        :param src: The path of the file to be copied from.
        :param dest: The destination path where the file is copied to.
        """
        raise NotImplementedError

    def open_input_stream(self, path: str) -> pa.NativeFile:
        """
        Open an input stream for sequential reading.

        :param source: The source to open for reading.
        :return:  NativeFile
        """
        raw = self._storage.get_obj(path)
        return pa.BufferReader(pa.py_buffer(raw))

    def open_input_file(self, path: str) -> pa.NativeFile:
        """
        Open an input file for random access reading.

        :param source: The source to open for reading.
        :return:  NativeFile
        """
        raw = self._storage.get_obj(path)
        return pa.BufferReader(pa.py_buffer(raw))

    def open_output_stream(
        self, path: str, metadata: Optional[Dict[str, Any]] = None
    ) -> pa.NativeFile:
        """
        Open an output stream for sequential writing.

        If the target already exists, existing data is truncated.

        :param path: The source to open for writing.
        :param metadata: If not None, a mapping of string keys to string values.
        :return:  NativeFile
        """
        raise NotImplementedError

    def open_append_stream(
        self, path: str, metadata: Optional[Dict[str, Any]] = None
    ) -> pa.NativeFile:
        """
        DEPRECATED: Open an output stream for appending.

        If the target doesn’t exist, a new empty file is created.

        :param path: The source to open for writing.
        :param metadata: If not None, a mapping of string keys to string values.
        :return:  NativeFile
        """
        raise NotImplementedError
