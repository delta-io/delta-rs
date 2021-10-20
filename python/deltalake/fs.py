from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.fs as pa_fs

from .deltalake import DeltaStorageFsBackend


class DeltaStorageHandler(pa_fs.FileSystemHandler):
    def __init__(self, table_uri: str) -> None:
        self._storage = DeltaStorageFsBackend(table_uri)

    def __eq__(self, other: Any) -> bool:
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return NotImplemented

    def get_type_name(self) -> str:
        return NotImplemented

    def normalize_path(self, path: str) -> str:
        """
        Normalize filesystem path.

        :param path:the path to normalize
        :return: the normalized path
        """
        return self._storage.normalize_path(path)

    def get_file_info(self, paths: List[str]) -> List[pa_fs.FileInfo]:
        """
        Get info for the given files.

        :param paths: List of file paths
        :return: list of file info objects
        """
        infos = []
        for path in paths:
            path, secs = self._storage.head_obj(path)
            infos.append(
                pa_fs.FileInfo(path, type=pa_fs.FileType.File, mtime=float(secs))
            )
        return infos

    def get_file_info_selector(
        self, selector: pa_fs.FileSelector
    ) -> List[pa_fs.FileInfo]:
        raise NotImplementedError

    def create_dir(self, path: str, *, recursive: bool = True) -> None:
        raise NotImplementedError

    def delete_dir(self, path: str) -> None:
        raise NotImplementedError

    def delete_dir_contents(self, path: str) -> None:
        raise NotImplementedError

    def delete_root_dir_contents(self) -> None:
        raise NotImplementedError

    def delete_file(self, path: str) -> None:
        raise NotImplementedError

    def move(self, src: str, dest: str) -> None:
        raise NotImplementedError

    def copy_file(self, src: str, dest: str) -> None:
        raise NotImplementedError

    def open_input_stream(self, path: str) -> pa.NativeFile:
        """
        Open an input stream for sequential reading.

        :param source The source to open for reading.
        :return:  NativeFile
        """
        raw = self._storage.get_obj(path)
        return pa.BufferReader(pa.py_buffer(raw))

    def open_input_file(self, path: str) -> pa.NativeFile:
        """
        Open an input file for random access reading.

        :param source The source to open for reading.
        :return:  NativeFile
        """
        raw = self._storage.get_obj(path)
        return pa.BufferReader(pa.py_buffer(raw))

    def open_output_stream(
        self, path: str, metadata: Optional[Dict[str, Any]] = None
    ) -> pa.NativeFile:
        raise NotImplementedError

    def open_append_stream(
        self, path: str, metadata: Optional[Dict[str, Any]] = None
    ) -> pa.NativeFile:
        raise NotImplementedError
