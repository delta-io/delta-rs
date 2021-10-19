from typing import Dict, List

import pyarrow as pa
import pyarrow.fs as pa_fs

from .deltalake import DeltaStorageFsBackend


class DeltaStorageHandler(pa_fs.FileSystemHandler):

    def __init__(self, table_uri: str) -> None:
        self._storage = DeltaStorageFsBackend(table_uri)

    def __eq__(self, other) -> bool:
        return NotImplemented

    def __ne__(self, other) -> bool:
        return NotImplemented

    def get_type_name(self) -> str:
        return NotImplemented

    def normalize_path(self, path: str) -> str:
        return self._storage.normalize_path(path)

    def get_file_info(self, paths: List[str]) -> List[pa_fs.FileInfo]:
        infos = []
        for path in paths:
            path, secs = self._storage.head_obj(path)
            infos.append(pa_fs.FileInfo(path, type=pa_fs.FileType.File, mtime=float(secs)))
        return infos

    def get_file_info_selector(self, selector):
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

    def open_input_stream(self, path) -> pa.NativeFile:
        raw = self._storage.get_obj(path)
        return pa.BufferReader(pa.py_buffer(raw))

    def open_input_file(self, path) -> pa.NativeFile:
        raw = self._storage.get_obj(path)
        return pa.BufferReader(pa.py_buffer(raw))

    def open_output_stream(self, path: str, metadata: Dict = None) -> pa.NativeFile:
        raise NotImplementedError

    def open_append_stream(self, path: str, metadata: Dict = None) -> pa.NativeFile:
        raise NotImplementedError
