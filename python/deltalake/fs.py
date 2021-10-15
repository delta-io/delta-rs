from io import BytesIO

import pyarrow as pa
from pyarrow.fs import FileInfo, FileSystemHandler, FileType, PyFileSystem

from .deltalake import DeltaStorageFsHandler


class ProxyHandler(FileSystemHandler):

    def __init__(self, handler: DeltaStorageFsHandler):
        self._handler = handler

    def __eq__(self, other):
        raise NotImplementedError
        if isinstance(other, ProxyHandler):
            return self._fs == other._fs
        return NotImplemented

    def __ne__(self, other):
        raise NotImplementedError
        if isinstance(other, ProxyHandler):
            return self._fs != other._fs
        return NotImplemented

    def get_type_name(self):
        return "abfss::"# + self._fs.type_name

    def normalize_path(self, path):
        raise NotImplementedError

    def get_file_info(self, paths):
        infos = []
        for path in paths:
            path, secs = self._handler.head_obj(path)
            infos.append(FileInfo(path, type=FileType.File, mtime=float(secs)))
        return infos

    def get_file_info_selector(self, selector):
        raise NotImplementedError

    def create_dir(self, path, recursive):
        raise NotImplementedError

    def delete_dir(self, path):
        raise NotImplementedError

    def delete_dir_contents(self, path):
        raise NotImplementedError

    def delete_root_dir_contents(self):
        raise NotImplementedError

    def delete_file(self, path):
        raise NotImplementedError

    def move(self, src, dest):
        raise NotImplementedError

    def copy_file(self, src, dest):
        raise NotImplementedError

    def open_input_stream(self, path):
        return self._fs.open_input_stream(path)

    def open_input_file(self, path):
        raw = self._handler.get_obj(path)
        buf = pa.py_buffer(raw)
        return pa.BufferReader(buf)

    def open_output_stream(self, path, metadata):
        raise NotImplementedError

    def open_append_stream(self, path, metadata):
        raise NotImplementedError
