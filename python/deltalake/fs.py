import pyarrow as pa
from pyarrow.fs import FileInfo, FileSelector, FileSystemHandler

from ._internal import DeltaFileSystemHandler


# NOTE  we need to inherit form FileSystemHandler to pass pyarrow's internal type checks.
class DeltaStorageHandler(DeltaFileSystemHandler, FileSystemHandler):
    """
    DeltaStorageHandler is a concrete implementations of a PyArrow FileSystemHandler.
    """

    def open_input_file(self, path: str) -> pa.PythonFile:
        return pa.PythonFile(DeltaFileSystemHandler.open_input_file(self, path))

    def open_input_stream(self, path: str) -> pa.PythonFile:
        return pa.PythonFile(DeltaFileSystemHandler.open_input_file(self, path))

    def open_output_stream(
        self, path: str, metadata: dict[str, str] | None = None
    ) -> pa.PythonFile:
        return pa.PythonFile(
            DeltaFileSystemHandler.open_output_stream(self, path, metadata)
        )

    def get_file_info_selector(self, selector: FileSelector) -> list[FileInfo]:
        return DeltaFileSystemHandler.get_file_info_selector(
            self, selector.base_dir, selector.allow_not_found, selector.recursive
        )
