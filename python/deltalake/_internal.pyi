import sys
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Union

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

import pyarrow as pa
import pyarrow.fs as fs

from deltalake.writer import AddAction

__version__: str

RawDeltaTable: Any
rust_core_version: Callable[[], str]

write_new_deltalake: Callable[
    [
        str,
        pa.Schema,
        List[AddAction],
        str,
        List[str],
        Optional[str],
        Optional[str],
        Optional[Mapping[str, Optional[str]]],
        Optional[Dict[str, str]],
    ],
    None,
]

def batch_distinct(batch: pa.RecordBatch) -> pa.RecordBatch: ...

# Can't implement inheritance (see note in src/schema.rs), so this is next
# best thing.
DataType = Union["PrimitiveType", "MapType", "StructType", "ArrayType"]

class PrimitiveType:
    def __init__(self, data_type: str) -> None: ...
    type: str

    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> "PrimitiveType": ...
    def to_pyarrow(self) -> pa.DataType: ...
    @staticmethod
    def from_pyarrow(type: pa.DataType) -> "PrimitiveType": ...

class ArrayType:
    def __init__(
        self, element_type: DataType, *, contains_null: bool = True
    ) -> None: ...
    type: Literal["array"]
    element_type: DataType
    contains_null: bool

    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> "ArrayType": ...
    def to_pyarrow(
        self,
    ) -> pa.ListType: ...
    @staticmethod
    def from_pyarrow(type: pa.ListType) -> "ArrayType": ...

class MapType:
    def __init__(
        self,
        key_type: DataType,
        value_type: DataType,
        *,
        value_contains_null: bool = True,
    ) -> None: ...
    type: Literal["map"]
    key_type: DataType
    value_type: DataType
    value_contains_null: bool

    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> "MapType": ...
    def to_pyarrow(self) -> pa.MapType: ...
    @staticmethod
    def from_pyarrow(type: pa.MapType) -> "MapType": ...

class Field:
    def __init__(
        self,
        name: str,
        type: DataType,
        *,
        nullable: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """A named field, with a data type, nullability, and optional metadata."""
    name: str
    """The field name."""
    type: DataType
    """The field data type."""
    nullable: bool
    """The field nullability."""
    metadata: Dict[str, Any]
    """The field metadata."""

    def to_json(self) -> str:
        """Get the JSON representation of the Field.

        :rtype: str
        """
    @staticmethod
    def from_json(json: str) -> "Field":
        """Create a new Field from a JSON string.

        :param json: A json string representing the Field.
        :rtype: Field
        """
    def to_pyarrow(self) -> pa.Field:
        """Convert field to a pyarrow.Field."""
    @staticmethod
    def from_pyarrow(type: pa.Field) -> "Field":
        """Create a new field from pyarrow.Field."""

class StructType:
    def __init__(self, fields: List[Field]) -> None: ...
    type: Literal["struct"]
    fields: List[Field]

    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> "StructType": ...
    def to_pyarrow(self) -> pa.StructType: ...
    @staticmethod
    def from_pyarrow(type: pa.StructType) -> "StructType": ...

class Schema:
    def __init__(self, fields: List[Field]) -> None: ...
    fields: List[Field]
    invariants: List[Tuple[str, str]]
    """The list of invariants defined on the table.
    
    The first string in each tuple is the field path, the second is the SQL of the invariant.
    """

    def to_json(self) -> str:
        """Get the JSON representation of the schema.

        :rtype: str
        """
    @staticmethod
    def from_json(json: str) -> "Schema":
        """Create a new Schema from a JSON string.

        :param schema_json: a JSON string
        :rtype: Schema
        """
    def to_pyarrow(self, as_large_types: bool = False) -> pa.Schema:
        """Return equivalent PyArrow schema.

        Note: this conversion is lossy as the Invariants are not stored in pyarrow.Schema.

        :param as_large_types: get schema with all variable size types (list,
            binary, string) as large variants (with int64 indices). This is for
            compatibility with systems like Polars that only support the large
            versions of Arrow types.
        :rtype: pyarrow.Schema
        """
    @staticmethod
    def from_pyarrow(type: pa.Schema) -> "Schema":
        """Create a new Schema from a pyarrow.Schema.

        :param data_type: a PyArrow schema
        :rtype: Schema
        """

class ObjectInputFile:
    @property
    def closed(self) -> bool: ...
    @property
    def mode(self) -> str: ...
    def isatty(self) -> bool: ...
    def readable(self) -> bool: ...
    def seekable(self) -> bool: ...
    def tell(self) -> int: ...
    def size(self) -> int: ...
    def seek(self, position: int, whence: int = 0) -> int: ...
    def read(self, nbytes: int) -> bytes: ...

class ObjectOutputStream:
    @property
    def closed(self) -> bool: ...
    @property
    def mode(self) -> str: ...
    def isatty(self) -> bool: ...
    def readable(self) -> bool: ...
    def seekable(self) -> bool: ...
    def writable(self) -> bool: ...
    def tell(self) -> int: ...
    def size(self) -> int: ...
    def seek(self, position: int, whence: int = 0) -> int: ...
    def read(self, nbytes: int) -> bytes: ...
    def write(self, data: bytes) -> int: ...

class DeltaFileSystemHandler:
    """Implementation of pyarrow.fs.FileSystemHandler for use with pyarrow.fs.PyFileSystem"""

    def __init__(self, root: str, options: dict[str, str] | None = None) -> None: ...
    def get_type_name(self) -> str: ...
    def copy_file(self, src: str, dst: str) -> None:
        """Copy a file.

        If the destination exists and is a directory, an error is returned. Otherwise, it is replaced.
        """
    def create_dir(self, path: str, recursive: bool = True) -> None:
        """Create a directory and subdirectories.

        This function succeeds if the directory already exists.
        """
    def delete_dir(self, path: str) -> None:
        """Delete a directory and its contents, recursively."""
    def delete_file(self, path: str) -> None:
        """Delete a file."""
    def equals(self, other: Any) -> bool: ...
    def delete_dir_contents(
        self, path: str, *, accept_root_dir: bool = False, missing_dir_ok: bool = False
    ) -> None:
        """Delete a directory's contents, recursively.

        Like delete_dir, but doesn't delete the directory itself.
        """
    def delete_root_dir_contents(self) -> None:
        """Delete the root directory contents, recursively."""
    def get_file_info(self, paths: list[str]) -> list[fs.FileInfo]:
        """Get info for the given files.

        A non-existing or unreachable file returns a FileStat object and has a FileType of value NotFound.
        An exception indicates a truly exceptional condition (low-level I/O error, etc.).
        """
    def get_file_info_selector(
        self, base_dir: str, allow_not_found: bool = False, recursive: bool = False
    ) -> list[fs.FileInfo]:
        """Get info for the given files.

        A non-existing or unreachable file returns a FileStat object and has a FileType of value NotFound.
        An exception indicates a truly exceptional condition (low-level I/O error, etc.).
        """
    def move_file(self, src: str, dest: str) -> None:
        """Move / rename a file or directory.

        If the destination exists: - if it is a non-empty directory, an error is returned - otherwise,
        if it has the same type as the source, it is replaced - otherwise, behavior is
        unspecified (implementation-dependent).
        """
    def normalize_path(self, path: str) -> str:
        """Normalize filesystem path."""
    def open_input_file(self, path: str) -> ObjectInputFile:
        """Open an input file for random access reading."""
    def open_output_stream(
        self, path: str, metadata: dict[str, str] | None = None
    ) -> ObjectOutputStream:
        """Open an output stream for sequential writing."""

class DeltaDataChecker:
    def __init__(self, invariants: List[Tuple[str, str]]) -> None: ...
    def check_batch(self, batch: pa.RecordBatch) -> None: ...

class DeltaError(Exception):
    """The base class for Delta-specific errors."""

    pass

class TableNotFoundError(DeltaError):
    """Raised when a Delta table cannot be loaded from a location."""

    pass

class CommitFailedError(DeltaError):
    """Raised when a commit to a Delta table fails."""

    pass

class DeltaProtocolError(DeltaError):
    """Raised when a violation with the Delta protocol specs ocurred."""

    pass
