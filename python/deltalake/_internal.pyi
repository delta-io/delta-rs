import sys
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

import pyarrow as pa
import pyarrow.fs as fs

from deltalake.writer import AddAction

__version__: str

class RawDeltaTableMetaData:
    id: int
    name: str
    description: str
    partition_columns: List[str]
    created_time: int
    configuration: Dict[str, str]

class RawDeltaTable:
    schema: Any

    def __init__(
        self,
        table_uri: str,
        version: Optional[int],
        storage_options: Optional[Dict[str, str]],
        without_files: bool,
        log_buffer_size: Optional[int],
    ) -> None: ...
    @staticmethod
    def get_table_uri_from_data_catalog(
        data_catalog: str,
        database_name: str,
        table_name: str,
        data_catalog_id: Optional[str] = None,
        catalog_options: Optional[Dict[str, str]] = None,
    ) -> str: ...
    def table_uri(self) -> str: ...
    def version(self) -> int: ...
    def get_latest_version(self) -> int: ...
    def metadata(self) -> RawDeltaTableMetaData: ...
    def protocol_versions(self) -> List[int]: ...
    def load_version(self, version: int) -> None: ...
    def load_with_datetime(self, ds: str) -> None: ...
    def files_by_partitions(
        self, partitions_filters: Optional[FilterType]
    ) -> List[str]: ...
    def files(self, partition_filters: Optional[FilterType]) -> List[str]: ...
    def file_uris(self, partition_filters: Optional[FilterType]) -> List[str]: ...
    def vacuum(
        self,
        dry_run: bool,
        retention_hours: Optional[int],
        enforce_retention_duration: bool,
    ) -> List[str]: ...
    def compact_optimize(
        self,
        partition_filters: Optional[FilterType],
        target_size: Optional[int],
        max_concurrent_tasks: Optional[int],
        min_commit_interval: Optional[int],
    ) -> str: ...
    def z_order_optimize(
        self,
        z_order_columns: List[str],
        partition_filters: Optional[FilterType],
        target_size: Optional[int],
        max_concurrent_tasks: Optional[int],
        max_spill_size: Optional[int],
        min_commit_interval: Optional[int],
    ) -> str: ...
    def restore(
        self,
        target: Optional[Any],
        ignore_missing_files: bool,
        protocol_downgrade_allowed: bool,
    ) -> str: ...
    def history(self, limit: Optional[int]) -> List[str]: ...
    def update_incremental(self) -> None: ...
    def dataset_partitions(
        self, schema: pa.Schema, partition_filters: Optional[FilterType]
    ) -> List[Any]: ...
    def create_checkpoint(self) -> None: ...
    def get_add_actions(self, flatten: bool) -> pa.RecordBatch: ...
    def delete(self, predicate: Optional[str]) -> str: ...
    def repair(self, dry_run: bool) -> str: ...
    def update(
        self,
        updates: Dict[str, str],
        predicate: Optional[str],
        writer_properties: Optional[Dict[str, int]],
        safe_cast: bool = False,
    ) -> str: ...
    def merge_execute(
        self,
        source: pa.RecordBatchReader,
        predicate: str,
        source_alias: Optional[str],
        target_alias: Optional[str],
        writer_properties: Optional[Dict[str, int | None]],
        safe_cast: bool,
        matched_update_updates: Optional[Dict[str, str]],
        matched_update_predicate: Optional[str],
        matched_delete_predicate: Optional[str],
        matched_delete_all: Optional[bool],
        not_matched_insert_updates: Optional[Dict[str, str]],
        not_matched_insert_predicate: Optional[str],
        not_matched_by_source_update_updates: Optional[Dict[str, str]],
        not_matched_by_source_update_predicate: Optional[str],
        not_matched_by_source_delete_predicate: Optional[str],
        not_matched_by_source_delete_all: Optional[bool],
    ) -> str: ...
    def get_active_partitions(
        self, partitions_filters: Optional[FilterType] = None
    ) -> Any: ...
    def create_write_transaction(
        self,
        add_actions: List[AddAction],
        mode: str,
        partition_by: List[str],
        schema: pa.Schema,
        partitions_filters: Optional[FilterType],
    ) -> None: ...

def rust_core_version() -> str: ...
def write_new_deltalake(
    table_uri: str,
    schema: pa.Schema,
    add_actions: List[AddAction],
    _mode: str,
    partition_by: List[str],
    name: Optional[str],
    description: Optional[str],
    configuration: Optional[Mapping[str, Optional[str]]],
    storage_options: Optional[Dict[str, str]],
) -> None: ...
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
    ) -> None: ...
    name: str
    type: DataType
    nullable: bool
    metadata: Dict[str, Any]

    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> "Field": ...
    def to_pyarrow(self) -> pa.Field: ...
    @staticmethod
    def from_pyarrow(type: pa.Field) -> "Field": ...

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

    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> "Schema": ...
    def to_pyarrow(self, as_large_types: bool = False) -> pa.Schema: ...
    @staticmethod
    def from_pyarrow(type: pa.Schema) -> "Schema": ...

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

    def __init__(
        self,
        root: str,
        options: dict[str, str] | None = None,
        known_sizes: dict[str, int] | None = None,
    ) -> None: ...
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

FilterLiteralType = Tuple[str, str, Any]
FilterConjunctionType = List[FilterLiteralType]
FilterDNFType = List[FilterConjunctionType]
FilterType = Union[FilterConjunctionType, FilterDNFType]
