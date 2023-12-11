import sys
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

import pyarrow
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
    def get_obj(self, version: int) -> bytes: ...
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
        self, schema: pyarrow.Schema, partition_filters: Optional[FilterType]
    ) -> List[Any]: ...
    def create_checkpoint(self) -> None: ...
    def get_add_actions(self, flatten: bool) -> pyarrow.RecordBatch: ...
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
        source: pyarrow.RecordBatchReader,
        predicate: str,
        source_alias: Optional[str],
        target_alias: Optional[str],
        writer_properties: Optional[Dict[str, int | None]],
        safe_cast: bool,
        matched_update_updates: Optional[List[Dict[str, str]]],
        matched_update_predicate: Optional[List[Optional[str]]],
        matched_delete_predicate: Optional[List[str]],
        matched_delete_all: Optional[bool],
        not_matched_insert_updates: Optional[List[Dict[str, str]]],
        not_matched_insert_predicate: Optional[List[Optional[str]]],
        not_matched_by_source_update_updates: Optional[List[Dict[str, str]]],
        not_matched_by_source_update_predicate: Optional[List[Optional[str]]],
        not_matched_by_source_delete_predicate: Optional[List[str]],
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
        schema: pyarrow.Schema,
        partitions_filters: Optional[FilterType],
    ) -> None: ...
    def cleanup_metadata(self) -> None: ...

def rust_core_version() -> str: ...
def write_new_deltalake(
    table_uri: str,
    schema: pyarrow.Schema,
    add_actions: List[AddAction],
    _mode: str,
    partition_by: List[str],
    name: Optional[str],
    description: Optional[str],
    configuration: Optional[Mapping[str, Optional[str]]],
    storage_options: Optional[Dict[str, str]],
) -> None: ...
def write_to_deltalake(
    table_uri: str,
    data: pyarrow.RecordBatchReader,
    partition_by: Optional[List[str]],
    mode: str,
    max_rows_per_group: int,
    overwrite_schema: bool,
    predicate: Optional[str],
    name: Optional[str],
    description: Optional[str],
    configuration: Optional[Mapping[str, Optional[str]]],
    storage_options: Optional[Dict[str, str]],
) -> None: ...
def convert_to_deltalake(
    uri: str,
    partition_by: Optional[pyarrow.Schema],
    partition_strategy: Optional[Literal["hive"]],
    name: Optional[str],
    description: Optional[str],
    configuration: Optional[Mapping[str, Optional[str]]],
    storage_options: Optional[Dict[str, str]],
    custom_metadata: Optional[Dict[str, str]],
) -> None: ...
def create_deltalake(
    table_uri: str,
    schema: pyarrow.Schema,
    partition_by: List[str],
    mode: str,
    name: Optional[str],
    description: Optional[str],
    configuration: Optional[Mapping[str, Optional[str]]],
    storage_options: Optional[Dict[str, str]],
) -> None: ...
def batch_distinct(batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch: ...

# Can't implement inheritance (see note in src/schema.rs), so this is next
# best thing.
DataType = Union["PrimitiveType", "MapType", "StructType", "ArrayType"]

class PrimitiveType:
    """A primitive datatype, such as a string or number.

    Can be initialized with a string value:

    ```
    PrimitiveType("integer")
    ```

    Valid primitive data types include:

     * "string",
     * "long",
     * "integer",
     * "short",
     * "byte",
     * "float",
     * "double",
     * "boolean",
     * "binary",
     * "date",
     * "timestamp",
     * "decimal(<precision>, <scale>)"

    Args:
        data_type: string representation of the data type
    """

    def __init__(self, data_type: str) -> None: ...
    type: str
    """ The inner type
    """

    def to_json(self) -> str: ...
    @staticmethod
    def from_json(json: str) -> PrimitiveType:
        """Create a PrimitiveType from a JSON string

        The JSON representation for a primitive type is just a quoted string: `PrimitiveType.from_json('"integer"')`

        Args:
            json: a JSON string

        Returns:
            a PrimitiveType type
        """
    def to_pyarrow(self) -> pyarrow.DataType:
        """Get the equivalent PyArrow type (pyarrow.DataType)"""
    @staticmethod
    def from_pyarrow(type: pyarrow.DataType) -> PrimitiveType:
        """Create a PrimitiveType from a PyArrow datatype

        Will raise `TypeError` if the PyArrow type is not a primitive type.

        Args:
            type: A PyArrow DataType

        Returns:
            a PrimitiveType
        """

class ArrayType:
    """An Array (List) DataType

    Example:
        Can either pass the element type explicitly or can pass a string
        if it is a primitive type:
        ```python
        ArrayType(PrimitiveType("integer"))
        # Returns ArrayType(PrimitiveType("integer"), contains_null=True)

        ArrayType("integer", contains_null=False)
        # Returns ArrayType(PrimitiveType("integer"), contains_null=False)
        ```
    """

    def __init__(
        self, element_type: DataType, *, contains_null: bool = True
    ) -> None: ...
    type: Literal["array"]
    """ The string "array"
    """

    element_type: DataType
    """ The type of the element, of type: 
        Union[
            [PrimitiveType][deltalake.schema.PrimitiveType], 
            [ArrayType][deltalake.schema.ArrayType], 
            [MapType][deltalake.schema.MapType], 
            [StructType][deltalake.schema.StructType]
        ]
    """

    contains_null: bool
    """ Whether the arrays may contain null values
    """

    def to_json(self) -> str:
        """Get the JSON string representation of the type."""
    @staticmethod
    def from_json(json: str) -> "ArrayType":
        """Create an ArrayType from a JSON string

        Args:
            json: a JSON string

        Returns:
            an ArrayType

        Example:
            The JSON representation for an array type is an object with `type` (set to
            `"array"`), `elementType`, and `containsNull`.
            ```python
            ArrayType.from_json(
                '''{
                    "type": "array",
                    "elementType": "integer",
                    "containsNull": false
                }'''
            )
            # Returns ArrayType(PrimitiveType("integer"), contains_null=False)
            ```
        """
    def to_pyarrow(
        self,
    ) -> pyarrow.ListType:
        """Get the equivalent PyArrow type."""
    @staticmethod
    def from_pyarrow(type: pyarrow.ListType) -> ArrayType:
        """Create an ArrayType from a pyarrow.ListType.

        Will raise `TypeError` if a different PyArrow DataType is provided.

        Args:
            type: The PyArrow ListType

        Returns:
            an ArrayType
        """

class MapType:
    """A map data type

    `key_type` and `value_type` should be [PrimitiveType][deltalake.schema.PrimitiveType], [ArrayType][deltalake.schema.ArrayType],
    or [StructType][deltalake.schema.StructType]. A string can also be passed, which will be
    parsed as a primitive type:

    Example:
        ```python
        MapType(PrimitiveType("integer"), PrimitiveType("string"))
        # Returns MapType(PrimitiveType("integer"), PrimitiveType("string"), value_contains_null=True)

        MapType("integer", "string", value_contains_null=False)
        # Returns MapType(PrimitiveType("integer"), PrimitiveType("string"), value_contains_null=False)
        ```
    """

    def __init__(
        self,
        key_type: DataType,
        value_type: DataType,
        *,
        value_contains_null: bool = True,
    ) -> None: ...
    type: Literal["map"]
    key_type: DataType
    """ The type of the keys, of type: 
        Union[
            [PrimitiveType][deltalake.schema.PrimitiveType], 
            [ArrayType][deltalake.schema.ArrayType], 
            [MapType][deltalake.schema.MapType], 
            [StructType][deltalake.schema.StructType]
        ]
    """

    value_type: DataType
    """The type of the values, of type: 
        Union[
            [PrimitiveType][deltalake.schema.PrimitiveType], 
            [ArrayType][deltalake.schema.ArrayType], 
            [MapType][deltalake.schema.MapType], 
            [StructType][deltalake.schema.StructType]
        ]
    """

    value_contains_null: bool
    """ Whether the values in a map may be null
    """

    def to_json(self) -> str:
        """Get JSON string representation of map type.

        Returns:
            a JSON string
        """
    @staticmethod
    def from_json(json: str) -> MapType:
        """Create a MapType from a JSON string

        Args:
            json: a JSON string

        Returns:
            an ArrayType

        Example:
            The JSON representation for a map type is an object with `type` (set to `map`),
            `keyType`, `valueType`, and `valueContainsNull`:

            ```python
            MapType.from_json(
                '''{
                    "type": "map",
                    "keyType": "integer",
                    "valueType": "string",
                    "valueContainsNull": true
                }'''
            )
            # Returns MapType(PrimitiveType("integer"), PrimitiveType("string"), value_contains_null=True)
            ```
        """
    def to_pyarrow(self) -> pyarrow.MapType:
        """Get the equivalent PyArrow data type."""
    @staticmethod
    def from_pyarrow(type: pyarrow.MapType) -> MapType:
        """Create a MapType from a PyArrow MapType.

        Will raise `TypeError` if passed a different type.

        Args:
            type: the PyArrow MapType

        Returns:
            a MapType
        """

class Field:
    """A field in a Delta StructType or Schema

    Example:
        Can create with just a name and a type:
        ```python
        Field("my_int_col", "integer")
        # Returns Field("my_int_col", PrimitiveType("integer"), nullable=True, metadata=None)
        ```

        Can also attach metadata to the field. Metadata should be a dictionary with
        string keys and JSON-serializable values (str, list, int, float, dict):

        ```python
        Field("my_col", "integer", metadata={"custom_metadata": {"test": 2}})
        # Returns Field("my_col", PrimitiveType("integer"), nullable=True, metadata={"custom_metadata": {"test": 2}})
        ```
    """

    def __init__(
        self,
        name: str,
        type: DataType,
        *,
        nullable: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None: ...
    name: str
    """ The name of the field
    """

    type: DataType
    """ The type of the field, of type: 
        Union[
            [PrimitiveType][deltalake.schema.PrimitiveType], 
            [ArrayType][deltalake.schema.ArrayType], 
            [MapType][deltalake.schema.MapType], 
            [StructType][deltalake.schema.StructType]
        ]
    """

    nullable: bool
    """ Whether there may be null values in the field
    """

    metadata: Dict[str, Any]
    """ The metadata of the field
    """

    def to_json(self) -> str:
        """Get the field as JSON string.

        Returns:
            a JSON string

        Example:
            ```python
            Field("col", "integer").to_json()
            # Returns '{"name":"col","type":"integer","nullable":true,"metadata":{}}'
            ```
        """
    @staticmethod
    def from_json(json: str) -> Field:
        """Create a Field from a JSON string.

        Args:
            json: the JSON string.

        Returns:
            Field

        Example:
            ```
            Field.from_json('''{
                    "name": "col",
                    "type": "integer",
                    "nullable": true,
                    "metadata": {}
                }'''
            )
            # Returns Field(col, PrimitiveType("integer"), nullable=True)
            ```
        """
    def to_pyarrow(self) -> pyarrow.Field:
        """Convert to an equivalent PyArrow field
        Note: This currently doesn't preserve field metadata.

        Returns:
            a pyarrow Field
        """
    @staticmethod
    def from_pyarrow(field: pyarrow.Field) -> Field:
        """Create a Field from a PyArrow field
        Note: This currently doesn't preserve field metadata.

        Args:
            field: a PyArrow Field

        Returns:
            a Field
        """

class StructType:
    """A struct datatype, containing one or more subfields

    Example:
        Create with a list of :class:`Field`:
        ```python
        StructType([Field("x", "integer"), Field("y", "string")])
        # Creates: StructType([Field(x, PrimitiveType("integer"), nullable=True), Field(y, PrimitiveType("string"), nullable=True)])
        ```
    """

    def __init__(self, fields: List[Field]) -> None: ...
    type: Literal["struct"]
    fields: List[Field]
    """ The fields within the struct
    """

    def to_json(self) -> str:
        """Get the JSON representation of the type.

        Returns:
            a JSON string

        Example:
            ```python
            StructType([Field("x", "integer")]).to_json()
            # Returns '{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}}]}'
            ```
        """
    @staticmethod
    def from_json(json: str) -> StructType:
        """Create a new StructType from a JSON string.

        Args:
            json: a JSON string

        Returns:
            a StructType

        Example:
            ```python
            StructType.from_json(
                '''{
                    "type": "struct",
                    "fields": [{"name": "x", "type": "integer", "nullable": true, "metadata": {}}]
                }'''
            )
            # Returns StructType([Field(x, PrimitiveType("integer"), nullable=True)])
            ```
        """
    def to_pyarrow(self) -> pyarrow.StructType:
        """Get the equivalent PyArrow StructType

        Returns:
            a PyArrow StructType
        """
    @staticmethod
    def from_pyarrow(type: pyarrow.StructType) -> StructType:
        """Create a new StructType from a PyArrow struct type.

        Will raise `TypeError` if a different data type is provided.

        Args:
            type: a PyArrow struct type.

        Returns:
            a StructType
        """

class Schema:
    def __init__(self, fields: List[Field]) -> None: ...
    fields: List[Field]

    invariants: List[Tuple[str, str]]
    """ The list of invariants on the table. Each invarint is a tuple of strings. The first string is the
        field path and the second is the SQL of the invariant.
    """
    def to_json(self) -> str:
        """Get the JSON string representation of the Schema.

        Returns:
            a JSON string

        Example:
            A schema has the same JSON format as a StructType.
            ```python
            Schema([Field("x", "integer")]).to_json()
            # Returns '{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}}]}'
            ```
        """
    @staticmethod
    def from_json(json: str) -> Schema:
        """Create a new Schema from a JSON string.

        Args:
            json: a JSON string

        Example:
            A schema has the same JSON format as a StructType.
            ```python
            Schema.from_json('''{
                "type": "struct",
                "fields": [{"name": "x", "type": "integer", "nullable": true, "metadata": {}}]
                }
            )'''
            # Returns Schema([Field(x, PrimitiveType("integer"), nullable=True)])
            ```
        """
    def to_pyarrow(self, as_large_types: bool = False) -> pyarrow.Schema:
        """Return equivalent PyArrow schema

        Args:
            as_large_types: get schema with all variable size types (list, binary, string) as large variants (with int64 indices).
                This is for compatibility with systems like Polars that only support the large versions of Arrow types.

        Returns:
            a PyArrow Schema
        """
    @staticmethod
    def from_pyarrow(type: pyarrow.Schema) -> Schema:
        """Create a [Schema][deltalake.schema.Schema] from a PyArrow Schema type

        Will raise `TypeError` if the PyArrow type is not a primitive type.

        Args:
            type: A PyArrow Schema

        Returns:
            a Schema
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
    """Implementation of [pyarrow.fs.FileSystemHandler][pyarrow.fs.FileSystemHandler] for use with [pyarrow.fs.PyFileSystem][pyarrow.fs.PyFileSystem]"""

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
    def check_batch(self, batch: pyarrow.RecordBatch) -> None: ...

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
