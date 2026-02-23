from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Optional,
    Union,
)

from arro3.core import DataType as ArrowDataType
from arro3.core import Field as ArrowField
from arro3.core import RecordBatchReader, Table
from arro3.core import Schema as ArrowSchema
from arro3.core.types import ArrowSchemaExportable

if TYPE_CHECKING:
    import pyarrow.fs as fs

    from deltalake.transaction import (
        AddAction,
        CommitProperties,
        PostCommitHookProperties,
    )
    from deltalake.writer.properties import (
        WriterProperties,
    )
__version__: str

class TableFeatures(Enum):
    # Mapping of one column to another
    ColumnMapping = "ColumnMapping"
    # Deletion vectors for merge, update, delete
    DeletionVectors = "DeletionVectors"
    # timestamps without timezone support
    TimestampWithoutTimezone = "TimestampWithoutTimezone"
    # version 2 of checkpointing
    V2Checkpoint = "V2Checkpoint"
    # Append Only Tables
    AppendOnly = "AppendOnly"
    # Table invariants
    Invariants = "Invariants"
    # Check constraints on columns
    CheckConstraints = "CheckConstraints"
    # CDF on a table
    ChangeDataFeed = "ChangeDataFeed"
    # Columns with generated values
    GeneratedColumns = "GeneratedColumns"
    # ID Columns
    IdentityColumns = "IdentityColumns"
    # Row tracking on tables
    RowTracking = "RowTracking"
    # domain specific metadata
    DomainMetadata = "DomainMetadata"
    # Iceberg compatibility support
    IcebergCompatV1 = "IcebergCompatV1"

class RawDeltaTableMetaData:
    id: int
    name: str
    description: str
    partition_columns: list[str]
    created_time: int
    configuration: dict[str, str]

class RawDeltaTable:
    schema: Any

    def __init__(
        self,
        table_uri: str,
        version: int | None,
        storage_options: dict[str, str] | None,
        without_files: bool,
        log_buffer_size: int | None,
    ) -> None: ...
    @staticmethod
    def get_table_uri_from_data_catalog(
        data_catalog: str,
        database_name: str,
        table_name: str,
        data_catalog_id: str | None = None,
        catalog_options: dict[str, str] | None = None,
    ) -> str: ...
    @staticmethod
    def is_deltatable(
        table_uri: str, storage_options: dict[str, str] | None
    ) -> bool: ...
    def table_uri(self) -> str: ...
    def version(self) -> int: ...
    def has_files(self) -> bool: ...
    def get_add_file_sizes(self) -> dict[str, int]: ...
    def get_latest_version(self) -> int: ...
    def metadata(self) -> RawDeltaTableMetaData: ...
    def protocol_versions(
        self,
    ) -> tuple[int, int, Optional[list[str]], Optional[list[str]]]: ...
    def table_config(self) -> tuple[bool, int]: ...
    def load_version(self, version: int) -> None: ...
    def load_with_datetime(self, ds: str) -> None: ...
    def files(self, partition_filters: PartitionFilterType | None) -> list[str]: ...
    def file_uris(self, partition_filters: PartitionFilterType | None) -> list[str]: ...
    def generate(self) -> None: ...
    def vacuum(
        self,
        dry_run: bool,
        retention_hours: int | None,
        enforce_retention_duration: bool,
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
        full: bool,
        keep_versions: list[int] | None,
    ) -> list[str]: ...
    def compact_optimize(
        self,
        partition_filters: PartitionFilterType | None,
        target_size: int | None,
        max_concurrent_tasks: int | None,
        max_spill_size: int | None,
        max_temp_directory_size: int | None,
        min_commit_interval: int | None,
        writer_properties: WriterProperties | None,
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
    ) -> str: ...
    def z_order_optimize(
        self,
        z_order_columns: list[str],
        partition_filters: PartitionFilterType | None,
        target_size: int | None,
        max_concurrent_tasks: int | None,
        max_spill_size: int | None,
        max_temp_directory_size: int | None,
        min_commit_interval: int | None,
        writer_properties: WriterProperties | None,
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
    ) -> str: ...
    def add_columns(
        self,
        fields: list[Field],
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
    ) -> None: ...
    def add_feature(
        self,
        feature: list[TableFeatures],
        allow_protocol_versions_increase: bool,
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
    ) -> None: ...
    def add_constraints(
        self,
        constraints: dict[str, str],
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
    ) -> None: ...
    def drop_constraints(
        self,
        name: str,
        raise_if_not_exists: bool,
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
    ) -> None: ...
    def set_table_properties(
        self,
        properties: dict[str, str],
        raise_if_not_exists: bool,
        commit_properties: CommitProperties | None,
    ) -> None: ...
    def set_table_name(
        self,
        name: str,
        commit_properties: CommitProperties | None = None,
    ) -> None: ...
    def set_table_description(
        self,
        description: str,
        commit_properties: CommitProperties | None = None,
    ) -> None: ...
    def restore(
        self,
        target: Any | None,
        ignore_missing_files: bool,
        protocol_downgrade_allowed: bool,
        commit_properties: CommitProperties | None,
    ) -> str: ...
    def history(self, limit: int | None) -> list[str]: ...
    def update_incremental(self) -> None: ...
    def dataset_partitions(
        self,
        schema: ArrowSchemaExportable,
        partition_filters: FilterConjunctionType | None,
    ) -> list[Any]: ...
    def create_checkpoint(self) -> None: ...
    def get_add_actions(self, flatten: bool) -> Table: ...
    def delete(
        self,
        predicate: str | None,
        writer_properties: WriterProperties | None,
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
    ) -> str: ...
    def repair(
        self,
        dry_run: bool,
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
    ) -> str: ...
    def update(
        self,
        updates: dict[str, str],
        predicate: str | None,
        writer_properties: WriterProperties | None,
        safe_cast: bool,
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
    ) -> str: ...
    def create_merge_builder(
        self,
        source: RecordBatchReader,
        batch_schema: ArrowSchema,
        predicate: str,
        source_alias: str | None,
        target_alias: str | None,
        merge_schema: bool,
        writer_properties: WriterProperties | None,
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
        safe_cast: bool,
        streamed_exec: bool,
    ) -> PyMergeBuilder: ...
    def merge_execute(self, merge_builder: PyMergeBuilder) -> str: ...
    def get_active_partitions(
        self, partitions_filters: FilterType | None = None
    ) -> Any: ...
    def create_write_transaction(
        self,
        add_actions: list[AddAction],
        mode: str,
        partition_by: list[str],
        schema: Schema,
        partitions_filters: FilterType | None,
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
    ) -> None: ...
    def cleanup_metadata(self) -> None: ...
    def load_cdf(
        self,
        columns: list[str] | None = None,
        predicate: str | None = None,
        starting_version: int = 0,
        ending_version: int | None = None,
        starting_timestamp: str | None = None,
        ending_timestamp: str | None = None,
        allow_out_of_range: bool = False,
    ) -> RecordBatchReader: ...
    def deletion_vectors(self) -> RecordBatchReader: ...
    def transaction_version(self, app_id: str) -> int | None: ...
    def set_column_metadata(
        self,
        column: str,
        metadata: dict[str, str],
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
    ) -> None: ...
    def __datafusion_table_provider__(self, session: Any | None = None) -> Any: ...
    def write(
        self,
        data: RecordBatchReader,
        batch_schema: ArrowSchema,
        partition_by: list[str] | None,
        mode: str,
        schema_mode: str | None,
        predicate: str | None,
        target_file_size: int | None,
        name: str | None,
        description: str | None,
        configuration: Mapping[str, str | None] | None,
        writer_properties: WriterProperties | None,
        commit_properties: CommitProperties | None,
        post_commithook_properties: PostCommitHookProperties | None,
    ) -> None: ...

def rust_core_version() -> str: ...
def init_tracing(endpoint: Optional[str] = None) -> None: ...
def shutdown_tracing() -> None: ...
def create_table_with_add_actions(
    table_uri: str,
    schema: Schema,
    add_actions: list[AddAction],
    mode: Literal["error", "append", "overwrite", "ignore"],
    partition_by: list[str],
    name: str | None,
    description: str | None,
    configuration: Mapping[str, str | None] | None,
    storage_options: dict[str, str] | None,
    commit_properties: CommitProperties | None,
    post_commithook_properties: PostCommitHookProperties | None,
) -> None: ...
def write_to_deltalake(
    table_uri: str,
    data: RecordBatchReader,
    batch_schema: ArrowSchema,
    partition_by: list[str] | None,
    mode: str,
    schema_mode: str | None,
    predicate: str | None,
    target_file_size: int | None,
    name: str | None,
    description: str | None,
    configuration: Mapping[str, str | None] | None,
    storage_options: dict[str, str] | None,
    writer_properties: WriterProperties | None,
    commit_properties: CommitProperties | None,
    post_commithook_properties: PostCommitHookProperties | None,
) -> None: ...
def convert_to_deltalake(
    uri: str,
    partition_schema: Schema | None,
    partition_strategy: Literal["hive"] | None,
    name: str | None,
    description: str | None,
    configuration: Mapping[str, str | None] | None,
    storage_options: dict[str, str] | None,
    commit_properties: CommitProperties | None,
    post_commithook_properties: PostCommitHookProperties | None,
) -> None: ...
def create_deltalake(
    table_uri: str,
    schema: Schema,
    partition_by: list[str],
    mode: str,
    raise_if_key_not_exists: bool,
    name: str | None,
    description: str | None,
    configuration: Mapping[str, str | None] | None,
    storage_options: dict[str, str] | None,
    commit_properties: CommitProperties | None,
    post_commithook_properties: PostCommitHookProperties | None,
) -> None: ...
def get_num_idx_cols_and_stats_columns(
    table: RawDeltaTable | None, configuration: Mapping[str, str | None] | None
) -> tuple[int, list[str] | None]: ...

class PyMergeBuilder:
    source_alias: str
    target_alias: str
    merge_schema: bool
    arrow_schema: ArrowSchema

    def when_matched_update(
        self, updates: dict[str, str], predicate: str | None
    ) -> None: ...
    def when_matched_delete(self, predicate: str | None) -> None: ...
    def when_not_matched_insert(
        self, updates: dict[str, str], predicate: str | None
    ) -> None: ...
    def when_not_matched_by_source_update(
        self, updates: dict[str, str], predicate: str | None
    ) -> None: ...
    def when_not_matched_by_source_delete(
        self,
        predicate: str | None,
    ) -> None: ...

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
     * "timestamp_ntz",
     * "decimal(<precision>, <scale>)" Max: decimal(38,38)

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
    def to_arrow(self) -> ArrowDataType:
        """Get the equivalent arro3 DataType (arro3.core.DataType)"""

    @staticmethod
    def from_arrow(type: ArrowSchemaExportable) -> PrimitiveType:
        """Create a PrimitiveType from an `ArrowSchemaExportable` datatype

        Will raise `TypeError` if the arrow type is not a primitive type.

        Args:
            type: an object that is `ArrowSchemaExportable`

        Returns:
            a PrimitiveType
        """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.schema()`][pyarrow.schema] to convert this
        array into a pyarrow schema, without copying memory.
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
    def from_json(json: str) -> ArrayType:
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
    def to_arrow(
        self,
    ) -> ArrowDataType:
        """Get the equivalent arro3 type."""
    @staticmethod
    def from_arrow(type: ArrowSchemaExportable) -> ArrayType:
        """Create an ArrayType from an `ArrowSchemaExportable` datatype.

        Will raise `TypeError` if a different arrow DataType is provided.

        Args:
            type: an object that is `ArrowSchemaExportable`

        Returns:
            an ArrayType
        """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.schema()`][pyarrow.schema] to convert this
        array into a pyarrow schema, without copying memory.
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
    def to_arrow(self) -> ArrowDataType:
        """Get the equivalent arro3 data type."""

    @staticmethod
    def from_arrow(type: ArrowSchemaExportable) -> MapType:
        """Create a MapType from an `ArrowSchemaExportable` datatype

        Will raise `TypeError` if passed a different type.

        Args:
            type: an object that is `ArrowSchemaExportable`

        Returns:
            a MapType
        """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.schema()`][pyarrow.schema] to convert this
        array into a pyarrow schema, without copying memory.
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
        metadata: dict[str, Any] | None = None,
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

    metadata: dict[str, Any]
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
    def to_arrow(self) -> ArrowField:
        """Convert to an equivalent arro3 field
        Note: This currently doesn't preserve field metadata.

        Returns:
            a arro3 Field
        """
    @staticmethod
    def from_arrow(field: ArrowSchemaExportable) -> Field:
        """Create a Field from an object with an `ArrowSchemaExportable` field

        Note: This currently doesn't preserve field metadata.

        Args:
            field: a Field object that is `ArrowSchemaExportable`

        Returns:
            a Field
        """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.schema()`][pyarrow.schema] to convert this
        array into a pyarrow schema, without copying memory.
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

    def __init__(self, fields: list[Field]) -> None: ...
    type: Literal["struct"]
    fields: list[Field]
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
    def to_arrow(self) -> ArrowDataType:
        """Get the equivalent arro3 DataType (arro3.core.DataType)"""

    @staticmethod
    def from_arrow(type: ArrowSchemaExportable) -> StructType:
        """Create a new StructType from an `ArrowSchemaExportable` datatype

        Will raise `TypeError` if a different data type is provided.

        Args:
            type: a struct type object that is `ArrowSchemaExportable`

        Returns:
            a StructType
        """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.schema()`][pyarrow.schema] to convert this
        array into a pyarrow schema, without copying memory.
        """

class Schema:
    def __init__(self, fields: list[Field]) -> None: ...
    fields: list[Field]

    invariants: list[tuple[str, str]]
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
    def to_arrow(self, as_large_types: bool = False) -> ArrowSchema:
        """Return equivalent arro3 schema

        Args:
            as_large_types: get schema with all variable size types (list, binary, string) as large variants (with int64 indices).
                This is for compatibility with systems like Polars that only support the large versions of Arrow types.

        Returns:
            an arro3 Schema
        """

    @staticmethod
    def from_arrow(type: ArrowSchemaExportable) -> Schema:
        """Create a [Schema][deltalake.schema.Schema] from a schema that implements Arrow C Data Interface.

        Will raise `TypeError` if one of the Arrow type is not a primitive type.

        Args:
            type: an object that is `ArrowSchemaExportable`

        Returns:
            a Schema
        """

    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.schema()`][pyarrow.schema] to convert this
        array into a pyarrow schema, without copying memory.
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
        table_uri: str,
        options: dict[str, str] | None = None,
        known_sizes: dict[str, int] | None = None,
    ) -> None: ...
    @classmethod
    def from_table(
        cls,
        table: RawDeltaTable,
        options: dict[str, str] | None = None,
        known_sizes: dict[str, int] | None = None,
    ) -> DeltaFileSystemHandler: ...
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

class PyQueryBuilder:
    def __init__(self) -> None: ...
    def register(self, table_name: str, delta_table: RawDeltaTable) -> None: ...
    def execute(self, sql: str) -> RecordBatchReader: ...

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
    """Raised when a violation with the Delta protocol specs occurred."""

    pass

class SchemaMismatchError(DeltaError):
    """Raised when a schema mismatch is detected."""

    pass

FilterLiteralType = tuple[str, str, Any]
FilterConjunctionType = list[FilterLiteralType]
FilterDNFType = list[FilterConjunctionType]
FilterType = FilterConjunctionType | FilterDNFType
PartitionFilterType = list[tuple[str, str, str | list[str]]]

class Transaction:
    app_id: str
    version: int
    last_updated: int | None

    def __init__(
        self, app_id: str, version: int, last_updated: int | None = None
    ) -> None: ...
