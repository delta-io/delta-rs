"""
Reading Delta Tables with Ray
"""
# Standard Libraries
from typing import Optional, List, Dict, Any, Tuple

# External Libraries
from deltalake import DeltaTable

from ray.data import read_parquet
from ray.data.dataset import Dataset
from ray.data.datasource import DefaultParquetMetadataProvider
from ray.data._internal.arrow_block import ArrowRow

import numpy as np


def read_delta(
    table_uri: str,
    *,
    version: Optional[str] = None,
    storage_options: Optional[Dict[str, str]] = None,
    without_files: bool = False,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    columns: Optional[List[str]] = None,
    parallelism: int = -1,
    ray_remote_args: Dict[str, Any] = None,
    tensor_column_schema: Optional[Dict[str, Tuple[np.dtype, Tuple[int, ...]]]] = None,
    meta_provider=DefaultParquetMetadataProvider(),
    **arrow_parquet_args,
) -> Dataset[ArrowRow]:
    """Create an Arrow dataset from a Delta Table using Ray

    Examples:
        >>> import deltaray
        >>> # Read current version of Delta Table
        >>> ds = deltaray.read_delta(table_uri)
        >>> ds.show()

        >>> # Read specific version of Delta Table
        >>> ds = deltaray.read_delta(table_uri, version=3)
        >>> ds.show()

    Args:
        table_uri: path to the Delta Lake Table
        version: version of the Delta Lake Table
        storage_options: dictionary of options to use for storage backend
        without_files: if True, loads table without tracking underlying files
        filesystem: filesystem implementation to read from
        columns: list of columns to read
        parallelism: requested parallelism of read, may be limited by number of files
        ray_remote_args: kwargs passed to `ray.remote` in read tasks
        tensor_column_schema: a dictionary of column name to tensor dtype and
            shape mappings for converting Parquet columns to Ray's tensor
            column extension type.
        meta_provider: file metadata provider
        arrow_parquet_args: other Parquet read options to pass to pyarrow

    Returns:
        Dataset holding Arrow records read from the Delta Lake Table
    """
    dt = DeltaTable(table_uri, version, storage_options, without_files)
    return read_parquet(
        paths=dt.file_uris(),
        filesystem=filesystem,
        columns=columns,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        tensor_column_schema=tensor_column_schema,
        meta_provider=meta_provider,
        **arrow_parquet_args,
    )