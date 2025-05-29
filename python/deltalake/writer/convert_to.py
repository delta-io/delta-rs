from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Literal,
)

from deltalake._internal import Schema
from deltalake._internal import convert_to_deltalake as _convert_to_deltalake
from deltalake.writer._utils import try_get_deltatable

if TYPE_CHECKING:
    from deltalake.transaction import (
        CommitProperties,
        PostCommitHookProperties,
    )


def convert_to_deltalake(
    uri: str | Path,
    mode: Literal["error", "ignore"] = "error",
    partition_by: Schema | None = None,
    partition_strategy: Literal["hive"] | None = None,
    name: str | None = None,
    description: str | None = None,
    configuration: Mapping[str, str | None] | None = None,
    storage_options: dict[str, str] | None = None,
    commit_properties: CommitProperties | None = None,
    post_commithook_properties: PostCommitHookProperties | None = None,
) -> None:
    """
    `Convert` parquet tables `to delta` tables.

    Currently only HIVE partitioned tables are supported. `Convert to delta` creates
    a transaction log commit with add actions, and additional properties provided such
    as configuration, name, and description.

    Args:
        uri: URI of a table.
        partition_by: Optional partitioning schema if table is partitioned.
        partition_strategy: Optional partition strategy to read and convert
        mode: How to handle existing data. Default is to error if table already exists.
            If 'ignore', will not convert anything if table already exists.
        name: User-provided identifier for this table.
        description: User-provided description for this table.
        configuration: A map containing configuration options for the metadata action.
        storage_options: options passed to the native delta filesystem. Unused if 'filesystem' is defined.
        commit_properties: properties of the transaction commit. If None, default values are used.
        post_commithook_properties: properties for the post commit hook. If None, default values are used.
    """
    if partition_by is not None and partition_strategy is None:
        raise ValueError("Partition strategy has to be provided with partition_by.")

    if partition_strategy is not None and partition_strategy != "hive":
        raise ValueError(
            "Currently only `hive` partition strategy is supported to be converted."
        )

    if mode == "ignore" and try_get_deltatable(uri, storage_options) is not None:
        return

    _convert_to_deltalake(
        uri=str(uri),
        partition_schema=partition_by,
        partition_strategy=partition_strategy,
        name=name,
        description=description,
        configuration=configuration,
        storage_options=storage_options,
        commit_properties=commit_properties,
        post_commithook_properties=post_commithook_properties,
    )
    return
