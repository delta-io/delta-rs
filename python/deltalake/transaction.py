from dataclasses import dataclass
from typing import Dict, List, Literal, Mapping, Optional, Union

import pyarrow as pa

from ._internal import Transaction as Transaction
from ._internal import create_table_with_add_actions as _create_table_with_add_actions


@dataclass
class AddAction:
    path: str
    size: int
    partition_values: Mapping[str, Optional[str]]
    modification_time: int
    data_change: bool
    stats: str


@dataclass(init=True)
class PostCommitHookProperties:
    """The post commit hook properties, only required for advanced usecases where you need to control this."""

    def __init__(
        self,
        create_checkpoint: bool = True,
        cleanup_expired_logs: Optional[bool] = None,
    ):
        """Checkpoints are by default created based on the delta.checkpointInterval config setting.
        cleanup_expired_logs can be set to override the delta.enableExpiredLogCleanup, otherwise the
        config setting will be used to decide whether to clean up logs automatically by taking also
        the delta.logRetentionDuration into account.

        Args:
            create_checkpoint (bool, optional): to create checkpoints based on checkpoint interval. Defaults to True.
            cleanup_expired_logs (Optional[bool], optional): to clean up logs based on interval. Defaults to None.
        """
        self.create_checkpoint = create_checkpoint
        self.cleanup_expired_logs = cleanup_expired_logs


@dataclass(init=True)
class CommitProperties:
    """The commit properties. Controls the behaviour of the commit."""

    def __init__(
        self,
        custom_metadata: Optional[Dict[str, str]] = None,
        max_commit_retries: Optional[int] = None,
        app_transactions: Optional[List["Transaction"]] = None,
    ):
        """Custom metadata to be stored in the commit. Controls the number of retries for the commit.

        Args:
            custom_metadata: custom metadata that will be added to the transaction commit.
            max_commit_retries: maximum number of times to retry the transaction commit.
        """
        self.custom_metadata = custom_metadata
        self.max_commit_retries = max_commit_retries
        self.app_transactions = app_transactions


def create_table_with_add_actions(
    table_uri: str,
    schema: pa.Schema,
    add_actions: List[AddAction],
    mode: Literal["error", "append", "overwrite", "ignore"] = "error",
    partition_by: Optional[Union[List[str], str]] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    configuration: Optional[Mapping[str, Optional[str]]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    commit_properties: Optional[CommitProperties] = None,
    post_commithook_properties: Optional[PostCommitHookProperties] = None,
) -> None:
    if isinstance(partition_by, str):
        partition_by = [partition_by]
    _create_table_with_add_actions(
        table_uri=table_uri,
        schema=schema,
        add_actions=add_actions,
        mode=mode,
        partition_by=partition_by or [],
        name=name,
        description=description,
        configuration=configuration,
        storage_options=storage_options,
        commit_properties=commit_properties,
        post_commithook_properties=post_commithook_properties,
    )
