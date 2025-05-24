from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from deltalake import Schema
from deltalake._internal import Transaction as Transaction
from deltalake._internal import (
    create_table_with_add_actions as _create_table_with_add_actions,
)


@dataclass
class AddAction:
    path: str
    size: int
    partition_values: Mapping[str, str | None]
    modification_time: int
    data_change: bool
    stats: str


@dataclass(init=True)
class PostCommitHookProperties:
    """The post commit hook properties, only required for advanced usecases where you need to control this."""

    def __init__(
        self,
        create_checkpoint: bool = True,
        cleanup_expired_logs: bool | None = None,
    ) -> None:
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
        custom_metadata: dict[str, str] | None = None,
        max_commit_retries: int | None = None,
        app_transactions: list[Transaction] | None = None,
    ) -> None:
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
    schema: Schema,
    add_actions: list[AddAction],
    mode: Literal["error", "append", "overwrite", "ignore"] = "error",
    partition_by: list[str] | str | None = None,
    name: str | None = None,
    description: str | None = None,
    configuration: Mapping[str, str | None] | None = None,
    storage_options: dict[str, str] | None = None,
    commit_properties: CommitProperties | None = None,
    post_commithook_properties: PostCommitHookProperties | None = None,
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
