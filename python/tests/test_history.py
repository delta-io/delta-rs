"""Tests for DeltaTable.history().

Includes a deterministic reproduction of issue
https://github.com/delta-io/delta-rs/issues/4488: history() assigns version
numbers via a second, non-atomic call to get_latest_version(); when a commit
lands between the two calls every returned commit's version is shifted.
"""

from __future__ import annotations

import pathlib

from arro3.core import Table

from deltalake import DeltaTable, write_deltalake


class _RaceProxy:
    """Wraps the inner Rust _table handle and injects an extra commit
    between history() and get_latest_version() to reproduce issue #4488
    deterministically (no threading required)."""

    def __init__(self, real, inject):
        self._real = real
        self._inject = inject

    def history(self, limit):
        commits = self._real.history(limit)
        # Simulate a concurrent writer landing a commit in between the two
        # underlying calls made by DeltaTable.history().
        self._inject()
        return commits

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_history_versions_are_stable_under_concurrent_write(
    tmp_path: pathlib.Path, sample_table: Table
):
    for _ in range(3):
        write_deltalake(tmp_path, sample_table, mode="overwrite")

    dt = DeltaTable(tmp_path)
    expected_versions = [2, 1, 0]

    def inject_concurrent_commit():
        write_deltalake(tmp_path, sample_table, mode="overwrite")

    dt._table = _RaceProxy(dt._table, inject_concurrent_commit)

    history = dt.history()

    assert len(history) == 3
    actual_versions = [entry["version"] for entry in history]
    assert actual_versions == expected_versions, (
        f"history() returned shifted versions {actual_versions}; "
        f"expected {expected_versions}."
    )
