"""Tests for DeltaTable.history().

Includes a deterministic reproduction of issue
https://github.com/delta-io/delta-rs/issues/4488: history() must number the
returned commits from a version captured together with the table snapshot; when
a commit lands after the history call every returned commit's version would
otherwise be shifted.
"""

from __future__ import annotations

import pathlib
from collections.abc import Iterator

import pytest
from arro3.core import Table

from deltalake import DeltaTable, write_deltalake


class _RaceProxy:
    """Wraps the inner Rust _table handle and injects an extra commit right
    after the Rust history() call returns, to reproduce issue #4488
    deterministically (no threading required)."""

    def __init__(self, real, inject):
        self._real = real
        self._inject = inject

    def history(self, limit):
        commits = self._real.history(limit)
        # Simulate a concurrent writer landing a commit before the commits
        # are consumed by DeltaTable.history().
        self._inject()
        return commits

    def __getattr__(self, name):
        return getattr(self._real, name)


def _write_versions(path: pathlib.Path, data: Table, count: int) -> None:
    for _ in range(count):
        write_deltalake(path, data, mode="overwrite")


def test_history_versions_are_stable_under_concurrent_write(
    tmp_path: pathlib.Path, sample_table: Table
):
    _write_versions(tmp_path, sample_table, 3)

    dt = DeltaTable(tmp_path)
    expected_versions = [2, 1, 0]

    def inject_concurrent_commit():
        write_deltalake(tmp_path, sample_table, mode="overwrite")

    dt._table = _RaceProxy(dt._table, inject_concurrent_commit)

    history = list(dt.history())

    assert len(history) == 3
    actual_versions = [entry["version"] for entry in history]
    assert actual_versions == expected_versions, (
        f"history() returned shifted versions {actual_versions}; "
        f"expected {expected_versions}."
    )


def test_history_returns_lazy_iterator(tmp_path: pathlib.Path, sample_table: Table):
    _write_versions(tmp_path, sample_table, 3)
    dt = DeltaTable(tmp_path)

    history = dt.history()

    assert isinstance(history, Iterator)
    assert not isinstance(history, list)
    newest = next(history)
    assert newest["version"] == 2
    assert newest["operation"] == "WRITE"


def test_history_limit(tmp_path: pathlib.Path, sample_table: Table):
    _write_versions(tmp_path, sample_table, 4)
    dt = DeltaTable(tmp_path)

    assert [c["version"] for c in dt.history(2)] == [3, 2]
    assert [c["version"] for c in dt.history()] == [3, 2, 1, 0]


def test_history_partial_consumption(tmp_path: pathlib.Path, sample_table: Table):
    _write_versions(tmp_path, sample_table, 5)
    dt = DeltaTable(tmp_path)

    history = dt.history()
    assert next(history)["version"] == 4
    assert next(history)["version"] == 3
    del history

    # The table stays usable after an iterator is dropped half-way.
    assert [c["version"] for c in dt.history()] == [4, 3, 2, 1, 0]


def test_history_versions_stable_when_commit_lands_mid_iteration(
    tmp_path: pathlib.Path, sample_table: Table
):
    _write_versions(tmp_path, sample_table, 3)
    dt = DeltaTable(tmp_path)

    history = dt.history()
    first = next(history)
    write_deltalake(tmp_path, sample_table, mode="overwrite")
    rest = list(history)

    assert [first["version"]] + [c["version"] for c in rest] == [2, 1, 0]


def test_history_unconsumed_is_pinned_to_call_time(
    tmp_path: pathlib.Path, sample_table: Table
):
    _write_versions(tmp_path, sample_table, 3)
    dt = DeltaTable(tmp_path)

    history = dt.history()
    write_deltalake(tmp_path, sample_table, mode="overwrite")
    write_deltalake(tmp_path, sample_table, mode="overwrite")

    assert [c["version"] for c in history] == [2, 1, 0]


def test_history_exhausted_keeps_stopping(tmp_path: pathlib.Path, sample_table: Table):
    _write_versions(tmp_path, sample_table, 1)
    dt = DeltaTable(tmp_path)

    history = dt.history()
    assert len(list(history)) == 1
    with pytest.raises(StopIteration):
        next(history)
    with pytest.raises(StopIteration):
        next(history)


def test_raw_history_iterator_exhausted_keeps_stopping(
    tmp_path: pathlib.Path, sample_table: Table
):
    _write_versions(tmp_path, sample_table, 2)
    dt = DeltaTable(tmp_path)

    raw = dt._table.history(None)
    assert raw.latest_version == 1
    assert iter(raw) is raw
    assert len(list(raw)) == 2
    with pytest.raises(StopIteration):
        next(raw)
    with pytest.raises(StopIteration):
        next(raw)


def test_history_limit_zero_is_empty(tmp_path: pathlib.Path, sample_table: Table):
    _write_versions(tmp_path, sample_table, 2)
    dt = DeltaTable(tmp_path)

    assert list(dt.history(0)) == []
