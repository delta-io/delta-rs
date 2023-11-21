import os
import pathlib

import pytest


@pytest.fixture(autouse=True)
def doctest_setup(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
):
    if isinstance(request.node, pytest.DoctestItem):
        # disable color for doctests so we don't have to include
        # escape codes in docstrings
        monkeypatch.setitem(os.environ, "NO_COLOR", "1")
        # Explicitly set the column width
        monkeypatch.setitem(os.environ, "COLUMNS", "80")
        # Work in a temporary directory
        monkeypatch.chdir(str(tmp_path))
