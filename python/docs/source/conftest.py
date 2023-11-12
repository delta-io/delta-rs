import os
import pathlib
import shutil

import pytest


@pytest.fixture(autouse=True)
def doctest_setup(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path):
    # disable color for doctests so we don't have to include
    # escape codes in docstrings
    monkeypatch.setitem(os.environ, "NO_COLOR", "1")
    # Explicitly set the column width
    monkeypatch.setitem(os.environ, "COLUMNS", "80")

    # Mirror repository folder structure
    python_path = tmp_path / "python"
    data_path = tmp_path / "crates" / "deltalake-core" / "tests" / "data"
    python_path.mkdir()
    data_path.mkdir(parents=True)

    # Move test data into the temporary directory
    shutil.copytree(
        "../crates/deltalake-core/tests/data", str(data_path), dirs_exist_ok=True
    )
    # Work in a temporary directory
    monkeypatch.chdir(str(python_path))
