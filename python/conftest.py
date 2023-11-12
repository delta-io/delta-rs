import os
import pathlib
import shutil

import pytest

_REQUIRES_TEST_DATA_IN_TMP_PATH = ["usage.rst"]


@pytest.fixture(autouse=True)
def doctest_setup(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
):
    if isinstance(request.node, pytest.DoctestItem):
        if request.node.name in _REQUIRES_TEST_DATA_IN_TMP_PATH:
            # Mirror repository folder structure and copy data into temp path
            python_path = tmp_path / "python"
            data_path = tmp_path / "crates" / "deltalake-core" / "tests" / "data"
            python_path.mkdir()
            shutil.copytree("../crates/deltalake-core/tests/data", str(data_path))
            tmp_path = python_path

        # disable color for doctests so we don't have to include
        # escape codes in docstrings
        monkeypatch.setitem(os.environ, "NO_COLOR", "1")
        # Explicitly set the column width
        monkeypatch.setitem(os.environ, "COLUMNS", "80")
        # Work in a temporary directory
        monkeypatch.chdir(str(tmp_path))
