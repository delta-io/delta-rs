import os
from importlib.metadata import PackageNotFoundError, version

import pytest
from arro3.core import Array, DataType, Field, Table

from deltalake import DeltaTable, write_deltalake


def _datafusion_major_version() -> int | None:
    try:
        return int(version("datafusion").split(".")[0])
    except (PackageNotFoundError, ValueError):
        return None


def test_datafusion_table_provider_incompatible_version_errors(tmp_path, monkeypatch):
    # Force the runtime check to behave like "datafusion==51.x" even if datafusion isn't installed.
    call_count = {"count": 0}

    def fake_version(pkg: str) -> str:
        assert pkg == "datafusion"
        call_count["count"] += 1
        if call_count["count"] == 1:
            return "51.0.0"
        return "52.0.0"

    monkeypatch.setattr("importlib.metadata.version", fake_version)

    table = Table(
        {"id": Array([1, 2, 3], Field("id", type=DataType.int64(), nullable=True))}
    )
    write_deltalake(tmp_path, table)
    dt = DeltaTable(tmp_path)

    # Call the FFI export hook directly; DO NOT call SessionContext.register_* (that is what segfaults).
    with pytest.raises(RuntimeError) as exc_info:
        dt.__datafusion_table_provider__()  # type: ignore[attr-defined]

    assert call_count["count"] == 1

    msg = str(exc_info.value)
    assert "datafusion" in msg
    assert "datafusion==52" in msg
    assert "QueryBuilder" in msg


def test_datafusion_table_provider_not_installed_errors(tmp_path, monkeypatch):
    def fake_version(pkg: str) -> str:
        raise PackageNotFoundError(pkg)

    monkeypatch.setattr("importlib.metadata.version", fake_version)

    table = Table(
        {"id": Array([1, 2, 3], Field("id", type=DataType.int64(), nullable=True))}
    )
    write_deltalake(tmp_path, table)
    dt = DeltaTable(tmp_path)

    with pytest.raises(RuntimeError) as exc_info:
        dt.__datafusion_table_provider__()  # type: ignore[attr-defined]

    msg = str(exc_info.value)
    assert "datafusion" in msg
    assert "not installed" in msg.lower()
    assert "QueryBuilder" in msg


@pytest.mark.datafusion
def test_datafusion_table_provider(tmp_path):
    if os.environ.get("DELTALAKE_RUN_DATAFUSION_TESTS") != "1":
        pytest.skip(
            "DataFusion Python integration tests are disabled by default; set DELTALAKE_RUN_DATAFUSION_TESTS=1"
        )

    datafusion_major = _datafusion_major_version()
    if datafusion_major is None or datafusion_major < 52:
        pytest.skip(
            "DataFusion Python integration requires datafusion>=52 wheels (PyPI currently provides 51.x)"
        )
    nrows = 5
    table = Table(
        {
            "id": Array(
                ["1", "2", "3", "4", "5"],
                Field("id", type=DataType.string(), nullable=True),
            ),
            "price": Array(
                list(range(nrows)), Field("price", type=DataType.int64(), nullable=True)
            ),
            "sold": Array(
                list(range(nrows)), Field("sold", type=DataType.int32(), nullable=True)
            ),
            "deleted": Array(
                [False] * nrows, Field("deleted", type=DataType.bool(), nullable=True)
            ),
        },
    )

    from datafusion import SessionContext

    write_deltalake(tmp_path, table)

    dt = DeltaTable(tmp_path)

    session = SessionContext()
    session.register_table("tbl", dt)
    data = session.sql("SELECT * FROM tbl")

    assert Table.from_arrow(data) == table
