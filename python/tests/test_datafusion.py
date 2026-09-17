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


@pytest.mark.parametrize("installed", ["53.0.0", "54.0.0", "56.0.0", "invalid"])
def test_datafusion_table_provider_incompatible_version_errors(
    tmp_path, monkeypatch, installed
):
    calls = []

    def fake_version(pkg: str) -> str:
        calls.append(pkg)
        return installed

    monkeypatch.setattr("importlib.metadata.version", fake_version)
    table = Table(
        {"id": Array([1, 2, 3], Field("id", type=DataType.int64(), nullable=True))}
    )
    write_deltalake(tmp_path, table)
    dt = DeltaTable(tmp_path)

    # Reject incompatible consumers before exporting a capsule or reading a session capsule.
    class UnreachableSession:
        def __datafusion_task_context_provider__(self):
            pytest.fail(
                "incompatible consumers must be rejected before accessing their session"
            )

    with pytest.raises(RuntimeError, match="datafusion==55"):
        dt.__datafusion_table_provider__(session=UnreachableSession())
    assert calls == ["datafusion"]


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


def test_datafusion_table_provider_accepts_session_keyword_argument(
    tmp_path, monkeypatch
):
    def fake_version(pkg: str) -> str:
        assert pkg == "datafusion"
        return "55.0.0"

    monkeypatch.setattr("importlib.metadata.version", fake_version)

    table = Table(
        {"id": Array([1, 2, 3], Field("id", type=DataType.int64(), nullable=True))}
    )
    write_deltalake(tmp_path, table)
    dt = DeltaTable(tmp_path)

    # DataFusion 53+ calls this hook with a session argument.
    capsule = dt.__datafusion_table_provider__(session=object())  # type: ignore[call-arg]
    assert capsule is not None


def test_datafusion_table_provider_invalid_task_ctx_capsule_name_errors(
    tmp_path, monkeypatch
):
    import ctypes

    def fake_version(pkg: str) -> str:
        assert pkg == "datafusion"
        return "55.0.0"

    monkeypatch.setattr("importlib.metadata.version", fake_version)

    pycapsule_new = ctypes.pythonapi.PyCapsule_New
    pycapsule_new.restype = ctypes.py_object
    pycapsule_new.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]

    class BadSession:
        def __datafusion_task_context_provider__(self):
            return pycapsule_new(ctypes.c_void_p(1), b"wrong_name", None)

    table = Table(
        {"id": Array([1, 2, 3], Field("id", type=DataType.int64(), nullable=True))}
    )
    write_deltalake(tmp_path, table)
    dt = DeltaTable(tmp_path)

    with pytest.raises(
        ValueError, match="PyCapsule_GetPointer called with incorrect name"
    ):
        dt.__datafusion_table_provider__(session=BadSession())  # type: ignore[call-arg]


@pytest.mark.datafusion
def test_datafusion_table_provider(tmp_path):
    if os.environ.get("DELTALAKE_RUN_DATAFUSION_TESTS") != "1":
        pytest.skip(
            "DataFusion Python integration tests are disabled by default; set DELTALAKE_RUN_DATAFUSION_TESTS=1"
        )

    # Skip until matching wheels are available; the runtime guard requires major 55.
    datafusion_major = _datafusion_major_version()
    if datafusion_major != 55:
        pytest.skip("DataFusion Python integration requires datafusion==55.x wheels")
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

    # DataFusion 53 can materialize string columns as Utf8View while our fixture uses Utf8.
    # Compare row content instead of requiring an exact Arrow string storage type match.
    import pyarrow as pa

    actual = pa.table(Table.from_arrow(data))
    expected = pa.table(table)
    assert actual.column_names == expected.column_names
    assert actual.to_pylist() == expected.to_pylist()
