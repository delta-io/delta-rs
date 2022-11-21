import pathlib
import pytest
import pyarrow as pa

import adbc_driver_manager

# Based on: https://github.com/apache/arrow-adbc/blob/main/python/adbc_driver_manager/tests/test_lowlevel.py

@pytest.fixture
def delta_adbc():
    with adbc_driver_manager.AdbcDatabase(driver="adbc_driver_deltalake") as db:
        with adbc_driver_manager.AdbcConnection(db) as conn:
            yield (db, conn)


def _import(handle):
    """Helper to import a C Data Interface handle."""
    if isinstance(handle, adbc_driver_manager.ArrowArrayStreamHandle):
        return pa.RecordBatchReader._import_from_c(handle.address)
    elif isinstance(handle, adbc_driver_manager.ArrowSchemaHandle):
        return pa.Schema._import_from_c(handle.address)
    raise NotImplementedError(f"Importing {handle!r}")


def _bind(stmt, batch):
    array = adbc_driver_manager.ArrowArrayHandle()
    schema = adbc_driver_manager.ArrowSchemaHandle()
    batch._export_to_c(array.address, schema.address)
    stmt.bind(array, schema)


def test_read(delta_adbc):
    _, conn = delta_adbc

    with adbc_driver_manager.AdbcStatement(conn) as stmt:
        # TODO: support Substrait plans with ReadRel
        stmt.set_sql_query("SELECT * FROM '../rust/tests/data/simple_table'")

        handle, _ = stmt.execut_query()
        table = _import(handle).read_all()
    
    assert table.to_pydict() == {"id": [5, 7, 9]}


def test_write(delta_adbc, tmp_path: pathlib.Path, sample_data: pa.Table):
    _, conn = delta_adbc

    with adbc_driver_manager.AdbcStatement(conn) as stmt:
        stmt.set_options(**{adbc_driver_manager.INGEST_OPTION_TARGET_TABLE: str(tmp_path)})
        _bind(stmt, sample_data)
        stmt.execute_update()

        stmt.set_sql_query(f"SELECT * FROM '{str(tmp_path)}'")

        handle, _ = stmt.execute_query()
        table = _import(handle).read_all()

        assert table == sample_data
