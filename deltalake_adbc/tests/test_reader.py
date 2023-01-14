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


def _import_RBR(handle) -> pa.ipc.RecordBatchReader:
    if isinstance(handle, adbc_driver_manager.ArrowArrayStreamHandle):
        return pa.RecordBatchReader._import_from_c(handle.address)
    raise NotImplementedError(f"Importing {handle!r}")


def _import_schema(handle) -> pa.Schema:
    """Helper to import a C Data Interface handle."""
    if isinstance(handle, adbc_driver_manager.ArrowSchemaHandle):
        return pa.Schema._import_from_c(handle.address)
    raise NotImplementedError(f"Importing {handle!r}")


def test_table_types(delta_adbc):
    _, conn = delta_adbc
    handle = conn.get_table_types()
    table = _import_RBR(handle).read_all()
    assert "table" in table[0].to_pylist()


def test_get_table_schema(delta_adbc):
    _, conn = delta_adbc

    with adbc_driver_manager.AdbcStatement(conn) as stmt:
        # TODO: do we care about databases? They may come into play with external
        # stores
        stmt.set_sql_query(
            "CREATE EXTERNAL TABLE simple_table "
            "LOCATION '../rust/tests/data/simple_table'"
        )
        stmt.execute_query()

    handle = conn.get_table_schema(catalog=None, db_schema=None, table_name="simple_table")

    expected_schema = pa.schema([pa.field("id", pa.int64(), nullable=True)])

    assert expected_schema == _import_schema(handle)


def test_register(delta_adbc):
    _, conn = delta_adbc

    with adbc_driver_manager.AdbcStatement(conn) as stmt:
        stmt.set_sql_query(
            "CREATE EXTERNAL TABLE simple_table "
            "LOCATION '../rust/tests/data/simple_table'"
        )
        stmt.execute_query()

    handle = conn.get_objects(adbc_driver_manager.GetObjectsDepth.ALL)
    table = _import_RBR(handle).read_all()

    # There is just one catalog, the null one?
    assert table.num_rows == 1
    assert table["catalog_name"][0].as_py() == None

    # There is just one schema, the null one?
    db_schemas = pa.concat_arrays(table[1].chunks).flatten()
    assert db_schemas.num_rows == 1
    assert db_schemas["db_schema_name"][0].as_py() == None

    # There is just one table, named "simple_table"
    tables = db_schemas.flatten()[1].flatten()
    assert tables.num_rows == 1
    assert table["table_name"][0].as_py() == "simple_table"
    # TODO: What should table types be? All are external and we have no notion of views
    assert table["table_type"][0].as_py() == "table"

    # Just one column id, which is long (nullable)
    columns = tables.flatten()[2].flatten()
    assert columns.num_rows == 1
    assert columns[0].as_py() == {
        "column_name": "id",
        "ordinal_position": 1,
        "remarks": None,
        "xdbc_data_type": None,
        "xdbc_type_name": None,
        "xdbc_column_size": None,
        "xdbc_decimal_digits": None,
        "xdbc_num_prec_radix": None,
        "xdbc_nullable": None,
        "xdbc_column_def": None,
        "xdbc_sql_data_type": None,
        "xdbc_datetime_sub": None,
        "xdbc_char_octet_length": None,
        "xdbc_is_nullable": None,
        "xdbc_scope_catalog": None,
        "xdbc_scope_schema": None,
        "xdbc_scope_table": None,
        "xdbc_is_autoincrement": None,
        "xdbc_is_generatedcolumn": None,
    }
