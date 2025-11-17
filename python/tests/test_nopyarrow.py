import pytest

from deltalake import DeltaTable


@pytest.mark.no_pyarrow
def test_import_error_reading_pyarrow(existing_sample_table: DeltaTable):
    with pytest.raises(ImportError, match="pyarrow"):
        existing_sample_table.to_pyarrow_dataset()

    with pytest.raises(ImportError, match="pyarrow"):
        existing_sample_table.to_pyarrow_table()

    with pytest.raises(ImportError, match="pyarrow"):
        existing_sample_table.to_pandas()


@pytest.mark.no_pyarrow
def test_import_error_storage_handler_from_table(existing_sample_table: DeltaTable):
    from deltalake.fs import DeltaStorageHandler

    with pytest.raises(ImportError, match="pyarrow"):
        DeltaStorageHandler.from_table(existing_sample_table)


@pytest.mark.no_pyarrow
def test_import_error_storage_handler_init():
    from deltalake.fs import DeltaStorageHandler

    with pytest.raises(ImportError, match="pyarrow"):
        DeltaStorageHandler(table_uri="random")
