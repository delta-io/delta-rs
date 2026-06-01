"""Tests for generic OpenDAL storage backends.

Any OpenDAL service compiled into the extension auto-registers its URL scheme.
Service config is passed as ``storage_options`` using the ``opendal.<key>``
convention (e.g. ``opendal.endpoint``, ``opendal.region``); delta's own option
keys are kept out of the OpenDAL config.

The test below uses the local filesystem service under the ``opendalfs://``
scheme, so it runs without any network access or external account.
"""

import pyarrow as pa

from deltalake import DeltaTable, write_deltalake


def test_opendal_fs_roundtrip(tmp_path):
    # `opendal.root` scopes the operator at the temp dir; the URL path is the
    # table prefix within it. The host segment is unused for the fs service.
    storage_options = {"opendal.root": str(tmp_path)}
    uri = "opendalfs://localhost/my_table"

    data = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["a", "b", "c"], type=pa.string()),
        }
    )

    write_deltalake(uri, data, storage_options=storage_options)
    write_deltalake(uri, data, mode="append", storage_options=storage_options)

    dt = DeltaTable(uri, storage_options=storage_options)
    assert dt.version() == 1

    result = dt.to_pyarrow_table().sort_by("id")
    assert result.num_rows == 6
    assert result.column("id").to_pylist() == [1, 1, 2, 2, 3, 3]
