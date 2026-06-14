"""Tests for generic OpenDAL storage backends.

Any OpenDAL service compiled into the extension auto-registers its URL scheme.
Service config is passed as ``storage_options`` using the ``opendal.<key>``
convention (e.g. ``opendal.endpoint``, ``opendal.region``); delta's own option
keys are kept out of the OpenDAL config.

Every service is reachable under ``opendal+<service>://`` (e.g.
``opendal+s3://``); services whose natural scheme does not collide with a
native delta backend also register that bare scheme (``hf://``, ``fs://``, …).

The test below uses the local filesystem service under the ``fs://`` scheme,
so it runs without any network access or external account.
"""

import pytest

from deltalake import DeltaTable, write_deltalake


@pytest.mark.pyarrow
def test_opendal_fs_roundtrip(tmp_path):
    import pyarrow as pa

    # `opendal.root` scopes the operator at the temp dir; the URL path is the
    # table prefix within it. The host segment is unused for the fs service.
    storage_options = {"opendal.root": str(tmp_path)}
    uri = "fs://localhost/my_table"

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


@pytest.mark.pyarrow
def test_opendal_prefixed_scheme_roundtrip(tmp_path):
    import pyarrow as pa

    # Every service is also reachable under the unambiguous `opendal+<service>://`
    # scheme; this exercises that registration path (the `fs` service has no
    # native counterpart, but the prefixed form must work regardless).
    storage_options = {"opendal.root": str(tmp_path)}
    uri = "opendal+fs://localhost/my_table"

    data = pa.table({"id": pa.array([1, 2, 3], type=pa.int32())})

    write_deltalake(uri, data, storage_options=storage_options)

    dt = DeltaTable(uri, storage_options=storage_options)
    assert dt.version() == 0
    assert dt.to_pyarrow_table().num_rows == 3
