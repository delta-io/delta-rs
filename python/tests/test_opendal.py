"""Tests for generic OpenDAL storage backends.

Any OpenDAL service compiled into the extension auto-registers its URL scheme.
Service config is passed as ``storage_options`` using the ``opendal.<key>``
convention (e.g. ``opendal.endpoint``, ``opendal.region``); delta's own option
keys are kept out of the OpenDAL config.

Every service is reachable under ``opendal+<service>://`` (e.g.
``opendal+s3://``); services whose natural scheme does not collide with a
native delta backend also register that bare scheme (``hf://``, ``fs://``, …).

Tests here use only services that need no external accounts or network access.
"""

import pytest

from deltalake import DeltaTable, write_deltalake


@pytest.mark.pyarrow
@pytest.mark.parametrize(
    "uri", ["fs://localhost/my_table", "opendal+fs://localhost/my_table"]
)
def test_opendal_local_roundtrip(tmp_path, uri):
    import pyarrow as pa

    storage_options = {"opendal.root": str(tmp_path)}
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
