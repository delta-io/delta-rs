import pickle

from deltalake import DeltaTable


def test_table_serialization():
    table_path = "../crates/test/tests/data/simple_table"
    dt = DeltaTable(table_path)

    dt2 = pickle.loads(pickle.dumps(dt))

    # Do some sanity checks
    assert dt2.version() == dt.version()
    assert dt2.table_uri == dt.table_uri
