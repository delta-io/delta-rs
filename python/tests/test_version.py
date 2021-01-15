from deltalake import rust_core_version


def test_read_simple_table_to_dict():
    v = rust_core_version()
    assert len(v.split(".")) == 3
