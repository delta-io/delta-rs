from deltalake import rust_core_version


def test_version() -> None:
    v = rust_core_version()
    assert len(v.split(".")) == 3
