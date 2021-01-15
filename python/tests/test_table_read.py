from deltalake import DeltaTable


def test_read_simple_table_to_dict():
    table_path = "../rust/tests/data/simple_table"
    dt = DeltaTable(table_path)
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == {"id": [5, 7, 9]}
