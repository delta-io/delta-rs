import pandas as pd
import pytest

from deltalake import DeltaTable


@pytest.mark.s3
@pytest.mark.integration
@pytest.mark.timeout(timeout=5, method="thread")
def test_read_simple_table_to_dict():
    table_path = "s3://deltars/simple"
    dt = DeltaTable(table_path)
    assert dt.to_pandas().equals(pd.DataFrame({"id": [5, 7, 9]}))
