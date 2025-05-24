import os

import pytest

from deltalake import DeltaTable
from deltalake._internal import DeltaError


@pytest.mark.unitycatalog_databricks
@pytest.mark.pyarrow
@pytest.mark.integration
@pytest.mark.timeout(timeout=10, method="thread")
def test_uc_read_deltatable():
    """Test delta table reads using Unity Catalog URL (uc://)"""

    os.environ["DATABRICKS_WORKSPACE_URL"] = "http://localhost:8080"
    os.environ["DATABRICKS_ACCESS_TOKEN"] = "123456"
    os.environ["UNITY_ALLOW_HTTP_URL"] = "true"

    dt = DeltaTable("uc://unity.default.testtable")

    assert dt.is_deltatable(dt.table_uri), True
    expected = {
        "value": ["1", "2", "3", "6", "7", "5", "4"],
        "year": ["2020", "2020", "2020", "2021", "2021", "2021", "2021"],
        "month": ["1", "2", "2", "12", "12", "12", "4"],
        "day": ["1", "3", "5", "20", "20", "4", "5"],
    }
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == expected


@pytest.mark.unitycatalog_oss
@pytest.mark.integration
@pytest.mark.pyarrow
@pytest.mark.timeout(timeout=10, method="thread")
def test_uc_read_deltatable_failing():
    """Test delta table reads using Unity Catalog URL (uc://)"""

    os.environ["DATABRICKS_WORKSPACE_URL"] = "http://localhost:8080"
    os.environ["DATABRICKS_ACCESS_TOKEN"] = "123456"
    os.environ["UNITY_ALLOW_HTTP_URL"] = "true"

    # @TODO: Currently, this will fail when used with Unity Catalog OSS
    # mock APIs. Need to add support for slightly different response payloads
    # of Unity Catalog OSS.
    with pytest.raises(DeltaError):
        dt = DeltaTable("uc://unity.default.testtable")
        assert dt.is_deltatable(dt.table_uri), True
        expected = {
            "value": ["1", "2", "3", "6", "7", "5", "4"],
            "year": ["2020", "2020", "2020", "2021", "2021", "2021", "2021"],
            "month": ["1", "2", "2", "12", "12", "12", "4"],
            "day": ["1", "3", "5", "20", "20", "4", "5"],
        }
        assert dt.to_pyarrow_dataset().to_table().to_pydict() == expected
