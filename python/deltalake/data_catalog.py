from enum import Enum


class DataCatalog(Enum):
    """List of the Data Catalogs"""

    AWS = "glue"  # AWS Glue Data Catalog
    UNITY = "unity"  # Databricks Unity Catalog
