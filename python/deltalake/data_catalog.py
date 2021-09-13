from enum import Enum


class DataCatalog(Enum):
    """List of the Data Catalogs"""

    AWS = "glue"  # Only AWS Glue Data Catalog is available
