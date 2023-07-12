from enum import Enum


class DataCatalog(Enum):
    """List of the Data Catalogs"""

    AWS = "glue"
    """
    Refers to the
    `AWS Glue Data Catalog <https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html>`_
    """

    UNITY = "unity"
    """
    Refers to the
    `Databricks Unity Catalog <https://docs.databricks.com/data-governance/unity-catalog/index.html>`_
    """
