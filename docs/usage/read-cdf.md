# Reading the Change Data Feed from a Delta Table

Reading the CDF data from a table with change data is easy.

The `delta.enableChangeDataFeed` configuration needs to be set to `true` when creating the delta table.

## Reading CDF Log

{{ code_example('read_cdf', None, []) }}

The output can then be used in various execution engines. The python example shows how one might
consume the cdf feed inside polars.