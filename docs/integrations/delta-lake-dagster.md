# Using Delta Lake with Dagster¶

Delta Lake is a great storage format for Dagster workflows. This page will explain why and how to use Delta Lake with Dagster.

You will learn how to use the Delta Lake I/O Manager to read and write your Dagster Software-Defined Assets (SDAs). You will also learn about the unique advantages Delta Lake offers the Dagster community.

Here are some of the benefits that Delta Lake provides Dagster users:
- native PyArrow integration for lazy computation of large datasets,
- more efficient querying with file skipping via Z Ordering and liquid clustering
- built-in vacuuming to remove unnecessary files and versions
- ACID transactions for reliable writes
- smooth versioning integration so that versions can be use to trigger downstream updates.
- surfacing table stats based on the file statistics 


## Dagster I/O Managers
Dagster uses [I/O Managers](https://docs.dagster.io/concepts/io-management/io-managers#overview) to simplify data reads and writes. I/O Managers help you reduce boilerplate code by storing Dagster Asset and Op outputs and loading them as inputs to downstream objects. They make it easy to change where and how your data is stored.

You only need to define your I/O Manager and its settings (such as storage location and schema) once and the I/O Manager will take care of correctly reading and writing all your Dagster Assets automatically.

If you need lower-level access than the Dagster I/O Managers provide, take a look at the Delta Table Resource.

## The Delta Lake I/O Manager
You can easily read and write Delta Lake Tables from Dagster by using the `DeltaLakeIOManager()`. 

Install the DeltaLakeIOManager:

```
pip install dagster-deltalake
```

Next, configure the following settings in your project’s `__init__.py` file:
- `io_manager`: set this to `DeltaLakeIOManager()`, this sets the default I/O Manager for all your Assets

Within the DeltaLakeIOManager, define:
- `root_uri`: the root path where your Delta Tables will be created
- `storage_options`: configuration for accessing storage location
- `schema`: name of schema to use (optional, defaults to public)

```
defs = Definitions(
   assets=all_assets,
   resources={
        "io_manager": DeltaLakePyarrowIOManager(
            root_uri="path/to/deltalake",
            storage_options=LocalConfig(),
            schema="dagster_deltalake",
        ),
   },
)
```

Now, when you materialize an Asset, it will be saved as a Delta Lake in a folder `dagster_deltalake/asset_name` under the root directory `path/to/deltalake`.

The default Delta Lake I/O Manager supports Arrow reads and writes. You can also use the Delta Lake I/O Manager with [pandas](#using-delta-lake-and-dagster-with-pandas) or [polars](#using-delta-lake-and-dagster-with-polars). 

## Creating Delta Lake Tables with Dagster
You don’t need to do anything else to store your Dagster Assets as Delta Lake tables. The I/O Manager will handle storing and loading your Assets as Delta Lake tables from now on.

You can proceed to write Dagster code as you normally would. For example, you can create an Asset that reads in some toy data about animals and writes it out to an Arrow Table:

```
import pyarrow as pa
from pyarrow import csv

from dagster import asset

@asset
def raw_dataset() -> pa.Table:
   n_legs = pa.array([2, 4, None, 100])
   animals = pa.array(["Flamingo", "Horse", "Brittle stars", "Centipede"])
   data = {'n_legs': n_legs, 'animals': animals}

   return pa.Table.from_pydict(data)
```

When you materialize the Asset defined above (using the config settings defined earlier), the Delta Lake I/O Manager will create the table `dagster_deltalake/iris_dataset` if it doesn’t exist yet.

### Overwrites when Rematerializing Assets
If the table does already exist at the specified location, the Delta Lake I/O Manager will perform an overwrite. Delta Lake’s transaction log maintains a record of all changes to your Delta Lake tables. You can inspect the record of changes to your Delta Lake tables by taking a look at these transaction logs.

## Loading Delta Lake Tables in Downstream Assets
You can use Assets stored as Delta Lake tables as input to downstream Assets. Dagster and the Delta Lake I/O Manager make this easy for you.

You can write Dagster code as you normally would. Pass the upstream Asset as an argument to the downstream object to set up the dependency. Make sure to define the correct data type.

The Delta Lake I/O Manager will handle reading and writing the data from your Delta Lake.

```
import pyarrow as pa
from dagster import asset

# ... raw_dataset asset is defined here ...

@asset
def clean_dataset(raw_dataset: pa.Table) -> pa.Table:
   return raw_dataset.drop_null()
```

## Reading Existing Delta Lake Tables into Dagster
You can make existing Delta Lake tables (that were not created in Dagster) available to your Dagster assets. Use the `SourceAsset` object and pass the table name as the key argument: 

```
from dagster import SourceAsset

iris_harvest_data = SourceAsset(key="more_animal_data")
```

This will load a table `more_animal_data` located at `<root_uri>/<schema>` as configured in the Definitions object above (see [Delta Lake I/O Manager](#the-delta-lake-io-manager) section).

## Column Pruning
You can often improve the efficiency of your computations by only loading specific columns of your Delta table. This is called column pruning. 

With the Delta Lake I/O manager, you can select specific columns to load defining the `columns` in the `metadata` parameter of the `AssetIn` that loads the upstream Asset:

```
import pyarrow as pa
from dagster import AssetIn, asset

# this example uses the clean_dataset Asset defined earlier

@asset(
       ins={
           "mammal_bool": AssetIn(
               key="clean_dataset",
               metadata={"columns": ["is_mammal", "animals"]},
           )
       }
)
def mammal_data(mammal_bool: pa.Table) -> pa.Table:
   mammals = mammal_bool["is_mammal"].cast("bool")
   animals = mammal_bool["animals"]
   data = {"mammal_bool": mammals, "animals": animals}
   return pa.Table.from_pydict(data)
```

Here, we select only the `sepal_length_cm` and `sepal_width_cm` columns from the `iris_dataset` table and load them into an `AssetIn` object called `iris_sepal`. This AssetIn object is used to create a new Asset `sepal_data`, containing only the selected columns.

## Working with Partitioned Assets
Partitioning is an important feature of Delta Lake that can make your computations more efficient. The Delta Lake I/O manager helps you read and write partitioned data easily. You can work with static partitions, time-based partitions, multi-partitions, and dynamic partitions.

For example, you can partition the Iris dataset on the `species` column as follows:

```
import pyarrow as pa

from dagster import StaticPartitionsDefinition, asset

@asset(
  partitions_def=StaticPartitionsDefinition(
      ["Human", "Horse",]
  ),
  metadata={"partition_expr": "n_legs"},
)
def dataset_partitioned(
   context,
   clean_dataset: pa.Table,
   ) -> pa.Table:
   animals = context.asset_partition_key_for_output()
   table = clean_dataset

   return table.filter(pc.field("animals") == animals)
```

To partition your data, make sure to include the relevant `partitions_def` and `metadata` arguments to the `@asset` decorator. Refer to the Dagster documentation on [partitioning assets](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitioning-assets) for more information.

## Using Delta Lake and Dagster with Pandas
To read and write data to Delta Lake using pandas, use the `DeltaLakePandasIOManager()`. 

You will need to install it using:

```
pip install dagster-deltalake-pandas
```

In your `Definitions` object, change the `io_manager` to `DeltaLakePandasIOManager()`:

```
from dagster_deltalake_pandas import DeltaLakePandasIOManager


defs = Definitions(
   assets=all_assets,
   resources={
        "io_manager": DeltaLakePandasIOManager(
            root_uri="path/to/deltalake",
            storage_options=LocalConfig(),
            schema="dagster_deltalake",
        ),
   },
)
```

Now you can read and write Dagster Assets defined as pandas DataFrames in Delta Lake format. For example:

```
import pandas as pd
from dagster import asset

@asset
def iris_dataset() -> pd.DataFrame:
   return pd.read_csv(
       "https://docs.dagster.io/assets/iris.csv",
       names=[
           "sepal_length_cm",
           "sepal_width_cm",
           "petal_length_cm",
           "petal_width_cm",
           "species",
       ],
   )
```

## Using Delta Lake and Dagster with Polars
To read and write data to Delta Lake using polars, use the `DeltaLakePolarsIOManager()`. 

You will need to install it using:

```
pip install dagster-deltalake-polars
```

In your `Definitions` object, change the `io_manager` to `DeltaLakePolarsIOManager()`:

```
from dagster_deltalake_polars import DeltaLakePolarsIOManager

defs = Definitions(
   assets=all_assets,
   resources={
        "io_manager": DeltaLakePolarsIOManager(
            root_uri="path/to/deltalake",
            storage_options=LocalConfig(),
            schema="dagster_deltalake",
        ),
   },
)
```

Now you can read and write Dagster Assets defined as Polars DataFrames in Delta Lake format. For example:

```
import polars as pl
from dagster import asset


@asset
def iris_dataset() -> pl.DataFrame:
   return pl.read_csv(
       "https://docs.dagster.io/assets/iris.csv",
       new_columns=[
          "sepal_length_cm",
          "sepal_width_cm",
          "petal_length_cm",
          "petal_width_cm",
          "species",
      ],
   has_header=False
)
```

## Delta Lake Table Resource
I/O managers are a helpful tool in many common usage situations. But when you need lower-level access, the I/O Manager might not be the right tool to use. In these cases you may want to use the Delta Lake Table Resource.

The Delta Lake Table Resource is a low-level access method to the table object. It gives you more fine-grained control and allows for modeling of more complex data. You can also use the Table Resource to run optimization and vacuuming jobs.

## Schema and Constraint Enforcement
Delta Lake provides built-in checks to ensure schema consistency when appending data to a table, as well as the ability to evolve the schema. This is a great feature for the Dagster community as it prevents bad data from being appended to tables, ensuring data consistency and accuracy. 

Read more about how to add constraints to a table in [the Delta Lake documentation](https://delta-io.github.io/delta-rs/usage/constraints/).

## Z-Ordering
Delta Lake offers Z-ordering functionality to colocate similar data in the same files. This can make your Delta Table queries much more efficient via file skipping. Dagster users can now benefit from this great feature through the Delta Lake I/O Manager.

Read more about Z-Ordering on [the Delta Lake blog](https://delta.io/blog/2023-06-03-delta-lake-z-order/).

## Contribute
To contribute to the Delta Lake and Dagster integration, go to [link]
