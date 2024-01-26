# Using Delta Lake with Dask

Delta Lake is a great storage format for Dask analyses. This page explains why and how to use Delta Lake with Dask.

You will learn how to read Delta Lakes into Dask DataFrames, how to query Delta tables with Dask, and the unique advantages Delta Lake offers the Dask community.

Here are some of the benefits that Delta Lake provides Dask users:
- better performance with file skipping
- enhanced file skipping via Z Ordering
- ACID transactions for reliable writes
- easy time-travel functionality

> ❗️ dask-deltatable currently works with deltalake<=13.0. See https://github.com/dask-contrib/dask-deltatable/issues/65

## Install Dask-Deltatable

To use Delta Lake with Dask, first install the library using

```
pip install dask-deltatable
```


## Reading Delta Tables into a Dask DataFrame

You can read data stored in a Delta Lake into a Dask DataFrame using `dask-deltatable.read_deltalake`. For example:

```
import dask_deltatable as ddt

delta_path = "../../data/people_countries_delta_dask/"
ddf = ddt.read_deltalake(delta_path)
```

Dask is a library for efficient distributed computing and works with lazy evaluation. Function calls to `dask.dataframe` build a task graph in the background. To trigger computation, call `.compute()`:

```
ddf.compute()
```

You can read in specific versions of Delta tables by specifying a `version` number or a timestamp:

```
# with specific version
ddf = ddt.read_deltalake(delta_path, version=3)

# with specific datetime
ddt.read_deltalake(delta_path, datetime="2018-12-19T16:39:57-08:00")```

`dask-deltatable` also supports reading from remote sources like S3 with:

```
ddt.read_deltalake("s3://bucket_name/delta_path", version=3)
```

> To read data from remote sources you'll need to make sure the credentials are properly configured in environment variables or config files. Refer to your cloud provider documentation to configure these.

## What can I do with a Dask Deltatable?

Reading a Delta Lake in with `dask-deltatable` returns a regular Dask DataFrame. You can perform [all the regular Dask operations](https://docs.dask.org/en/stable/dataframe.html) on this DataFrame.


