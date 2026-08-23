<p align="center">
  <a href="https://delta.io/">
    <img src="https://github.com/delta-io/delta-rs/blob/main/docs\delta-rust-no-whitespace.svg?raw=true" alt="delta-rs logo" height="200">
  </a>
</p>
<p align="center">
  A native Rust library for Delta Lake, with bindings to Python
  <br>
  <a href="https://delta-io.github.io/delta-rs/">Python docs</a>
  ·
  <a href="https://docs.rs/deltalake/latest/deltalake/">Rust docs</a>
  ·
  <a href="https://github.com/delta-io/delta-rs/issues/new?template=bug_report.md">Report a bug</a>
  ·
  <a href="https://github.com/delta-io/delta-rs/issues/new?template=feature_request.md">Request a feature</a>
  ·
  <a href="https://github.com/delta-io/delta-rs/issues/1128">Roadmap</a>
  <br>
  <br>
  <a href="https://pypi.python.org/pypi/deltalake">
    <img alt="Deltalake" src="https://img.shields.io/pypi/l/deltalake.svg?style=flat-square&color=00ADD4&logo=apache">
  </a>
  <a target="_blank" href="https://github.com/delta-io/delta-rs" style="background:none">
    <img src="https://img.shields.io/github/stars/delta-io/delta-rs?logo=github&color=F75101">
  </a>
  <a target="_blank" href="https://crates.io/crates/deltalake" style="background:none">
    <img alt="Crate" src="https://img.shields.io/crates/v/deltalake.svg?style=flat-square&color=00ADD4&logo=rust" >
  </a>
  <a href="https://pypi.python.org/pypi/deltalake">
    <img alt="Deltalake" src="https://img.shields.io/pypi/v/deltalake.svg?style=flat-square&color=F75101&logo=pypi" >
  </a>
  <a href="https://pypi.python.org/pypi/deltalake">
    <img alt="Deltalake" src="https://img.shields.io/pypi/pyversions/deltalake.svg?style=flat-square&color=00ADD4&logo=python">
  </a>
  <a target="_blank" href="https://go.delta.io/slack">
    <img alt="#delta-rs in the Delta Lake Slack workspace" src="https://img.shields.io/badge/slack-delta-blue.svg?logo=slack&style=flat-square&color=F75101">
  </a>
</p>
Delta Lake is an open-source storage format that runs on top of existing data lakes. Delta Lake is compatible with processing engines like Apache Spark and provides benefits such as ACID transaction guarantees, schema enforcement, and scalable data handling.

The Delta Lake project aims to unlock the power of the Deltalake for as many users and projects as possible
by providing native low-level APIs aimed at developers and integrators, as well as a high-level operations
API that lets you query, inspect, and operate your Delta Lake with ease.

| Source                  | Downloads                         | Installation Command    | Docs            |
| ----------------------- | --------------------------------- | ----------------------- | --------------- |
| **[PyPi][pypi]**        | [![Downloads][pypi-dl]][pypi]     | `pip install deltalake` | [Docs][py-docs] |
| **[Crates.io][crates]** | [![Downloads][crates-dl]][crates] | `cargo add deltalake`   | [Docs][rs-docs] |

[pypi]: https://pypi.org/project/deltalake/
[pypi-dl]: https://img.shields.io/pypi/dm/deltalake?style=flat-square&color=00ADD4
[py-docs]: https://delta-io.github.io/delta-rs/
[rs-docs]: https://docs.rs/deltalake/latest/deltalake/
[crates]: https://crates.io/crates/deltalake
[crates-dl]: https://img.shields.io/crates/d/deltalake?color=F75101

## Quick Start

The `deltalake` library aims to adopt patterns from other libraries in data processing,
so getting started should look familiar.

```py3
from deltalake import DeltaTable, write_deltalake
import pandas as pd

# write some data into a delta table
df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"]})
write_deltalake("./data/delta", df)

# Load data from the delta table
dt = DeltaTable("./data/delta")
df2 = dt.to_pandas()

assert df.equals(df2)
```

The same table can also be loaded using the core Rust crate:

```rs
use deltalake::{open_table, DeltaTableError};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), DeltaTableError> {
    // open the table written in python
    let delta_path = Url::from_directory_path("/abs/data/delta").unwrap();
    let table = open_table(delta_path).await?;

    // show all active files in the table
    let files: Vec<_> = table.get_file_uris()?.collect();
    println!("{files:?}");

    Ok(())
}
```

You can also try Delta Lake in Docker at [DockerHub](https://go.delta.io/dockerhub) | [Docker Repo](https://go.delta.io/docker)

## Get Involved

The `delta-rs` team is [committed](https://github.com/delta-io/delta-rs/blob/main/CODE_OF_CONDUCT.md)
to providing a welcoming community.

- [Join us in our Slack workspace](https://go.delta.io/slack).
- [Report an issue](https://github.com/delta-io/delta-rs/issues/new?template=bug_report.md).
- Looking to contribute?
  - Read our [contributing guide](CONTRIBUTING.md).
  - See our [good first issues](https://github.com/delta-io/delta-rs/contribute).

## Integrations

Libraries and frameworks that interoperate with delta-rs - in alphabetical order.

- [AWS SDK for Pandas](https://github.com/aws/aws-sdk-pandas)
- [ballista][ballista]
- [datafusion][datafusion]
- [Daft](https://www.getdaft.io/)
- [Dask](https://github.com/dask-contrib/dask-deltatable)
- [datahub](https://datahubproject.io/)
- [DuckDB](https://duckdb.org/)
- [polars](https://www.pola.rs/)
- [Ray](https://github.com/delta-incubator/deltaray)

## Features

View the complete [feature table][feature-table].

[datafusion]: https://github.com/apache/datafusion
[ballista]: https://github.com/apache/arrow-ballista
[polars]: https://github.com/pola-rs/polars
[open]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueNeutral.svg
[semi-done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChangesGrey.svg
[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg
[roadmap]: https://github.com/delta-io/delta-rs/issues/1128
[writer-rs]: https://github.com/delta-io/delta-rs/issues/851
[onelake-rs]: https://github.com/delta-io/delta-rs/issues/1418
[feature-table]: https://delta-io.github.io/delta-rs/feature-table/
