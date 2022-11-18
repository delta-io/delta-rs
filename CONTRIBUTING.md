# Contributing to delta-rs

Development on this project is mostly driven by volunteer contributors. We welcome new contributors, including not only those who develop new features, but also those who are able to help with documentation and provide detailed bug reports. 

Please take note of our [code of conduct](CODE_OF_CONDUCT.md).

## Good first issues 

These projects have been scoped and are available for new contributors to work on. If you are new to the project, consider picking one of these up. You can comment in the issue (linked) to claim it and ask any questions there; we are happy to guide your implementation.

* [ ] Write Rust user guide (#831): get the Rust docs homepage up to par with the Python user guide.
* [ ] Publish Python bindings to Conda (#735)
* [ ] Implement batch delete for faster vacuum (#408 / #407)
* [ ] Improve Python writer Pandas support (#686)
* [ ] Simple delete transactions (#832)

This list was last updated in September 2022. See more good first issues here: https://github.com/delta-io/delta-rs/contribute

And if you have an idea, feel free to open a new issue to discuss it.

## Roadmap 2022H2

These projects have been scoped and a contributor(s) who will work on them. They are also reflected on the official Delta Lake roadmap: https://delta.io/roadmap/

* [x] (P0) **Support V2 writer protocol** (#575; @wjones127) 2022H2
* [ ] (P1) **Data Acceptance Tests running** in CI (@wjones127) P0 for 2023H1 & P1 for 2022H2
* [ ] (P0) **First pass at Rust documentation**: Create some basic documentation that shows how to create a Delta Lake, how to read a Delta Lake into a Polars DataFrame, and how to append a Polars DataFrame to an existing Delta Lake.  Publish the Rust documentation, so it’s easily accessible. (@MrPowers) 
* [ ] (P0) **Blogging for Rust community**: grow Delta Rust awareness in the Rust community by blogging and posting in the Rust subreddit (@MrPowers)
* [ ] (P0) **Fully protocol compliant optimistic commit protocol (conflict resolution)**. - (#632) (@roeap) 2023H1 P0 for 2023H1 & P1 for 2022H2
* [ ] (P0) **Refactor Rust writer API to be flexible for others wishing to build upon delta-rs.** (#851) Separate three layers: First, a transaction layer for those who want to use their own Parquet writer to handle data writes (you write data; we write transaction); second, a parametrized writer layer, who want to use their own query engine but will use the built-in data writer (you verify data; we write data and transaction); third, a DataFusion-based writer that handles everything (verification, writing, transaction). Ideally we can design this so that delta-rs does most of the work enforcing the Delta Lake protocol (make it hard to mess up the table; possibly provide a test suite). (@wjones127, @roeap) 2023H1
* [ ] (P2) **Release improvements:** Commit to releasing more often; On Rust, at least in cadence with arrow-rs and datafusion. Enhance the change log. Try out dev releases of Python module. (@wjones127, @roeap) 2022H1

## Backlog

These issues are planned eventually, but need investigation or to be unblocked by work currently in the roadmap.

* Polars read support: make it possible to read a Delta Lake into a Polars DataFrame
* Polars write support: make it possible to write a Polars DataFrame to a Delta Lake
* Support operations that require rewriting data files (these are blocked on #851):
    * Delete transactions via Rust: Implement proper Delta delete row operations via the Rust interface.  Something like this:  deltaTable.delete("birthDate < '1955-01-01'")
    * Delete transactions via Python: Python bindings for the Delta delete row operations
    * Merge transactions via Rust: Full support for merge operations via Rust
    * Merge transactions via Python: Python bindings for merge operations
* Airbyte connector support to read/write Delta Table as a source using the Python binding CDK source and destination (Airbyte issue: 16322)
* Blog post on how to use delta-rs Airbyte connector
* Add Z-ORDER to the Rust bindings
* (P2) Some sort of file caching layer (#769). It would be equivalent to the Databricks disk cache, but as an object store (if that is viable).. 
* (P2) Substrait support (#830): Generate query plans for operations requiring current table data as substrait (https://substrait.io) plans to integrate with different query backends
