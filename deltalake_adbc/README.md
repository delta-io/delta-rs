# deltalake_adbc (Experimental)

This experimental repo is an [ADBC](https://arrow.apache.org/blog/2023/01/05/introducing-arrow-adbc/) driver for Delta Lake, built on top of
delta-rs and [DataFusion](https://arrow.apache.org/datafusion/user-guide/introduction.html). It's primarily designed to be used by authors of
Delta Lake connectors who don't want to have to write all the complex logic
required for writers themselves.

The original design doc can be found here: https://docs.google.com/document/d/1ud-iBPg8VVz2N3HxySz9qbrffw6a9I7TiGZJ2MBs7ZE/edit?usp=sharing

For development instructions, see [Contributing to Delta Lake ADBC](./CONTRIBUTING.md).