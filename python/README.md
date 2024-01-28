# Deltalake-python

[![PyPI](https://img.shields.io/pypi/v/deltalake.svg?style=flat-square)](https://pypi.org/project/deltalake/)
[![userdoc](https://img.shields.io/badge/docs-user-blue)](https://delta-io.github.io/delta-rs/)
[![apidoc](https://img.shields.io/badge/docs-api-blue)](https://delta-io.github.io/delta-rs/api/delta_table/)

Native [Delta Lake](https://delta.io/) Python binding based on
[delta-rs](https://github.com/delta-io/delta-rs) with
[Pandas](https://pandas.pydata.org/) integration.


## Example

```python
from deltalake import DeltaTable
dt = DeltaTable("../rust/tests/data/delta-0.2.0")
dt.version()
3
dt.files()
['part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet',
 'part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet',
 'part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet']
```

See the [user guide](https://delta-io.github.io/delta-rs/usage/installation/) for more examples.

## Installation

```bash
pip install deltalake
```

NOTE: official binary wheels are linked against openssl statically for remote
objection store communication. Please file Github issue to request for critical
openssl upgrade.


## Build custom wheels

Sometimes you may wish to build custom wheels. Maybe you want to try out some
unreleased features. Or maybe you want to tweak the optimization of the Rust code.

To compile the package, you will need the Rust compiler and [maturin](https://github.com/PyO3/maturin):

```sh
curl https://sh.rustup.rs -sSf | sh -s
pip install maturin
```

Then you can build wheels for your own platform like so:

```sh
maturin build --release --out wheels
```

For a build that is optimized for the system you are on (but sacrificing portability):

```sh
RUSTFLAGS="-C target-cpu=native" maturin build --release --out wheels
```

#### Cross compilation

The above command only works for your current platform. To create wheels for other
platforms, you'll need to cross compile. Cross compilation requires installing
two additional components: to cross compile Rust code, you will need to install
the target with `rustup`; to cross compile the Python bindings, you will need
to install `ziglang`.

The following example is for manylinux2014. Other targets will require different
Rust `target` and Python `compatibility` tags.

```sh
rustup target add x86_64-unknown-linux-gnu
pip install ziglang
```

Then you can build the wheel with:

```sh
maturin build --release --zig \
    --target x86_64-unknown-linux-gnu \
    --compatibility manylinux2014 \
    --out wheels
```

If you expect to only run on more modern system, you can set a newer `target-cpu`
flag to Rust and use a newer compatibility tag for Linux. For example, here
we set compatibility with CPUs newer than Haswell (2013) and Linux OS with 
glibc version of at least 2.24:

```sh
RUSTFLAGS="-C target-cpu=haswell" maturin build --release --zig \
    --target x86_64-unknown-linux-gnu \
    --compatibility manylinux_2_24 \
    --out wheels
```

See note about `RUSTFLAGS` from [the arrow-rs readme](https://github.com/apache/arrow-rs/blob/master/arrow/README.md#performance-tips).
