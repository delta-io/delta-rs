Deltalake-python
================

[![PyPI](https://img.shields.io/pypi/v/deltalake.svg?style=flat-square)](https://pypi.org/project/deltalake/)

Native [Delta Lake](https://delta.io/) binding for Python based on
[delta-rs](https://github.com/delta-io/delta-rs).


Installation
------------

```bash
pip install deltalake
```


Usage
-----

Resolve partitions for current version of the DeltaTable:

```
>>> from deltalake import DeltaTable
>>> dt = DeltaTable("../rust/tests/data/delta-0.2.0")
>>> dt.version()
3
>>> dt.files()
['part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet', 'part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet', 'part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet']
```

Convert DeltaTable into PyArrow Table and Pandas Dataframe:

```
>>> from deltalake import DeltaTable
>>> dt = DeltaTable("../rust/tests/data/simple_table")
>>> df = dt.to_pyarrow_table().to_pandas()
>>> df
   id
0   5
1   7
2   9
>>> df[df['id'] > 5]
   id
1   7
2   9
```

Time travel:

```
>>> from deltalake import DeltaTable
>>> dt = DeltaTable("../rust/tests/data/simple_table")
>>> dt.load_version(2)
>>> dt.to_pyarrow_table().to_pandas()
   id
0   5
1   7
2   9
3   5
4   6
5   7
6   8
7   9
```


Develop
-------

[maturin](https://github.com/PyO3/maturin) is used to build the python package.

To install development version of the package into your current Python environment:

```bash
$ maturin develop
```

Build manylinux wheels
----------------------

```bash
docker run -e PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig -it -v `pwd`:/io apache/arrow-dev:amd64-centos-6.10-python-manylinux2010 bash
curl https://sh.rustup.rs -sSf | sh -s -- -y
source $HOME/.cargo/env
rustup default stable
cargo install --git https://github.com/PyO3/maturin.git --rev 98636cea89c328b3eba4ebb548124f75c8018200 maturin
cd /io/python
export PATH=/opt/python/cp37-cp37m/bin:/opt/python/cp38-cp38/bin:$PATH
maturin publish -b pyo3 --target x86_64-unknown-linux-gnu --no-sdist
```
