Deltalake-python
================

[![PyPI](https://img.shields.io/pypi/v/deltalake.svg?style=flat-square)](https://pypi.org/project/deltalake/)
[![userdoc](https://img.shields.io/badge/docs-user-blue)](https://delta-io.github.io/delta-rs/python/)
[![apidoc](https://img.shields.io/badge/docs-api-blue)](https://delta-io.github.io/delta-rs/python/api_reference.html)

Native [Delta Lake](https://delta.io/) Python binding based on
[delta-rs](https://github.com/delta-io/delta-rs) with
[Pandas](https://pandas.pydata.org/) integration.


Installation
------------

```bash
pip install deltalake
```

NOTE: official binary wheels are linked against openssl statically for remote
objection store communication. Please file Github issue to request for critical
openssl upgrade.


Develop
-------

#### Setup your local environment with virtualenv
```bash
$ make setup-venv
```

#### Activate it
```bash
$ source ./venv/bin/activate
```

#### Ready to develop with maturin

[maturin](https://github.com/PyO3/maturin) is used to build the python package.

To install development version of the package into your current Python environment:

```bash
$ make develop
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

#### PyPI release

Publish a new GitHub release with name and tag version set to `python-vx.y.z`.
This will trigger our automated release pipeline.
