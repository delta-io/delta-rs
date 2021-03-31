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

NOTE: official binary wheels are linked against openssl statically for remote
objection store communication. Please file Github issue to request for critical
openssl upgrade.


Documentation
-----
<!--- Add the readthedocs link --->

Develop
-------

[maturin](https://github.com/PyO3/maturin) is used to build the python package.

To install development version of the package into your current Python environment:

```bash
$ maturin develop
```

Code are formatted with https://github.com/psf/black.


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
