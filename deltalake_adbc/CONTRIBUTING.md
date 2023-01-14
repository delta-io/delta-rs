# Contributing to Delta Lake ADBC

The ADBC connector is a Rust project that builds a dynamic library.

The tests are written in Python using the `adbc_driver_manager` package

To build the library:

```sh
cargo build
```

To setup tests:

```sh
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Run tests:

```sh
export DYLD_LIBRARY_PATH=$(pwd)/target/debug
pytest tests
```
