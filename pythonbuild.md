<!--
Python Binding Build Instructions
Save this file as `pythonbuild.md` to guide building and testing the Python bindings.
-->
# Building the Python Bindings

Follow these steps to rebuild and install the Python bindings against your local Rust code, then validate the new global-sort feature.

## 1. Enter the Python directory
```bash
cd python
```

## 2. Ensure `pip` is accessible to maturin
Maturin needs to invoke `pip`. If `pip` isn’t on your PATH, you can shim it:
```bash
cat << 'EOF' > /usr/local/bin/pip
#!/usr/bin/env bash
python3 -m pip "$@"
EOF
chmod +x /usr/local/bin/pip
```

## 2.5 Create and activate a local virtual environment
If you haven't already set up a `.venv` folder for isolating Python dependencies,
create and activate one:
```bash
python3 -m venv .venv
source .venv/bin/activate
```
This ensures `pip` and Python commands refer to your local environment.

## 3. Install `maturin` and develop-install
```bash
# If a previous `deltalake` package is installed, remove it first
pip uninstall -y deltalake

python3 -m pip install maturin
# Build and install into the current venv
maturin develop --release
```
This compiles the Rust extension in release mode and installs it into your current Python environment.

## 4. Verify the new API
```bash
python3 - << 'EOF'
from deltalake import DeltaTable
help(DeltaTable.optimize.compact)
EOF
```
You should see `sort_columns`, `sort_ascending`, and `nulls_first` in the signature.

## 5. Run your example or test

### Standalone script with local bindings
When running against the *local* `python/` folder, Python may still load a stale extension. You can force usage of the freshly built wheel by:
```bash
# Install the newly built wheel into your environment
python3 -m pip install --force-reinstall python/target/wheels/deltalake-*.whl

# Change into the examples directory, and clear PYTHONPATH so that Python uses the installed wheel
cd examples
PYTHONPATH= python3 test_global_sort.py
```

### Pytest test
```bash
pytest -q tests/test_optimize.py::test_python_optimize_sort_flag
```

### Full test suite
```bash
pytest
# or
make unit-test
```