# Contributing to delta-rs

Development on this project is mostly driven by volunteer contributors. We welcome new contributors, including not only those who develop new features, but also those who are able to help with documentation and provide detailed bug reports. 

Please take note of our [code of conduct](CODE_OF_CONDUCT.md).

If you want to start contributing, first look at our good first issues: https://github.com/delta-io/delta-rs/contribute

If you want to contribute something more substantial, see our "Projects seeking contributors" section on our roadmap: https://github.com/delta-io/delta-rs/issues/1128

## Claiming an issue

If you want to claim an issue to work on, you can write the word `take` as a comment in it and you will be automatically assigned.

## Quick start

- Install Rust, e.g. as described [here](https://doc.rust-lang.org/cargo/getting-started/installation.html)
- Have a compatible Python version installed (check `python/pyproject.toml` for current requirement)
- Create a Python virtual environment (required for development builds), e.g. as described [here](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/)
- Build the project for development (this requires an active virtual environment and will also install `deltalake` in that virtual environment)
```
cd python
make develop
```

- Run some Python code, e.g. to run a specific test
```
python -m pytest tests/test_writer.py -s -k "test_with_deltalake_schema"
```

- Run some Rust code, e.g. run an example
```
cd crates/deltalake
cargo run --examples basic_operations
```

## Run the docs locally
*This serves your local contens of docs via a web browser, handy for checking what they look like if you are making changes to docs or docstings*
```
(cd python; make develop)
pip install -r docs/requirements.txt
mkdocs serve
```

## To make a pull request (PR)
- Make sure all the following steps run/pass locally before submitting a PR
```
cargo fmt -- --check
cd python
make check-rust
make check-python
make develop
make unit-test
make build-docs
```

## Developing in VSCode

*These are just some basic steps/components to get you started, there are many other very useful extensions for VSCode*

- For a better Rust development experience, install [rust extention](https://marketplace.visualstudio.com/items?itemName=1YiB.rust-bundle)
- For debugging Rust code, install [CodeLLDB](https://marketplace.visualstudio.com/items?itemName=vadimcn.vscode-lldb). The extension should even create Debug launch configurations for the project if you allow it, an easy way to get started. Just set a breakpoint and run the relevant configuration.
- For debugging from Python into Rust, follow this procedure:
1. Add this to `.vscode/launch.json`
```
{
            "type": "lldb",
            "request": "attach",
            "name": "LLDB Attach to Python'",
            "program": "${command:python.interpreterPath}",
            "pid": "${command:pickMyProcess}",
            "args": [],
            "stopOnEntry": false,
            "environment": [],
            "externalConsole": true,
            "MIMode": "lldb",
            "cwd": "${workspaceFolder}"
        }
```
2. Add a `breakpoint()` statement somewhere in your Python code (main function or at any point in Python code you know will be executed when you run it)
3. Add a breakpoint in Rust code in VSCode editor where you want to drop into the debugger
4. Run the relevant Python code function in your terminal, execution should drop into the Python debugger showing `PDB` prompt
5. Run the following in that promt to get the Python process ID: `import os; os.getpid()`
6. Run the `LLDB Attach to Python` from the `Run and Debug` panel of VSCode. This will prompt you for a Process ID to attach to, enter the Python process ID obtained earlier (this will also be in the dropdown but that dropdown will have many process IDs)
7. LLDB make take couple of seconds to attach to the process
8. When the debugger is attached to the process (you will notice the debugger panels get filled with extra info), enter `c`+Enter in the `PDB` prompt in your terminal - the execution should continue until the breakpoint in Rust code is hit. From this point it's a standard debugging procecess.


