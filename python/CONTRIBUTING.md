# Contributing to Python deltalake package

## Workflow

Most of the workflow is based on the `Makefile`, [`maturin`](https://www.maturin.rs/) CLI tool and [`uv`](https://docs.astral.sh/uv/).

### Setup your local environment with virtualenv

```bash
make setup
```

### Activate it

```bash
source ./.venv/bin/activate
```

### Ready to develop with maturin

[maturin](https://github.com/PyO3/maturin) is used to build the python package.

Install delta-rs in the current virtualenv:

```bash
make develop
```

Then, list all the available tasks

```bash
make help
```

Format:

```bash
make format
```

Check:

```bash
make check-python
```

Unit test:

```bash
make unit-test
```

## Release process

1. Make a new PR to update the version in `python/Cargo.toml`.
2. Once merged, push a tag of the format `python-vX.Y.Z`. This will trigger CI
   to create and publish release artifacts.
3. In GitHub, create a new release based on the new tag. For release notes,
   use the generator at a starting point, but please revise them for brevity.
   Remove anything that is dev-facing only (chores), and bring all important
   changes to the top, leaving less important changes (such as dependabot
   updates) at the bottom.
4. Once the artifacts are showing up in PyPI, announce the release in the delta-rs
   Slack channel. Be sure to give a shout-out to the new contributors.

### Windows ARM64

Install native ARM64 Python 3.11+, `uv`, Rust, Visual Studio 2022 C++ ARM64 build
tools, a Windows SDK, and LLVM. The crypto dependencies require Clang.
From the repository root in an ARM64 developer PowerShell:

```powershell
rustup toolchain install stable
$env:RUSTUP_TOOLCHAIN = "stable"
rustup target add aarch64-pc-windows-msvc --toolchain stable
$env:PATH = "$env:ProgramFiles\LLVM\bin;$env:PATH"
$env:CC_aarch64_pc_windows_msvc = "clang-cl"
$env:CXX_aarch64_pc_windows_msvc = "clang-cl"
uvx maturin build -m .\python\Cargo.toml --target aarch64-pc-windows-msvc --interpreter python --profile python-release-arm
```

Adjust the LLVM path if needed. Wheels are written to `target\wheels`; installing
a prebuilt wheel needs only Python of the matching architecture.

Windows ARM64 CI runs unit tests without PyArrow or Pandas and omits the `other`
dependency group used for integration tooling. Windows x64 runs the full unit
suite and doctests with PyArrow.
