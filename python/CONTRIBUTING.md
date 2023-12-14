# Contributing to Python deltalake package

## Workflow

Most of the workflow is based on the `Makefile` and the `maturin` CLI tool.

#### Setup your local environment with virtualenv

```bash
make setup-venv
```

#### Activate it
```bash
source ./venv/bin/activate
```

#### Ready to develop with maturin

[maturin](https://github.com/PyO3/maturin) is used to build the python package.
Install delta-rs in the current virtualenv

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

1. Make a new PR to update the version in pyproject.toml.
2. Once merged, push a tag of the format `python-vX.Y.Z`. This will trigger CI
   to create and publish release artifacts.
3. In GitHub, create a new release based on the new tag. For release notes, 
   use the generator at a starting point, but please revise them for brevity.
   Remove anything that is dev-facing only (chores), and bring all important
   changes to the top, leaving less important changes (such as dependabot 
   updates) at the bottom.
4. Once the artifacts are showing up in PyPI, announce the release in the delta-rs
   Slack channel. Be sure to give a shout-out to the new contributors.


