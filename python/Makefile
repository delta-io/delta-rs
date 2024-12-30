.DEFAULT_GOAL := help

VENV := venv
MATURIN_VERSION := $(shell grep 'requires =' pyproject.toml | cut -d= -f2- | tr -d '[ "]')
PACKAGE_VERSION := $(shell grep version Cargo.toml | head -n 1 | awk '{print $$3}' | tr -d '"' )
DAT_VERSION := 0.0.2

.PHONY: setup
setup: ## Setup the requirements
	$(info --- Setup dependencies ---)
	uv sync --extra devel --extra pandas --extra polars

.PHONY: setup-dat
setup-dat: ## Download DAT test files
	mkdir -p dat-data
	rm -rf dat-data/v$(DAT_VERSION)
	curl -L --silent --output dat-data/deltalake-dat-v$(DAT_VERSION).tar.gz \
		https://github.com/delta-incubator/dat/releases/download/v$(DAT_VERSION)/deltalake-dat-v$(DAT_VERSION).tar.gz 
	tar --no-same-permissions -xzf dat-data/deltalake-dat-v$(DAT_VERSION).tar.gz
	mv out dat-data/v$(DAT_VERSION)
	rm dat-data/deltalake-dat-v$(DAT_VERSION).tar.gz

.PHONY: build
build: setup ## Build Python binding of delta-rs
	$(info --- Build Python binding ---)
	uvx --from 'maturin[zig]' maturin build $(MATURIN_EXTRA_ARGS)

.PHONY: develop
develop: setup ## Install Python binding of delta-rs
	$(info --- Develop with Python binding ---)
	uvx --from 'maturin[zig]' maturin develop --extras=devel,pandas,polars $(MATURIN_EXTRA_ARGS)

.PHONY: install
install: build ## Install Python binding of delta-rs
	$(info --- Uninstall Python binding ---)
	uv pip uninstall deltalake
	$(info --- Install Python binding ---)
	$(eval TARGET_WHEEL := $(shell ls ../target/wheels/deltalake-${PACKAGE_VERSION}-*.whl))
	uv pip install $(TARGET_WHEEL)[devel,pandas,polars]

.PHONY: develop-pyspark
develop-pyspark:
	uv sync --all-extras
	$(info --- Develop with Python binding ---)
	uvx --from 'maturin[zig]' maturin develop --extras=devel,pandas,polars,pyspark $(MATURIN_EXTRA_ARGS)

.PHONY: format
format: ## Format the code
	$(info --- Rust format ---)
	cargo fmt
	$(info --- Python format ---)
	uv run ruff check . --fix
	uv run ruff format .

.PHONY: check-rust
check-rust: ## Run check on Rust
	$(info --- Check Rust clippy ---)
	cargo clippy
	$(info --- Check Rust format ---)
	cargo fmt -- --check

.PHONY: check-python
check-python: ## Run check on Python
	$(info Check Python format)
	uv run ruff format --check --diff .
	$(info Check Python linting)
	uv run ruff check .
	$(info Check Python mypy)
	uv run mypy

.PHONY: unit-test
unit-test: ## Run unit test
	$(info --- Run Python unit-test ---)
	uv run pytest --doctest-modules 

.PHONY: test-cov
test-cov: ## Create coverage report
	$(info --- Run Python unit-test ---)
	uv run pytest --doctest-modules --cov --cov-config=pyproject.toml --cov-report=term --cov-report=html

.PHONY: test-pyspark
test-pyspark:
	uv run pytest -m 'pyspark and integration'

.PHONY: build-documentation
build-documentation: ## Build documentation with Sphinx
	$(info --- Run build of the Sphinx documentation ---)
	uv run sphinx-build -Wn -b html -d ./docs/build/doctrees ./docs/source ./docs/build/html

.PHONY: build-docs
build-docs: ## Build documentation with mkdocs
	$(info --- Run build of the documentation ---)
	(cd ..; uv pip install -r docs/requirements.txt; mkdocs build)

.PHONY: clean
clean: ## Run clean
	$(warning --- Clean virtualenv and target directory ---)
	cargo clean
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
