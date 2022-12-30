.DEFAULT_GOAL := help

VENV := venv
MATURIN_VERSION := $(shell grep 'requires =' pyproject.toml | cut -d= -f2- | tr -d '[ "]')
PACKAGE_VERSION := $(shell grep version Cargo.toml | head -n 1 | awk '{print $$3}' | tr -d '"' )

.PHONY: setup-venv
setup-venv: ## Setup the virtualenv
	$(info --- Setup virtualenv ---)
	python -m venv $(VENV)

.PHONY: setup
setup: ## Setup the requirements
	$(info --- Setup dependencies ---)
	pip install "$(MATURIN_VERSION)"

.PHONY: build
build: setup ## Build Python binding of delta-rs
	$(info --- Build Python binding ---)
	maturin build $(MATURIN_EXTRA_ARGS)

.PHONY: develop
develop: setup ## Install Python binding of delta-rs
	$(info --- Develop with Python binding ---)
	maturin develop --extras=devel,pandas $(MATURIN_EXTRA_ARGS)

.PHONY: install
install: build ## Install Python binding of delta-rs
	$(info --- Uninstall Python binding ---)
	pip uninstall -y deltalake
	$(info --- Install Python binding ---)
	$(eval TARGET_WHEEL := $(shell ls ../target/wheels/deltalake-${PACKAGE_VERSION}-*.whl))
	pip install $(TARGET_WHEEL)[devel,pandas]

.PHONY: develop-pyspark
develop-pyspark: setup
	$(info --- Develop with Python binding ---)
	maturin develop --extras=devel,pandas,pyspark $(MATURIN_EXTRA_ARGS)

.PHONY: format
format: ## Format the code
	$(info --- Rust format ---)
	cargo fmt
	$(info --- Python format ---)
	black .
	isort .

.PHONY: check-rust
check-rust: ## Run check on Rust
	$(info --- Check Rust clippy ---)
	cargo clippy
	$(info --- Check Rust format ---)
	cargo fmt -- --check

.PHONY: check-python
check-python: ## Run check on Python
	$(info Check Python isort)
	isort --diff --check-only .
	$(info Check Python black)
	black --check .
	$(info Check Python mypy)
	mypy

.PHONY: unit-test
unit-test: ## Run unit test
	$(info --- Run Python unit-test ---)
	python -m pytest

.PHONY: test-pyspark
test-pyspark:
	python -m pytest -m 'pyspark and integration'

.PHONY: build-documentation
build-documentation: ## Build documentation with Sphinx
	$(info --- Run build of the Sphinx documentation ---)
	sphinx-build -Wn -b html -d ./docs/build/doctrees ./docs/source ./docs/build/html

.PHONY: clean
clean: ## Run clean
	$(warning --- Clean virtualenv and target directory ---)
	cargo clean
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
