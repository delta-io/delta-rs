.DEFAULT_GOAL := help

VENV := venv
MATURIN_VERSION := $(shell grep 'requires =' pyproject.toml | cut -d= -f2- | tr -d '[ "]')
PACKAGE_VERSION := $(shell grep version Cargo.toml | head -n 1 | awk '{print $$3}' | tr -d '"' )
DAT_VERSION := 0.0.2

.PHONY: setup-venv
setup-venv: ## Setup the virtualenv
	$(info --- Setup virtualenv ---)
	python -m venv $(VENV)

.PHONY: setup
setup: ## Setup the requirements
	$(info --- Setup dependencies ---)
	pip install "$(MATURIN_VERSION)"

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
	ruff check . --fix
	ruff format .

.PHONY: check-rust
check-rust: ## Run check on Rust
	$(info --- Check Rust clippy ---)
	cargo clippy
	$(info --- Check Rust format ---)
	cargo fmt -- --check

.PHONY: check-python
check-python: ## Run check on Python
	$(info Check Python format)
	ruff format --check --diff .
	$(info Check Python linting)
	ruff check .
	$(info Check Python mypy)
	mypy

.PHONY: unit-test
unit-test: ## Run unit test
	$(info --- Run Python unit-test ---)
	python -m pytest --doctest-modules 

.PHONY: test-cov
test-cov: ## Create coverage report
	$(info --- Run Python unit-test ---)
	python -m pytest --doctest-modules --cov --cov-config=pyproject.toml --cov-report=term --cov-report=html

.PHONY: test-pyspark
test-pyspark:
	python -m pytest -m 'pyspark and integration'

.PHONY: build-documentation
build-documentation: ## Build documentation with Sphinx
	$(info --- Run build of the Sphinx documentation ---)
	sphinx-build -Wn -b html -d ./docs/build/doctrees ./docs/source ./docs/build/html

.PHONY: build-docs
build-docs: ## Build documentation with mkdocs
	$(info --- Run build of the documentation ---)
	(cd ..; pip install -r docs/requirements.txt; mkdocs build)

.PHONY: clean
clean: ## Run clean
	$(warning --- Clean virtualenv and target directory ---)
	cargo clean
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
