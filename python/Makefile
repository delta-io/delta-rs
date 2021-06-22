.DEFAULT_GOAL := help

VENV := venv
PY_SOURCE_DIR := deltalake tests


.PHONY: setup-venv
setup-venv: ## Setup the virtualenv
	$(info --- Setup virtualenv ---)
	python -m venv $(VENV)

.PHONY: setup
setup: ## Setup the requirements
	$(info --- Setup dependencies ---)
	pip install maturin==0.10.6

.PHONY: develop
develop: setup ## Install Python binding of delta-rs
	$(info --- Maturin develop ---)
	maturin develop

.PHONY: format
format: ## Format the code
	$(info --- Rust format ---)
	cargo fmt
	$(info --- Python format ---)
	black deltalake tests *.py
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
	isort --diff --check-only $(PY_SOURCE_DIR)
	$(info Check Python black)
	black --check $(PY_SOURCE_DIR)
	$(info Check Python mypy)
	mypy --config-file setup.cfg $(PY_SOURCE_DIR)

.PHONY: unit-test
unit-test: ## Run unit test
	$(info --- Run Python unit-test ---)
	py.test --cov tests -m 'not integration'

.PHONY: build-documentation
build-documentation: ## Build documentation with Sphinx
	$(info --- Run build of the Sphinx documentation ---)
	sphinx-build -Wn -b html -d ./docs/build/doctrees ./docs/source ./docs/build/html

.PHONY: publish
publish: ## Publish Python binding version in Pypi
	$(info --- Run publish of the Python binding ---)
	maturin publish -b pyo3 --target $(TARGET) $(EXTRA_ARGS)

.PHONY: clean
clean: ## Run clean
	$(warning --- Clean virtualenv and target directory ---)
	cargo clean
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
