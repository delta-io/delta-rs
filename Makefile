.DEFAULT_GOAL := help

.PHONY: install-ruff
install-ruff: ## install ruff
	$(info --- Installing ruff ---)
	pip install ruff==0.5.2

.PHONY: format
format: install-ruff ## format code with ruff
	$(info --- format Python code in docs ---)
	ruff format .

.PHONY: check
check: install-ruff ## check if code is formatted with ruff
	$(info --- format Python code in docs ---)
	ruff format --check .

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
