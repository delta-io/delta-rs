
.PHONY: help
help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: all build check test clean
all: check build test ## Perform all the checks builds and testing

check: ## Ensure that the crate meets the basic formatting and structure
	cargo fmt --check
	cargo clippy
	cargo clippy --features native-tls --no-default-features

build: ## Build the crate with each set of features
	cargo build
	cargo build --features native-tls --no-default-features

test: ## Run the crate's tests with each set of features
	cargo test
	cargo test  --features native-tls --no-default-features

clean: ## Clean up resources from build
	cargo clean
