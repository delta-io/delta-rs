#
# This Makefile exists largely to help ensure that some of the common behaviors
# between CI and local development can be consistently replicated
#
# For the most part you should be able to rely on cargo for development.

.DEFAULT_GOAL := help
DAT_VERSION := 0.0.3

## begin dat related
####################
.PHONY: setup-dat
setup-dat: dat/deltalake-dat-v$(DAT_VERSION) ## Download and setup the Delta Acceptance Tests (dat)

dat:
	mkdir -p dat

dat/deltalake-dat-v$(DAT_VERSION): dat  ## Download DAT test files into ./dat
	rm -rf dat/v$(DAT_VERSION)
	curl -L --silent --output dat/deltalake-dat-v$(DAT_VERSION).tar.gz \
		https://github.com/delta-incubator/dat/releases/download/v$(DAT_VERSION)/deltalake-dat-v$(DAT_VERSION).tar.gz
	tar --no-same-permissions -xzf dat/deltalake-dat-v$(DAT_VERSION).tar.gz
	mv out dat/v$(DAT_VERSION)
	rm dat/deltalake-dat-v$(DAT_VERSION).tar.gz


####################
## end dat related


.PHONY: clean
clean: ## Remove temporary and downloaded artifacts
	rm -rf dat

.PHONY: help
help: ## Produce the helpful command listing
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
