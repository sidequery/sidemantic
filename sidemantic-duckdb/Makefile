PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
.DEFAULT_GOAL := all

# Configuration of extension
EXT_NAME=sidemantic
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
SUPPORTED_DUCKDB_VERSION := v1.5.6
SUPPORTED_DUCKDB_NEXT := v2.0-cyanoptera
# Pin the moving development branch to the native PEG API exercised by CI.
DUCKDB_NEXT_COMMIT := 80e17fc252edd6d9e9b090ae00a1100daef4876a
DUCKDB_VERSION ?= $(SUPPORTED_DUCKDB_VERSION)
# Exercise coexistence with the parser override preloaded by packaged shells.
DEFAULT_TEST_EXTENSION_DEPS := autocomplete

.PHONY: deps
deps:
	@if [ "$(DUCKDB_VERSION)" != "$(SUPPORTED_DUCKDB_VERSION)" ] && [ "$(DUCKDB_VERSION)" != "$(SUPPORTED_DUCKDB_NEXT)" ]; then \
		echo "Use DUCKDB_VERSION=$(SUPPORTED_DUCKDB_VERSION) or $(SUPPORTED_DUCKDB_NEXT)."; \
		exit 1; \
	fi
	@if [ ! -d duckdb/.git ]; then \
		rm -rf duckdb; \
		git clone --depth 1 --branch $(DUCKDB_VERSION) https://github.com/duckdb/duckdb.git duckdb; \
	fi
	@if [ "$(DUCKDB_VERSION)" = "$(SUPPORTED_DUCKDB_NEXT)" ]; then \
		git -C duckdb fetch --depth 1 origin $(DUCKDB_NEXT_COMMIT); \
		git -C duckdb checkout --detach $(DUCKDB_NEXT_COMMIT); \
	else \
		git -C duckdb fetch --depth 1 origin refs/tags/$(DUCKDB_VERSION):refs/tags/$(DUCKDB_VERSION); \
		git -C duckdb checkout $(DUCKDB_VERSION); \
	fi
	@test -f extension-ci-tools/makefiles/duckdb_extension.Makefile || \
		(echo "extension-ci-tools is missing from this checkout" && exit 1)

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
