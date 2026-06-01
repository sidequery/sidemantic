PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
.DEFAULT_GOAL := all

# Configuration of extension
EXT_NAME=sidemantic
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
SUPPORTED_DUCKDB_VERSION := v1.4.2
DUCKDB_VERSION ?= $(SUPPORTED_DUCKDB_VERSION)

.PHONY: deps
deps:
	@if [ "$(DUCKDB_VERSION)" != "$(SUPPORTED_DUCKDB_VERSION)" ]; then \
		echo "DUCKDB_VERSION=$(DUCKDB_VERSION) is unsupported by the vendored extension-ci-tools checkout."; \
		echo "Use DUCKDB_VERSION=$(SUPPORTED_DUCKDB_VERSION), or update extension-ci-tools and this guard together."; \
		exit 1; \
	fi
	@if [ ! -d duckdb/.git ]; then \
		rm -rf duckdb; \
		git clone --depth 1 --branch $(DUCKDB_VERSION) https://github.com/duckdb/duckdb.git duckdb; \
	else \
		git -C duckdb fetch --depth 1 origin $(DUCKDB_VERSION); \
		git -C duckdb checkout FETCH_HEAD; \
	fi
	@test -f extension-ci-tools/makefiles/duckdb_extension.Makefile || \
		(echo "extension-ci-tools is missing from this checkout" && exit 1)

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
