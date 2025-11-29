# Reusable makefile for C API based extensions
#
# Inputs
#   EXTENSION_NAME         : name of the extension (lower case)
#   TARGET_DUCKDB_VERSION  : the target version of DuckDB that the extension targets
# 	USE_UNSTABLE_C_API     : if set to 1, will allow usage of the unstable C API. (This pins the produced binaries to the exact DuckDB version)
#   EXTENSION_VERSION      : the version of the extension, if left blank it will be autodetected
#   DUCKDB_PLATFORM        : the platform of the extension, if left blank it will be autodetected
#   DUCKDB_TEST_VERSION    : the version of DuckDB to test with, if left blank will default to latest stable on PyPi
#   LINUX_CI_IN_DOCKER     : indicates that the build is being run in/out of Docker in the linux CI
#   SKIP_TESTS             : makes the test targets turn into NOPs

.PHONY: platform extension_version test_extension_release test_extension_debug test_extension_release_internal test_extension_debug_internal tests_skipped clean_build clean_configure nop set_duckdb_tag set_duckdb_version output_distribution_matrix venv configure_ci check_configure move_wasm_extension

#############################################
### Platform dependent config
#############################################
PYTHON_BIN?=python3

ifeq ($(OS),Windows_NT)
	EXTENSION_LIB_FILENAME=$(EXTENSION_NAME).dll
	PYTHON_VENV_BIN=./configure/venv/Scripts/python.exe
else
	PYTHON_VENV_BIN=./configure/venv/bin/python3
    UNAME_S := $(shell uname -s)
    ifeq ($(UNAME_S),Linux)
        EXTENSION_LIB_FILENAME=lib$(EXTENSION_NAME).so
    endif
    ifeq ($(UNAME_S),Darwin)
        EXTENSION_LIB_FILENAME=lib$(EXTENSION_NAME).dylib
    endif
endif

# Override lib filename for mingw
ifeq ($(DUCKDB_PLATFORM),windows_amd64_rtools)
	EXTENSION_LIB_FILENAME=lib$(EXTENSION_NAME).dll
endif
ifeq ($(DUCKDB_PLATFORM),windows_amd64_mingw)
	EXTENSION_LIB_FILENAME=lib$(EXTENSION_NAME).dll
endif

# Propagate toolchains
ifeq ($(DUCKDB_PLATFORM),windows_amd64)
       TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_OVERLAY_TRIPLETS=${PROJ_DIR}extension-ci-tools/toolchains/
endif

#############################################
### Main extension parameters
#############################################

# The target DuckDB version
ifeq ($(TARGET_DUCKDB_VERSION),)
	TARGET_DUCKDB_VERSION = v0.0.1
endif

EXTENSION_FILENAME=$(EXTENSION_NAME).duckdb_extension
EXTENSION_FILENAME_NO_METADATA=$(EXTENSION_LIB_FILENAME)

DUCKDB_WASM_PLATFORM=$(filter wasm_mvp wasm_eh wasm_threads,$(DUCKDB_PLATFORM))

ifneq ($(DUCKDB_WASM_PLATFORM),)
	EXTENSION_FILENAME=$(EXTENSION_NAME).duckdb_extension.wasm
	EXTENSION_FILENAME_NO_METADATA=$(EXTENSION_NAME).no_metadata.wasm
	EXTENSION_LIB_FILENAME=lib$(EXTENSION_NAME).a
	EXTENSION_BUILD_PATH=./build/$(DUCKDB_WASM_PLATFORM)
else
	EXTENSION_BUILD_PATH=./build
endif

#############################################
### Platform Detection
#############################################

# Either autodetect or use the provided value
PLATFORM_COMMAND?=
ifeq ($(DUCKDB_PLATFORM),)
	PLATFORM_COMMAND=$(PYTHON_VENV_BIN) extension-ci-tools/scripts/configure_helper.py --duckdb-platform
else
	# Sets the platform using DUCKDB_PLATFORM variable
	PLATFORM_COMMAND=echo $(DUCKDB_PLATFORM) > configure/platform.txt
endif

# Write the platform we are building for
platform:
	@$(PLATFORM_COMMAND)

#############################################
### Extension Version Detection
#############################################

# Either autodetect or use the provided value
VERSION_COMMAND?=
ifeq ($(EXTENSION_VERSION),)
    VERSION_COMMAND=$(PYTHON_VENV_BIN) extension-ci-tools/scripts/configure_helper.py --extension-version
else
	# Sets the platform using DUCKDB_PLATFORM variable
	VERSION_COMMAND=echo "$(EXTENSION_VERSION)" > configure/extension_version.txt
endif

extension_version: configure/extension_version.txt

configure/extension_version.txt:
	@ $(VERSION_COMMAND)

#############################################
### Testing
#############################################

# Note: to override the default test runner, create a symlink to a different venv
TEST_RUNNER=$(PYTHON_VENV_BIN) -m duckdb_sqllogictest

TEST_RUNNER_BASE=$(TEST_RUNNER) --test-dir test/sql $(EXTRA_EXTENSIONS_PARAM)
TEST_RUNNER_DEBUG=$(TEST_RUNNER_BASE) --external-extension build/debug/$(EXTENSION_NAME).duckdb_extension
TEST_RUNNER_RELEASE=$(TEST_RUNNER_BASE) --external-extension build/release/$(EXTENSION_NAME).duckdb_extension

# By default latest duckdb is installed, set DUCKDB_TEST_VERSION to switch to a different version
DUCKDB_PIP_INSTALL?=duckdb
ifeq ($(DUCKDB_TEST_VERSION),main)
	DUCKDB_PIP_INSTALL=--pre duckdb
else ifneq ($(DUCKDB_TEST_VERSION),)
	DUCKDB_PIP_INSTALL=duckdb==$(DUCKDB_TEST_VERSION)
endif

# This allows C API extensions to be tested against a prerelease of DuckDB. This only really makes sense when DuckDB already
# has stabilized the C API for the upcoming release.
ifeq ($(DUCKDB_GIT_VERSION),main)
	DUCKDB_PIP_INSTALL=--pre duckdb
endif

TEST_RELEASE_TARGET=test_extension_release_internal
TEST_DEBUG_TARGET=test_extension_debug_internal

# Disable testing outside docker: the unittester is currently dynamically linked by default
ifeq ($(LINUX_CI_IN_DOCKER),1)
	SKIP_TESTS=1
endif

# TODO: for some weird reason the Ubuntu 22.04 Runners on Github Actions don't actually grab the glibc 2.24 wheels but the
#       gilbc 2.17 ones. What this means is that we can't run the tests on linux_amd64 because we are installing the duckdb
#	    linux_amd64_gcc4 test runner
ifeq ($(DUCKDB_PLATFORM),linux_amd64)
	SKIP_TESTS=1
endif

# _musl tests would need to be run in the container
ifeq ($(DUCKDB_PLATFORM),linux_amd64_musl)
	SKIP_TESTS=1
endif

# The mingw/rtools can not be tested using the python test runner unfortunately
ifeq ($(DUCKDB_PLATFORM),windows_amd64_rtools)
	SKIP_TESTS=1
endif
ifeq ($(DUCKDB_PLATFORM),windows_amd64_mingw)
	SKIP_TESTS=1
endif

ifeq ($(SKIP_TESTS),1)
	TEST_RELEASE_TARGET=tests_skipped
	TEST_DEBUG_TARGET=tests_skipped
endif

test_extension_release: $(TEST_RELEASE_TARGET)
test_extension_debug: $(TEST_DEBUG_TARGET)

test_extension_release_internal: check_configure
	@echo "Running RELEASE tests.."
	@$(TEST_RUNNER_RELEASE)

test_extension_debug_internal: check_configure
	@echo "Running DEBUG tests.."
	@$(TEST_RUNNER_DEBUG)

tests_skipped:
	@echo "Skipping tests.."


#############################################
### Misc
#############################################

clean_build:
	rm -rf build
	rm -rf duckdb_unittest_tempdir

clean_configure:
	rm -rf configure

nop:
	@echo "NOP"

set_duckdb_tag: nop

set_duckdb_version: nop

output_distribution_matrix:
	cat extension-ci-tools/config/distribution_matrix.json

#############################################
### Linking
#############################################
ifneq ($(DUCKDB_WASM_PLATFORM),)

link_wasm_debug:
	emcc $(EXTENSION_BUILD_PATH)/debug/$(EXTENSION_LIB_FILENAME) -o $(EXTENSION_BUILD_PATH)/debug/$(EXTENSION_FILENAME_NO_METADATA) -O3 -g -sSIDE_MODULE=2 -sEXPORTED_FUNCTIONS="_$(EXTENSION_NAME)_init_c_api"

link_wasm_release:
	emcc $(EXTENSION_BUILD_PATH)/release/$(EXTENSION_LIB_FILENAME) -o $(EXTENSION_BUILD_PATH)/release/$(EXTENSION_FILENAME_NO_METADATA) -O3 -sSIDE_MODULE=2 -sEXPORTED_FUNCTIONS="_$(EXTENSION_NAME)_init_c_api"

else
link_wasm_debug:
link_wasm_release:

endif

#############################################
### Adding metadata
#############################################
UNSTABLE_C_API_FLAG=
ifeq ($(USE_UNSTABLE_C_API),1)
	UNSTABLE_C_API_FLAG+=--abi-type C_STRUCT_UNSTABLE
endif

build_extension_with_metadata_debug: check_configure link_wasm_debug build_extension_library_debug
	$(PYTHON_VENV_BIN) extension-ci-tools/scripts/append_extension_metadata.py \
			-l $(EXTENSION_BUILD_PATH)/debug/$(EXTENSION_FILENAME_NO_METADATA) \
			-o $(EXTENSION_BUILD_PATH)/debug/$(EXTENSION_FILENAME) \
			-n $(EXTENSION_NAME) \
			-dv $(TARGET_DUCKDB_VERSION) \
			-evf configure/extension_version.txt \
			-pf configure/platform.txt $(UNSTABLE_C_API_FLAG)
	$(PYTHON_VENV_BIN) -c "import shutil;shutil.copyfile('$(EXTENSION_BUILD_PATH)/debug/$(EXTENSION_FILENAME)', '$(EXTENSION_BUILD_PATH)/debug/extension/$(EXTENSION_NAME)/$(EXTENSION_FILENAME)')"

build_extension_with_metadata_release: check_configure link_wasm_release build_extension_library_release
	$(PYTHON_VENV_BIN) extension-ci-tools/scripts/append_extension_metadata.py \
			-l $(EXTENSION_BUILD_PATH)/release/$(EXTENSION_FILENAME_NO_METADATA) \
			-o $(EXTENSION_BUILD_PATH)/release/$(EXTENSION_FILENAME) \
			-n $(EXTENSION_NAME) \
			-dv $(TARGET_DUCKDB_VERSION) \
			-evf configure/extension_version.txt \
			-pf configure/platform.txt $(UNSTABLE_C_API_FLAG)
	$(PYTHON_VENV_BIN) -c "import shutil;shutil.copyfile('$(EXTENSION_BUILD_PATH)/release/$(EXTENSION_FILENAME)', '$(EXTENSION_BUILD_PATH)/release/extension/$(EXTENSION_NAME)/$(EXTENSION_FILENAME)')"

#############################################
### Python
#############################################

# Installs the test runner using the selected DuckDB version (latest stable by default)
# TODO: switch to PyPI distribution
venv: configure/venv

configure/venv:
	$(PYTHON_BIN) -m venv configure/venv
	$(PYTHON_VENV_BIN) -m pip install $(DUCKDB_PIP_INSTALL)
	$(PYTHON_VENV_BIN) -m pip install git+https://github.com/duckdb/duckdb-sqllogictest-python@2ac8dbc012ddbd96a57dca37784fd8ee3c0eb021
	$(PYTHON_VENV_BIN) -m pip install packaging

#############################################
### Configure
#############################################

CONFIGURE_CI_STEP?=
ifeq ($(LINUX_CI_IN_DOCKER),1)
	CONFIGURE_CI_STEP=nop
else
	CONFIGURE_CI_STEP=configure
endif

configure_ci: $(CONFIGURE_CI_STEP)

# Because the configure_ci may differ from configure, we don't automatically run configure on make build, this makes the error a bit nicer
check_configure:
	@$(PYTHON_BIN) -c "import os; assert os.path.exists('configure/platform.txt'), 'The configure step appears to not be run. Please try running make configure'"
	@$(PYTHON_BIN) -c "import os; assert os.path.exists('configure/venv'), 'The configure step appears to not be run. Please try running make configure'"

move_wasm_extension:
	$(PYTHON_VENV_BIN) -c "from pathlib import Path;Path('$(EXTENSION_BUILD_PATH)/extension/$(EXTENSION_NAME)').mkdir(parents=True, exist_ok=True)"
	$(PYTHON_VENV_BIN) -c "import shutil;shutil.copyfile('$(EXTENSION_BUILD_PATH)/release/extension/$(EXTENSION_NAME)/$(EXTENSION_FILENAME)', '$(EXTENSION_BUILD_PATH)/extension/$(EXTENSION_NAME)/$(EXTENSION_FILENAME)')"

wasm_mvp:
	DUCKDB_PLATFORM=wasm_mvp make configure release move_wasm_extension

wasm_eh:
	DUCKDB_PLATFORM=wasm_eh make configure release move_wasm_extension

wasm_threads:
	DUCKDB_PLATFORM=wasm_threads make configure release move_wasm_extension
