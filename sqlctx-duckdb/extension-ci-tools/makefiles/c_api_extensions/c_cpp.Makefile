# Reusable makefile for the C/C++ extensions targeting the C extension API
#
# Inputs
#   EXTENSION_NAME               : name of the extension (lower case)
#   EXTENSION_CANONICAL          : name of the canonical extension (lower case)
#   EXTENSION_LIB_FILENAME       : the library name that is produced by the build
# 	USE_UNSTABLE_C_API           : if set to 1, will allow usage of the unstable C API. (This pins the produced binaries to the exact DuckDB version)
#   TARGET_DUCKDB_VERSION        : the target version of DuckDB that the extension targets
#	CMAKE_EXTRA_BUILD_FLAGS      : additional CMake flags to pass
#	VCPKG_TOOLCHAIN_PATH         : path to vcpkg toolchain
#	VCPKG_TARGET_TRIPLET         : vcpkg triplet to override
#	GEN                          : allow specifying ninja as generator

.PHONY: build_extension_library_debug build_extension_library_release update_duckdb_headers

#############################################
### Base config
#############################################

# Get parsed SemVer for Stable C API
VERSION_PARTS = $(subst ., ,$(TARGET_DUCKDB_VERSION))
MAJOR_VERSION=
MINOR_VERSION=
PATCH_VERSION=
ifeq ($(word 1,$(VERSION_PARTS)), v1)
	MAJOR_VERSION = 1
	MINOR_VERSION = $(word 2,$(VERSION_PARTS))
	PATCH_VERSION = $(word 3,$(VERSION_PARTS))
endif

# Create build params to pass name and version
CMAKE_VERSION_PARAMS = -DEXTENSION_NAME=$(EXTENSION_NAME)

# Set the parsed semver defines
ifneq ($(MAJOR_VERSION),)
	CMAKE_VERSION_PARAMS += -DTARGET_DUCKDB_VERSION_MAJOR=$(MAJOR_VERSION)
endif
ifneq ($(EXTENSION_CANONICAL),)
	CMAKE_VERSION_PARAMS += -DEXTENSION_CANONICAL=$(EXTENSION_CANONICAL)
endif
ifneq ($(MINOR_VERSION),)
	CMAKE_VERSION_PARAMS += -DTARGET_DUCKDB_VERSION_MINOR=$(MINOR_VERSION)
endif
ifneq ($(PATCH_VERSION),)
	CMAKE_VERSION_PARAMS += -DTARGET_DUCKDB_VERSION_PATCH=$(PATCH_VERSION)
endif

ifeq ($(USE_UNSTABLE_C_API),1)
	CMAKE_VERSION_PARAMS += -DDUCKDB_EXTENSION_API_VERSION_UNSTABLE=$(TARGET_DUCKDB_VERSION)
endif

CMAKE_BUILD_FLAGS = $(CMAKE_VERSION_PARAMS) $(CMAKE_EXTRA_BUILD_FLAGS)

#############################################
### Vcpkg
#############################################

ifneq ("${VCPKG_TOOLCHAIN_PATH}", "")
	CMAKE_BUILD_FLAGS += -DCMAKE_TOOLCHAIN_FILE='${VCPKG_TOOLCHAIN_PATH}'
	ifneq ($(DUCKDB_WASM_PLATFORM),)
		CMAKE_BUILD_FLAGS += -DVCPKG_CHAINLOAD_TOOLCHAIN_FILE=$(EMSDK)/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake
	endif
endif
ifneq ("${VCPKG_TARGET_TRIPLET}", "")
	CMAKE_BUILD_FLAGS += -DVCPKG_TARGET_TRIPLET='${VCPKG_TARGET_TRIPLET}'
endif
ifneq ("${VCPKG_HOST_TRIPLET}", "")
	CMAKE_BUILD_FLAGS += -DVCPKG_HOST_TRIPLET='${VCPKG_HOST_TRIPLET}'
endif

#############################################
### Ninja
#############################################

MAKE_INVOCATION = make

ifeq ($(GEN),ninja)
	CMAKE_BUILD_FLAGS += -G "Ninja"
	MAKE_INVOCATION = ninja
endif

OUTPUT_LIB_PATH_DEBUG=cmake_build/debug/$(EXTENSION_LIB_FILENAME)
OUTPUT_LIB_PATH_RELEASE=cmake_build/release/$(EXTENSION_LIB_FILENAME)

ifeq ($(DUCKDB_PLATFORM),windows_amd64_rtools)
	MINGW=1
endif
ifeq ($(DUCKDB_PLATFORM),windows_amd64_mingw)
	MINGW=1
endif

CMAKE_WRAPPER=
CMAKE_BUILD_DEBUG = cmake --build cmake_build/debug --config Debug
CMAKE_BUILD_RELEASE = cmake --build cmake_build/release --config Release
EXTRA_CMAKE_FLAGS ?=

ifneq ($(DUCKDB_WASM_PLATFORM),)
	CMAKE_BUILD_DEBUG = $(MAKE_INVOCATION) -C cmake_build/debug
	CMAKE_BUILD_RELEASE = $(MAKE_INVOCATION) -C cmake_build/release
	CMAKE_WRAPPER=emcmake
	CMAKE_BUILD_FLAGS += -DCMAKE_POSITION_INDEPENDENT_CODE=ON
	EXTRA_CMAKE_FLAGS += -DDUCKDB_WASM_EXTENSION=1
	ifeq ($(DUCKDB_WASM_PLATFORM), 'wasm_mvp')
	endif
	ifeq ($(DUCKDB_WASM_PLATFORM), 'wasm_eh')
		CMAKE_CXX_FLAGS += -fwasm-exceptions
	endif
	ifeq ($(DUCKDB_WASM_PLATFORM), 'wasm_threads')
		CMAKE_CXX_FLAGS += -fwasm-exceptions -DWITH_WASM_THREADS=1 -DWITH_WASM_SIMD=1 -DWITH_WASM_BULK_MEMORY=1 -pthread
	endif
endif

#############################################
### Build targets
#############################################


build_extension_library_debug: check_configure
	$(CMAKE_WRAPPER) cmake $(CMAKE_BUILD_FLAGS) -DCMAKE_BUILD_TYPE=Debug -S $(PROJ_DIR) -B cmake_build/debug $(EXTRA_CMAKE_FLAGS)
	$(CMAKE_BUILD_DEBUG)
	$(EXTRA_COPY_STEP_DEBUG)
	$(PYTHON_VENV_BIN) -c "from pathlib import Path;Path('$(EXTENSION_BUILD_PATH)/debug/extension/$(EXTENSION_NAME)').mkdir(parents=True, exist_ok=True)"
	$(PYTHON_VENV_BIN) -c "import shutil;shutil.copyfile('$(OUTPUT_LIB_PATH_DEBUG)', '$(EXTENSION_BUILD_PATH)/debug/$(EXTENSION_LIB_FILENAME)')"

build_extension_library_release: check_configure
	$(CMAKE_WRAPPER) cmake $(CMAKE_BUILD_FLAGS) -DCMAKE_BUILD_TYPE=Release -S $(PROJ_DIR) -B cmake_build/release $(EXTRA_CMAKE_FLAGS)
	$(CMAKE_BUILD_RELEASE)
	$(EXTRA_COPY_STEP_RELEASE)
	$(PYTHON_VENV_BIN) -c "from pathlib import Path;Path('$(EXTENSION_BUILD_PATH)/release/extension/$(EXTENSION_NAME)').mkdir(parents=True, exist_ok=True)"
	$(PYTHON_VENV_BIN) -c "import shutil;shutil.copyfile('$(OUTPUT_LIB_PATH_RELEASE)', '$(EXTENSION_BUILD_PATH)/release/$(EXTENSION_LIB_FILENAME)')"

#############################################
### Misc
#############################################
BASE_HEADER_URL=
ifneq ($(TARGET_DUCKDB_VERSION),)
	BASE_HEADER_URL=https://raw.githubusercontent.com/duckdb/duckdb/$(TARGET_DUCKDB_VERSION)/src/include
else
	BASE_HEADER_URL=https://raw.githubusercontent.com/duckdb/duckdb/refs/heads/main/src/include
endif
DUCKDB_C_HEADER_URL=$(BASE_HEADER_URL)/duckdb.h
DUCKDB_C_EXTENSION_HEADER_URL=$(BASE_HEADER_URL)/duckdb_extension.h

update_duckdb_headers:
	$(PYTHON_VENV_BIN) -c "import urllib.request;urllib.request.urlretrieve('$(DUCKDB_C_HEADER_URL)', 'duckdb_capi/duckdb.h')"
	$(PYTHON_VENV_BIN) -c "import urllib.request;urllib.request.urlretrieve('$(DUCKDB_C_EXTENSION_HEADER_URL)', 'duckdb_capi/duckdb_extension.h')"

clean_cmake:
	rm -rf cmake_build
