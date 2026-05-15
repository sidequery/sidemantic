cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME sidemantic)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Path to sidemantic-rs
set(SIDEMANTIC_RS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../sidemantic-rs")
set(SIDEMANTIC_WORKSPACE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/..")
execute_process(
    COMMAND cargo metadata --format-version 1 --no-deps
    WORKING_DIRECTORY "${SIDEMANTIC_RS_DIR}"
    OUTPUT_VARIABLE SIDEMANTIC_CARGO_METADATA
    ERROR_QUIET
    RESULT_VARIABLE SIDEMANTIC_CARGO_METADATA_RESULT)

set(SIDEMANTIC_LIB_CANDIDATES)
if(SIDEMANTIC_CARGO_METADATA_RESULT EQUAL 0)
    string(REGEX MATCH "\"target_directory\":\"([^\"]+)\"" _SIDEMANTIC_TARGET_MATCH "${SIDEMANTIC_CARGO_METADATA}")
    if(_SIDEMANTIC_TARGET_MATCH)
        list(APPEND SIDEMANTIC_LIB_CANDIDATES "${CMAKE_MATCH_1}/release/libsidemantic.a")
    endif()
endif()
list(APPEND SIDEMANTIC_LIB_CANDIDATES
    "${SIDEMANTIC_RS_DIR}/target/release/libsidemantic.a"
    "${SIDEMANTIC_WORKSPACE_DIR}/target/release/libsidemantic.a")
foreach(SIDEMANTIC_LIB_CANDIDATE ${SIDEMANTIC_LIB_CANDIDATES})
    if(EXISTS "${SIDEMANTIC_LIB_CANDIDATE}")
        set(SIDEMANTIC_LIB "${SIDEMANTIC_LIB_CANDIDATE}")
        break()
    endif()
endforeach()
if(NOT SIDEMANTIC_LIB)
    message(FATAL_ERROR "libsidemantic.a not found. Run `cargo build --release` before building the DuckDB extension.")
endif()
set(SIDEMANTIC_INCLUDE "${SIDEMANTIC_RS_DIR}/include")

# Include Rust library headers
include_directories(${SIDEMANTIC_INCLUDE})

set(EXTENSION_SOURCES src/sidemantic_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link the Rust static library
target_link_libraries(${EXTENSION_NAME} ${SIDEMANTIC_LIB})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${SIDEMANTIC_LIB})

# On macOS, we need to link against some system frameworks
if(APPLE)
    target_link_libraries(${EXTENSION_NAME} "-framework Security" "-framework CoreFoundation")
    target_link_libraries(${LOADABLE_EXTENSION_NAME} "-framework Security" "-framework CoreFoundation")
endif()

# On Linux, we need pthread and dl
if(UNIX AND NOT APPLE)
    target_link_libraries(${EXTENSION_NAME} pthread dl)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} pthread dl)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
