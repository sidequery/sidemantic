cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME sidemantic)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Path to sidemantic-rs
set(SIDEMANTIC_RS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../sidemantic-rs")
set(SIDEMANTIC_CARGO_TARGET_DIR "${SIDEMANTIC_RS_DIR}/target" CACHE PATH "Cargo target directory for sidemantic-rs")
set(SIDEMANTIC_CARGO_PROFILE "release" CACHE STRING "Cargo profile used for the sidemantic-rs static library")
set(SIDEMANTIC_INCLUDE "${SIDEMANTIC_RS_DIR}/include")

if(WIN32)
    set(SIDEMANTIC_STATICLIB_NAME "sidemantic.lib")
else()
    set(SIDEMANTIC_STATICLIB_NAME "libsidemantic.a")
endif()

set(SIDEMANTIC_LIB "${SIDEMANTIC_CARGO_TARGET_DIR}/${SIDEMANTIC_CARGO_PROFILE}/${SIDEMANTIC_STATICLIB_NAME}")

find_program(CARGO_EXECUTABLE cargo)
if(NOT CARGO_EXECUTABLE)
    message(FATAL_ERROR "cargo is required to build the sidemantic DuckDB extension")
endif()

set(SIDEMANTIC_CARGO_BUILD_ARGS build --manifest-path "${SIDEMANTIC_RS_DIR}/Cargo.toml" --lib)
if(SIDEMANTIC_CARGO_PROFILE STREQUAL "release")
    list(APPEND SIDEMANTIC_CARGO_BUILD_ARGS --release)
elseif(NOT SIDEMANTIC_CARGO_PROFILE STREQUAL "debug")
    list(APPEND SIDEMANTIC_CARGO_BUILD_ARGS --profile "${SIDEMANTIC_CARGO_PROFILE}")
endif()

file(GLOB_RECURSE SIDEMANTIC_RUST_SOURCES
     "${SIDEMANTIC_RS_DIR}/src/*.rs"
     "${SIDEMANTIC_RS_DIR}/include/*.h")

add_custom_command(
    OUTPUT "${SIDEMANTIC_LIB}"
    COMMAND ${CMAKE_COMMAND} -E env "CARGO_TARGET_DIR=${SIDEMANTIC_CARGO_TARGET_DIR}" "${CARGO_EXECUTABLE}" ${SIDEMANTIC_CARGO_BUILD_ARGS}
    WORKING_DIRECTORY "${SIDEMANTIC_RS_DIR}"
    DEPENDS "${SIDEMANTIC_RS_DIR}/Cargo.toml" "${SIDEMANTIC_RS_DIR}/Cargo.lock" ${SIDEMANTIC_RUST_SOURCES}
    COMMENT "Building sidemantic-rs static library"
    VERBATIM)

add_custom_target(sidemantic_rust_staticlib DEPENDS "${SIDEMANTIC_LIB}")

# Include Rust library headers
include_directories(${SIDEMANTIC_INCLUDE})

set(EXTENSION_SOURCES src/sidemantic_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

add_dependencies(${EXTENSION_NAME} sidemantic_rust_staticlib)
add_dependencies(${LOADABLE_EXTENSION_NAME} sidemantic_rust_staticlib)

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
