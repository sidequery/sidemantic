cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME sidemantic)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Path to sidemantic-rs
set(SIDEMANTIC_RS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../sidemantic-rs")
set(SIDEMANTIC_LIB "${SIDEMANTIC_RS_DIR}/target/release/libsidemantic.a")
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
