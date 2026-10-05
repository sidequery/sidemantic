cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME sidemantic)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Probe the host headers: extension builds can contain multiple DuckDB checkouts.
include(CheckCXXSourceCompiles)
include(CMakePushCheckState)
function(sidemantic_detect_apis)
    cmake_push_check_state(RESET)
    get_directory_property(CMAKE_REQUIRED_INCLUDES INCLUDE_DIRECTORIES)
    set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
    set(host_api "${CMAKE_SOURCE_DIR};${CMAKE_CXX_COMPILER}")
    foreach(header parser/parser_extension.hpp function/table_function.hpp
            parser/expression/columnref_expression.hpp parser/tableref/basetableref.hpp
            parser/parser_options.hpp parser/expression/constant_expression.hpp
            parser/statement/insert_statement.hpp
            parser/grammar_extension.hpp parser/peg/compiled_grammar.hpp)
        if(EXISTS "${CMAKE_SOURCE_DIR}/src/include/duckdb/${header}")
            file(SHA256 "${CMAKE_SOURCE_DIR}/src/include/duckdb/${header}" header_hash)
            string(APPEND host_api ";${header_hash}")
        endif()
    endforeach()
    if(NOT SIDEMANTIC_HOST_API STREQUAL host_api)
        foreach(api TOKEN_PARSE_FN NEW_IDENTIFIER_API NEW_EXPRESSION_API QUALIFIED_TABLE_API BUILTIN_PARSER_OPTIONS LITERAL_API GRAMMAR_EXTENSION INSERT_QUERY_NODE)
            unset(SIDEMANTIC_${api} CACHE)
        endforeach()
        set(SIDEMANTIC_HOST_API "${host_api}" CACHE INTERNAL "DuckDB headers used for compatibility probes" FORCE)
    endif()
    check_cxx_source_compiles("
        #include <duckdb/parser/parser_extension.hpp>
        #include <type_traits>
        using namespace duckdb;
        using Callback = ParserExtensionParseResult (*)(ParserExtensionInfo *, const vector<SimpleToken> &);
        static_assert(std::is_same<parse_function_t, Callback>::value, \"token callback\");
    " SIDEMANTIC_TOKEN_PARSE_FN)
    check_cxx_source_compiles("
        #include <duckdb/function/table_function.hpp>
        #include <type_traits>
        using namespace duckdb;
        using Callback = unique_ptr<FunctionData> (*)(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &, vector<Identifier> &);
        static_assert(std::is_same<table_function_bind_t, Callback>::value, \"identifier names\");
    " SIDEMANTIC_NEW_IDENTIFIER_API)
    check_cxx_source_compiles("
        #include <duckdb/parser/expression/columnref_expression.hpp>
        void probe(duckdb::ColumnRefExpression &expr) { expr.ColumnNames(); }
    " SIDEMANTIC_NEW_EXPRESSION_API)
    check_cxx_source_compiles("
        #include <duckdb/parser/tableref/basetableref.hpp>
        void probe(duckdb::BaseTableRef &ref) { ref.GetQualifiedName(); }
    " SIDEMANTIC_QUALIFIED_TABLE_API)
    check_cxx_source_compiles("
        #include <duckdb/parser/parser_options.hpp>
        void probe() { duckdb::ParserOptions::Builtin(); }
    " SIDEMANTIC_BUILTIN_PARSER_OPTIONS)
    check_cxx_source_compiles("
        #include <duckdb/parser/expression/constant_expression.hpp>
        void probe() { duckdb::ConstantExpression::String(duckdb::string()); }
    " SIDEMANTIC_LITERAL_API)
    check_cxx_source_compiles("
        #include <duckdb/parser/statement/insert_statement.hpp>
        void probe(duckdb::InsertStatement &stmt) { (void)stmt.node->select_statement; }
    " SIDEMANTIC_INSERT_QUERY_NODE)
    check_cxx_source_compiles("
        #include <duckdb/parser/grammar_extension.hpp>
        #include <duckdb/parser/peg/compiled_grammar.hpp>
        void probe(const duckdb::ClientContext &context) {
            duckdb::CompiledGrammar::Create(context, {\"sidemantic\"});
            duckdb::GrammarChange::PrependChoice(\"Statement\", \"SidemanticStatement\");
        }
    " SIDEMANTIC_GRAMMAR_EXTENSION)
    configure_file(src/include/sidemantic_compat.hpp.in sidemantic_compat.hpp)
    cmake_pop_check_state()
endfunction()
sidemantic_detect_apis()
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Path to sidemantic-rs
set(SIDEMANTIC_RS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../sidemantic-rs")
set(SIDEMANTIC_CARGO_TARGET_DIR "${SIDEMANTIC_RS_DIR}/target" CACHE PATH "Cargo target directory for sidemantic-rs")
set(SIDEMANTIC_CARGO_PROFILE "release" CACHE STRING "Cargo profile used for the sidemantic-rs static library")
set(SIDEMANTIC_CARGO_TARGET "" CACHE STRING "Rust target triple for sidemantic-rs static library")
set(SIDEMANTIC_RUST_NATIVE_STATIC_LIBS "" CACHE STRING "Additional native libraries required by the sidemantic-rs staticlib")
set(SIDEMANTIC_INCLUDE "${SIDEMANTIC_RS_DIR}/include")

if(NOT SIDEMANTIC_CARGO_TARGET AND DEFINED Rust_CARGO_TARGET)
    set(SIDEMANTIC_CARGO_TARGET "${Rust_CARGO_TARGET}" CACHE STRING "Rust target triple for sidemantic-rs static library" FORCE)
endif()

if(SIDEMANTIC_CARGO_TARGET MATCHES "pc-windows-msvc$")
    set(SIDEMANTIC_STATICLIB_NAME "sidemantic.lib")
elseif(SIDEMANTIC_CARGO_TARGET)
    set(SIDEMANTIC_STATICLIB_NAME "libsidemantic.a")
elseif(WIN32)
    set(SIDEMANTIC_STATICLIB_NAME "sidemantic.lib")
else()
    set(SIDEMANTIC_STATICLIB_NAME "libsidemantic.a")
endif()

if(SIDEMANTIC_CARGO_TARGET)
    set(SIDEMANTIC_LIB_DIR "${SIDEMANTIC_CARGO_TARGET_DIR}/${SIDEMANTIC_CARGO_TARGET}/${SIDEMANTIC_CARGO_PROFILE}")
else()
    set(SIDEMANTIC_LIB_DIR "${SIDEMANTIC_CARGO_TARGET_DIR}/${SIDEMANTIC_CARGO_PROFILE}")
endif()

set(SIDEMANTIC_LIB "${SIDEMANTIC_LIB_DIR}/${SIDEMANTIC_STATICLIB_NAME}")
message(STATUS "sidemantic-rs cargo target: '${SIDEMANTIC_CARGO_TARGET}'")
message(STATUS "sidemantic-rs static library: ${SIDEMANTIC_LIB}")

find_program(CARGO_EXECUTABLE cargo)
if(NOT CARGO_EXECUTABLE)
    message(FATAL_ERROR "cargo is required to build the sidemantic DuckDB extension")
endif()

set(SIDEMANTIC_CARGO_BUILD_ARGS build --manifest-path "${SIDEMANTIC_RS_DIR}/Cargo.toml" --lib)
if(SIDEMANTIC_CARGO_TARGET)
    list(APPEND SIDEMANTIC_CARGO_BUILD_ARGS --target "${SIDEMANTIC_CARGO_TARGET}")
endif()
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

set(EXTENSION_SOURCES src/sidemantic_extension.cpp src/sidemantic_parser.cpp src/sidemantic_grammar.cpp src/sidemantic_catalog.cpp src/sidemantic_api.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

add_dependencies(${EXTENSION_NAME} sidemantic_rust_staticlib)
add_dependencies(${LOADABLE_EXTENSION_NAME} sidemantic_rust_staticlib)

# Link the Rust static library and native libraries rustc would normally pass
# when producing a final binary for the target platform.
if(APPLE)
    list(APPEND SIDEMANTIC_RUST_NATIVE_STATIC_LIBS "-framework Security" "-framework CoreFoundation")
endif()
if(UNIX AND NOT APPLE)
    list(APPEND SIDEMANTIC_RUST_NATIVE_STATIC_LIBS pthread dl m)
endif()
if(WIN32 OR SIDEMANTIC_CARGO_TARGET MATCHES "pc-windows")
    list(APPEND SIDEMANTIC_RUST_NATIVE_STATIC_LIBS advapi32 ws2_32 userenv bcrypt ntdll)
endif()

target_link_libraries(${EXTENSION_NAME} ${SIDEMANTIC_LIB} ${SIDEMANTIC_RUST_NATIVE_STATIC_LIBS})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${SIDEMANTIC_LIB} ${SIDEMANTIC_RUST_NATIVE_STATIC_LIBS})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
