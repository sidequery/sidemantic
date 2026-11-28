cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME sqlctx)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Probe the host headers: extension builds can contain multiple DuckDB checkouts.
include(CheckCXXSourceCompiles)
include(CMakePushCheckState)
function(sqlctx_detect_apis)
    cmake_push_check_state(RESET)
    get_directory_property(CMAKE_REQUIRED_INCLUDES INCLUDE_DIRECTORIES)
    set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
    set(host_api "${CMAKE_SOURCE_DIR};${CMAKE_CXX_COMPILER}")
    foreach(header parser/parser_extension.hpp function/table_function.hpp
            parser/expression/columnref_expression.hpp parser/tableref/basetableref.hpp
            parser/parser_options.hpp parser/expression/constant_expression.hpp
            parser/statement/insert_statement.hpp
            parser/grammar_extension.hpp parser/peg/compiled_grammar.hpp parser/peg/transformer/peg_transformer.hpp
            planner/operator/logical_get.hpp planner/planner_extension.hpp main/client_config.hpp
            common/types/data_chunk.hpp common/types/vector.hpp)
        if(EXISTS "${CMAKE_SOURCE_DIR}/src/include/duckdb/${header}")
            file(SHA256 "${CMAKE_SOURCE_DIR}/src/include/duckdb/${header}" header_hash)
            string(APPEND host_api ";${header_hash}")
        endif()
    endforeach()
    if(NOT SQLCTX_HOST_API STREQUAL host_api)
        foreach(api TOKEN_PARSE_FN NEW_IDENTIFIER_API NEW_EXPRESSION_API QUALIFIED_TABLE_API BUILTIN_PARSER_OPTIONS LITERAL_API GRAMMAR_EXTENSION INSERT_QUERY_NODE BOUND_TABLE_FUNCTION GRAMMAR_EXTENSION_LIST ARENA_TRANSFORM SHELL_GRAMMAR_CONTEXT PRE_BIND VECTOR_APPEND)
            unset(SQLCTX_${api} CACHE)
        endforeach()
        set(SQLCTX_HOST_API "${host_api}" CACHE INTERNAL "DuckDB headers used for compatibility probes" FORCE)
    endif()
    check_cxx_source_compiles("
        #include <duckdb/parser/parser_extension.hpp>
        #include <type_traits>
        using namespace duckdb;
        using Callback = ParserExtensionParseResult (*)(ParserExtensionInfo *, const vector<SimpleToken> &);
        static_assert(std::is_same<parse_function_t, Callback>::value, \"token callback\");
    " SQLCTX_TOKEN_PARSE_FN)
    check_cxx_source_compiles("
        #include <duckdb/function/table_function.hpp>
        #include <type_traits>
        using namespace duckdb;
        using Callback = unique_ptr<FunctionData> (*)(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &, vector<Identifier> &);
        static_assert(std::is_same<table_function_bind_t, Callback>::value, \"identifier names\");
    " SQLCTX_NEW_IDENTIFIER_API)
    check_cxx_source_compiles("
        #include <duckdb/parser/expression/columnref_expression.hpp>
        void probe(duckdb::ColumnRefExpression &expr) { expr.ColumnNames(); }
    " SQLCTX_NEW_EXPRESSION_API)
    check_cxx_source_compiles("
        #include <duckdb/parser/tableref/basetableref.hpp>
        void probe(duckdb::BaseTableRef &ref) { ref.GetQualifiedName(); }
    " SQLCTX_QUALIFIED_TABLE_API)
    check_cxx_source_compiles("
        #include <duckdb/parser/parser_options.hpp>
        void probe() { duckdb::ParserOptions::Builtin(); }
    " SQLCTX_BUILTIN_PARSER_OPTIONS)
    check_cxx_source_compiles("
        #include <duckdb/parser/expression/constant_expression.hpp>
        void probe() { duckdb::ConstantExpression::String(duckdb::string()); }
    " SQLCTX_LITERAL_API)
    check_cxx_source_compiles("
        #include <duckdb/parser/statement/insert_statement.hpp>
        void probe(duckdb::InsertStatement &stmt) { (void)stmt.node->select_statement; }
    " SQLCTX_INSERT_QUERY_NODE)
    check_cxx_source_compiles("
        #include <duckdb/parser/grammar_extension.hpp>
        #include <duckdb/parser/peg/compiled_grammar.hpp>
        void probe(const duckdb::ClientContext &context) {
            duckdb::CompiledGrammar::Create(context, {\"sqlctx\"});
            duckdb::GrammarChange::PrependChoice(\"Statement\", \"SqlctxStatement\");
        }
    " SQLCTX_GRAMMAR_EXTENSION)
    check_cxx_source_compiles("
        #include <duckdb/planner/operator/logical_get.hpp>
        void probe(duckdb::TableFunction &function) { (void)duckdb::BoundTableFunction(function); }
    " SQLCTX_BOUND_TABLE_FUNCTION)
    check_cxx_source_compiles("
        #include <duckdb/parser/grammar_extension.hpp>
        #include <duckdb/parser/peg/compiled_grammar.hpp>
        void probe(duckdb::GrammarExtension &extension) {
            duckdb::CompiledGrammar::Create(duckdb::vector<duckdb::reference<duckdb::GrammarExtension>> {extension});
        }
    " SQLCTX_GRAMMAR_EXTENSION_LIST)
    check_cxx_source_compiles("
        #include <duckdb/parser/peg/transformer/peg_transformer.hpp>
        void probe(duckdb::PEGTransformer &transformer) { transformer.MakeResult<bool>(true); }
    " SQLCTX_ARENA_TRANSFORM)
    check_cxx_source_compiles("
        #include <duckdb/main/client_config.hpp>
        #include <duckdb/parser/parser_options.hpp>
        #include <duckdb/parser/peg/compiled_grammar.hpp>
        using namespace duckdb;
        void probe(ParserOptions &options) {
            auto &config = ClientConfig::GetConfig(*options.context);
            if (!config.current_dialect && !config.connected_grammar)
                CompiledGrammar::Create(*options.context, vector<string>{\"shell\", \"sqlctx\"});
        }
    " SQLCTX_SHELL_GRAMMAR_CONTEXT)
    check_cxx_source_compiles("
        #include <duckdb/planner/planner_extension.hpp>
        void probe(duckdb::PlannerExtension &extension) { extension.pre_bind_function = nullptr; }
    " SQLCTX_PRE_BIND)
    check_cxx_source_compiles("
        #include <duckdb/common/types/data_chunk.hpp>
        void probe(duckdb::DataChunk &chunk, const duckdb::Value &value) {
            chunk.data[0].Append(value);
            chunk.CheckCardinality(1);
        }
    " SQLCTX_VECTOR_APPEND)
    configure_file(src/include/sqlctx_compat.hpp.in sqlctx_compat.hpp)
    cmake_pop_check_state()
endfunction()
sqlctx_detect_apis()
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Path to sqlctx-rs
set(SQLCTX_RS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../sqlctx-rs")
set(SQLCTX_CARGO_TARGET_DIR "${SQLCTX_RS_DIR}/target" CACHE PATH "Cargo target directory for sqlctx-rs")
set(SQLCTX_CARGO_PROFILE "release" CACHE STRING "Cargo profile used for the sqlctx-rs static library")
set(SQLCTX_CARGO_TARGET "" CACHE STRING "Rust target triple for sqlctx-rs static library")
set(SQLCTX_RUST_NATIVE_STATIC_LIBS "" CACHE STRING "Additional native libraries required by the sqlctx-rs staticlib")
set(SQLCTX_INCLUDE "${SQLCTX_RS_DIR}/include")

if(NOT SQLCTX_CARGO_TARGET AND DEFINED Rust_CARGO_TARGET)
    set(SQLCTX_CARGO_TARGET "${Rust_CARGO_TARGET}" CACHE STRING "Rust target triple for sqlctx-rs static library" FORCE)
endif()

if(SQLCTX_CARGO_TARGET MATCHES "pc-windows-msvc$")
    set(SQLCTX_STATICLIB_NAME "sqlctx.lib")
elseif(SQLCTX_CARGO_TARGET)
    set(SQLCTX_STATICLIB_NAME "libsqlctx.a")
elseif(WIN32)
    set(SQLCTX_STATICLIB_NAME "sqlctx.lib")
else()
    set(SQLCTX_STATICLIB_NAME "libsqlctx.a")
endif()

if(SQLCTX_CARGO_TARGET)
    set(SQLCTX_LIB_DIR "${SQLCTX_CARGO_TARGET_DIR}/${SQLCTX_CARGO_TARGET}/${SQLCTX_CARGO_PROFILE}")
else()
    set(SQLCTX_LIB_DIR "${SQLCTX_CARGO_TARGET_DIR}/${SQLCTX_CARGO_PROFILE}")
endif()

set(SQLCTX_LIB "${SQLCTX_LIB_DIR}/${SQLCTX_STATICLIB_NAME}")
message(STATUS "sqlctx-rs cargo target: '${SQLCTX_CARGO_TARGET}'")
message(STATUS "sqlctx-rs static library: ${SQLCTX_LIB}")

find_program(CARGO_EXECUTABLE cargo)
if(NOT CARGO_EXECUTABLE)
    message(FATAL_ERROR "cargo is required to build the sqlctx DuckDB extension")
endif()

set(SQLCTX_CARGO_BUILD_ARGS build --locked --manifest-path "${SQLCTX_RS_DIR}/Cargo.toml" --lib)
if(SQLCTX_CARGO_TARGET)
    list(APPEND SQLCTX_CARGO_BUILD_ARGS --target "${SQLCTX_CARGO_TARGET}")
endif()
if(SQLCTX_CARGO_PROFILE STREQUAL "release")
    list(APPEND SQLCTX_CARGO_BUILD_ARGS --release)
elseif(NOT SQLCTX_CARGO_PROFILE STREQUAL "debug")
    list(APPEND SQLCTX_CARGO_BUILD_ARGS --profile "${SQLCTX_CARGO_PROFILE}")
endif()

file(GLOB_RECURSE SQLCTX_RUST_SOURCES
     "${SQLCTX_RS_DIR}/src/*.rs"
     "${SQLCTX_RS_DIR}/schemas/ossie/*.json"
     "${SQLCTX_RS_DIR}/include/*.h")

add_custom_command(
    OUTPUT "${SQLCTX_LIB}"
    COMMAND ${CMAKE_COMMAND} -E env "CARGO_TARGET_DIR=${SQLCTX_CARGO_TARGET_DIR}" "${CARGO_EXECUTABLE}" ${SQLCTX_CARGO_BUILD_ARGS}
    WORKING_DIRECTORY "${SQLCTX_RS_DIR}"
    DEPENDS "${SQLCTX_RS_DIR}/Cargo.toml" "${SQLCTX_RS_DIR}/Cargo.lock" ${SQLCTX_RUST_SOURCES}
    COMMENT "Building sqlctx-rs static library"
    VERBATIM)

add_custom_target(sqlctx_rust_staticlib DEPENDS "${SQLCTX_LIB}")

# Include Rust library headers
include_directories(${SQLCTX_INCLUDE})

set(EXTENSION_SOURCES src/sqlctx_extension.cpp src/sqlctx_parser.cpp src/sqlctx_grammar.cpp src/sqlctx_catalog.cpp src/sqlctx_api.cpp src/sqlctx_discovery.cpp src/sqlctx_semantic_input.cpp src/sqlctx_explain.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

add_dependencies(${EXTENSION_NAME} sqlctx_rust_staticlib)
add_dependencies(${LOADABLE_EXTENSION_NAME} sqlctx_rust_staticlib)

# Link the Rust static library and native libraries rustc would normally pass
# when producing a final binary for the target platform.
if(APPLE)
    list(APPEND SQLCTX_RUST_NATIVE_STATIC_LIBS "-framework Security" "-framework CoreFoundation")
endif()
if(UNIX AND NOT APPLE)
    list(APPEND SQLCTX_RUST_NATIVE_STATIC_LIBS pthread dl m)
endif()
if(WIN32 OR SQLCTX_CARGO_TARGET MATCHES "pc-windows")
    list(APPEND SQLCTX_RUST_NATIVE_STATIC_LIBS advapi32 ws2_32 userenv bcrypt ntdll)
endif()

target_link_libraries(${EXTENSION_NAME} ${SQLCTX_LIB} ${SQLCTX_RUST_NATIVE_STATIC_LIBS})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${SQLCTX_LIB} ${SQLCTX_RUST_NATIVE_STATIC_LIBS})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
