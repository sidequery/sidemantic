#include "sqlctx_semantic_input.hpp"
#include "sqlctx_compat.hpp"
#include "sqlctx.h"

#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {
namespace {

// Stateless SemanticInput entrypoints retain policies in the canonical input.
// Reject NULs before crossing the C string boundary instead of truncating input.
static string SemanticInputArgument(const string &text) {
    if (text.find('\0') != string::npos) {
        throw InvalidInputException("SemanticInput argument contains a NUL byte");
    }
    return text;
}

static string SemanticInputSQL(SqlctxRewriteResult compiled) {
    if (compiled.error) {
        string message(compiled.error);
        sqlctx_free_result(compiled);
        throw InvalidInputException("SemanticInput failed: %s", message);
    }
    if (!compiled.sql) {
        sqlctx_free_result(compiled);
        throw InvalidInputException("SemanticInput returned no SQL");
    }
    string sql(compiled.sql);
    sqlctx_free_result(compiled);
    return sql;
}

static void SqlctxCompileSemanticInputFunction(DataChunk &args, ExpressionState &, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, string_t>(
        args.data[0], args.data[1], result, args.size(), [&](string_t input, string_t query) {
            auto input_json = SemanticInputArgument(input.GetString());
            auto query_json = SemanticInputArgument(query.GetString());
            return StringVector::AddString(result, SemanticInputSQL(
                sqlctx_compile_semantic_input(input_json.c_str(), query_json.c_str())));
        });
}

static void SqlctxRewriteSemanticInputFunction(DataChunk &args, ExpressionState &, Vector &result) {
    TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
        args.data[0], args.data[1], args.data[2], result, args.size(),
        [&](string_t input, string_t sql, string_t context) {
            auto input_json = SemanticInputArgument(input.GetString());
            auto sql_text = SemanticInputArgument(sql.GetString());
            auto context_json = SemanticInputArgument(context.GetString());
            return StringVector::AddString(result, SemanticInputSQL(sqlctx_rewrite_semantic_input(
                input_json.c_str(), sql_text.c_str(), context_json.c_str())));
        });
}

static unique_ptr<TableRef> SqlctxQuerySemanticInputBindReplace(ClientContext &, TableFunctionBindInput &input) {
    if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
        throw InvalidInputException("SemanticInput relation arguments cannot be NULL");
    }
    auto input_json = SemanticInputArgument(input.inputs[0].GetValue<string>());
    auto query_json = SemanticInputArgument(input.inputs[1].GetValue<string>());
    auto sql = SemanticInputSQL(sqlctx_compile_semantic_input(input_json.c_str(), query_json.c_str()));

    // Let DuckDB's query() binder expand the generated SELECT in the caller's
    // scope and transaction. It also owns the host's prepared-parameter checks.
    // No separate connection, execution, or result materialization is involved.
    vector<unique_ptr<ParsedExpression>> arguments;
#if SQLCTX_LITERAL_API
    arguments.push_back(ConstantExpression::String(sql));
#else
    arguments.push_back(make_uniq<ConstantExpression>(Value(sql)));
#endif
    auto relation = make_uniq<TableFunctionRef>();
    relation->function = make_uniq<FunctionExpression>(SqlctxIdentifier("query"), std::move(arguments));
    return std::move(relation);
}

} // namespace

void RegisterSqlctxSemanticInput(ExtensionLoader &loader) {
    auto compile_input = ScalarFunction("sqlctx_compile_semantic_input",
        {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, SqlctxCompileSemanticInputFunction);
    compile_input.SetFallible();
    loader.RegisterFunction(compile_input);
    auto rewrite_input = ScalarFunction("sqlctx_rewrite_semantic_input",
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
        SqlctxRewriteSemanticInputFunction);
    rewrite_input.SetFallible();
    loader.RegisterFunction(rewrite_input);
    TableFunction query_input("sqlctx_query_semantic_input", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                              nullptr, nullptr);
    query_input.bind_replace = SqlctxQuerySemanticInputBindReplace;
    loader.RegisterFunction(query_input);
}

} // namespace duckdb
