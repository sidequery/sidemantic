#include "sqlctx_explain.hpp"
#include "sqlctx_catalog.hpp"
#include "sqlctx.h"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include <memory>

namespace duckdb {

static void ExplainFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto snapshot = ReadSqlctxCatalog(state.GetContext());
    char *error = nullptr;
    std::unique_ptr<SqlctxCompiler, decltype(&sqlctx_compiler_free)> compiler(
        sqlctx_compiler_create(snapshot.payload.c_str(), &error), sqlctx_compiler_free);
    if (error) {
        string message(error);
        sqlctx_free(error);
        throw InvalidInputException("sqlctx: %s", SqlctxErrorText(message));
    }
    if (!compiler) throw InternalException("sqlctx returned no compiler");
    UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
        auto sql = input.GetString();
        if (sql.find('\0') != string::npos) throw InvalidInputException("sqlctx SQL contains a NUL byte");
        auto report = sqlctx_compiler_explain(compiler.get(), sql.c_str());
        if (report.error) {
            string error(report.error);
            sqlctx_free_result(report);
            throw InvalidInputException("sqlctx: %s", SqlctxErrorText(error));
        }
        if (!report.sql) {
            sqlctx_free_result(report);
            throw InternalException("sqlctx returned no explanation");
        }
        string json(report.sql);
        sqlctx_free_result(report);
        return StringVector::AddString(result, json);
    });
}

void RegisterSqlctxExplain(ExtensionLoader &loader) {
    auto explain = ScalarFunction("sqlctx_explain", {LogicalType::VARCHAR}, LogicalType::VARCHAR, ExplainFunction);
    explain.SetStability(FunctionStability::VOLATILE);
    explain.SetFallible();
    loader.RegisterFunction(explain);
}

} // namespace duckdb
