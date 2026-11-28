#pragma once

#include "sqlctx.h"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

// Only the semantic graph is retained. DuckDB source binding stays live in the
// caller's transaction. Input snapshot bytes are borrowed only during creation.
class SqlctxCompilerOwner {
public:
    explicit SqlctxCompilerOwner(const string &snapshot) {
        char *error = nullptr;
        compiler = sqlctx_compiler_create(snapshot.c_str(), &error);
        if (!compiler) {
            string message = error ? error : "cannot create semantic compiler";
            sqlctx_free(error);
            throw InvalidInputException("sqlctx: %s", message);
        }
    }
    ~SqlctxCompilerOwner() { sqlctx_compiler_free(compiler); }
    SqlctxCompilerOwner(const SqlctxCompilerOwner &) = delete;
    SqlctxCompilerOwner &operator=(const SqlctxCompilerOwner &) = delete;

    SqlctxRewriteResult Rewrite(const string &sql) const {
        return sqlctx_compiler_rewrite(compiler, sql.c_str());
    }

private:
    SqlctxCompiler *compiler = nullptr;
};

LogicalType SqlctxFieldType(ClientContext &context, const SqlctxCompilerOwner &compiler,
                           const string &model, const string &field, const string &catalog_name);

} // namespace duckdb
