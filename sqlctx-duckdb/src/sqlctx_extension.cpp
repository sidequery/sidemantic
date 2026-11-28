#define DUCKDB_EXTENSION_MAIN

#include "sqlctx_extension.hpp"
#include "sqlctx_explain.hpp"
#include "sqlctx_parser.hpp"
#include "sqlctx_catalog.hpp"
#include "sqlctx_semantic_input.hpp"
#include "sqlctx.h"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

struct LoadData : public TableFunctionData {
    string operation;
    string content;
};

struct LoadState : public GlobalTableFunctionState {
    bool done = false;
};

template <bool FROM_FILE>
static unique_ptr<FunctionData> LoadBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &types, vector<SqlctxIdentifier> &names) {
    auto data = make_uniq<LoadData>();
    data->operation = FROM_FILE ? "file" : "yaml";
    if (input.inputs[0].IsNull()) throw InvalidInputException("sqlctx load argument cannot be NULL");
    data->content = input.inputs[0].GetValue<string>();
    types.push_back(LogicalType::VARCHAR);
    names.emplace_back("result");
    if (input.binder) RegisterSqlctxCatalogWrite(context, input.binder->GetStatementProperties());
    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> LoadInit(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<LoadState>();
}

static void LoadFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<LoadState>();
    if (state.done) return;
    auto &data = input.bind_data->Cast<LoadData>();
    auto summary = ExecuteSqlctxMutation(context, data.operation, data.content, false);
    state.done = true;
    output.SetCardinality(1);
    output.SetValue(0, 0, Value(summary));
}

struct ModelsState : public GlobalTableFunctionState {
    vector<string> names;
    idx_t offset = 0;
};

static unique_ptr<FunctionData> ModelsBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &types, vector<SqlctxIdentifier> &names) {
    types.push_back(LogicalType::VARCHAR);
    names.emplace_back("model_name");
    if (input.binder) RegisterSqlctxCatalogRead(context, input.binder->GetStatementProperties());
    return nullptr;
}

static unique_ptr<GlobalTableFunctionState> ModelsInit(ClientContext &context, TableFunctionInitInput &) {
    auto state = make_uniq<ModelsState>();
    auto snapshot = ReadSqlctxCatalog(context);
    auto models = sqlctx_snapshot_list_models(snapshot.payload.c_str());
    if (models.error) {
        string error(models.error);
        sqlctx_free_model_list(models);
        throw InvalidInputException("sqlctx: %s", SqlctxErrorText(error));
    }
    for (idx_t i = 0; i < models.count; ++i) state->names.emplace_back(models.models[i].name);
    sqlctx_free_model_list(models);
    return std::move(state);
}

static void ModelsFunction(ClientContext &, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<ModelsState>();
    auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.names.size() - state.offset);
    for (idx_t i = 0; i < count; ++i) output.SetValue(0, i, Value(state.names[state.offset + i]));
    state.offset += count;
    output.SetCardinality(count);
}

static void RewriteFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto snapshot = ReadSqlctxCatalog(state.GetContext());
    UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
        auto sql = input.GetString();
        if (sql.find('\0') != string::npos) throw InvalidInputException("sqlctx SQL contains a NUL byte");
        auto rewritten = sqlctx_snapshot_rewrite(snapshot.payload.c_str(), sql.c_str());
        if (rewritten.error) {
            string error(rewritten.error);
            sqlctx_free_result(rewritten);
            throw InvalidInputException("sqlctx: %s", SqlctxErrorText(error));
        }
        if (!rewritten.sql) {
            sqlctx_free_result(rewritten);
            throw InternalException("sqlctx returned no SQL");
        }
        string sql_result(rewritten.sql);
        sqlctx_free_result(rewritten);
        return StringVector::AddString(result, sql_result);
    });
}

static void LoadInternal(ExtensionLoader &loader) {
    auto &db = loader.GetDatabaseInstance();
    auto &config = DBConfig::GetConfig(db);
    config.SetOptionByName("allow_parser_override_extension", Value("fallback"));
    SqlctxParserExtension parser;
    parser.parser_info = RegisterSqlctxGrammar(db);
    ParserExtension::Register(config, parser);
    OperatorExtension::Register(config, make_shared_ptr<SqlctxOperatorExtension>());
    RegisterSqlctxRouting(db);
    RegisterSqlctxQueryCarrier(loader);
    RegisterSqlctxDiscovery(db);
    RegisterSqlctxExplain(loader);

    loader.RegisterFunction(TableFunction("sqlctx_load", {LogicalType::VARCHAR}, LoadFunction, LoadBind<false>, LoadInit));
    loader.RegisterFunction(TableFunction("sqlctx_load_file", {LogicalType::VARCHAR}, LoadFunction, LoadBind<true>, LoadInit));
    loader.RegisterFunction(TableFunction("sqlctx_models", {}, ModelsFunction, ModelsBind, ModelsInit));

    auto rewrite = ScalarFunction("sqlctx_rewrite_sql", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RewriteFunction);
    rewrite.SetStability(FunctionStability::VOLATILE);
    rewrite.SetFallible();
    loader.RegisterFunction(rewrite);
    RegisterSqlctxSemanticInput(loader);
}

void SqlctxExtension::Load(ExtensionLoader &loader) { LoadInternal(loader); }

std::string SqlctxExtension::Version() const {
#ifdef EXT_VERSION_SQLCTX
    return EXT_VERSION_SQLCTX;
#else
    return "0.1.0";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(sqlctx, loader) { duckdb::LoadInternal(loader); }
}
