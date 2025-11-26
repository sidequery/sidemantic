#define DUCKDB_EXTENSION_MAIN

#include "sidemantic_extension.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/function/table_function.hpp"

// Rust FFI
extern "C" {
    struct SidemanticRewriteResult {
        char *sql;
        char *error;
        bool was_rewritten;
    };

    char *sidemantic_load_yaml(const char *yaml);
    void sidemantic_clear(void);
    bool sidemantic_is_model(const char *table_name);
    char *sidemantic_list_models(void);
    SidemanticRewriteResult sidemantic_rewrite(const char *sql);
    void sidemantic_free(char *ptr);
    void sidemantic_free_result(SidemanticRewriteResult result);
}

namespace duckdb {

//=============================================================================
// TABLE FUNCTION: sidemantic_load(yaml)
//=============================================================================

struct SidemanticLoadData : public TableFunctionData {
    string yaml_content;
    bool done = false;
};

static unique_ptr<FunctionData> SidemanticLoadBind(ClientContext &context,
                                                    TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types,
                                                    vector<string> &names) {
    auto result = make_uniq<SidemanticLoadData>();
    result->yaml_content = input.inputs[0].GetValue<string>();

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("result");

    return std::move(result);
}

static void SidemanticLoadFunction(ClientContext &context, TableFunctionInput &data_p,
                                   DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<SidemanticLoadData>();
    if (data.done) {
        return;
    }
    data.done = true;

    char *error = sidemantic_load_yaml(data.yaml_content.c_str());
    if (error) {
        string error_msg(error);
        sidemantic_free(error);
        throw InvalidInputException("Failed to load semantic models: %s", error_msg);
    }

    output.SetCardinality(1);
    output.SetValue(0, 0, Value("Models loaded successfully"));
}

//=============================================================================
// TABLE FUNCTION: sidemantic_models()
//=============================================================================

struct SidemanticModelsData : public TableFunctionData {
    bool done = false;
};

static unique_ptr<FunctionData> SidemanticModelsBind(ClientContext &context,
                                                      TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types,
                                                      vector<string> &names) {
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("model_name");
    return make_uniq<SidemanticModelsData>();
}

static void SidemanticModelsFunction(ClientContext &context, TableFunctionInput &data_p,
                                     DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<SidemanticModelsData>();
    if (data.done) {
        return;
    }
    data.done = true;

    char *models_str = sidemantic_list_models();
    if (!models_str) {
        output.SetCardinality(0);
        return;
    }

    string models(models_str);
    sidemantic_free(models_str);

    if (models.empty()) {
        output.SetCardinality(0);
        return;
    }

    // Split by comma
    vector<string> model_names;
    size_t pos = 0;
    while ((pos = models.find(',')) != string::npos) {
        model_names.push_back(models.substr(0, pos));
        models.erase(0, pos + 1);
    }
    if (!models.empty()) {
        model_names.push_back(models);
    }

    output.SetCardinality(model_names.size());
    for (idx_t i = 0; i < model_names.size(); i++) {
        output.SetValue(0, i, Value(model_names[i]));
    }
}

//=============================================================================
// SCALAR FUNCTION: sidemantic_rewrite_sql(sql)
//=============================================================================

static void SidemanticRewriteSqlFunction(DataChunk &args, ExpressionState &state,
                                          Vector &result) {
    auto &sql_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
        sql_vector, result, args.size(), [&](string_t sql) {
            SidemanticRewriteResult res = sidemantic_rewrite(sql.GetString().c_str());

            if (res.error) {
                string error_msg(res.error);
                sidemantic_free_result(res);
                throw InvalidInputException("Rewrite failed: %s", error_msg);
            }

            string rewritten(res.sql);
            sidemantic_free_result(res);
            return StringVector::AddString(result, rewritten);
        });
}

//=============================================================================
// PARSER EXTENSION
//=============================================================================

// Check if query starts with SEMANTIC keyword (case insensitive)
static bool StartsWithSemantic(const std::string &query, std::string &stripped_query) {
    // Skip leading whitespace
    size_t start = 0;
    while (start < query.size() && std::isspace(query[start])) {
        start++;
    }

    // Check for "SEMANTIC" prefix (case insensitive)
    const char *prefix = "SEMANTIC";
    size_t prefix_len = 8;

    if (query.size() - start < prefix_len) {
        return false;
    }

    for (size_t i = 0; i < prefix_len; i++) {
        if (std::toupper(query[start + i]) != prefix[i]) {
            return false;
        }
    }

    // Must be followed by whitespace
    if (start + prefix_len < query.size() && !std::isspace(query[start + prefix_len])) {
        return false;
    }

    // Strip the SEMANTIC prefix
    stripped_query = query.substr(start + prefix_len);
    return true;
}

ParserExtensionParseResult sidemantic_parse(ParserExtensionInfo *,
                                            const std::string &query) {
    // Check for SEMANTIC prefix
    std::string stripped_query;
    if (!StartsWithSemantic(query, stripped_query)) {
        // Not a semantic query, let DuckDB handle it
        return ParserExtensionParseResult();
    }

    // Try to rewrite the query using sidemantic
    SidemanticRewriteResult result = sidemantic_rewrite(stripped_query.c_str());

    // If there was an error, return it
    if (result.error) {
        string error_msg(result.error);
        sidemantic_free_result(result);
        return ParserExtensionParseResult(error_msg);
    }

    // Parse the rewritten SQL using DuckDB's parser
    string rewritten_sql(result.sql);
    sidemantic_free_result(result);

    Parser parser;
    parser.ParseQuery(rewritten_sql);
    auto statements = std::move(parser.statements);

    if (statements.empty()) {
        return ParserExtensionParseResult("Rewritten query produced no statements");
    }

    // Return the parsed statement
    return ParserExtensionParseResult(
        make_uniq_base<ParserExtensionParseData, SidemanticParseData>(
            std::move(statements[0])));
}

ParserExtensionPlanResult sidemantic_plan(ParserExtensionInfo *,
                                          ClientContext &context,
                                          unique_ptr<ParserExtensionParseData> parse_data) {
    // Store parse data in client context state
    auto state = make_shared_ptr<SidemanticState>(std::move(parse_data));
    context.registered_state->Remove("sidemantic");
    context.registered_state->Insert("sidemantic", state);

    // Throw to trigger the operator extension's Bind function
    throw BinderException("Use sidemantic_bind instead");
}

BoundStatement sidemantic_bind(ClientContext &context, Binder &binder,
                               OperatorExtensionInfo *info, SQLStatement &statement) {
    switch (statement.type) {
    case StatementType::EXTENSION_STATEMENT: {
        auto &ext_statement = dynamic_cast<ExtensionStatement &>(statement);

        // Check this is our extension's statement
        if (ext_statement.extension.parse_function == sidemantic_parse) {
            // Retrieve stashed parse data
            auto lookup = context.registered_state->Get<SidemanticState>("sidemantic");
            if (lookup) {
                auto state = (SidemanticState *)lookup.get();
                auto sidemantic_binder = Binder::CreateBinder(context, &binder);
                auto parse_data = dynamic_cast<SidemanticParseData *>(state->parse_data.get());

                // Bind the SQL statement we generated
                return sidemantic_binder->Bind(*(parse_data->statement));
            }
            throw BinderException("Registered state not found");
        }
    }
    default:
        return {};  // Not ours
    }
}

//=============================================================================
// EXTENSION LOADING
//=============================================================================

static void LoadInternal(ExtensionLoader &loader) {
    auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());

    // Register parser extension
    SidemanticParserExtension parser;
    config.parser_extensions.push_back(parser);

    // Register operator extension
    config.operator_extensions.push_back(make_uniq<SidemanticOperatorExtension>());

    // Register table functions
    TableFunction load_func("sidemantic_load", {LogicalType::VARCHAR},
                            SidemanticLoadFunction, SidemanticLoadBind);
    loader.RegisterFunction(load_func);

    TableFunction models_func("sidemantic_models", {},
                              SidemanticModelsFunction, SidemanticModelsBind);
    loader.RegisterFunction(models_func);

    // Register scalar function for manual rewriting
    auto rewrite_func = ScalarFunction("sidemantic_rewrite_sql",
                                        {LogicalType::VARCHAR},
                                        LogicalType::VARCHAR,
                                        SidemanticRewriteSqlFunction);
    loader.RegisterFunction(rewrite_func);
}

void SidemanticExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string SidemanticExtension::Version() const {
#ifdef EXT_VERSION_SIDEMANTIC
    return EXT_VERSION_SIDEMANTIC;
#else
    return "0.1.0";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(sidemantic, loader) {
    duckdb::LoadInternal(loader);
}

}
