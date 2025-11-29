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
    char *sidemantic_load_file(const char *path);
    void sidemantic_clear(void);
    bool sidemantic_is_model(const char *table_name);
    char *sidemantic_list_models(void);
    SidemanticRewriteResult sidemantic_rewrite(const char *sql);
    void sidemantic_free(char *ptr);
    void sidemantic_free_result(SidemanticRewriteResult result);
    char *sidemantic_define(const char *definition_sql, const char *db_path, bool replace);
    char *sidemantic_autoload(const char *db_path);
    char *sidemantic_add_definition(const char *definition_sql, const char *db_path, bool is_replace);
    char *sidemantic_use(const char *model_name);
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
// TABLE FUNCTION: sidemantic_load_file(path)
//=============================================================================

struct SidemanticLoadFileData : public TableFunctionData {
    string file_path;
    bool done = false;
};

static unique_ptr<FunctionData> SidemanticLoadFileBind(ClientContext &context,
                                                        TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types,
                                                        vector<string> &names) {
    auto result = make_uniq<SidemanticLoadFileData>();
    result->file_path = input.inputs[0].GetValue<string>();

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("result");

    return std::move(result);
}

static void SidemanticLoadFileFunction(ClientContext &context, TableFunctionInput &data_p,
                                        DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<SidemanticLoadFileData>();
    if (data.done) {
        return;
    }
    data.done = true;

    char *error = sidemantic_load_file(data.file_path.c_str());
    if (error) {
        string error_msg(error);
        sidemantic_free(error);
        throw InvalidInputException("Failed to load semantic models: %s", error_msg);
    }

    output.SetCardinality(1);
    output.SetValue(0, 0, Value("Models loaded from: " + data.file_path));
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

// Check if a string starts with a keyword (case insensitive), skipping whitespace
static bool StartsWithKeyword(const std::string &str, const std::string &keyword, size_t &end_pos) {
    size_t start = 0;
    while (start < str.size() && std::isspace(str[start])) {
        start++;
    }

    if (str.size() - start < keyword.size()) {
        return false;
    }

    for (size_t i = 0; i < keyword.size(); i++) {
        if (std::toupper(str[start + i]) != std::toupper(keyword[i])) {
            return false;
        }
    }

    // Must be followed by whitespace or end of string
    if (start + keyword.size() < str.size() && !std::isspace(str[start + keyword.size()])) {
        return false;
    }

    end_pos = start + keyword.size();
    return true;
}

// Check if query is a CREATE [OR REPLACE] METRIC/DIMENSION/SEGMENT statement
// Returns the statement type or empty string if not matched
// Handles syntaxes like:
//   - "CREATE METRIC name AS expr"
//   - "CREATE OR REPLACE METRIC name AS expr"
//   - "CREATE METRIC (...)"
//   - "CREATE METRIC model.name AS expr"
static std::string IsDefinitionStatement(const std::string &query, std::string &definition, bool &is_replace) {
    size_t pos = 0;
    is_replace = false;

    // Must start with CREATE
    if (!StartsWithKeyword(query, "CREATE", pos)) {
        return "";
    }

    std::string rest = query.substr(pos);

    // Check for optional OR REPLACE
    size_t or_pos = 0;
    if (StartsWithKeyword(rest, "OR", or_pos)) {
        rest = rest.substr(or_pos);
        size_t replace_pos = 0;
        if (StartsWithKeyword(rest, "REPLACE", replace_pos)) {
            rest = rest.substr(replace_pos);
            is_replace = true;
        } else {
            return ""; // "CREATE OR" without "REPLACE" is invalid
        }
    }

    // Helper to check for AS keyword (case-insensitive)
    auto is_as_keyword = [](const std::string &s, size_t p) -> bool {
        if (p + 2 > s.size()) return false;
        return (s[p] == 'A' || s[p] == 'a') &&
               (s[p + 1] == 'S' || s[p + 1] == 's') &&
               (p + 2 >= s.size() || std::isspace(s[p + 2]));
    };

    // Helper lambda to check for definition patterns after keyword
    auto check_definition = [&is_as_keyword](const std::string &after_kw, const std::string &keyword, std::string &def) -> bool {
        size_t start = 0;
        while (start < after_kw.size() && std::isspace(after_kw[start])) start++;

        // Case 1: Direct opening paren - "KEYWORD (..."
        if (start < after_kw.size() && after_kw[start] == '(') {
            def = keyword + " " + after_kw.substr(start);
            return true;
        }

        // Read first identifier
        size_t name_start = start;
        while (start < after_kw.size() && (std::isalnum(after_kw[start]) || after_kw[start] == '_')) start++;
        if (start == name_start) {
            return false;
        }

        // Skip whitespace
        while (start < after_kw.size() && std::isspace(after_kw[start])) start++;

        // Case 2: Check for "AS" keyword - simple SQL syntax "KEYWORD name AS expr"
        if (is_as_keyword(after_kw, start)) {
            def = keyword + " " + after_kw.substr(name_start);
            return true;
        }

        // Case 3: Check for dot (model.name syntax)
        if (start < after_kw.size() && after_kw[start] == '.') {
            start++; // skip dot
            // Read second identifier (field name)
            size_t field_start = start;
            while (start < after_kw.size() && (std::isalnum(after_kw[start]) || after_kw[start] == '_')) start++;
            if (start == field_start) {
                return false;
            }
            // Skip whitespace
            while (start < after_kw.size() && std::isspace(after_kw[start])) start++;

            // Check for AS (model.name AS expr syntax)
            if (is_as_keyword(after_kw, start)) {
                def = keyword + " " + after_kw.substr(name_start);
                return true;
            }

            // Check for paren (model.name (props) syntax)
            if (start < after_kw.size() && after_kw[start] == '(') {
                def = keyword + " " + after_kw.substr(name_start);
                return true;
            }
        }

        return false;
    };

    // Check for METRIC
    size_t metric_pos = 0;
    if (StartsWithKeyword(rest, "METRIC", metric_pos)) {
        std::string after_metric = rest.substr(metric_pos);
        if (check_definition(after_metric, "METRIC", definition)) {
            return "METRIC";
        }
    }

    // Check for DIMENSION
    size_t dim_pos = 0;
    if (StartsWithKeyword(rest, "DIMENSION", dim_pos)) {
        std::string after_dim = rest.substr(dim_pos);
        if (check_definition(after_dim, "DIMENSION", definition)) {
            return "DIMENSION";
        }
    }

    // Check for SEGMENT
    size_t seg_pos = 0;
    if (StartsWithKeyword(rest, "SEGMENT", seg_pos)) {
        std::string after_seg = rest.substr(seg_pos);
        if (check_definition(after_seg, "SEGMENT", definition)) {
            return "SEGMENT";
        }
    }

    return "";
}

// Check if stripped query is a CREATE [OR REPLACE] MODEL statement
// Returns: 0 = not a create model, 1 = create model, 2 = create or replace model
// Sets definition to be in nom-parser format: "MODEL (name ..., ...)"
static int IsCreateModelStatement(const std::string &query, std::string &definition) {
    size_t pos = 0;

    // Check for CREATE
    if (!StartsWithKeyword(query, "CREATE", pos)) {
        return 0;
    }

    std::string rest = query.substr(pos);
    bool is_replace = false;

    // Check for optional OR REPLACE
    size_t or_pos = 0;
    if (StartsWithKeyword(rest, "OR", or_pos)) {
        rest = rest.substr(or_pos);
        size_t replace_pos = 0;
        if (StartsWithKeyword(rest, "REPLACE", replace_pos)) {
            rest = rest.substr(replace_pos);
            is_replace = true;
        } else {
            return 0; // "CREATE OR" without "REPLACE" is invalid
        }
    }

    // Check for MODEL
    size_t model_pos = 0;
    if (!StartsWithKeyword(rest, "MODEL", model_pos)) {
        return 0;
    }

    // Skip whitespace after MODEL
    rest = rest.substr(model_pos);
    size_t start = 0;
    while (start < rest.size() && std::isspace(rest[start])) {
        start++;
    }
    rest = rest.substr(start);

    // Find the opening parenthesis - everything from there is the definition body
    size_t paren_pos = rest.find('(');
    if (paren_pos == std::string::npos) {
        return 0; // No parenthesis found
    }

    // Build definition in nom format: "MODEL (name ..., ...)"
    // The content inside parens should already have "name xxx" as first property
    definition = "MODEL " + rest.substr(paren_pos);
    return is_replace ? 2 : 1;
}

// Global to store database path for parser extension (set during extension load)
static std::string g_db_path;

ParserExtensionParseResult sidemantic_parse(ParserExtensionInfo *,
                                            const std::string &query) {
    // Check for SEMANTIC prefix
    std::string stripped_query;
    if (!StartsWithSemantic(query, stripped_query)) {
        // Not a semantic query, let DuckDB handle it
        return ParserExtensionParseResult();
    }

    // Check if this is a CREATE [OR REPLACE] MODEL statement
    std::string definition;
    int create_type = IsCreateModelStatement(stripped_query, definition);

    if (create_type > 0) {
        // This is a CREATE MODEL statement - handle specially
        bool replace = (create_type == 2);
        const char *db_path_ptr = g_db_path.empty() ? nullptr : g_db_path.c_str();

        char *error = sidemantic_define(definition.c_str(), db_path_ptr, replace);
        if (error) {
            string error_msg(error);
            sidemantic_free(error);
            return ParserExtensionParseResult(error_msg);
        }

        // Return a simple SELECT statement as acknowledgment
        Parser parser;
        parser.ParseQuery("SELECT 'Model created successfully' AS result");
        auto statements = std::move(parser.statements);

        return ParserExtensionParseResult(
            make_uniq_base<ParserExtensionParseData, SidemanticParseData>(
                std::move(statements[0])));
    }

    // Check if this is a MODEL <model> statement (to switch active model)
    // Using "SEMANTIC MODEL orders" instead of "SEMANTIC USE orders" because DuckDB
    // handles USE statements specially before parser extensions are called
    size_t model_pos = 0;
    if (StartsWithKeyword(stripped_query, "MODEL", model_pos)) {
        std::string rest = stripped_query.substr(model_pos);
        // Trim whitespace and get model name (until semicolon, paren, or end)
        size_t start = 0;
        while (start < rest.size() && std::isspace(rest[start])) start++;
        size_t end = start;
        while (end < rest.size() && !std::isspace(rest[end]) && rest[end] != ';' && rest[end] != '(') end++;
        std::string model_name = rest.substr(start, end - start);

        // Skip if there's a paren after - that's CREATE MODEL syntax
        size_t paren_check = end;
        while (paren_check < rest.size() && std::isspace(rest[paren_check])) paren_check++;
        if (paren_check < rest.size() && rest[paren_check] == '(') {
            // This looks like a CREATE MODEL with inline parens, skip
            goto not_model_switch;
        }

        if (!model_name.empty()) {
            char *error = sidemantic_use(model_name.c_str());
            if (error) {
                string error_msg(error);
                sidemantic_free(error);
                return ParserExtensionParseResult(error_msg);
            }

            // Return acknowledgment
            Parser parser;
            parser.ParseQuery("SELECT 'Using model: " + model_name + "' AS result");
            auto statements = std::move(parser.statements);

            return ParserExtensionParseResult(
                make_uniq_base<ParserExtensionParseData, SidemanticParseData>(
                    std::move(statements[0])));
        }
    }

not_model_switch:
    // Check if this is a CREATE [OR REPLACE] METRIC/DIMENSION/SEGMENT statement
    bool is_replace = false;
    std::string def_type = IsDefinitionStatement(stripped_query, definition, is_replace);
    if (!def_type.empty()) {
        const char *db_path_ptr = g_db_path.empty() ? nullptr : g_db_path.c_str();

        char *error = sidemantic_add_definition(definition.c_str(), db_path_ptr, is_replace);
        if (error) {
            string error_msg(error);
            sidemantic_free(error);
            return ParserExtensionParseResult(error_msg);
        }

        // Return acknowledgment
        Parser parser;
        std::string action = is_replace ? "replaced" : "created";
        parser.ParseQuery("SELECT '" + def_type + " " + action + " successfully' AS result");
        auto statements = std::move(parser.statements);

        return ParserExtensionParseResult(
            make_uniq_base<ParserExtensionParseData, SidemanticParseData>(
                std::move(statements[0])));
    }

    // Regular SEMANTIC SELECT query - try to rewrite using sidemantic
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
    auto &db = loader.GetDatabaseInstance();
    auto &config = DBConfig::GetConfig(db);

    // Capture database path for CREATE MODEL statements
    auto &db_config = db.config;
    if (!db_config.options.database_path.empty()) {
        g_db_path = db_config.options.database_path;
    } else {
        g_db_path.clear();
    }

    // Auto-load definitions from file if it exists
    const char *db_path_ptr = g_db_path.empty() ? nullptr : g_db_path.c_str();
    char *error = sidemantic_autoload(db_path_ptr);
    if (error) {
        // Log warning but don't fail extension load
        // fprintf(stderr, "Warning: failed to autoload sidemantic definitions: %s\n", error);
        sidemantic_free(error);
    }

    // Register parser extension
    SidemanticParserExtension parser;
    config.parser_extensions.push_back(parser);

    // Register operator extension
    config.operator_extensions.push_back(make_uniq<SidemanticOperatorExtension>());

    // Register table functions
    TableFunction load_func("sidemantic_load", {LogicalType::VARCHAR},
                            SidemanticLoadFunction, SidemanticLoadBind);
    loader.RegisterFunction(load_func);

    TableFunction load_file_func("sidemantic_load_file", {LogicalType::VARCHAR},
                                  SidemanticLoadFileFunction, SidemanticLoadFileBind);
    loader.RegisterFunction(load_file_func);

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
