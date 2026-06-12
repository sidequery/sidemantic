#define DUCKDB_EXTENSION_MAIN

#include "sidemantic_extension.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/function/table_function.hpp"
#include "sidemantic.h"

#include <cctype>
#include <cstdint>

namespace duckdb {

static std::string DatabasePath(DatabaseInstance &db) {
    auto &db_config = db.config;
    if (!db_config.options.database_path.empty()) {
        return db_config.options.database_path;
    }
    return "";
}

static std::string ContextKey(DatabaseInstance &db) {
    auto path = DatabasePath(db);
    if (!path.empty()) {
        return "duckdb:" + path;
    }
    return "duckdb:memory:" + std::to_string(reinterpret_cast<uintptr_t>(&db));
}

static const char *ContextKeyPtr(const std::string &context_key) {
    return context_key.empty() ? nullptr : context_key.c_str();
}

static const SidemanticParserInfo *ParserInfo(ParserExtensionInfo *info) {
    return dynamic_cast<const SidemanticParserInfo *>(info);
}

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

    auto context_key = ContextKey(*context.db);
    char *error = sidemantic_load_yaml_for_context(ContextKeyPtr(context_key), data.yaml_content.c_str());
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

    auto context_key = ContextKey(*context.db);
    char *error = sidemantic_load_file_for_context(ContextKeyPtr(context_key), data.file_path.c_str());
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

    auto context_key = ContextKey(*context.db);
    char *models_str = sidemantic_list_models_for_context(ContextKeyPtr(context_key));
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
            std::string context_key;
            if (state.HasContext()) {
                context_key = ContextKey(*state.GetContext().db);
            }
            SidemanticRewriteResult res =
                sidemantic_rewrite_for_context(ContextKeyPtr(context_key), sql.GetString().c_str());

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

static std::string TrimCopy(const std::string &value) {
    size_t start = 0;
    while (start < value.size() && std::isspace(value[start])) {
        start++;
    }
    size_t end = value.size();
    while (end > start && std::isspace(value[end - 1])) {
        end--;
    }
    return value.substr(start, end - start);
}

static std::string LowerCopy(const std::string &value) {
    std::string result;
    result.reserve(value.size());
    for (auto ch : value) {
        result.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    }
    return result;
}

static bool StartsWithSemanticQueryKeyword(const std::string &query) {
    size_t pos = 0;
    return StartsWithKeyword(query, "SELECT", pos) || StartsWithKeyword(query, "WITH", pos);
}

static bool QueryContainsLoadedModelQualifier(const char *context_key_ptr,
                                              const std::string &query) {
    char *models_ptr = sidemantic_list_models_for_context(context_key_ptr);
    if (!models_ptr) {
        return false;
    }

    std::string models(models_ptr);
    sidemantic_free(models_ptr);
    auto lower_query = LowerCopy(query);

    size_t start = 0;
    while (start < models.size()) {
        auto comma = models.find(',', start);
        auto model = TrimCopy(models.substr(
            start, comma == std::string::npos ? std::string::npos : comma - start));
        if (!model.empty()) {
            auto qualifier = LowerCopy(model) + ".";
            if (lower_query.find(qualifier) != std::string::npos) {
                return true;
            }
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }

    return false;
}

static bool IsCompactModelStatement(const std::string &query) {
    size_t model_pos = 0;
    if (!StartsWithKeyword(query, "MODEL", model_pos)) {
        return false;
    }

    auto rest = query.substr(model_pos);
    size_t start = 0;
    while (start < rest.size() && std::isspace(static_cast<unsigned char>(rest[start]))) {
        start++;
    }

    size_t end = start;
    while (end < rest.size() &&
           (std::isalnum(static_cast<unsigned char>(rest[end])) || rest[end] == '_')) {
        end++;
    }
    if (end == start) {
        return false;
    }

    size_t from_pos = 0;
    return StartsWithKeyword(rest.substr(end), "FROM", from_pos);
}

static bool IsModelBlockStatement(const std::string &query) {
    size_t model_pos = 0;
    if (!StartsWithKeyword(query, "MODEL", model_pos)) {
        return false;
    }

    auto rest = query.substr(model_pos);
    size_t start = 0;
    while (start < rest.size() && std::isspace(static_cast<unsigned char>(rest[start]))) {
        start++;
    }
    return start < rest.size() && rest[start] == '(';
}

static std::string NativeItemDefinitionType(const std::string &query) {
    size_t pos = 0;
    if (StartsWithKeyword(query, "METRIC", pos)) {
        return "METRIC";
    }
    if (StartsWithKeyword(query, "DIMENSION", pos)) {
        return "DIMENSION";
    }
    if (StartsWithKeyword(query, "SEGMENT", pos)) {
        return "SEGMENT";
    }
    return "";
}

static bool IsBareIdentifier(const std::string &value) {
    if (value.empty() || !(std::isalpha(value[0]) || value[0] == '_')) {
        return false;
    }
    for (auto ch : value) {
        if (!(std::isalnum(ch) || ch == '_')) {
            return false;
        }
    }
    return true;
}

static bool StartsWithNameProperty(const std::string &value, size_t pos) {
    const std::string keyword = "name";
    if (pos + keyword.size() >= value.size()) {
        return false;
    }
    for (size_t i = 0; i < keyword.size(); i++) {
        if (std::tolower(value[pos + i]) != keyword[i]) {
            return false;
        }
    }
    auto separator = value[pos + keyword.size()];
    return std::isspace(separator) || separator == ':' || separator == '=';
}

static bool ExtractModelNameProperty(const std::string &body, std::string &name, std::string &error) {
    auto open = body.find('(');
    if (open == std::string::npos) {
        return false;
    }

    size_t pos = open + 1;
    bool in_single_quote = false;
    bool in_double_quote = false;
    int paren_depth = 0;
    int bracket_depth = 0;
    int brace_depth = 0;

    while (pos < body.size()) {
        while (pos < body.size() && paren_depth == 0 && bracket_depth == 0 &&
               brace_depth == 0 && (std::isspace(body[pos]) || body[pos] == ',')) {
            pos++;
        }
        if (pos >= body.size() ||
            (paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 && body[pos] == ')')) {
            return false;
        }

        if (paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 &&
            StartsWithNameProperty(body, pos)) {
            pos += 4;
            while (pos < body.size() && std::isspace(body[pos])) {
                pos++;
            }
            if (pos < body.size() && (body[pos] == ':' || body[pos] == '=')) {
                pos++;
                while (pos < body.size() && std::isspace(body[pos])) {
                    pos++;
                }
            }
            if (pos < body.size() && (body[pos] == '\'' || body[pos] == '"')) {
                auto quote = body[pos];
                pos++;
                std::string quoted_name;
                while (pos < body.size()) {
                    auto ch = body[pos];
                    if (ch == quote) {
                        if (pos + 1 < body.size() && body[pos + 1] == quote) {
                            quoted_name.push_back(quote);
                            pos += 2;
                            continue;
                        }
                        name = quoted_name;
                        return true;
                    }
                    quoted_name.push_back(ch);
                    pos++;
                }
                error = "CREATE MODEL body name is unterminated";
                return false;
            }
            size_t name_start = pos;
            while (pos < body.size() && (std::isalnum(body[pos]) || body[pos] == '_')) {
                pos++;
            }
            if (pos > name_start) {
                name = body.substr(name_start, pos - name_start);
                return true;
            }
            error = "CREATE MODEL body name must be a bare or quoted identifier";
            return false;
        }

        while (pos < body.size()) {
            auto ch = body[pos];
            if (in_single_quote) {
                if (ch == '\'' && pos + 1 < body.size() && body[pos + 1] == '\'') {
                    pos += 2;
                    continue;
                }
                if (ch == '\'') {
                    in_single_quote = false;
                }
                pos++;
                continue;
            }
            if (in_double_quote) {
                if (ch == '"' && pos + 1 < body.size() && body[pos + 1] == '"') {
                    pos += 2;
                    continue;
                }
                if (ch == '"') {
                    in_double_quote = false;
                }
                pos++;
                continue;
            }

            if (ch == '\'') {
                in_single_quote = true;
            } else if (ch == '"') {
                in_double_quote = true;
            } else if (ch == '(') {
                paren_depth++;
            } else if (ch == ')') {
                if (paren_depth > 0) {
                    paren_depth--;
                } else if (bracket_depth == 0 && brace_depth == 0) {
                    return false;
                }
            } else if (ch == '[') {
                bracket_depth++;
            } else if (ch == ']') {
                if (bracket_depth > 0) {
                    bracket_depth--;
                }
            } else if (ch == '{') {
                brace_depth++;
            } else if (ch == '}') {
                if (brace_depth > 0) {
                    brace_depth--;
                }
            } else if (ch == ',' && paren_depth == 0 && bracket_depth == 0 &&
                       brace_depth == 0) {
                break;
            }
            pos++;
        }
    }

    return false;
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
static int IsCreateModelStatement(const std::string &query, std::string &definition, std::string &error) {
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

    auto outer_name = TrimCopy(rest.substr(0, paren_pos));
    auto body = rest.substr(paren_pos);
    if (!outer_name.empty() && !IsBareIdentifier(outer_name)) {
        error = "Invalid CREATE MODEL name: " + outer_name;
        return -1;
    }

    std::string inner_name;
    std::string name_error;
    if (!outer_name.empty() && ExtractModelNameProperty(body, inner_name, name_error)) {
        if (inner_name != outer_name) {
            error = "CREATE MODEL name '" + outer_name + "' does not match body name '" + inner_name + "'";
            return -1;
        }
        definition = "MODEL " + body;
        return is_replace ? 2 : 1;
    }
    if (!name_error.empty()) {
        error = name_error;
        return -1;
    }

    if (!outer_name.empty()) {
        auto after_open = body.substr(1);
        auto trimmed_after_open = TrimCopy(after_open);
        if (!trimmed_after_open.empty() && trimmed_after_open[0] == ')') {
            definition = "MODEL (name " + outer_name + after_open;
        } else {
            definition = "MODEL (name " + outer_name + ", " + after_open;
        }
        return is_replace ? 2 : 1;
    }

    definition = "MODEL " + body;
    return is_replace ? 2 : 1;
}

ParserOverrideResult sidemantic_parser_override(ParserExtensionInfo *info,
                                                const std::string &query,
                                                ParserOptions &options) {
    (void)options;

    // Keep the legacy SEMANTIC prefix on the parse_function path so definitions
    // and explicit SEMANTIC SELECT keep their existing behavior.
    std::string stripped_query;
    if (StartsWithSemantic(query, stripped_query)) {
        return ParserOverrideResult();
    }

    // Sidemantic SQL is valid DuckDB SQL, so avoid intercepting broad table scans
    // such as SELECT * FROM model. No-prefix mode only takes over qualified model
    // references like SELECT orders.revenue FROM orders.
    if (!StartsWithSemanticQueryKeyword(query)) {
        return ParserOverrideResult();
    }

    auto parser_info = ParserInfo(info);
    const char *context_key_ptr = parser_info ? ContextKeyPtr(parser_info->context_key) : nullptr;
    if (!QueryContainsLoadedModelQualifier(context_key_ptr, query)) {
        return ParserOverrideResult();
    }

    SidemanticRewriteResult result = sidemantic_rewrite_for_context(context_key_ptr, query.c_str());
    if (result.error) {
        sidemantic_free_result(result);
        return ParserOverrideResult();
    }
    if (!result.was_rewritten || !result.sql) {
        sidemantic_free_result(result);
        return ParserOverrideResult();
    }

    string rewritten_sql(result.sql);
    sidemantic_free_result(result);
    if (TrimCopy(rewritten_sql) == TrimCopy(query)) {
        return ParserOverrideResult();
    }

    try {
        Parser parser;
        parser.ParseQuery(rewritten_sql);
        if (parser.statements.empty()) {
            return ParserOverrideResult();
        }
        return ParserOverrideResult(std::move(parser.statements));
    } catch (...) {
        return ParserOverrideResult();
    }
}

ParserExtensionParseResult sidemantic_parse(ParserExtensionInfo *info,
                                            const std::string &query) {
    auto parser_info = ParserInfo(info);
    const char *context_key_ptr = parser_info ? ContextKeyPtr(parser_info->context_key) : nullptr;
    const char *db_path_ptr =
        parser_info && !parser_info->db_path.empty() ? parser_info->db_path.c_str() : nullptr;

    std::string stripped_query;
    bool had_semantic_prefix = StartsWithSemantic(query, stripped_query);
    if (!had_semantic_prefix) {
        stripped_query = query;
    }

    // Check if this is a CREATE [OR REPLACE] MODEL statement
    std::string definition;
    std::string create_model_error;
    int create_type = IsCreateModelStatement(stripped_query, definition, create_model_error);

    if (create_type < 0) {
        return ParserExtensionParseResult(create_model_error);
    }
    if (create_type > 0) {
        // This is a CREATE MODEL statement - handle specially
        bool replace = (create_type == 2);

        char *error = sidemantic_define_for_context(context_key_ptr, definition.c_str(), db_path_ptr, replace);
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

    // Check if this is a native SQL model block:
    //   MODEL (name orders, table orders, primary_key order_id)
    if (IsModelBlockStatement(stripped_query)) {
        char *error =
            sidemantic_define_for_context(context_key_ptr, stripped_query.c_str(), db_path_ptr, false);
        if (error) {
            string error_msg(error);
            sidemantic_free(error);
            return ParserExtensionParseResult(error_msg);
        }

        Parser parser;
        parser.ParseQuery("SELECT 'Model created successfully' AS result");
        auto statements = std::move(parser.statements);

        return ParserExtensionParseResult(
            make_uniq_base<ParserExtensionParseData, SidemanticParseData>(
                std::move(statements[0])));
    }

    // Check if this is a compact native SQL model block:
    //   model orders from orders (
    //     primary key (order_id)
    //     sum(amount) as revenue
    //   )
    if (IsCompactModelStatement(stripped_query)) {
        char *error =
            sidemantic_define_for_context(context_key_ptr, stripped_query.c_str(), db_path_ptr, false);
        if (error) {
            string error_msg(error);
            sidemantic_free(error);
            return ParserExtensionParseResult(error_msg);
        }

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
            char *error = sidemantic_use_for_context(context_key_ptr, model_name.c_str());
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
    // Check if this is a native SQL metric/dimension/segment definition:
    //   METRIC (name revenue, agg sum, sql amount)
    //   METRIC revenue AS SUM(amount)
    std::string native_def_type = NativeItemDefinitionType(stripped_query);
    if (!native_def_type.empty()) {
        char *error =
            sidemantic_add_definition_for_context(context_key_ptr, stripped_query.c_str(), db_path_ptr, false);
        if (error) {
            string error_msg(error);
            sidemantic_free(error);
            return ParserExtensionParseResult(error_msg);
        }

        Parser parser;
        parser.ParseQuery("SELECT '" + native_def_type + " created successfully' AS result");
        auto statements = std::move(parser.statements);

        return ParserExtensionParseResult(
            make_uniq_base<ParserExtensionParseData, SidemanticParseData>(
                std::move(statements[0])));
    }

    // Check if this is a CREATE [OR REPLACE] METRIC/DIMENSION/SEGMENT statement
    bool is_replace = false;
    std::string def_type = IsDefinitionStatement(stripped_query, definition, is_replace);
    if (!def_type.empty()) {
        char *error =
            sidemantic_add_definition_for_context(context_key_ptr, definition.c_str(), db_path_ptr, is_replace);
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

    if (!had_semantic_prefix) {
        // No-prefix parser fallback only handles Sidemantic statements that DuckDB
        // cannot parse natively. Valid SELECTs are handled by parser_override.
        return ParserExtensionParseResult();
    }

    // Regular SEMANTIC SELECT query - try to rewrite using sidemantic
    SidemanticRewriteResult result = sidemantic_rewrite_for_context(context_key_ptr, stripped_query.c_str());

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

    auto db_path = DatabasePath(db);
    auto context_key = ContextKey(db);

    // Auto-load definitions from file if it exists
    const char *db_path_ptr = db_path.empty() ? nullptr : db_path.c_str();
    char *error = sidemantic_autoload_for_context(ContextKeyPtr(context_key), db_path_ptr);
    if (error) {
        std::string message = "Failed to autoload sidemantic definitions: " + std::string(error);
        sidemantic_free(error);
        throw InvalidInputException("%s", message.c_str());
    }

    // Enable parser_override so qualified semantic SELECTs can omit the SEMANTIC prefix.
    // FALLBACK mode lets ordinary DuckDB SQL continue through the native parser.
    config.SetOptionByName("allow_parser_override_extension", Value("fallback"));

    // Register parser extension
    SidemanticParserExtension parser(db_path, context_key);
    ParserExtension::Register(config, parser);

    // Register operator extension
    OperatorExtension::Register(config, make_shared_ptr<SidemanticOperatorExtension>());

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
