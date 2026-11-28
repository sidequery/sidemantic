#include "sqlctx_parser.hpp"
#include "sqlctx_catalog.hpp"
#include "sqlctx_api.hpp"
#include "sqlctx.h"

#include "duckdb/common/error_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension_manager.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/planner/bound_parameter_map.hpp"
#include "duckdb/planner/planner_extension.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {
namespace {

struct SourceToken {
    string text;
    idx_t start;
    idx_t end;
};

// Use DuckDB's lexer for strings, quoted names and comments on both hosts.
// Offsets remain byte offsets into the original SQL, including UTF-8 text.
vector<SourceToken> Tokens(const string &sql, string *without_comments = nullptr) {
    auto tokens = Parser::Tokenize(sql);
    vector<SourceToken> result;
    if (without_comments) {
        *without_comments = sql;
    }
    for (idx_t i = 0; i < tokens.size(); ++i) {
        auto start = tokens[i].start;
        auto end = i + 1 < tokens.size() ? tokens[i + 1].start : sql.size();
        if (tokens[i].type == SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT) {
            if (without_comments) {
                for (auto j = start; j < end; ++j) {
                    if ((*without_comments)[j] != '\n') {
                        (*without_comments)[j] = ' ';
                    }
                }
            }
            continue;
        }
        while (end > start && StringUtil::CharacterIsSpace(sql[end - 1])) {
            --end;
        }
        if (end > start) {
            result.push_back({sql.substr(start, end - start), start, end});
        }
    }
    return result;
}

bool Keyword(const vector<SourceToken> &tokens, idx_t index, const char *keyword) {
    return index < tokens.size() && StringUtil::CIEquals(tokens[index].text, keyword);
}

string IdentifierText(const string &text) {
    if (text.size() >= 2 && (text.front() == '"' || text.front() == '\'') && text.back() == text.front()) {
        string quote(1, text.front());
        return StringUtil::Replace(text.substr(1, text.size() - 2), quote + quote, quote);
    }
    return text;
}

string SqlctxLiteral(const string &text) {
    return "'" + StringUtil::Replace(text, "'", "''") + "'";
}

string QualifiedNameJson(const vector<SourceToken> &tokens, idx_t pos, idx_t max_names) {
    string result = "[";
    idx_t count = 0;
    while (pos < tokens.size()) {
        auto token = tokens[pos++].text;
        if (token.empty() || token == "." || token == "(" || token == ")" || token == ",") {
            throw ParserException("Expected a semantic identifier");
        }
        if (count++) result += ",";
        result += '"';
        for (auto character : IdentifierText(token)) {
            auto byte = static_cast<unsigned char>(character);
            if (character == '"' || character == '\\') result += '\\';
            if (byte < 0x20) {
                static const char hex[] = "0123456789abcdef";
                result += "\\u00";
                result += hex[byte >> 4];
                result += hex[byte & 15];
            } else result += character;
        }
        result += '"';
        if (pos == tokens.size()) break;
        if (tokens[pos++].text != "." || pos == tokens.size()) {
            throw ParserException("Expected one qualified semantic name");
        }
    }
    if (count == 0 || count > max_names) throw ParserException("Expected a model name or model.field");
    return result + "]";
}

class SqlctxSQLStatement : public ExtensionStatement {
public:
    explicit SqlctxSQLStatement(unique_ptr<ParserExtensionParseData> data)
        : ExtensionStatement(SqlctxParserExtension(), std::move(data)) {}

    unique_ptr<SQLStatement> Copy() const override {
        auto result = make_uniq<SqlctxSQLStatement>(parse_data->Copy());
        result->named_param_map = named_param_map;
        result->query = query;
        result->stmt_location = stmt_location;
#if SQLCTX_NEW_IDENTIFIER_API
        result->has_anonymous_parameters = has_anonymous_parameters;
#else
        result->stmt_length = stmt_length;
#endif
        return std::move(result);
    }
};

ParserOptions WithoutOverrides(const ParserOptions &options) {
    auto result = options;
    result.extensions = nullptr;
    return result;
}

// The compatibility frontend only recognizes sqlctx declarations. Ordinary
// SQL, including PREPARE/EXPLAIN syntax, is still parsed by DuckDB.
unique_ptr<SQLStatement> ParseCompatibilityStatement(const string &sql, const ParserOptions &options) {
    auto tokens = Tokens(sql);
    if (tokens.empty()) {
        return nullptr;
    }
    auto definition = ParseSqlctxDefinition(sql);
    if (definition) {
        return SqlctxStatement(std::move(definition));
    }
    bool explicit_semantic = Keyword(tokens, 0, "SEMANTIC");
    auto input = explicit_semantic ? sql.substr(tokens[0].end) : sql;
    if (Keyword(tokens, 0, "EXPLAIN") || Keyword(tokens, 0, "PREPARE")) {
        // Replace only the nested statement when it is custom syntax. Let the
        // native parser validate all surrounding options and parameter syntax.
        idx_t nested = 1;
        if (Keyword(tokens, 0, "PREPARE")) {
            int depth = 0;
            for (; nested < tokens.size(); ++nested) {
                if (tokens[nested].text == "(") ++depth;
                if (tokens[nested].text == ")") --depth;
                if (depth == 0 && Keyword(tokens, nested, "AS")) {
                    ++nested;
                    break;
                }
            }
        } else {
            if (Keyword(tokens, nested, "ANALYZE") || Keyword(tokens, nested, "ANALYSE")) ++nested;
            if (nested < tokens.size() && tokens[nested].text == "(") {
                int depth = 0;
                do {
                    if (tokens[nested].text == "(") ++depth;
                    if (tokens[nested].text == ")") --depth;
                    ++nested;
                } while (nested < tokens.size() && depth > 0);
            }
        }
        if (nested < tokens.size()) {
            auto nested_sql = sql.substr(tokens[nested].start);
            if (ParseSqlctxDefinition(nested_sql) || Keyword(tokens, nested, "SEMANTIC")) {
                auto child = ParseCompatibilityStatement(nested_sql, options);
                Parser parser(WithoutOverrides(options));
                parser.ParseQuery(sql.substr(0, tokens[nested].start) + "SELECT 1");
                auto statement = std::move(parser.statements.at(0));
                if (statement->type == StatementType::EXPLAIN_STATEMENT) {
                    statement->Cast<ExplainStatement>().stmt = std::move(child);
                } else {
                    statement->Cast<PrepareStatement>().statement = std::move(child);
                }
                return WrapSqlctxQuery(std::move(statement), false, "", &options);
            }
        }
    }
    Parser parser(WithoutOverrides(options));
    parser.ParseQuery(input);
    if (parser.statements.size() != 1) {
        throw ParserException("Expected one sqlctx statement");
    }
    return WrapSqlctxQuery(std::move(parser.statements[0]), explicit_semantic, "", &options);
}

void ParseCompatibility(const string &sql, const ParserOptions &options,
                        vector<unique_ptr<SQLStatement>> &statements) {
    auto tokens = Tokens(sql);
    idx_t start = 0;
    for (auto &token : tokens) {
        if (token.text != ";") continue;
        auto statement = ParseCompatibilityStatement(sql.substr(start, token.start - start), options);
        if (statement) statements.push_back(std::move(statement));
        start = token.end;
    }
    auto statement = ParseCompatibilityStatement(sql.substr(start), options);
    if (statement) statements.push_back(std::move(statement));
}

struct ModelListOwner {
    explicit ModelListOwner(const string &snapshot) : list(sqlctx_snapshot_list_models(snapshot.c_str())) {
        if (list.error) {
            string error(list.error);
            sqlctx_free_model_list(list);
            throw InvalidInputException("sqlctx catalog: %s", error);
        }
    }
    ~ModelListOwner() { sqlctx_free_model_list(list); }
    SqlctxModelList list;
};

// Fields are canonical model names; a time grain suffix applies to its base.
bool CanonicalField(const SqlctxModelInfo *model, string &name) {
    if (!model) return false;
    for (idx_t i = 0; i < model->field_count; ++i) {
        if (StringUtil::CIEquals(name, model->fields[i])) {
            name = model->fields[i];
            return true;
        }
    }
    auto field = name;
    auto suffix = field.rfind("__");
    string grain_suffix;
    if (suffix != string::npos) {
        auto grain = StringUtil::Lower(field.substr(suffix + 2));
        static const case_insensitive_set_t grains {"year", "quarter", "month", "week", "day", "hour", "minute", "second"};
        if (grains.count(grain)) {
            field.resize(suffix);
            grain_suffix = "__" + grain;
        }
    }
    for (idx_t i = 0; i < model->field_count; ++i) {
        if (StringUtil::CIEquals(field, model->fields[i])) {
            name = model->fields[i] + grain_suffix;
            return true;
        }
    }
    return false;
}

string UnknownFieldMessage(const SqlctxModelInfo &model, const string &field) {
    vector<string> fields(model.fields, model.fields + model.field_count);
    auto suffix = field.rfind("__");
    if (suffix != string::npos) {
        for (auto &candidate : fields) {
            if (StringUtil::CIEquals(candidate, field.substr(0, suffix))) {
                return StringUtil::Format("sqlctx: unknown time grain '%s' in %s.%s. Valid grains: year, "
                                          "quarter, month, week, day, hour, minute, second",
                                          field.substr(suffix + 2), model.name, field);
            }
        }
    }
    auto message = StringUtil::Format("sqlctx: model %s has no field '%s'.", model.name, field);
    auto close = StringUtil::TopNLevenshtein(fields, field, 3);
    if (!close.empty()) message += " Did you mean " + StringUtil::Join(close, ", ") + "?";
    return message + " Fields: " + StringUtil::Join(fields, ", ");
}

// A same-name table or view and a semantic model. Each reference through the
// name is classified, so ordinary SQL against the table keeps its meaning.
struct RelationUse {
    BaseTableRef *table = nullptr;
    string model;
    SqlctxPhysicalRelation physical;
    string semantic_field; // first field only the model provides
    string physical_field; // first column only the table provides
};

// A null binding shadows a semantic model name (e.g. a CTE or a physical table
// aliased to that name). Each query has its own scope, including nested SELECTs.
struct ModelBinding {
    ModelBinding(const SqlctxModelInfo *model_p = nullptr, bool semantic_schema_p = false,
                 shared_ptr<RelationUse> use_p = nullptr)
        : model(model_p), semantic_schema(semantic_schema_p), use(std::move(use_p)) {
    }
    const SqlctxModelInfo *model;
    bool semantic_schema;
    shared_ptr<RelationUse> use;
};
using ModelScope = case_insensitive_map_t<ModelBinding>;
using CteNames = case_insensitive_set_t;

class SemanticReferences {
public:
    SemanticReferences(ClientContext &context, const SqlctxModelList &list, bool explicit_semantic,
                       const unordered_set<const TableRef *> &compiled)
        : context(context), explicit_semantic(explicit_semantic), compiled(compiled) {
        for (idx_t i = 0; i < list.count; ++i) {
            models[list.models[i].name] = {&list.models[i], false};
        }
    }

    bool Query(QueryNode &node, ModelScope inherited = {}, CteNames ctes = {}) {
        bool found = false;
        for (auto &cte : node.cte_map.map) {
#if SQLCTX_NEW_IDENTIFIER_API
            found |= Query(*cte.second->query_node, inherited, ctes);
#else
            found |= Query(*cte.second->query->node, inherited, ctes);
#endif
            ctes.insert(SqlctxName(cte.first));
        }
        if (node.type == QueryNodeType::SET_OPERATION_NODE) {
            for (auto &child : node.Cast<SetOperationNode>().children) found |= Query(*child, inherited, ctes);
            return found;
        }
        if (node.type == QueryNodeType::RECURSIVE_CTE_NODE) {
            auto &recursive = node.Cast<RecursiveCTENode>();
            ctes.insert(SqlctxName(recursive.ctename));
            return Query(*recursive.left, inherited, ctes) | Query(*recursive.right, inherited, ctes) | found;
        }
        if (node.type == QueryNodeType::CTE_NODE) {
            auto &cte = node.Cast<CTENode>();
            found |= Query(*cte.query, inherited, ctes);
            ctes.insert(SqlctxName(cte.ctename));
            return Query(*cte.child, inherited, ctes) | found;
        }
        if (node.type != QueryNodeType::SELECT_NODE) return found;
        auto &select = node.Cast<SelectNode>();
        auto scope = std::move(inherited);
        for (auto &name : ctes) scope[name] = {};
        vector<shared_ptr<RelationUse>> uses;
        vector<string> sources;
        if (select.from_table) CollectTables(*select.from_table, scope, ctes, found, uses, sources);
        // A lone model source owns unqualified names that are not output aliases.
        lone_model = nullptr;
        output_aliases.clear();
        if (sources.size() == 1) {
            auto &binding = scope[sources[0]];
            if (binding.model && !binding.use) lone_model = binding.model;
            for (auto &expression : select.select_list) {
                if (expression->HasAlias()) output_aliases.insert(SqlctxName(expression->GetAlias()));
            }
        }
        auto inspect = [&](unique_ptr<ParsedExpression> &expression) {
            found |= Expression(*expression, scope, ctes);
        };
        for (auto &expression : select.select_list) inspect(expression);
        for (auto &expression : select.groups.group_expressions) inspect(expression);
        if (select.where_clause) inspect(select.where_clause);
        if (select.having) inspect(select.having);
        if (select.qualify) inspect(select.qualify);
        if (select.from_table) found |= TableExpressions(*select.from_table, scope, ctes);
        ParsedExpressionIterator::EnumerateQueryNodeModifiers(node, inspect);
        lone_model = nullptr;
        for (auto &use : uses) ResolveShadowing(*use);
        return found;
    }

    // Unknown qualified fields are always errors. A bare unknown name could be
    // a lambda parameter or alias, so it is reported only for an otherwise
    // non-semantic query, which could not bind against a model natively either.
    void ThrowUnknownField(bool found) {
        if (qualified_unknown.model) throw BinderException(UnknownFieldMessage(*qualified_unknown.model, qualified_unknown.field));
        if (!found && bare_unknown.model) throw BinderException(UnknownFieldMessage(*bare_unknown.model, bare_unknown.field));
    }

private:
    ClientContext &context;
    bool explicit_semantic;
    const unordered_set<const TableRef *> &compiled;
    ModelScope models;
    const SqlctxModelInfo *lone_model = nullptr;
    case_insensitive_set_t output_aliases;
    struct UnknownField {
        const SqlctxModelInfo *model = nullptr;
        string field;
    };
    UnknownField qualified_unknown;
    UnknownField bare_unknown;

    void Unknown(UnknownField &unknown, const SqlctxModelInfo &model, const string &field) {
        if (unknown.model) return;
        unknown.model = &model;
        unknown.field = field;
    }

    void ResolveShadowing(RelationUse &use) {
        if (!use.semantic_field.empty() && !use.physical_field.empty()) {
            throw BinderException("sqlctx: '%s' names both table %s.%s.%s and a semantic model. This query "
                                  "uses table column '%s' and model field '%s'. Query the model as semantic.%s "
                                  "or the table as %s.%s",
                                  use.model, use.physical.catalog, use.physical.schema, use.model,
                                  use.physical_field, use.semantic_field, use.model, use.physical.schema,
                                  use.model);
        }
        if (!use.semantic_field.empty() || explicit_semantic) return;
        // Every reference binds to the table, so the query keeps its native
        // meaning, including inside a statement that the compiler rewrites.
        QualifyPhysicalTable(*use.table, use.model, use.physical);
    }

    static void QualifyPhysicalTable(BaseTableRef &table, const string &name, const SqlctxPhysicalRelation &physical) {
#if SQLCTX_QUALIFIED_TABLE_API
        table.SetQualifiedName(Identifier(physical.catalog), Identifier(physical.schema), Identifier(name));
#else
        table.catalog_name = physical.catalog;
        table.schema_name = physical.schema;
#endif
    }

    void CollectTables(TableRef &ref, ModelScope &scope, const CteNames &ctes, bool &found,
                       vector<shared_ptr<RelationUse>> &uses, vector<string> &sources) {
        if (ref.type == TableReferenceType::JOIN) {
            auto &join = ref.Cast<JoinRef>();
            CollectTables(*join.left, scope, ctes, found, uses, sources);
            CollectTables(*join.right, scope, ctes, found, uses, sources);
            return;
        }
        // Semantic relations compiled ahead of this pass are ordinary SQL.
        if (ref.type == TableReferenceType::SUBQUERY && !compiled.count(&ref)) {
            found |= Query(*ref.Cast<SubqueryRef>().subquery->node, scope, ctes);
        }
        const SqlctxModelInfo *model = nullptr;
        bool virtual_relation = false;
        shared_ptr<RelationUse> use;
        string name;
        if (ref.type == TableReferenceType::BASE_TABLE) {
            auto &table = ref.Cast<BaseTableRef>();
#if SQLCTX_QUALIFIED_TABLE_API
            name = SqlctxName(table.Table());
            auto schema = SqlctxName(table.GetQualifiedName().Schema());
            auto catalog = SqlctxName(table.GetQualifiedName().Catalog());
#else
            name = table.table_name;
            auto schema = table.schema_name;
            auto catalog = table.catalog_name;
#endif
            bool qualified = !schema.empty() || !catalog.empty();
            bool semantic_schema = StringUtil::CIEquals(schema, "semantic") &&
                (catalog.empty() || StringUtil::CIEquals(catalog, SqlctxName(DatabaseManager::GetDefaultDatabase(context)))) &&
                !SqlctxPhysicalRelationExists(context, name);
            auto entry = models.find(name);
            if (((!qualified && !ctes.count(name)) || semantic_schema) && entry != models.end()) model = entry->second.model;
            // FROM metrics is the global semantic namespace. A CTE or a real
            // table on the search path still owns its name in ordinary SQL.
            if (!model && !qualified && !ctes.count(name) && !models.empty() &&
                StringUtil::CIEquals(name, "metrics")) {
                SqlctxPhysicalRelation physical;
                if (!explicit_semantic && SqlctxFindPhysicalRelation(context, name, physical)) {
                    QualifyPhysicalTable(table, name, physical);
                } else {
                    found = true;
                }
            }
            if (model) {
                virtual_relation = semantic_schema;
                name = model->name;
                SqlctxPhysicalRelation physical;
                if (!semantic_schema && SqlctxFindPhysicalRelation(context, name, physical)) {
                    use = make_shared_ptr<RelationUse>();
                    use->table = &table;
                    use->model = name;
                    use->physical = std::move(physical);
                    uses.push_back(use);
                }
#if SQLCTX_QUALIFIED_TABLE_API
                table.SetTable(Identifier(name));
#else
                table.table_name = name;
#endif
                found |= semantic_schema;
            }
        }
        auto alias = ref.alias.empty() ? name : SqlctxName(ref.alias);
        if (!alias.empty()) {
            scope[alias] = {model, virtual_relation, use};
            sources.push_back(alias);
        }
    }

    bool TableExpressions(TableRef &ref, const ModelScope &scope, const CteNames &ctes) {
        if (ref.type != TableReferenceType::JOIN) return false;
        auto &join = ref.Cast<JoinRef>();
        bool found = join.condition && Expression(*join.condition, scope, ctes);
        return TableExpressions(*join.left, scope, ctes) | TableExpressions(*join.right, scope, ctes) | found;
    }

    // Returns whether the reference can only mean a semantic field.
    bool Field(const SqlctxModelInfo *model, const shared_ptr<RelationUse> &use, string &field) {
        auto original = field;
        bool semantic = CanonicalField(model, field);
        bool physical = use && use->physical.HasColumn(original);
        if (use && semantic && !physical && use->semantic_field.empty()) use->semantic_field = field;
        if (use && physical && !semantic && use->physical_field.empty()) use->physical_field = original;
        return semantic && !physical;
    }

    bool Expression(ParsedExpression &expression, const ModelScope &scope, const CteNames &ctes) {
        if (expression.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
            auto &column = expression.Cast<ColumnRefExpression>();
#if SQLCTX_NEW_EXPRESSION_API
            auto &names = column.ColumnNamesMutable();
#else
            auto &names = column.column_names;
#endif
            if (names.size() >= 3) {
                auto qualifier = SqlctxName(names[names.size() - 2]);
                auto local = scope.find(qualifier);
                // Explicit physical qualifiers retain native resolution even
                // when the unqualified FROM name also names a semantic model.
                if (!StringUtil::CIEquals(SqlctxName(names[names.size() - 3]), "semantic")) {
                    if (local != scope.end() && local->second.semantic_schema) {
                        throw BinderException("Column qualifier does not match the semantic relation");
                    }
                    return false;
                }
                bool semantic_model = local != scope.end() ? local->second.model != nullptr : models.count(qualifier) != 0;
                if (semantic_model) {
                    bool current_catalog = names.size() == 3 ||
                        (names.size() == 4 && StringUtil::CIEquals(SqlctxName(names[0]),
                            SqlctxName(DatabaseManager::GetDefaultDatabase(context))));
                    if (!current_catalog) {
                        throw BinderException("Semantic column qualifier must name the current database's semantic schema");
                    }
                    names.erase(names.begin(), names.end() - 2);
                }
            }
            if (names.size() == 2) {
                auto qualifier = SqlctxName(names[0]);
                auto local = scope.find(qualifier);
                const SqlctxModelInfo *model = nullptr;
                shared_ptr<RelationUse> use;
                if (local != scope.end()) {
                    model = local->second.model;
                    use = local->second.use;
                    qualifier = local->first;
                } else {
                    auto entry = models.find(qualifier);
                    if (entry != models.end()) {
                        model = entry->second.model;
                        qualifier = entry->first;
                    }
                }
                if (!model) return false;
                names[0] = SqlctxIdentifier(qualifier);
                auto field = SqlctxName(names[1]);
                bool semantic = Field(model, use, field);
                if (field != SqlctxName(names[1]) || semantic) {
                    names[1] = SqlctxIdentifier(field);
                } else if (!use || !use->physical.HasColumn(field)) {
                    Unknown(qualified_unknown, *model, field);
                }
                return semantic;
            }
            if (names.size() == 1) {
                auto original = SqlctxName(names[0]);
                for (auto &binding : scope) {
                    if (!binding.second.model) continue;
                    auto field = original;
                    bool semantic = Field(binding.second.model, binding.second.use, field);
                    if (field == original && !semantic &&
                        !(binding.second.use && binding.second.use->physical.HasColumn(original))) {
                        continue;
                    }
                    if (field != original || semantic) names[0] = SqlctxIdentifier(field);
                    return semantic;
                }
                if (lone_model && !output_aliases.count(original)) Unknown(bare_unknown, *lone_model, original);
            }
            return false;
        }
        bool found = false;
        if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY) {
#if SQLCTX_NEW_EXPRESSION_API
            auto &query = expression.Cast<SubqueryExpression>().Subquery();
#else
            auto &query = expression.Cast<SubqueryExpression>().subquery;
#endif
            auto lone = lone_model;
            auto aliases = output_aliases;
            found |= Query(*query->node, scope, ctes);
            lone_model = lone;
            output_aliases = std::move(aliases);
        }
        ParsedExpressionIterator::EnumerateChildren(expression, [&](ParsedExpression &child) {
            found |= Expression(child, scope, ctes);
        });
        return found;
    }
};

struct FieldInfo {
    bool metric = false;
    string aggregation;
    string type;
    idx_t position = 0;
};

struct CatalogFieldsOwner {
    explicit CatalogFieldsOwner(const string &snapshot)
        : result(sqlctx_snapshot_catalog(snapshot.c_str(), "", "", nullptr)) {
        if (result.error) {
            string error(result.error);
            sqlctx_free_catalog_entries(result);
            throw InvalidInputException("sqlctx catalog: %s", error);
        }
    }
    ~CatalogFieldsOwner() { sqlctx_free_catalog_entries(result); }
    SqlctxCatalogEntries result;
};

bool IsAggregateFunction(const string &name) {
    static const case_insensitive_set_t aggregates {
        "sum", "count", "count_star", "avg", "mean", "min", "max", "any_value", "first", "last", "arbitrary",
        "median", "mode", "quantile", "quantile_cont", "quantile_disc", "approx_count_distinct",
        "approx_quantile", "stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp",
        "string_agg", "group_concat", "listagg", "list", "array_agg", "product", "bool_and", "bool_or",
        "fsum", "sumkahan", "kahan_sum", "favg", "arg_min", "arg_max", "argmin", "argmax", "min_by", "max_by",
        "histogram", "entropy", "kurtosis", "skewness", "geomean", "geometric_mean", "bit_and", "bit_or",
        "bit_xor", "count_if", "countif", "mad", "corr", "covar_pop", "covar_samp"};
    return aggregates.count(name);
}

string FunctionName(ParsedExpression &expression) {
    auto &function = expression.Cast<FunctionExpression>();
#if SQLCTX_NEW_EXPRESSION_API
    return StringUtil::Lower(SqlctxName(function.FunctionName()));
#else
    return StringUtil::Lower(function.function_name);
#endif
}

string StarRelation(ParsedExpression &expression) {
    auto &star = expression.Cast<StarExpression>();
#if SQLCTX_NEW_EXPRESSION_API
    return SqlctxName(star.RelationName());
#else
    return star.relation_name;
#endif
}

string QuoteName(const string &name) {
    return "\"" + StringUtil::Replace(name, "\"", "\"\"") + "\"";
}

// semantic.<model> behaves like a view whose grain is the set of model
// dimensions the surrounding query uses. Each reference becomes a derived
// table; DuckDB keeps ownership of joins, outer aggregates and other SQL.
class SemanticRelations {
public:
    SemanticRelations(ClientContext &context, const SqlctxModelList &list, const string &snapshot)
        : context(context) {
        auto current = SqlctxName(DatabaseManager::GetDefaultDatabase(context));
        auto catalog = make_uniq<RelationCatalog>();
        catalog->snapshot = {snapshot, "", current};
        for (idx_t i = 0; i < list.count; ++i) catalog->models[list.models[i].name] = &list.models[i];
        catalogs[current] = std::move(catalog);
    }

    // Derived tables created for semantic relations, in creation order.
    struct CompiledRelation {
        reference<SubqueryRef> relation;
        const SqlctxCatalogSnapshot &snapshot;
    };
    vector<CompiledRelation> relations;

    void Query(QueryNode &node) {
        for (auto &cte : node.cte_map.map) {
#if SQLCTX_NEW_IDENTIFIER_API
            Query(*cte.second->query_node);
#else
            Query(*cte.second->query->node);
#endif
        }
        switch (node.type) {
        case QueryNodeType::SET_OPERATION_NODE:
            for (auto &child : node.Cast<SetOperationNode>().children) Query(*child);
            return;
        case QueryNodeType::RECURSIVE_CTE_NODE:
            Query(*node.Cast<RecursiveCTENode>().left);
            Query(*node.Cast<RecursiveCTENode>().right);
            return;
        case QueryNodeType::CTE_NODE:
            Query(*node.Cast<CTENode>().query);
            Query(*node.Cast<CTENode>().child);
            return;
        case QueryNodeType::SELECT_NODE:
            Select(node.Cast<SelectNode>());
            return;
        default:
            return;
        }
    }

private:
    struct RelationCatalog {
        SqlctxCatalogSnapshot snapshot;
        unique_ptr<ModelListOwner> list;
        case_insensitive_map_t<const SqlctxModelInfo *> models;
        unique_ptr<CatalogFieldsOwner> catalog;
        case_insensitive_map_t<case_insensitive_map_t<FieldInfo>> fields;
    };

    struct Relation {
        unique_ptr<TableRef> *slot;
        const SqlctxModelInfo *model;
        RelationCatalog *catalog;
        string alias;
        bool nullable;
        bool star = false;
        vector<string> fields;
        vector<unique_ptr<ParsedExpression>> filters;
    };

    ClientContext &context;
    case_insensitive_map_t<unique_ptr<RelationCatalog>> catalogs;

    RelationCatalog &GetCatalog(string name) {
        if (name.empty()) name = SqlctxName(DatabaseManager::GetDefaultDatabase(context));
        auto entry = catalogs.find(name);
        if (entry != catalogs.end()) return *entry->second;
        auto catalog = make_uniq<RelationCatalog>();
        catalog->snapshot = ReadSqlctxCatalog(context, name);
        catalog->list = make_uniq<ModelListOwner>(catalog->snapshot.payload);
        for (idx_t i = 0; i < catalog->list->list.count; ++i) {
            auto &model = catalog->list->list.models[i];
            catalog->models[model.name] = &model;
        }
        auto &result = *catalog;
        catalogs[name] = std::move(catalog);
        return result;
    }

    // Selectable fields (dimensions and metrics), in SELECT * order.
    vector<string> Columns(const Relation &relation) {
        Info(relation, relation.model->name);
        vector<std::pair<idx_t, string>> ordered;
        for (auto &field : relation.catalog->fields[relation.model->name]) {
            if (field.second.position) ordered.emplace_back(field.second.position, field.first);
        }
        std::sort(ordered.begin(), ordered.end());
        vector<string> result;
        for (auto &field : ordered) result.push_back(field.second);
        return result;
    }

    const FieldInfo &Info(const Relation &relation, const string &field) {
        auto &catalog = relation.catalog->catalog;
        auto &fields = relation.catalog->fields;
        if (!catalog) {
            catalog = make_uniq<CatalogFieldsOwner>(relation.catalog->snapshot.payload);
            for (idx_t i = 0; i < catalog->result.count; ++i) {
                auto &entry = catalog->result.entries[i];
                string kind(entry.kind);
                if (!entry.model_name || (kind != "metric" && kind != "dimension")) continue;
                auto &info = fields[entry.model_name][entry.name];
                info.metric = kind == "metric";
                info.aggregation = entry.aggregation ? entry.aggregation : "";
                info.type = entry.semantic_type ? entry.semantic_type : "";
                info.position = entry.column_index;
            }
        }
        auto base = field;
        auto suffix = base.rfind("__");
        auto &model_fields = fields[relation.model->name];
        if (!model_fields.count(base) && suffix != string::npos) base.resize(suffix);
        return model_fields[base];
    }

    void Select(SelectNode &select) {
        vector<Relation> found;
        if (select.from_table) Collect(select.from_table, false, found);
        auto nested = [&](unique_ptr<ParsedExpression> &expression) { Subqueries(*expression); };
        Expressions(select, nested);
        if (found.empty()) return;

        // Predicates on one relation's dimensions filter that relation before
        // grouping. They do not add to its grain.
        if (select.where_clause) {
            vector<unique_ptr<ParsedExpression>> conjuncts;
            Conjuncts(std::move(select.where_clause), conjuncts);
            vector<unique_ptr<ParsedExpression>> remaining;
            for (auto &conjunct : conjuncts) {
                auto target = FilterTarget(*conjunct, found);
                if (target) target->filters.push_back(std::move(conjunct));
                else remaining.push_back(std::move(conjunct));
            }
            for (auto &conjunct : remaining) {
                select.where_clause = select.where_clause
                    ? make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(select.where_clause),
                                                       std::move(conjunct))
                    : std::move(conjunct);
            }
        }
        auto grain = [&](unique_ptr<ParsedExpression> &expression) { References(*expression, found, true); };
        Expressions(select, grain);
        if (select.from_table) UsingColumns(*select.from_table, found);
        for (auto &relation : found) {
            if (relation.star) {
                for (auto &field : Columns(relation)) AddField(relation, field);
            }
            if (relation.fields.empty()) {
                throw BinderException("sqlctx: semantic.%s has no row grain. Select at least one of its "
                                      "dimensions or metrics, for example SELECT %s FROM semantic.%s",
                                      relation.model->name, Columns(relation).empty() ? "*" : Columns(relation)[0],
                                      relation.model->name);
            }
        }
        CheckReaggregation(select, found);
        for (auto &relation : found) Replace(relation);
    }

    template <class CALLBACK>
    void Expressions(SelectNode &select, CALLBACK &callback) {
        for (auto &expression : select.select_list) callback(expression);
        for (auto &expression : select.groups.group_expressions) callback(expression);
        if (select.where_clause) callback(select.where_clause);
        if (select.having) callback(select.having);
        if (select.qualify) callback(select.qualify);
        if (select.from_table) JoinConditions(*select.from_table, callback);
        ParsedExpressionIterator::EnumerateQueryNodeModifiers(select, callback);
    }

    template <class CALLBACK>
    void JoinConditions(TableRef &ref, CALLBACK &callback) {
        if (ref.type != TableReferenceType::JOIN) return;
        auto &join = ref.Cast<JoinRef>();
        if (join.condition) callback(join.condition);
        JoinConditions(*join.left, callback);
        JoinConditions(*join.right, callback);
    }

    void Subqueries(ParsedExpression &expression) {
        if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY) {
#if SQLCTX_NEW_EXPRESSION_API
            Query(*expression.Cast<SubqueryExpression>().Subquery()->node);
#else
            Query(*expression.Cast<SubqueryExpression>().subquery->node);
#endif
        }
        ParsedExpressionIterator::EnumerateChildren(expression, [&](ParsedExpression &child) { Subqueries(child); });
    }

    void Collect(unique_ptr<TableRef> &slot, bool nullable, vector<Relation> &found) {
        auto &ref = *slot;
        switch (ref.type) {
        case TableReferenceType::JOIN: {
            auto &join = ref.Cast<JoinRef>();
            bool left_nullable = join.type == JoinType::RIGHT || join.type == JoinType::OUTER;
            bool right_nullable = join.type == JoinType::LEFT || join.type == JoinType::OUTER;
            Collect(join.left, nullable || left_nullable, found);
            Collect(join.right, nullable || right_nullable, found);
            return;
        }
        case TableReferenceType::SUBQUERY:
            Query(*ref.Cast<SubqueryRef>().subquery->node);
            return;
        case TableReferenceType::SHOW_REF: {
            auto &show = ref.Cast<ShowRef>();
            if (show.query) Query(*show.query);
            return;
        }
        case TableReferenceType::BASE_TABLE:
            break;
        default:
            return;
        }
        auto &table = ref.Cast<BaseTableRef>();
#if SQLCTX_QUALIFIED_TABLE_API
        auto name = SqlctxName(table.Table());
        auto schema = SqlctxName(table.GetQualifiedName().Schema());
        auto catalog_name = SqlctxName(table.GetQualifiedName().Catalog());
#else
        auto name = table.table_name;
        auto schema = table.schema_name;
        auto catalog_name = table.catalog_name;
#endif
        if (!StringUtil::CIEquals(schema, "semantic")) return;
        // A user-created semantic schema keeps its physical relations.
        if (SqlctxPhysicalRelationExists(context, name, catalog_name)) return;
        auto &catalog = GetCatalog(catalog_name);
        auto &models = catalog.models;
        auto entry = models.find(name);
        if (entry == models.end()) {
            vector<string> names;
            for (auto &model : models) names.push_back(model.first);
            auto close = StringUtil::TopNLevenshtein(names, name, 3);
            throw BinderException("sqlctx: no semantic model named '%s'%s", name,
                                  close.empty() ? "" : ". Did you mean " + StringUtil::Join(close, ", ") + "?");
        }
        Relation relation;
        relation.slot = &slot;
        relation.model = entry->second;
        relation.catalog = &catalog;
        relation.alias = ref.alias.empty() ? string(entry->second->name) : SqlctxName(ref.alias);
        relation.nullable = nullable;
        found.push_back(std::move(relation));
    }

    // Resolve a column reference to a relation field. Rewrites semantic.model.field
    // to alias.field and canonicalizes field names.
    Relation *Resolve(ColumnRefExpression &column, vector<Relation> &found, string &field) {
#if SQLCTX_NEW_EXPRESSION_API
        auto &names = column.ColumnNamesMutable();
#else
        auto &names = column.column_names;
#endif
        Relation *relation = nullptr;
        if (names.size() >= 3 && StringUtil::CIEquals(SqlctxName(names[names.size() - 3]), "semantic")) {
            for (auto &candidate : found) {
                if (StringUtil::CIEquals(candidate.model->name, SqlctxName(names[names.size() - 2])) &&
                    StringUtil::CIEquals(candidate.alias, candidate.model->name)) {
                    relation = &candidate;
                }
            }
            if (!relation) return nullptr;
            bool current_catalog = names.size() == 3 ||
                (names.size() == 4 && StringUtil::CIEquals(SqlctxName(names[0]),
                    relation->catalog->snapshot.catalog));
            if (!current_catalog) {
                throw BinderException("Semantic column qualifier must name the relation's database");
            }
            names.erase(names.begin(), names.end() - 2);
        } else if (names.size() >= 3) {
            for (auto &candidate : found) {
                if (StringUtil::CIEquals(candidate.alias, SqlctxName(names[names.size() - 2]))) {
                    throw BinderException("Column qualifier does not match the semantic relation");
                }
            }
            return nullptr;
        } else if (names.size() == 2) {
            for (auto &candidate : found) {
                if (StringUtil::CIEquals(candidate.alias, SqlctxName(names[0]))) relation = &candidate;
            }
            if (!relation) return nullptr;
        } else if (names.size() == 1) {
            for (auto &candidate : found) {
                auto name = SqlctxName(names[0]);
                if (!CanonicalField(candidate.model, name)) continue;
                if (relation) {
                    throw BinderException("sqlctx: field '%s' is ambiguous between semantic.%s and semantic.%s; "
                                          "qualify it", name, relation->model->name, candidate.model->name);
                }
                relation = &candidate;
            }
            if (!relation) {
                // An unknown grain on a field of exactly one relation is a
                // semantic reference, not a column of another source.
                auto name = SqlctxName(names[0]);
                auto suffix = name.rfind("__");
                for (auto &candidate : found) {
                    auto base = suffix == string::npos ? string() : name.substr(0, suffix);
                    if (!base.empty() && CanonicalField(candidate.model, base)) {
                        throw BinderException(UnknownFieldMessage(*candidate.model, name));
                    }
                }
                return nullptr;
            }
        } else {
            return nullptr;
        }
        field = SqlctxName(names.back());
        if (!CanonicalField(relation->model, field)) {
            throw BinderException(UnknownFieldMessage(*relation->model, field));
        }
        return relation;
    }

    // JOIN ... USING (name) reads that column from both sides. Relations
    // outside the join's operands do not join on it.
    void UsingColumns(TableRef &ref, vector<Relation> &found) {
        if (ref.type != TableReferenceType::JOIN) return;
        auto &join = ref.Cast<JoinRef>();
        for (auto &column : join.using_columns) {
            for (auto &relation : found) {
                auto field = SqlctxName(column);
                if (Contains(ref, relation) && CanonicalField(relation.model, field)) AddField(relation, field);
            }
        }
        UsingColumns(*join.left, found);
        UsingColumns(*join.right, found);
    }

    bool Contains(TableRef &ref, const Relation &relation) {
        if (relation.slot->get() == &ref) return true;
        if (ref.type != TableReferenceType::JOIN) return false;
        auto &join = ref.Cast<JoinRef>();
        return Contains(*join.left, relation) || Contains(*join.right, relation);
    }

    void AddField(Relation &relation, const string &field) {
        for (auto &existing : relation.fields) {
            if (StringUtil::CIEquals(existing, field)) return;
        }
        relation.fields.push_back(field);
    }

    void References(ParsedExpression &expression, vector<Relation> &found, bool record) {
        if (expression.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
            string field;
            auto relation = Resolve(expression.Cast<ColumnRefExpression>(), found, field);
            if (relation && record) AddField(*relation, field);
            return;
        }
        if (expression.GetExpressionClass() == ExpressionClass::STAR) {
            auto name = StarRelation(expression);
            for (auto &relation : found) {
                if (name.empty() || StringUtil::CIEquals(name, relation.alias)) relation.star = true;
            }
        }
        if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY) {
            // Correlated references to an outer semantic relation add to its grain.
#if SQLCTX_NEW_EXPRESSION_API
            auto &node = *expression.Cast<SubqueryExpression>().Subquery()->node;
#else
            auto &node = *expression.Cast<SubqueryExpression>().subquery->node;
#endif
            if (node.type == QueryNodeType::SELECT_NODE) {
                auto correlated = [&](unique_ptr<ParsedExpression> &child) { Qualified(*child, found); };
                Expressions(node.Cast<SelectNode>(), correlated);
            }
        }
        ParsedExpressionIterator::EnumerateChildren(expression, [&](ParsedExpression &child) {
            References(child, found, record);
        });
    }

    void Qualified(ParsedExpression &expression, vector<Relation> &found) {
        if (expression.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
            auto &column = expression.Cast<ColumnRefExpression>();
#if SQLCTX_NEW_EXPRESSION_API
            auto size = column.ColumnNames().size();
#else
            auto size = column.column_names.size();
#endif
            if (size >= 2) References(expression, found, true);
            return;
        }
        ParsedExpressionIterator::EnumerateChildren(expression, [&](ParsedExpression &child) { Qualified(child, found); });
    }

    void Conjuncts(unique_ptr<ParsedExpression> expression, vector<unique_ptr<ParsedExpression>> &result) {
        if (expression->GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
            result.push_back(std::move(expression));
            return;
        }
        ParsedExpressionIterator::EnumerateChildren(*expression, [&](unique_ptr<ParsedExpression> &child) {
            Conjuncts(std::move(child), result);
        });
    }

    // The relation a predicate can filter directly: it reads only dimensions of
    // one relation that is never NULL-extended, without aggregates or subqueries.
    Relation *FilterTarget(ParsedExpression &predicate, vector<Relation> &found) {
        Relation *target = nullptr;
        bool pushable = true;
        std::function<void(ParsedExpression &)> visit = [&](ParsedExpression &expression) {
            if (!pushable) return;
            switch (expression.GetExpressionClass()) {
            case ExpressionClass::SUBQUERY:
            case ExpressionClass::WINDOW:
            case ExpressionClass::STAR:
                pushable = false;
                return;
            case ExpressionClass::FUNCTION:
                if (IsAggregateFunction(FunctionName(expression))) {
                    pushable = false;
                    return;
                }
                break;
            case ExpressionClass::COLUMN_REF: {
                string field;
                auto relation = Resolve(expression.Cast<ColumnRefExpression>(), found, field);
                if (!relation || relation->nullable || Info(*relation, field).metric ||
                    (target && target != relation)) {
                    pushable = false;
                }
                target = relation;
                return;
            }
            default:
                break;
            }
            ParsedExpressionIterator::EnumerateChildren(expression, visit);
        };
        visit(predicate);
        return pushable ? target : nullptr;
    }

    bool Reaggregatable(const string &function, const FieldInfo &metric) {
        if (!metric.type.empty() && metric.type != "simple") return false;
        auto aggregation = StringUtil::Lower(metric.aggregation);
        if (function == "sum") return aggregation == "sum" || aggregation == "count";
        if (function == "min" || function == "max") return aggregation == function;
        return false;
    }

    // Aggregating a metric again is exact when each group holds one relation
    // row, or when the metric's own aggregation composes (SUM of SUM/COUNT,
    // MIN of MIN, MAX of MAX). Anything else silently computes a different number.
    void CheckReaggregation(SelectNode &select, vector<Relation> &found) {
        case_insensitive_map_t<case_insensitive_set_t> grouped;
        auto group = [&](ParsedExpression &expression) {
            if (expression.GetExpressionClass() != ExpressionClass::COLUMN_REF) return;
            string field;
            auto relation = Resolve(expression.Cast<ColumnRefExpression>(), found, field);
            if (relation) grouped[relation->alias].insert(field);
        };
        bool group_all = select.aggregate_handling == AggregateHandling::FORCE_AGGREGATES;
        for (auto &expression : select.groups.group_expressions) group(*expression);
        if (group_all) {
            for (auto &expression : select.select_list) group(*expression);
        }
        std::function<void(ParsedExpression &, const string &)> visit = [&](ParsedExpression &expression,
                                                                               const string &aggregate) {
            switch (expression.GetExpressionClass()) {
            case ExpressionClass::WINDOW:
            case ExpressionClass::SUBQUERY:
                return;
            case ExpressionClass::FUNCTION: {
                auto name = FunctionName(expression);
                if (aggregate.empty() && IsAggregateFunction(name)) {
                    ParsedExpressionIterator::EnumerateChildren(expression, [&](ParsedExpression &child) {
                        visit(child, name);
                    });
                    return;
                }
                break;
            }
            case ExpressionClass::COLUMN_REF: {
                if (aggregate.empty()) return;
                string field;
                auto relation = Resolve(expression.Cast<ColumnRefExpression>(), found, field);
                if (!relation || !Info(*relation, field).metric) return;
                bool single_row_groups = true;
                for (auto &dimension : relation->fields) {
                    if (!Info(*relation, dimension).metric && !grouped[relation->alias].count(dimension)) {
                        single_row_groups = false;
                    }
                }
                if (single_row_groups || Reaggregatable(aggregate, Info(*relation, field))) return;
                throw BinderException("sqlctx: %s(%s) re-aggregates metric %s.%s over the grain of semantic.%s, "
                                      "which gives a different result than the metric itself. Select %s directly "
                                      "and group by the dimensions you need",
                                      aggregate, field, relation->model->name, field, relation->model->name, field);
            }
            default:
                break;
            }
            ParsedExpressionIterator::EnumerateChildren(expression, [&](ParsedExpression &child) {
                visit(child, aggregate);
            });
        };
        for (auto &expression : select.select_list) visit(*expression, "");
        if (select.having) visit(*select.having, "");
        if (select.qualify) visit(*select.qualify, "");
        ParsedExpressionIterator::EnumerateQueryNodeModifiers(select, [&](unique_ptr<ParsedExpression> &child) {
            visit(*child, "");
        });
    }

    void Replace(Relation &relation) {
        string sql = "SELECT ";
        for (idx_t i = 0; i < relation.fields.size(); ++i) {
            if (i) sql += ", ";
            sql += QuoteName(relation.alias) + "." + QuoteName(relation.fields[i]) + " AS " +
                   QuoteName(relation.fields[i]);
        }
        // The compiler sees one catalog at a time. The host resolves the owner
        // first; only its semantic schema/model name belongs in compiler SQL.
        auto source = "semantic." + QuoteName(relation.model->name) + " AS " + QuoteName(relation.alias);
        sql += " FROM " + source;
        for (idx_t i = 0; i < relation.filters.size(); ++i) {
            sql += (i ? " AND (" : " WHERE (") + relation.filters[i]->ToString() + ")";
        }
        Parser parser(SqlctxBuiltinParserOptions());
        parser.ParseQuery("SELECT * FROM (" + sql + ") AS " + QuoteName(relation.alias));
        auto &wrapper = parser.statements.at(0)->Cast<SelectStatement>().node->Cast<SelectNode>();
        *relation.slot = std::move(wrapper.from_table);
        relations.push_back({(*relation.slot)->Cast<SubqueryRef>(), relation.catalog->snapshot});
    }
};

class SqlctxBindState : public ClientContextState {
public:
    explicit SqlctxBindState(unique_ptr<ParserExtensionParseData> data) : data(std::move(data)) {}
    void QueryEnd() override { data.reset(); }
    unique_ptr<ParserExtensionParseData> data;
};

// Keep the native container intact: DuckDB owns destination names, column
// mapping, RETURNING, write properties, transactions and view persistence.
SelectStatement *QueryInStatement(SQLStatement &statement, bool include_insert_ctes = false) {
    if (statement.type == StatementType::EXPLAIN_STATEMENT) {
        return QueryInStatement(*statement.Cast<ExplainStatement>().stmt, include_insert_ctes);
    }
    if (statement.type == StatementType::SELECT_STATEMENT) {
        return &statement.Cast<SelectStatement>();
    }
    if (statement.type == StatementType::CREATE_STATEMENT) {
        auto &info = *statement.Cast<CreateStatement>().info;
        if (info.type == CatalogType::VIEW_ENTRY) return info.Cast<CreateViewInfo>().query.get();
        if (info.type == CatalogType::TABLE_ENTRY) return info.Cast<CreateTableInfo>().query.get();
    }
    if (statement.type == StatementType::INSERT_STATEMENT) {
#if SQLCTX_INSERT_QUERY_NODE
        auto &insert = *statement.Cast<InsertStatement>().node;
#else
        auto &insert = statement.Cast<InsertStatement>();
#endif
        auto &query = insert.select_statement;
        if (query && include_insert_ctes && !insert.cte_map.map.empty()) {
            // WITH before INSERT is an outer scope. Preserve it around the
            // source SELECT instead of merging it with that SELECT's own WITH.
            auto wrapper = make_uniq<SelectNode>();
            wrapper->select_list.push_back(make_uniq<StarExpression>());
            wrapper->cte_map = std::move(insert.cte_map);
            auto inner = make_uniq<SelectStatement>();
            inner->node = std::move(query->node);
            wrapper->from_table = make_uniq<SubqueryRef>(std::move(inner), "__sqlctx_insert_source");
            query->node = std::move(wrapper);
        }
        return query.get();
    }
    return nullptr;
}

unique_ptr<QueryNode> CompileSemanticSelect(const SqlctxCatalogSnapshot &snapshot, const string &sql) {
    auto result = sqlctx_snapshot_rewrite(snapshot.payload.c_str(), sql.c_str());
    if (result.error) {
        string error(result.error);
        sqlctx_free_result(result);
        throw BinderException("sqlctx: %s", SqlctxErrorText(error));
    }
    if (!result.sql) {
        sqlctx_free_result(result);
        throw InternalException("sqlctx returned no rewritten SQL");
    }
    string compiled(result.sql);
    sqlctx_free_result(result);
    Parser parser(SqlctxBuiltinParserOptions());
    parser.ParseQuery(compiled);
    if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
        throw BinderException("sqlctx rewrite must produce one SELECT statement");
    }
    return std::move(parser.statements[0]->Cast<SelectStatement>().node);
}

unique_ptr<SQLStatement> CompileSemanticStatement(ClientContext &context, SQLStatement &statement,
                                                 const SqlctxCatalogSnapshot &snapshot,
                                                 bool explicit_semantic = false,
                                                 const SqlctxModelList *cached_models = nullptr) {
    auto rewritten = statement.Copy();
    auto query = QueryInStatement(*rewritten, true);
    if (!query) return nullptr;
    unique_ptr<ModelListOwner> owned_models;
    if (!cached_models) owned_models = make_uniq<ModelListOwner>(snapshot.payload);
    auto &models = cached_models ? *cached_models : owned_models->list;
    SemanticRelations relations(context, models, snapshot.payload);
    relations.Query(*query->node);
    unordered_set<const TableRef *> derived;
    for (auto &relation : relations.relations) derived.insert(&relation.relation.get());
    SemanticReferences references(context, models, explicit_semantic, derived);
    bool found = references.Query(*query->node) || explicit_semantic;
    references.ThrowUnknownField(found);
    for (auto &relation : relations.relations) {
        bool attached = !StringUtil::CIEquals(relation.snapshot.catalog,
                                              SqlctxName(DatabaseManager::GetDefaultDatabase(context)));
        // A whole-query rewrite already compiles current-catalog relations.
        // Compile attached relations separately before giving that compiler
        // their fully qualified physical SQL.
        if (found && !attached) continue;
        auto &subquery = *relation.relation.get().subquery;
        subquery.node = CompileSemanticSelect(relation.snapshot, subquery.node->ToString());
        if (attached) QualifySqlctxSources(context, *subquery.node, relation.snapshot.catalog);
    }
    if (!found) {
        if (relations.relations.empty()) return nullptr;
        // Only semantic relations: compile each derived table and leave the
        // rest of the statement, including DESCRIBE/SUMMARIZE, to DuckDB.
        return rewritten;
    }
    query->node = CompileSemanticSelect(snapshot, query->ToString());
    return rewritten;
}

// Autocomplete can parse SHOW MODELS as a native SHOW/DESCRIBE reference before
// our parser sees it. Recognize that AST, without reparsing the current query.
unique_ptr<SQLStatement> SemanticShowStatement(SQLStatement &statement) {
    if (statement.type != StatementType::SELECT_STATEMENT) return nullptr;
    auto &node = *statement.Cast<SelectStatement>().node;
    if (node.type != QueryNodeType::SELECT_NODE) return nullptr;
    auto &select = node.Cast<SelectNode>();
    if (!select.from_table || select.from_table->type != TableReferenceType::SHOW_REF) return nullptr;
    auto &show = select.from_table->Cast<ShowRef>();
#if SQLCTX_NEW_IDENTIFIER_API
    if (show.show_type != ShowType::SHOW || !show.GetCatalogName().empty() || !show.GetSchemaName().empty()) {
        return nullptr;
    }
    auto name = SqlctxName(show.GetTableName());
#else
    // The stable AST represents SHOW name and DESCRIBE name identically.
    // Consult only this statement's preserved source to distinguish them.
    auto source = statement.query;
    if (statement.stmt_location < source.size()) {
        source = source.substr(statement.stmt_location, statement.stmt_length ? statement.stmt_length : string::npos);
    }
    auto tokens = Tokens(source);
    if (!Keyword(tokens, 0, "SHOW")) return nullptr;
    string name = show.table_name;
    if (show.query && show.query->type == QueryNodeType::SELECT_NODE) {
        auto &inner = show.query->Cast<SelectNode>();
        if (!inner.from_table || inner.from_table->type != TableReferenceType::BASE_TABLE) return nullptr;
        auto &table = inner.from_table->Cast<BaseTableRef>();
        if (!table.catalog_name.empty() || !table.schema_name.empty()) return nullptr;
        name = table.table_name;
    } else if (!show.catalog_name.empty() || !show.schema_name.empty()) return nullptr;
#endif
    for (auto plural : {"models", "metrics", "dimensions", "segments", "relationships"}) {
        if (!StringUtil::CIEquals(name, plural)) continue;
        auto data = make_uniq<SqlctxParseData>();
        data->operation = "show_" + string(plural);
        data->operation.pop_back();
        return SqlctxStatement(std::move(data));
    }
    return nullptr;
}

constexpr const char *ROUTING_STATE = "sqlctx_native_routing";
constexpr const char *QUERY_CARRIER = "__sqlctx_bind_query";
constexpr const char *ROUTING_PROBE = "sqlctx_pristine_show";

// A cheap AST scan bounds catalog work. Scope and shadowing remain the
// responsibility of SemanticReferences, after this candidate-name filter.
struct RoutingReferences {
    case_insensitive_set_t names;
    bool semantic = false;

    void Query(QueryNode &node) {
        auto expression = [&](unique_ptr<ParsedExpression> &expr) { Expression(*expr); };
        auto table = [&](TableRef &ref) {
#if !SQLCTX_NEW_IDENTIFIER_API
            // The stable host's generic iterator does not visit SHOW's query.
            if (ref.type == TableReferenceType::SHOW_REF) {
                auto &show = ref.Cast<ShowRef>();
                if (show.query) Query(*show.query);
                return;
            }
#endif
            if (ref.type != TableReferenceType::BASE_TABLE) return;
            auto &base = ref.Cast<BaseTableRef>();
#if SQLCTX_QUALIFIED_TABLE_API
            auto schema = SqlctxName(base.GetQualifiedName().Schema());
            auto catalog = SqlctxName(base.GetQualifiedName().Catalog());
            auto name = SqlctxName(base.Table());
#else
            auto schema = base.schema_name;
            auto catalog = base.catalog_name;
            auto name = base.table_name;
#endif
            if (StringUtil::CIEquals(schema, "semantic")) semantic = true;
            if (schema.empty() && catalog.empty()) names.insert(name);
        };
        if (node.type == QueryNodeType::CTE_NODE) {
            auto &cte = node.Cast<CTENode>();
            Query(*cte.query);
            Query(*cte.child);
            return;
        }
        ParsedExpressionIterator::EnumerateQueryNodeChildren(node, expression, table);
    }

    void Expression(ParsedExpression &expr) {
        if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
#if SQLCTX_NEW_EXPRESSION_API
            Query(*expr.Cast<SubqueryExpression>().Subquery()->node);
#else
            Query(*expr.Cast<SubqueryExpression>().subquery->node);
#endif
        }
        ParsedExpressionIterator::EnumerateChildren(expr, [&](ParsedExpression &child) { Expression(child); });
    }

    bool Possible() const { return semantic || !names.empty(); }
    bool Matches(const SqlctxModelList &models) const {
        if (semantic || names.count("metrics")) return true;
        for (idx_t i = 0; i < models.count; ++i) if (names.count(models.models[i].name)) return true;
        return false;
    }
};

struct QueryCarrierData : DependencyItem {
    explicit QueryCarrierData(unique_ptr<SQLStatement> query) : query(std::move(query)) {}
    unique_ptr<SQLStatement> query;
};

unique_ptr<SelectStatement> PrivateCarrierQuery(const TableFunctionRef &table) {
    if (!table.external_dependency) return nullptr;
    auto item = table.external_dependency->GetDependency(QUERY_CARRIER);
    if (!item) return nullptr;
    return unique_ptr_cast<SQLStatement, SelectStatement>(item->Cast<QueryCarrierData>().query->Copy());
}

string CarrierParserSettings(const ParserOptions &options) {
    string settings = options.integer_division ? "1:" : "0:";
#if SQLCTX_NEW_IDENTIFIER_API
    switch (options.identifier_case_mode) {
    case IdentifierCaseMode::PRESERVE_CASE: settings += "preserve:"; break;
    case IdentifierCaseMode::LOWERCASE: settings += "lower:"; break;
    case IdentifierCaseMode::UPPERCASE: settings += "upper:"; break;
    }
    settings += options.regex_match_operator_semantics == RegexMatchOperatorSemantics::FULL ? "full" : "partial";
#else
    settings += options.preserve_identifier_case ? "preserve:full" : "lower:full";
#endif
    return settings;
}

unique_ptr<SelectStatement> ParseCarrier(const string &sql, const string &settings) {
    auto options = SqlctxBuiltinParserOptions();
    if (!settings.empty()) {
        auto parts = StringUtil::Split(settings, ':');
        if (parts.size() != 3 || (parts[0] != "0" && parts[0] != "1") ||
            (parts[1] != "preserve" && parts[1] != "lower" && parts[1] != "upper") ||
            (parts[2] != "partial" && parts[2] != "full")) {
            throw InvalidInputException("Invalid sqlctx query carrier parser settings");
        }
        options.integer_division = parts[0] == "1";
#if SQLCTX_NEW_IDENTIFIER_API
        options.identifier_case_mode = parts[1] == "lower" ? IdentifierCaseMode::LOWERCASE :
            (parts[1] == "upper" ? IdentifierCaseMode::UPPERCASE : IdentifierCaseMode::PRESERVE_CASE);
        options.regex_match_operator_semantics = parts[2] == "full" ? RegexMatchOperatorSemantics::FULL :
                                                                                    RegexMatchOperatorSemantics::PARTIAL;
#else
        options.preserve_identifier_case = parts[1] == "preserve";
#endif
    }
    Parser parser(options);
    parser.ParseQuery(sql);
    if (parser.statements.size() != 1) throw BinderException("sqlctx query carrier expects one query");
    auto statement = std::move(parser.statements[0]);
    while (statement->type == StatementType::PREPARE_STATEMENT) {
        auto prepared = std::move(statement->Cast<PrepareStatement>().statement);
        statement = std::move(prepared);
    }
    auto query = QueryInStatement(*statement, true);
    if (!query) throw BinderException("sqlctx query carrier expects a SELECT query source");
    return unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
}

// con.sql and relational callers bind QueryNodes directly, bypassing Planner.
// Retain SELECT and defer catalog reads to their active binding transaction.
void WrapQueryCarrier(SelectStatement &query, const string &source, const ParserOptions &options) {
    if (source.empty()) return;
    RoutingReferences refs;
    refs.Query(*query.node);
    if (!refs.Possible()) return;
    auto relation = make_uniq<TableFunctionRef>();
    vector<unique_ptr<ParsedExpression>> arguments;
#if SQLCTX_LITERAL_API
    arguments.push_back(ConstantExpression::String(source));
    arguments.push_back(ConstantExpression::String(CarrierParserSettings(options)));
#else
    arguments.push_back(make_uniq<ConstantExpression>(Value(source)));
    arguments.push_back(make_uniq<ConstantExpression>(Value(CarrierParserSettings(options))));
#endif
    relation->function = make_uniq<FunctionExpression>(QUERY_CARRIER, std::move(arguments));
    // Native AST copies keep the private input. DuckDB omits external
    // dependencies from serialization; persisted views retain exact SQL source
    // and reconstruct it through the parser on reopen, never a binary decoder.
    relation->external_dependency = make_shared_ptr<ExternalDependency>();
    relation->external_dependency->AddDependency(QUERY_CARRIER, make_shared_ptr<QueryCarrierData>(query.Copy()));
    auto wrapper = make_uniq<SelectNode>();
    // A normal subquery deduplicates result names (x, x -> x, x_1). Give the
    // binder unique internal names, then restore the exact native labels with
    // COLUMNS capture aliases, including SELECT * and expanding expressions.
    auto columns = make_uniq<StarExpression>();
    const string pattern = "^__sqlctx_column_[0-9]+_([\\s\\S]*)$";
#if SQLCTX_NEW_EXPRESSION_API
    columns->IsColumnsMutable() = true;
    columns->ExpressionMutable() = ConstantExpression::String(pattern);
#else
    columns->columns = true;
    columns->expr = make_uniq<ConstantExpression>(Value(pattern));
#endif
    columns->SetAlias(SqlctxIdentifier("\\1"));
    wrapper->select_list.push_back(std::move(columns));
    wrapper->from_table = std::move(relation);
    query.node = std::move(wrapper);
}

unique_ptr<SelectStatement> UnwrapQueryCarrier(SelectStatement &query) {
    if (query.node->type != QueryNodeType::SELECT_NODE) return nullptr;
    auto &select = query.node->Cast<SelectNode>();
    if (!select.from_table || select.from_table->type != TableReferenceType::TABLE_FUNCTION) return nullptr;
    auto &table = select.from_table->Cast<TableFunctionRef>();
    if (FunctionName(*table.function) != QUERY_CARRIER) return nullptr;
    // Only our private, untouched carrier is eligible. A user's direct call
    // must keep its projections and predicates and receive normal validation.
    if (select.select_list.size() != 1 || select.select_list[0]->GetExpressionClass() != ExpressionClass::STAR ||
        select.where_clause || select.having || select.qualify || !select.groups.group_expressions.empty() ||
        !select.modifiers.empty() || !select.cte_map.map.empty() || select.sample) return nullptr;
    return PrivateCarrierQuery(table);
}

class SqlctxRoutingState : public ClientContextState {
public:
    explicit SqlctxRoutingState(ClientContext &context) : context(context) {}

    bool CanRequestRebind() override {
        candidate.reset();
        probing = !SQLCTX_PRE_BIND;
        probe_error = false;
        // Compatibility recovery for ASTs supplied by other parsers. Normal
        // SQL and relational calls use the carrier without exception probes.
        return !SQLCTX_PRE_BIND && context.transaction.HasActiveTransaction() &&
               !ValidChecker::IsInvalidated(context.ActiveTransaction());
    }

    RebindQueryInfo OnPlanningError(ClientContext &, SQLStatement &statement, ErrorData &error) override {
        bool own_probe = probe_error && error.ExtraInfo().count(ROUTING_PROBE);
        probing = false;
        probe_error = false;
        if (own_probe) {
            candidate = SemanticShowStatement(statement);
            return RebindQueryInfo::ATTEMPT_TO_REBIND;
        }
        if (error.Type() != ExceptionType::BINDER && error.Type() != ExceptionType::CATALOG) {
            return RebindQueryInfo::DO_NOT_REBIND;
        }
        candidate = Candidate(statement);
        return candidate ? RebindQueryInfo::ATTEMPT_TO_REBIND : RebindQueryInfo::DO_NOT_REBIND;
    }

    RebindQueryInfo OnRebindPreparedStatement(ClientContext &, BindPreparedStatementCallbackInfo &info,
                                             RebindQueryInfo) override {
        // On 1.5 a preceding parser may supply an unwrapped SELECT. SQL
        // EXECUTE creates a nested Planner without the client error callback.
        if (SQLCTX_PRE_BIND || !info.prepared_statement.unbound_statement) return RebindQueryInfo::DO_NOT_REBIND;
        candidate = Candidate(*info.prepared_statement.unbound_statement);
        return candidate ? RebindQueryInfo::ATTEMPT_TO_REBIND : RebindQueryInfo::DO_NOT_REBIND;
    }

    RebindQueryInfo OnFinalizePrepare(ClientContext &, PreparedStatementData &, PreparedStatementMode) override {
        probing = false;
        probe_error = false;
        return RebindQueryInfo::DO_NOT_REBIND;
    }

    void QueryEnd() override {
        candidate.reset();
        probing = false;
        probe_error = false;
    }

    unique_ptr<SQLStatement> Candidate(SQLStatement &statement) {
        if (statement.type == StatementType::PREPARE_STATEMENT) {
            return Candidate(*statement.Cast<PrepareStatement>().statement);
        }
        if (auto show = SemanticShowStatement(statement)) return show;
        auto query = QueryInStatement(statement);
        if (!query) return nullptr;
        RoutingReferences refs;
        refs.Query(*query->node);
        if (!refs.Possible()) return nullptr;
        SqlctxCatalogSnapshot snapshot;
        try {
            snapshot = ReadSqlctxCatalog(context);
        } catch (const std::exception &exception) {
            ErrorData error(exception);
            if (refs.semantic || (error.Type() != ExceptionType::INVALID_INPUT &&
                error.Type() != ExceptionType::PARSER && error.Type() != ExceptionType::IO &&
                error.Type() != ExceptionType::PERMISSION)) throw;
            // A malformed legacy sidecar must not disable ordinary tables.
            // Keep the catalog diagnostic for references that need semantics.
            for (auto &name : refs.names) {
                SqlctxPhysicalRelation physical;
                if (!SqlctxFindPhysicalRelation(context, name, physical)) throw;
            }
            return nullptr;
        }
        if (!models || snapshot.payload != model_snapshot) {
            auto replacement = make_uniq<ModelListOwner>(snapshot.payload);
            model_snapshot = snapshot.payload;
            models = std::move(replacement);
        }
        if (!refs.Matches(models->list)) return nullptr;
        return CompileSemanticStatement(context, statement, snapshot, false, &models->list);
    }

    BoundStatement BindCandidate(Binder &binder) {
        auto statement = std::move(candidate);
        if (auto parameters = binder.GetParameters()) {
            parameters->GetParametersPtr()->clear();
            parameters->rebind = false;
        }
        binder.GetStatementProperties() = StatementProperties();
        RegisterSqlctxCatalogRead(context, binder.GetStatementProperties());
        auto child = Binder::CreateBinder(context, &binder);
        return child->Bind(*statement);
    }

    unique_ptr<SQLStatement> candidate;
    bool probing = false;
    bool probe_error = false;
private:
    ClientContext &context;
    string model_snapshot;
    unique_ptr<ModelListOwner> models;
};

unique_ptr<LogicalOperator> BindQueryCarrier(ClientContext &context, TableFunctionBindInput &input,
#if SQLCTX_NEW_IDENTIFIER_API
                                           TableIndex bind_index,
#else
                                           idx_t bind_index,
#endif
                                           vector<SqlctxIdentifier> &names) {
    if (input.inputs[0].IsNull()) throw InvalidInputException("sqlctx query carrier cannot be NULL");
    unique_ptr<SQLStatement> statement = PrivateCarrierQuery(input.ref);
    if (!statement) {
        if (input.inputs.size() > 1 && input.inputs[1].IsNull()) {
            throw InvalidInputException("sqlctx query carrier parser settings cannot be NULL");
        }
        statement = ParseCarrier(input.inputs[0].GetValue<string>(),
                                 input.inputs.size() > 1 ? input.inputs[1].GetValue<string>() : "");
    }
    auto state = context.registered_state->GetOrCreate<SqlctxRoutingState>(ROUTING_STATE, context);
    auto compiled = state->Candidate(*statement);
    if (compiled) {
        statement = std::move(compiled);
        if (input.binder) RegisterSqlctxCatalogRead(context, input.binder->GetStatementProperties());
    }
    if (!input.binder) throw InternalException("sqlctx query carrier requires a binder");
    auto child = Binder::CreateBinder(context, input.binder.get());
    child->SetCanContainNulls(true);
    auto bound = child->Bind(*statement);
    input.binder->MergeCorrelatedColumns(child->correlated_columns);
    auto bindings = bound.plan->GetColumnBindings();
    vector<unique_ptr<Expression>> expressions;
    for (idx_t i = 0; i < bound.names.size(); ++i) {
        names.emplace_back("__sqlctx_column_" + to_string(i) + "_" + SqlctxName(bound.names[i]));
        expressions.push_back(make_uniq<BoundColumnRefExpression>(bound.types[i], bindings[i]));
    }
    auto projection = make_uniq<LogicalProjection>(bind_index, std::move(expressions));
    projection->children.push_back(std::move(bound.plan));
    return std::move(projection);
}

#if SQLCTX_PRE_BIND
void SqlctxPreBind(PlannerExtensionInput &input, SQLStatement &statement) {
    auto query = QueryInStatement(statement);
    if (!query) return;
    if (auto original = UnwrapQueryCarrier(*query)) query->node = std::move(original->node);
    auto state = input.context.registered_state->GetOrCreate<SqlctxRoutingState>(ROUTING_STATE, input.context);
    auto compiled = state->Candidate(statement);
    if (!compiled) return;
    if (compiled->type != statement.type) {
        state->candidate = std::move(compiled);
        return;
    }
    auto rewritten = QueryInStatement(*compiled);
    if (!rewritten) return;
    query->node = std::move(rewritten->node);
    RegisterSqlctxCatalogRead(input.context, input.binder.GetStatementProperties());
}
#endif

void SqlctxPostBind(PlannerExtensionInput &input, BoundStatement &bound) {
    auto state = input.context.registered_state->Get<SqlctxRoutingState>(ROUTING_STATE);
    if (!state) return;
    if (state->candidate) {
        bound = state->BindCandidate(input.binder);
        return;
    }
    // Only legacy SHOW needs the pristine client AST after a successful bind:
    // another parser can interpret SHOW MODELS as DESCRIBE a physical table.
    if (!state->probing || state->probe_error || bound.names.size() < 2 ||
        bound.names[0] != "column_name" || bound.names[1] != "column_type") return;
    state->probe_error = true;
    throw Exception(unordered_map<string, string> {{ROUTING_PROBE, "true"}}, ExceptionType::INVALID,
                    "sqlctx requires the pristine SHOW statement");
}

class SqlctxConnectionCallback : public ExtensionCallback {
public:
    void OnConnectionOpened(ClientContext &context) override {
        context.registered_state->GetOrCreate<SqlctxRoutingState>(ROUTING_STATE, context);
    }
};

} // namespace

void RegisterSqlctxRouting(DatabaseInstance &db) {
    auto &config = DBConfig::GetConfig(db);
    auto callback = make_shared_ptr<SqlctxConnectionCallback>();
    ExtensionCallback::Register(config, callback);
    // Register future connections before visiting existing ones. GetOrCreate
    // makes connections opened concurrently with LOAD harmless duplicates.
    for (auto &context : ConnectionManager::Get(db).GetConnectionList()) callback->OnConnectionOpened(*context);
    PlannerExtension planner;
#if SQLCTX_PRE_BIND
    planner.pre_bind_function = SqlctxPreBind;
#endif
    planner.post_bind_function = SqlctxPostBind;
    PlannerExtension::Register(config, planner);
}

void RegisterSqlctxQueryCarrier(ExtensionLoader &loader) {
    TableFunctionSet carriers {SqlctxIdentifier(QUERY_CARRIER)};
    for (idx_t count = 1; count <= 2; ++count) {
        TableFunction carrier(QUERY_CARRIER, vector<LogicalType>(count, LogicalType::VARCHAR), nullptr, nullptr);
        carrier.bind_operator = BindQueryCarrier;
        carriers.AddFunction(carrier);
    }
    loader.RegisterFunction(carriers);
}

unique_ptr<ParserExtensionParseData> SqlctxParseData::Copy() const {
    auto copy = make_uniq<SqlctxParseData>();
    copy->operation = operation;
    copy->content = content;
    copy->replace = replace;
    copy->explicit_semantic = explicit_semantic;
    if (statement) copy->statement = statement->Copy();
    return std::move(copy);
}

string SqlctxParseData::ToString() const {
    return statement ? statement->ToString() : content;
}

unique_ptr<SQLStatement> SqlctxStatement(unique_ptr<SqlctxParseData> data) {
    auto result = make_uniq<SqlctxSQLStatement>(std::move(data));
    auto &parsed = static_cast<SqlctxParseData &>(*result->parse_data);
    if (parsed.statement) {
        result->named_param_map = parsed.statement->named_param_map;
        result->query = parsed.statement->query;
        result->stmt_location = parsed.statement->stmt_location;
#if SQLCTX_NEW_IDENTIFIER_API
        result->has_anonymous_parameters = parsed.statement->has_anonymous_parameters;
#else
        result->stmt_length = parsed.statement->stmt_length;
#endif
    }
    return std::move(result);
}

unique_ptr<SqlctxParseData> ParseSqlctxDefinition(const string &sql) {
    string clean;
    auto tokens = Tokens(sql, &clean);
    while (!tokens.empty() && tokens.back().text == ";") tokens.pop_back();
    idx_t pos = Keyword(tokens, 0, "SEMANTIC") ? 1 : 0;
    if (Keyword(tokens, pos, "SHOW")) {
        ++pos;
        if (Keyword(tokens, pos, "SEMANTIC")) ++pos;
        string kind;
        for (auto name : {"MODELS", "METRICS", "DIMENSIONS", "SEGMENTS", "RELATIONSHIPS"}) {
            if (Keyword(tokens, pos, name)) kind = StringUtil::Lower(name);
        }
        if (kind.empty()) return nullptr;
        kind.pop_back();
        ++pos;
        auto result = make_uniq<SqlctxParseData>();
        result->operation = "show_" + kind;
        if (Keyword(tokens, pos, "FOR") && kind == "dimension") {
            result->operation = "show_compatible_dimensions";
            result->content = QualifiedNameJson(tokens, pos + 1, 2);
        } else if (pos < tokens.size()) {
            if (!Keyword(tokens, pos, "FROM") || pos + 2 != tokens.size()) {
                throw ParserException("SHOW semantic definitions expects FROM model or DIMENSIONS FOR model.metric");
            }
            result->content = IdentifierText(tokens[pos + 1].text);
        }
        return result;
    }
    if ((Keyword(tokens, pos, "DESCRIBE") || Keyword(tokens, pos, "DESC")) && Keyword(tokens, pos + 1, "MODEL")) {
        if (pos + 3 != tokens.size()) throw ParserException("DESCRIBE MODEL expects one model name");
        auto result = make_uniq<SqlctxParseData>();
        result->operation = "show_";
        result->content = IdentifierText(tokens[pos + 2].text);
        return result;
    }
    if ((Keyword(tokens, pos, "EXPORT") || Keyword(tokens, pos, "IMPORT")) &&
        Keyword(tokens, pos + 1, "SEMANTIC") && Keyword(tokens, pos + 2, "CATALOG")) {
        bool importing = Keyword(tokens, pos, "IMPORT");
        auto result = make_uniq<SqlctxParseData>();
        result->operation = importing ? "import" : "export";
        if (importing) {
            if (pos + 4 != tokens.size() || tokens[pos + 3].text.front() != '\'') {
                throw ParserException("IMPORT SEMANTIC CATALOG expects a quoted catalog snapshot");
            }
            result->content = IdentifierText(tokens[pos + 3].text);
        } else if (pos + 3 != tokens.size()) {
            throw ParserException("EXPORT SEMANTIC CATALOG takes no arguments");
        }
        return result;
    }
    if (Keyword(tokens, pos, "DROP")) {
        ++pos;
        bool view = Keyword(tokens, pos, "SEMANTIC") && Keyword(tokens, pos + 1, "VIEW");
        if (view) ++pos;
        if (!(Keyword(tokens, pos, "MODEL") || Keyword(tokens, pos, "METRIC") ||
              Keyword(tokens, pos, "DIMENSION") || Keyword(tokens, pos, "SEGMENT") || view)) return nullptr;
        auto result = make_uniq<SqlctxParseData>();
        result->operation = "drop_" + StringUtil::Lower(tokens[pos++].text);
        if (Keyword(tokens, pos, "IF") && Keyword(tokens, pos + 1, "EXISTS")) {
            result->replace = true;
            pos += 2;
        }
        result->content = QualifiedNameJson(tokens, pos, view || result->operation == "drop_model" ? 1 : 2);
        return result;
    }
    bool create = Keyword(tokens, pos, "CREATE");
    bool replace = false;
    if (create) {
        ++pos;
        if (Keyword(tokens, pos, "OR") && Keyword(tokens, pos + 1, "REPLACE")) {
            pos += 2;
            replace = true;
        }
    }
    if (pos >= tokens.size()) return nullptr;
    if (create && Keyword(tokens, pos, "SEMANTIC") && Keyword(tokens, pos + 1, "VIEW")) {
        pos += 2;
        if (pos + 2 >= tokens.size() || !Keyword(tokens, pos + 1, "AS")) {
            throw ParserException("CREATE SEMANTIC VIEW expects name AS SELECT dimensions and metrics FROM model");
        }
        auto result = make_uniq<SqlctxParseData>();
        result->replace = replace;
        result->operation = "view";
        result->content = clean.substr(tokens[pos].start, tokens.back().end - tokens[pos].start);
        return result;
    }
    bool model = Keyword(tokens, pos, "MODEL");
    bool item = Keyword(tokens, pos, "METRIC") || Keyword(tokens, pos, "DIMENSION") || Keyword(tokens, pos, "SEGMENT");
    if (!model && !item) return nullptr;
    auto result = make_uniq<SqlctxParseData>();
    result->replace = replace;
    result->operation = model ? "model" : "item";
    result->content = clean.substr(tokens[pos].start, tokens.back().end - tokens[pos].start);
    if (model && !create && pos + 2 == tokens.size() && tokens[pos + 1].text != "(") {
        result->operation = "use";
        result->content = IdentifierText(tokens[pos + 1].text);
    } else if (model && create && pos + 2 < tokens.size() && tokens[pos + 2].text == "(") {
        auto name = IdentifierText(tokens[pos + 1].text);
        int depth = 0;
        bool has_name = false;
        bool property_start = false;
        for (idx_t i = pos + 2; i < tokens.size(); ++i) {
            auto &token = tokens[i].text;
            if (token == "(" || token == "[" || token == "{") {
                ++depth;
                property_start = i == pos + 2;
                continue;
            }
            if (token == ")" || token == "]" || token == "}") --depth;
            if (depth == 1 && property_start && Keyword(tokens, i, "NAME") && i + 1 < tokens.size()) {
                auto value = i + 1;
                if (tokens[value].text == ":" || tokens[value].text == "=") ++value;
                if (value >= tokens.size()) {
                    throw ParserException("CREATE MODEL name '%s' is missing a body name", name);
                }
                auto body_name = IdentifierText(tokens[value].text);
                if (body_name != name) {
                    throw ParserException("CREATE MODEL name '%s' does not match body name '%s'", name, body_name);
                }
                has_name = true;
                break;
            }
            property_start = depth == 1 && token == ",";
        }
        auto open = tokens[pos + 2].start;
        if (has_name) {
            result->content = "MODEL " + clean.substr(open, tokens.back().end - open);
        } else {
            auto rest = clean.substr(tokens[pos + 2].end, tokens.back().end - tokens[pos + 2].end);
            auto separator = pos + 3 < tokens.size() && tokens[pos + 3].text == ")" ? "" : ", ";
            result->content = "MODEL (name " + SqlctxLiteral(name) + separator + rest;
        }
    } else if (model && create) {
        result->content = "MODEL " + clean.substr(tokens[pos].end, tokens.back().end - tokens[pos].end);
    }
    return result;
}

unique_ptr<SQLStatement> WrapSqlctxQuery(unique_ptr<SQLStatement> statement, bool explicit_semantic,
                                       const string &source_override, const ParserOptions *parser_options) {
    auto source = source_override.empty() ? statement->query : source_override;
#if !SQLCTX_NEW_IDENTIFIER_API
    if (source_override.empty() && statement->stmt_location < source.size()) {
        source = source.substr(statement->stmt_location, statement->stmt_length ? statement->stmt_length : string::npos);
    }
#endif
    if (statement->type == StatementType::EXPLAIN_STATEMENT) {
        auto &explain = statement->Cast<ExplainStatement>();
        if (explain.stmt->type == StatementType::EXTENSION_STATEMENT) {
            auto &extension = explain.stmt->Cast<ExtensionStatement>();
            if (extension.extension.plan_function == sqlctx_plan) {
                auto &data = static_cast<SqlctxParseData &>(*extension.parse_data);
                if (data.statement) {
                    explicit_semantic |= data.explicit_semantic;
                    auto inner = std::move(data.statement);
                    explain.stmt = std::move(inner);
                }
            }
        }
    } else if (statement->type == StatementType::PREPARE_STATEMENT) {
        auto &prepare = statement->Cast<PrepareStatement>();
        prepare.statement = WrapSqlctxQuery(std::move(prepare.statement), explicit_semantic, source, parser_options);
        return statement;
    }
    // Ordinary queries retain their native statement type. Routing happens
    // after parsing, including when another parser extension accepted them.
    if (!explicit_semantic) {
        if (auto query = QueryInStatement(*statement, true)) {
            auto options = parser_options ? *parser_options : SqlctxBuiltinParserOptions();
            WrapQueryCarrier(*query, source, options);
        }
        return statement;
    }
    if (QueryInStatement(*statement)) {
        auto data = make_uniq<SqlctxParseData>();
        data->statement = std::move(statement);
        data->explicit_semantic = explicit_semantic;
        return SqlctxStatement(std::move(data));
    } else if (explicit_semantic && statement->type != StatementType::EXTENSION_STATEMENT) {
        throw ParserException("SEMANTIC expects SELECT, CREATE VIEW, CREATE TABLE AS, INSERT SELECT or a semantic definition");
    }
    return statement;
}

static bool YardstickOwnsSyntax(ParserExtensionInfo *info, const string &query) {
    auto parser_info = dynamic_cast<SqlctxParserInfo *>(info);
    if (!parser_info || !ExtensionManager::Get(parser_info->database).ExtensionIsLoaded("yardstick")) return false;
    auto tokens = Tokens(query);
    // SQLCtx declarations and explicit semantic queries remain ours, even if a
    // definition contains an expression whose function is named aggregate.
    if (Keyword(tokens, 0, "SEMANTIC") || Keyword(tokens, 0, "MODEL") || Keyword(tokens, 0, "METRIC") ||
        Keyword(tokens, 0, "DIMENSION") || Keyword(tokens, 0, "SEGMENT")) return false;
    idx_t create_kind = Keyword(tokens, 1, "OR") && Keyword(tokens, 2, "REPLACE") ? 3 : 1;
    if (Keyword(tokens, 0, "CREATE") && Keyword(tokens, create_kind, "SEMANTIC")) return false;
    for (idx_t i = 0; i + 1 < tokens.size(); ++i) {
        if (Keyword(tokens, i, "AS") && Keyword(tokens, i + 1, "MEASURE")) return true;
        if (!Keyword(tokens, i, "AGGREGATE") || tokens[i + 1].text != "(") continue;
        // DuckDB's aggregate(list, name, ...) is ordinary SQL. A measure call
        // has one argument; commas inside nested expressions are not arguments.
        idx_t depth = 1;
        bool multiple_arguments = false;
        for (idx_t argument = i + 2; argument < tokens.size(); ++argument) {
            auto &token = tokens[argument].text;
            if (token == "(" || token == "[" || token == "{") ++depth;
            else if (token == ")" || token == "]" || token == "}") {
                if (--depth == 0) {
                    if (!multiple_arguments) return true;
                    break;
                }
            } else if (token == "," && depth == 1) multiple_arguments = true;
        }
    }
    return false;
}

ParserOverrideResult sqlctx_parser_override(ParserExtensionInfo *info, const string &query, ParserOptions &options) {
    // A composed PEG grammar shares syntax, not another extension's private
    // transformer state. Let Yardstick establish its capture scope for measure
    // syntax before parsing it. The lexer excludes comments and quoted text.
    if (YardstickOwnsSyntax(info, query)) return ParserOverrideResult();
    vector<unique_ptr<SQLStatement>> statements;
    if (ParseSqlctxGrammar(info, query, options, statements)) {
        for (auto &statement : statements) statement = WrapSqlctxQuery(std::move(statement), false, "", &options);
    } else {
        ParseCompatibility(query, options, statements);
    }
    return ParserOverrideResult(std::move(statements));
}

#if SQLCTX_TOKEN_PARSE_FN
ParserExtensionParseResult sqlctx_parse(ParserExtensionInfo *, const vector<SimpleToken> &) {
    // All custom forms, including SEMANTIC, are handled by the full-source
    // override. Never reconstruct source from a post-failure token tail.
    return ParserExtensionParseResult();
}
#else
ParserExtensionParseResult sqlctx_parse(ParserExtensionInfo *, const string &query) {
    auto definition = ParseSqlctxDefinition(query);
    if (definition) return ParserExtensionParseResult(std::move(definition));
    return ParserExtensionParseResult();
}
#endif

ParserExtensionPlanResult sqlctx_plan(ParserExtensionInfo *, ClientContext &context,
                                         unique_ptr<ParserExtensionParseData> parsed) {
    auto &data = static_cast<SqlctxParseData &>(*parsed);
    if (data.operation == "export") return PlanSqlctxCatalog("export", "", "");
    if (data.operation == "show_compatible_dimensions") return PlanSqlctxCatalog("dimension", "", data.content);
    if (StringUtil::StartsWith(data.operation, "show_")) {
        return PlanSqlctxCatalog(data.operation.substr(5), data.content, "");
    }
    if (!data.operation.empty()) {
        return PlanSqlctxMutation(context, data.operation, data.content, data.replace);
    }
    context.registered_state->Remove("sqlctx_bind");
    context.registered_state->Insert("sqlctx_bind", make_shared_ptr<SqlctxBindState>(std::move(parsed)));
    // DuckDB's operator extension binds the original or rewritten AST using the
    // same ClientContext, preserving temporary tables, parameters and snapshots.
    throw BinderException("Use sqlctx_bind instead");
}

BoundStatement sqlctx_bind(ClientContext &context, Binder &binder, OperatorExtensionInfo *, SQLStatement &statement) {
    auto routing = context.registered_state->Get<SqlctxRoutingState>(ROUTING_STATE);
    if (routing && routing->probe_error) return {};
    if (routing && routing->candidate) return routing->BindCandidate(binder);
    if (statement.type != StatementType::EXTENSION_STATEMENT) return {};
    auto &extension = statement.Cast<ExtensionStatement>();
    if (extension.extension.plan_function != sqlctx_plan) return {};
    auto state = context.registered_state->Get<SqlctxBindState>("sqlctx_bind");
    // Definition and discovery statements bind directly through their plan
    // function. Only query wrappers leave deferred bind data; preserve any
    // genuine planning error from the other statement forms.
    if (!state || !state->data) return {};
    // Take ownership before a nested bind can replace the registered state.
    auto parsed = std::move(state->data);
    auto &data = static_cast<SqlctxParseData &>(*parsed);
    if (!data.statement) throw InternalException("sqlctx query statement is missing");
    RegisterSqlctxCatalogRead(context, binder.GetStatementProperties());
    auto snapshot = ReadSqlctxCatalog(context);
    auto original = CompileSemanticStatement(context, *data.statement, snapshot, data.explicit_semantic);
    if (!original) original = data.statement->Copy();
    auto child_binder = Binder::CreateBinder(context, &binder);
    return child_binder->Bind(*original);
}

unique_ptr<LogicalExtensionOperator> SqlctxOperatorExtension::Deserialize(Deserializer &) {
    throw InternalException("sqlctx operator should not be serialized");
}

} // namespace duckdb
