#include "sidemantic_parser.hpp"
#include "sidemantic_catalog.hpp"
#include "sidemantic.h"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

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

string Literal(const string &text) {
    return "'" + StringUtil::Replace(text, "'", "''") + "'";
}

class SidemanticSQLStatement : public ExtensionStatement {
public:
    explicit SidemanticSQLStatement(unique_ptr<ParserExtensionParseData> data)
        : ExtensionStatement(SidemanticParserExtension(), std::move(data)) {}

    unique_ptr<SQLStatement> Copy() const override {
        auto result = make_uniq<SidemanticSQLStatement>(parse_data->Copy());
        result->named_param_map = named_param_map;
        result->query = query;
        result->stmt_location = stmt_location;
#if SIDEMANTIC_NEW_IDENTIFIER_API
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

// The compatibility frontend only recognizes Sidemantic declarations. Ordinary
// SQL, including PREPARE/EXPLAIN syntax, is still parsed by DuckDB.
unique_ptr<SQLStatement> ParseCompatibilityStatement(const string &sql, const ParserOptions &options) {
    auto tokens = Tokens(sql);
    if (tokens.empty()) {
        return nullptr;
    }
    auto definition = ParseSidemanticDefinition(sql);
    if (definition) {
        return SidemanticStatement(std::move(definition));
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
            if (ParseSidemanticDefinition(nested_sql) || Keyword(tokens, nested, "SEMANTIC")) {
                auto child = ParseCompatibilityStatement(nested_sql, options);
                Parser parser(WithoutOverrides(options));
                parser.ParseQuery(sql.substr(0, tokens[nested].start) + "SELECT 1");
                auto statement = std::move(parser.statements.at(0));
                if (statement->type == StatementType::EXPLAIN_STATEMENT) {
                    statement->Cast<ExplainStatement>().stmt = std::move(child);
                } else {
                    statement->Cast<PrepareStatement>().statement = std::move(child);
                }
                return statement;
            }
        }
    }
    Parser parser(WithoutOverrides(options));
    parser.ParseQuery(input);
    if (parser.statements.size() != 1) {
        throw ParserException("Expected one Sidemantic statement");
    }
    return WrapSidemanticQuery(std::move(parser.statements[0]), explicit_semantic);
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
    explicit ModelListOwner(const string &snapshot) : list(sidemantic_snapshot_list_models(snapshot.c_str())) {
        if (list.error) {
            string error(list.error);
            sidemantic_free_model_list(list);
            throw InvalidInputException("Sidemantic catalog: %s", error);
        }
    }
    ~ModelListOwner() { sidemantic_free_model_list(list); }
    SidemanticModelList list;
};

// A null binding shadows a semantic model name (e.g. a CTE or a physical table
// aliased to that name). Each query has its own scope, including nested SELECTs.
using ModelScope = case_insensitive_map_t<const SidemanticModelInfo *>;
using CteNames = case_insensitive_set_t;

class SemanticReferences {
public:
    explicit SemanticReferences(const SidemanticModelList &list) {
        for (idx_t i = 0; i < list.count; ++i) {
            models[list.models[i].name] = &list.models[i];
        }
    }

    bool Query(QueryNode &node, ModelScope inherited = {}, CteNames ctes = {}) {
        bool found = false;
        for (auto &cte : node.cte_map.map) {
#if SIDEMANTIC_NEW_IDENTIFIER_API
            found |= Query(*cte.second->query_node, inherited, ctes);
#else
            found |= Query(*cte.second->query->node, inherited, ctes);
#endif
            ctes.insert(SidemanticName(cte.first));
        }
        if (node.type == QueryNodeType::SET_OPERATION_NODE) {
            for (auto &child : node.Cast<SetOperationNode>().children) found |= Query(*child, inherited, ctes);
            return found;
        }
        if (node.type == QueryNodeType::RECURSIVE_CTE_NODE) {
            auto &recursive = node.Cast<RecursiveCTENode>();
            ctes.insert(SidemanticName(recursive.ctename));
            return Query(*recursive.left, inherited, ctes) | Query(*recursive.right, inherited, ctes) | found;
        }
        if (node.type == QueryNodeType::CTE_NODE) {
            auto &cte = node.Cast<CTENode>();
            found |= Query(*cte.query, inherited, ctes);
            ctes.insert(SidemanticName(cte.ctename));
            return Query(*cte.child, inherited, ctes) | found;
        }
        if (node.type != QueryNodeType::SELECT_NODE) return found;
        auto &select = node.Cast<SelectNode>();
        auto scope = std::move(inherited);
        for (auto &name : ctes) scope[name] = nullptr;
        if (select.from_table) CollectTables(*select.from_table, scope, ctes, found);
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
        return found;
    }

private:
    ModelScope models;

    bool CanonicalField(const SidemanticModelInfo *model, string &name) {
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

    void CollectTables(TableRef &ref, ModelScope &scope, const CteNames &ctes, bool &found) {
        if (ref.type == TableReferenceType::JOIN) {
            auto &join = ref.Cast<JoinRef>();
            CollectTables(*join.left, scope, ctes, found);
            CollectTables(*join.right, scope, ctes, found);
            return;
        }
        if (ref.type == TableReferenceType::SUBQUERY) {
            found |= Query(*ref.Cast<SubqueryRef>().subquery->node, scope, ctes);
        }
        const SidemanticModelInfo *model = nullptr;
        string name;
        if (ref.type == TableReferenceType::BASE_TABLE) {
            auto &table = ref.Cast<BaseTableRef>();
#if SIDEMANTIC_QUALIFIED_TABLE_API
            name = SidemanticName(table.Table());
            bool qualified = !table.GetQualifiedName().Schema().empty() || !table.GetQualifiedName().Catalog().empty();
#else
            name = table.table_name;
            bool qualified = !table.schema_name.empty() || !table.catalog_name.empty();
#endif
            auto entry = models.find(name);
            if (!qualified && !ctes.count(name) && entry != models.end()) model = entry->second;
            if (model) {
                name = model->name;
#if SIDEMANTIC_QUALIFIED_TABLE_API
                table.SetTable(Identifier(name));
#else
                table.table_name = name;
#endif
            }
        }
        auto alias = ref.alias.empty() ? name : SidemanticName(ref.alias);
        if (!alias.empty()) scope[alias] = model;
    }

    bool TableExpressions(TableRef &ref, const ModelScope &scope, const CteNames &ctes) {
        if (ref.type != TableReferenceType::JOIN) return false;
        auto &join = ref.Cast<JoinRef>();
        bool found = join.condition && Expression(*join.condition, scope, ctes);
        return TableExpressions(*join.left, scope, ctes) | TableExpressions(*join.right, scope, ctes) | found;
    }

    bool Expression(ParsedExpression &expression, const ModelScope &scope, const CteNames &ctes) {
        if (expression.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
            auto &column = expression.Cast<ColumnRefExpression>();
#if SIDEMANTIC_NEW_EXPRESSION_API
            auto &names = column.ColumnNamesMutable();
#else
            auto &names = column.column_names;
#endif
            if (names.size() == 2) {
                auto qualifier = SidemanticName(names[0]);
                auto local = scope.find(qualifier);
                const SidemanticModelInfo *model = nullptr;
                if (local != scope.end()) {
                    model = local->second;
                    qualifier = local->first;
                } else {
                    auto entry = models.find(qualifier);
                    if (entry != models.end()) {
                        model = entry->second;
                        qualifier = entry->first;
                    }
                }
                if (!model) return false;
                names[0] = SidemanticIdentifier(qualifier);
                auto field = SidemanticName(names[1]);
                bool found = CanonicalField(model, field);
                if (found) names[1] = SidemanticIdentifier(field);
                return found;
            }
            if (names.size() == 1) {
                for (auto &binding : scope) {
                    auto field = SidemanticName(names[0]);
                    if (CanonicalField(binding.second, field)) {
                        names[0] = SidemanticIdentifier(field);
                        return true;
                    }
                }
            }
            return false;
        }
        bool found = false;
        if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY) {
#if SIDEMANTIC_NEW_EXPRESSION_API
            auto &query = expression.Cast<SubqueryExpression>().Subquery();
#else
            auto &query = expression.Cast<SubqueryExpression>().subquery;
#endif
            found |= Query(*query->node, scope, ctes);
        }
        ParsedExpressionIterator::EnumerateChildren(expression, [&](ParsedExpression &child) {
            found |= Expression(child, scope, ctes);
        });
        return found;
    }
};

class SidemanticBindState : public ClientContextState {
public:
    explicit SidemanticBindState(unique_ptr<ParserExtensionParseData> data) : data(std::move(data)) {}
    void QueryEnd() override { data.reset(); }
    unique_ptr<ParserExtensionParseData> data;
};

} // namespace

unique_ptr<ParserExtensionParseData> SidemanticParseData::Copy() const {
    auto copy = make_uniq<SidemanticParseData>();
    copy->operation = operation;
    copy->content = content;
    copy->replace = replace;
    copy->explicit_semantic = explicit_semantic;
    if (statement) copy->statement = statement->Copy();
    return std::move(copy);
}

string SidemanticParseData::ToString() const {
    return statement ? statement->ToString() : content;
}

unique_ptr<SQLStatement> SidemanticStatement(unique_ptr<SidemanticParseData> data) {
    auto result = make_uniq<SidemanticSQLStatement>(std::move(data));
    auto &parsed = static_cast<SidemanticParseData &>(*result->parse_data);
    if (parsed.statement) {
        result->named_param_map = parsed.statement->named_param_map;
        result->query = parsed.statement->query;
        result->stmt_location = parsed.statement->stmt_location;
#if SIDEMANTIC_NEW_IDENTIFIER_API
        result->has_anonymous_parameters = parsed.statement->has_anonymous_parameters;
#else
        result->stmt_length = parsed.statement->stmt_length;
#endif
    }
    return std::move(result);
}

unique_ptr<SidemanticParseData> ParseSidemanticDefinition(const string &sql) {
    string clean;
    auto tokens = Tokens(sql, &clean);
    while (!tokens.empty() && tokens.back().text == ";") tokens.pop_back();
    idx_t pos = Keyword(tokens, 0, "SEMANTIC") ? 1 : 0;
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
    bool model = Keyword(tokens, pos, "MODEL");
    bool item = Keyword(tokens, pos, "METRIC") || Keyword(tokens, pos, "DIMENSION") || Keyword(tokens, pos, "SEGMENT");
    if (!model && !item) return nullptr;
    auto result = make_uniq<SidemanticParseData>();
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
            result->content = "MODEL (name " + Literal(name) + separator + rest;
        }
    } else if (model && create) {
        result->content = "MODEL " + clean.substr(tokens[pos].end, tokens.back().end - tokens[pos].end);
    }
    return result;
}

unique_ptr<SQLStatement> WrapSidemanticQuery(unique_ptr<SQLStatement> statement, bool explicit_semantic) {
    if (statement->type == StatementType::EXPLAIN_STATEMENT) {
        auto &explain = statement->Cast<ExplainStatement>();
        if (explain.stmt->type == StatementType::EXTENSION_STATEMENT) {
            auto &extension = explain.stmt->Cast<ExtensionStatement>();
            if (extension.extension.plan_function == sidemantic_plan) {
                auto &data = static_cast<SidemanticParseData &>(*extension.parse_data);
                if (data.statement) {
                    explicit_semantic |= data.explicit_semantic;
                    auto inner = std::move(data.statement);
                    explain.stmt = std::move(inner);
                }
            }
        }
        if (explain.stmt->type == StatementType::SELECT_STATEMENT) {
            // Operator-extension fallback occurs at the outer planner boundary.
            // Keep EXPLAIN outside the rewritten SELECT when binding the result.
            auto data = make_uniq<SidemanticParseData>();
            data->statement = std::move(statement);
            data->explicit_semantic = explicit_semantic;
            return SidemanticStatement(std::move(data));
        }
    } else if (statement->type == StatementType::PREPARE_STATEMENT) {
        auto &prepare = statement->Cast<PrepareStatement>();
        prepare.statement = WrapSidemanticQuery(std::move(prepare.statement), explicit_semantic);
    } else if (statement->type == StatementType::SELECT_STATEMENT) {
        auto data = make_uniq<SidemanticParseData>();
        data->statement = std::move(statement);
        data->explicit_semantic = explicit_semantic;
        return SidemanticStatement(std::move(data));
    } else if (explicit_semantic && statement->type != StatementType::EXTENSION_STATEMENT) {
        throw ParserException("SEMANTIC expects a SELECT or a semantic definition");
    }
    return statement;
}

ParserOverrideResult sidemantic_parser_override(ParserExtensionInfo *info, const string &query, ParserOptions &options) {
    vector<unique_ptr<SQLStatement>> statements;
    if (ParseSidemanticGrammar(info, query, options, statements)) {
        for (auto &statement : statements) statement = WrapSidemanticQuery(std::move(statement));
    } else {
        ParseCompatibility(query, options, statements);
    }
    return ParserOverrideResult(std::move(statements));
}

#if SIDEMANTIC_TOKEN_PARSE_FN
ParserExtensionParseResult sidemantic_parse(ParserExtensionInfo *, const vector<SimpleToken> &) {
    // All custom forms, including SEMANTIC, are handled by the full-source
    // override. Never reconstruct source from a post-failure token tail.
    return ParserExtensionParseResult();
}
#else
ParserExtensionParseResult sidemantic_parse(ParserExtensionInfo *, const string &query) {
    auto definition = ParseSidemanticDefinition(query);
    if (definition) return ParserExtensionParseResult(std::move(definition));
    return ParserExtensionParseResult();
}
#endif

ParserExtensionPlanResult sidemantic_plan(ParserExtensionInfo *, ClientContext &context,
                                         unique_ptr<ParserExtensionParseData> parsed) {
    auto &data = static_cast<SidemanticParseData &>(*parsed);
    if (!data.operation.empty()) {
        return PlanSidemanticMutation(context, data.operation, data.content, data.replace, "Semantic definitions updated");
    }
    context.registered_state->Remove("sidemantic_bind");
    context.registered_state->Insert("sidemantic_bind", make_shared_ptr<SidemanticBindState>(std::move(parsed)));
    // DuckDB's operator extension binds the original or rewritten AST using the
    // same ClientContext, preserving temporary tables, parameters and snapshots.
    throw BinderException("Use sidemantic_bind instead");
}

BoundStatement sidemantic_bind(ClientContext &context, Binder &binder, OperatorExtensionInfo *, SQLStatement &statement) {
    if (statement.type != StatementType::EXTENSION_STATEMENT) return {};
    auto &extension = statement.Cast<ExtensionStatement>();
    if (extension.extension.plan_function != sidemantic_plan) return {};
    auto state = context.registered_state->Get<SidemanticBindState>("sidemantic_bind");
    if (!state || !state->data) throw InternalException("Sidemantic query bind state is missing");
    // Take ownership before a nested bind can replace the registered state.
    auto parsed = std::move(state->data);
    auto &data = static_cast<SidemanticParseData &>(*parsed);
    if (!data.statement) throw InternalException("Sidemantic query statement is missing");
    RegisterSidemanticCatalogRead(context, binder.GetStatementProperties());
    auto snapshot = ReadSidemanticCatalog(context);
    ModelListOwner models(snapshot.payload);
    auto original = data.statement->Copy();
    auto query = &original;
    if (original->type == StatementType::EXPLAIN_STATEMENT) {
        query = &original->Cast<ExplainStatement>().stmt;
    }
    if ((*query)->type != StatementType::SELECT_STATEMENT) {
        throw InternalException("Sidemantic expected a SELECT statement");
    }
    // DuckDB identifiers are case-insensitive even when quoted. Normalize only
    // semantic bindings to the declared spelling before passing them to Rust.
    bool detected = SemanticReferences(models.list).Query(*(*query)->Cast<SelectStatement>().node);
    bool semantic = data.explicit_semantic || detected;
    if (semantic) {
        auto sql = (*query)->ToString();
        auto result = sidemantic_snapshot_rewrite(snapshot.payload.c_str(), sql.c_str());
        if (result.error) {
            string error(result.error);
            sidemantic_free_result(result);
            throw BinderException("Sidemantic: %s", error);
        }
        if (!result.sql) {
            sidemantic_free_result(result);
            throw InternalException("Sidemantic returned no rewritten SQL");
        }
        string rewritten(result.sql);
        sidemantic_free_result(result);
        Parser parser(SidemanticBuiltinParserOptions());
        parser.ParseQuery(rewritten);
        if (parser.statements.size() != 1) throw BinderException("Sidemantic rewrite must produce one statement");
        *query = std::move(parser.statements[0]);
    }
    auto child_binder = Binder::CreateBinder(context, &binder);
    return child_binder->Bind(*original);
}

unique_ptr<LogicalExtensionOperator> SidemanticOperatorExtension::Deserialize(Deserializer &) {
    throw InternalException("Sidemantic operator should not be serialized");
}

} // namespace duckdb
