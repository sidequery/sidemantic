#include "sidemantic_parser.hpp"
#include "sidemantic_catalog.hpp"
#include "sidemantic_api.hpp"
#include "sidemantic.h"

#include "duckdb/common/error_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
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
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/planner/bound_parameter_map.hpp"
#include "duckdb/planner/planner_extension.hpp"
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
                return WrapSidemanticQuery(std::move(statement));
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
#if SIDEMANTIC_INSERT_QUERY_NODE
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
            wrapper->from_table = make_uniq<SubqueryRef>(std::move(inner), "__sidemantic_insert_source");
            query->node = std::move(wrapper);
        }
        return query.get();
    }
    return nullptr;
}

unique_ptr<SQLStatement> CompileSemanticStatement(SQLStatement &statement,
                                                 const SidemanticCatalogSnapshot &snapshot,
                                                 bool explicit_semantic = false) {
    auto rewritten = statement.Copy();
    auto query = QueryInStatement(*rewritten, true);
    if (!query) return nullptr;
    ModelListOwner models(snapshot.payload);
    if (!SemanticReferences(models.list).Query(*query->node) && !explicit_semantic) return nullptr;
    auto sql = query->ToString();
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
    string compiled(result.sql);
    sidemantic_free_result(result);
    Parser parser(SidemanticBuiltinParserOptions());
    parser.ParseQuery(compiled);
    if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
        throw BinderException("Sidemantic rewrite must produce one SELECT statement");
    }
    query->node = std::move(parser.statements[0]->Cast<SelectStatement>().node);
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
#if SIDEMANTIC_NEW_IDENTIFIER_API
    if (show.show_type != ShowType::SHOW || !show.GetCatalogName().empty() || !show.GetSchemaName().empty()) {
        return nullptr;
    }
    auto name = SidemanticName(show.GetTableName());
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
        auto data = make_uniq<SidemanticParseData>();
        data->operation = "show_" + string(plural);
        data->operation.pop_back();
        return SidemanticStatement(std::move(data));
    }
    return nullptr;
}

constexpr const char *ROUTING_STATE = "sidemantic_native_routing";
constexpr const char *ROUTING_PROBE = "sidemantic_pristine_statement";

// DuckDB's native binder consumes expressions as it visits them. The client
// rebind API protects a pristine statement copy, unlike OperatorExtension's
// failure callback. A private post-bind signal also reaches that API when a
// same-name physical table made the first native bind succeed.
class SidemanticRoutingState : public ClientContextState {
public:
    explicit SidemanticRoutingState(ClientContext &context) : context(context) {}

    bool CanRequestRebind() override {
        candidate.reset();
        probe_error = false;
        probing = false;
        has_models = false;
        catalog_error = ErrorData();
        snapshot = {};
        // ROLLBACK must remain bindable after a transaction error. Do not read
        // the semantic catalog while DuckDB's transaction is invalidated.
        if (!context.transaction.HasActiveTransaction() || ValidChecker::IsInvalidated(context.ActiveTransaction())) {
            return false;
        }
        probing = true;
        try {
            snapshot = ReadSidemanticCatalog(context);
            if (!snapshot.payload.empty()) {
                ModelListOwner models(snapshot.payload);
                has_models = models.list.count != 0;
            }
        } catch (const std::exception &exception) {
            ErrorData error(exception);
            switch (error.Type()) {
            case ExceptionType::INVALID_INPUT:
            case ExceptionType::PARSER:
            case ExceptionType::IO:
            case ExceptionType::PERMISSION:
                // Catalog discovery and explicit semantic statements perform
                // their own read. Ordinary SQL and extension introspection
                // must remain usable even if legacy definitions are invalid.
                catalog_error = std::move(error);
                snapshot = {};
                break;
            default:
                throw;
            }
        }
        // With no models, normal SQL binds once. Keeping a pristine copy still
        // lets a native SHOW failure reach our catalog discovery statements.
        return true;
    }

    RebindQueryInfo OnPlanningError(ClientContext &, SQLStatement &statement, ErrorData &error) override {
        if (!probing) return RebindQueryInfo::DO_NOT_REBIND;
        bool own_probe = probe_error && error.ExtraInfo().count(ROUTING_PROBE);
        probing = false;
        probe_error = false;
        if (!own_probe && error.Type() != ExceptionType::BINDER && error.Type() != ExceptionType::CATALOG) {
            return RebindQueryInfo::DO_NOT_REBIND;
        }
        candidate = Candidate(statement, !own_probe);
        return candidate || own_probe ? RebindQueryInfo::ATTEMPT_TO_REBIND : RebindQueryInfo::DO_NOT_REBIND;
    }

    RebindQueryInfo OnRebindPreparedStatement(ClientContext &, BindPreparedStatementCallbackInfo &info,
                                              RebindQueryInfo) override {
        // SQL EXECUTE uses a nested Planner instead of the client preparation
        // API. Its callback supplies the original prepared AST before binding.
        if (!info.prepared_statement.unbound_statement) return RebindQueryInfo::DO_NOT_REBIND;
        auto prepared = Candidate(*info.prepared_statement.unbound_statement);
        if (!prepared) return RebindQueryInfo::DO_NOT_REBIND;
        probing = false;
        probe_error = false;
        candidate = std::move(prepared);
        return RebindQueryInfo::ATTEMPT_TO_REBIND;
    }

    RebindQueryInfo OnFinalizePrepare(ClientContext &, PreparedStatementData &, PreparedStatementMode) override {
        // C/API Prepare does not necessarily end a query. Disarm successful
        // probes here too, so a later direct ExtractPlan never sees our signal.
        probing = false;
        probe_error = false;
        return RebindQueryInfo::DO_NOT_REBIND;
    }

    void QueryEnd() override {
        candidate.reset();
        probing = false;
        probe_error = false;
    }

    BoundStatement BindCandidate(Binder &binder) {
        // Consume before binding: PREPARE and EXECUTE have nested planners,
        // while our rewritten query may itself contain subqueries and views.
        auto statement = std::move(candidate);
        probing = false;
        // The discarded native plan may have inferred parameter types from
        // same-name physical columns. Infer them again from the semantic plan,
        // retaining only the values/types explicitly supplied by the caller.
        if (auto parameters = binder.GetParameters()) {
            parameters->GetParametersPtr()->clear();
            parameters->rebind = false;
        }
        binder.GetStatementProperties() = StatementProperties();
        RegisterSidemanticCatalogRead(context, binder.GetStatementProperties());
        auto child = Binder::CreateBinder(context, &binder);
        return child->Bind(*statement);
    }

    bool probing = false;
    bool probe_error = false;
    bool has_models = false;
    unique_ptr<SQLStatement> candidate;

private:
    unique_ptr<SQLStatement> Candidate(SQLStatement &statement, bool native_failed = false) {
        if (statement.type == StatementType::PREPARE_STATEMENT) {
            return Candidate(*statement.Cast<PrepareStatement>().statement, native_failed);
        }
        if (auto show = SemanticShowStatement(statement)) return show;
        // A native query that already succeeded needs no recovery. If native
        // binding failed, expose the real catalog error instead of pretending
        // that semantic definitions were absent or returning a partial rewrite.
        if (native_failed && catalog_error.HasError() && QueryInStatement(statement)) catalog_error.Throw();
        if (!has_models) return nullptr;
        return CompileSemanticStatement(statement, snapshot);
    }

    ClientContext &context;
    SidemanticCatalogSnapshot snapshot;
    ErrorData catalog_error;
};

void SidemanticPostBind(PlannerExtensionInput &input, BoundStatement &bound) {
    auto state = input.context.registered_state->Get<SidemanticRoutingState>(ROUTING_STATE);
    if (!state) return;
    if (state->candidate) {
        bound = state->BindCandidate(input.binder);
        return;
    }
    if (!state->probing || state->probe_error) return;
    // SHOW of an existing physical table also binds successfully. Probe its
    // small fixed description shape even in a database with no semantic models.
    bool description = bound.names.size() >= 2 && bound.names[0] == "column_name" && bound.names[1] == "column_type";
    if (!state->has_models && !description) return;
    state->probe_error = true;
    // INVALID bypasses operator-extension error recovery in both supported
    // planners. Only the client rebind callback should consume this signal.
    throw Exception(unordered_map<string, string> {{ROUTING_PROBE, "true"}}, ExceptionType::INVALID,
                    "Sidemantic requires the pristine statement for semantic binding");
}

class SidemanticConnectionCallback : public ExtensionCallback {
public:
    void OnConnectionOpened(ClientContext &context) override {
        context.registered_state->GetOrCreate<SidemanticRoutingState>(ROUTING_STATE, context);
    }
};

} // namespace

void RegisterSidemanticRouting(DatabaseInstance &db) {
    auto &config = DBConfig::GetConfig(db);
    auto callback = make_shared_ptr<SidemanticConnectionCallback>();
    ExtensionCallback::Register(config, callback);
    // Register future connections before visiting existing ones. GetOrCreate
    // makes connections opened concurrently with LOAD harmless duplicates.
    for (auto &context : ConnectionManager::Get(db).GetConnectionList()) callback->OnConnectionOpened(*context);
    PlannerExtension planner;
    planner.post_bind_function = SidemanticPostBind;
    PlannerExtension::Register(config, planner);
}

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
        auto result = make_uniq<SidemanticParseData>();
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
        auto result = make_uniq<SidemanticParseData>();
        result->operation = "show_";
        result->content = IdentifierText(tokens[pos + 2].text);
        return result;
    }
    if ((Keyword(tokens, pos, "EXPORT") || Keyword(tokens, pos, "IMPORT")) &&
        Keyword(tokens, pos + 1, "SEMANTIC") && Keyword(tokens, pos + 2, "CATALOG")) {
        bool importing = Keyword(tokens, pos, "IMPORT");
        auto result = make_uniq<SidemanticParseData>();
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
        if (!(Keyword(tokens, pos, "MODEL") || Keyword(tokens, pos, "METRIC") ||
              Keyword(tokens, pos, "DIMENSION") || Keyword(tokens, pos, "SEGMENT"))) return nullptr;
        auto result = make_uniq<SidemanticParseData>();
        result->operation = "drop_" + StringUtil::Lower(tokens[pos++].text);
        if (Keyword(tokens, pos, "IF") && Keyword(tokens, pos + 1, "EXISTS")) {
            result->replace = true;
            pos += 2;
        }
        result->content = QualifiedNameJson(tokens, pos, result->operation == "drop_model" ? 1 : 2);
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
    } else if (statement->type == StatementType::PREPARE_STATEMENT) {
        auto &prepare = statement->Cast<PrepareStatement>();
        prepare.statement = WrapSidemanticQuery(std::move(prepare.statement), explicit_semantic);
        return statement;
    }
    // Ordinary queries retain their native statement type. Routing happens
    // after parsing, including when another parser extension accepted them.
    if (!explicit_semantic) return statement;
    if (QueryInStatement(*statement)) {
        auto data = make_uniq<SidemanticParseData>();
        data->statement = std::move(statement);
        data->explicit_semantic = explicit_semantic;
        return SidemanticStatement(std::move(data));
    } else if (explicit_semantic && statement->type != StatementType::EXTENSION_STATEMENT) {
        throw ParserException("SEMANTIC expects SELECT, CREATE VIEW, CREATE TABLE AS, INSERT SELECT or a semantic definition");
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
    if (data.operation == "export") return PlanSidemanticCatalog("export", "", "");
    if (data.operation == "show_compatible_dimensions") return PlanSidemanticCatalog("dimension", "", data.content);
    if (StringUtil::StartsWith(data.operation, "show_")) {
        return PlanSidemanticCatalog(data.operation.substr(5), data.content, "");
    }
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
    auto routing = context.registered_state->Get<SidemanticRoutingState>(ROUTING_STATE);
    if (routing && routing->probe_error) return {};
    if (routing && routing->candidate) return routing->BindCandidate(binder);
    if (statement.type != StatementType::EXTENSION_STATEMENT) return {};
    auto &extension = statement.Cast<ExtensionStatement>();
    if (extension.extension.plan_function != sidemantic_plan) return {};
    auto state = context.registered_state->Get<SidemanticBindState>("sidemantic_bind");
    // Definition and discovery statements bind directly through their plan
    // function. Only query wrappers leave deferred bind data; preserve any
    // genuine planning error from the other statement forms.
    if (!state || !state->data) return {};
    // Take ownership before a nested bind can replace the registered state.
    auto parsed = std::move(state->data);
    auto &data = static_cast<SidemanticParseData &>(*parsed);
    if (!data.statement) throw InternalException("Sidemantic query statement is missing");
    if (routing) routing->probing = false;
    RegisterSidemanticCatalogRead(context, binder.GetStatementProperties());
    auto snapshot = ReadSidemanticCatalog(context);
    auto original = CompileSemanticStatement(*data.statement, snapshot, data.explicit_semantic);
    if (!original) original = data.statement->Copy();
    auto child_binder = Binder::CreateBinder(context, &binder);
    return child_binder->Bind(*original);
}

unique_ptr<LogicalExtensionOperator> SidemanticOperatorExtension::Deserialize(Deserializer &) {
    throw InternalException("Sidemantic operator should not be serialized");
}

} // namespace duckdb
