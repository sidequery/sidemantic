#include "sqlctx_catalog.hpp"
#include "sqlctx_compat.hpp"
#include "sqlctx.h"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include <algorithm>
#include <map>

namespace duckdb {

static constexpr const char *CATALOG_MACRO = "__sqlctx_catalog";
static constexpr const char *LEGACY_CATALOG_MACRO = "__sidemantic_catalog";
static constexpr const char *SESSION_STATE = "sqlctx_catalog_session";

// Only session preferences live outside DuckDB's catalog. Capture their values
// lazily: the first semantic statement may arrive after BEGIN already ran.
class SqlctxCatalogSession : public ClientContextState {
public:
    unordered_map<idx_t, string> active_models;
    unordered_map<idx_t, string> previous_active_models;
    unordered_set<idx_t> previously_unset;

    void SetActive(idx_t catalog_oid, string model) {
        if (!previous_active_models.count(catalog_oid) && !previously_unset.count(catalog_oid)) {
            auto current = active_models.find(catalog_oid);
            if (current == active_models.end()) {
                previously_unset.insert(catalog_oid);
            } else {
                previous_active_models.emplace(catalog_oid, current->second);
            }
        }
        active_models[catalog_oid] = std::move(model);
    }

    void TransactionCommit(MetaTransaction &, ClientContext &) override {
        previous_active_models.clear();
        previously_unset.clear();
    }

    void TransactionRollback(MetaTransaction &, ClientContext &) override {
        for (auto &entry : previous_active_models) {
            active_models[entry.first] = std::move(entry.second);
        }
        for (auto oid : previously_unset) {
            active_models.erase(oid);
        }
        previous_active_models.clear();
        previously_unset.clear();
    }
};

static Catalog &SemanticCatalog(ClientContext &context, const string &name = "") {
    if (name.empty()) return Catalog::GetCatalog(context, DatabaseManager::GetDefaultDatabase(context));
#if SQLCTX_NEW_IDENTIFIER_API
    return Catalog::GetCatalog(context, Identifier(name));
#else
    return Catalog::GetCatalog(context, name);
#endif
}

static shared_ptr<SqlctxCatalogSession> Session(ClientContext &context) {
    return context.registered_state->GetOrCreate<SqlctxCatalogSession>(SESSION_STATE);
}

string SqlctxErrorText(string error) {
    if (StringUtil::StartsWith(error, "Error: ")) error = error.substr(strlen("Error: "));
    return StringUtil::Replace(error, "Validation error: ", "");
}

struct SnapshotResultOwner {
    explicit SnapshotResultOwner(SqlctxSnapshotResult result) : result(result) {
    }
    ~SnapshotResultOwner() {
        sqlctx_free_snapshot_result(result);
    }
    SqlctxSnapshotResult result;

    SqlctxCatalogSnapshot Get() const {
        if (result.error) {
            throw InvalidInputException("sqlctx: %s", SqlctxErrorText(result.error));
        }
        if (!result.snapshot) {
            throw InternalException("sqlctx returned no catalog snapshot");
        }
        return {result.snapshot, result.active_model ? result.active_model : ""};
    }
};

static string ReadDefinitionFile(FileSystem &fs, const string &path) {
    auto file = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
    auto size = file->GetFileSize();
    if (size < 0 || file->GetType() != FileType::FILE_TYPE_REGULAR) {
        throw InvalidInputException("sqlctx definitions require a regular file: %s", path);
    }
    string content(size, '\0');
    if (!content.empty()) file->Read(&content[0], content.size(), 0);
    if (content.find('\0') != string::npos) {
        throw InvalidInputException("sqlctx definitions contain a NUL byte: %s", path);
    }
    return content;
}

static void CollectDefinitionFiles(FileSystem &fs, const string &directory, vector<string> &paths, idx_t depth = 0) {
    // Bound recursive traversal, including directory symlink cycles.
    static constexpr idx_t MAX_DIRECTORY_DEPTH = 64;
    if (depth >= MAX_DIRECTORY_DEPTH) {
        throw InvalidInputException("sqlctx model directory exceeds maximum nesting depth: %s", directory);
    }
    if (!fs.ListFiles(directory, [&](const string &name, bool is_directory) {
        auto path = fs.JoinPath(directory, name);
        if (is_directory) {
            CollectDefinitionFiles(fs, path, paths, depth + 1);
        } else {
            auto lower = StringUtil::Lower(name);
            if (StringUtil::EndsWith(lower, ".yml") || StringUtil::EndsWith(lower, ".yaml") ||
                StringUtil::EndsWith(lower, ".sql")) paths.push_back(std::move(path));
        }
    })) {
        throw IOException("Could not list sqlctx model directory: %s", directory);
    }
}

static SqlctxSnapshotResult ImportDefinitionFiles(ClientContext &context, const string &snapshot,
                                                     const string &path) {
    // The context filesystem enforces enable_external_access, allowed paths
    // and disabled filesystems for every listing and open. Rust receives only
    // captured bytes and cannot bypass the host's file or environment boundary.
    auto &fs = FileSystem::GetFileSystem(context);
    bool directory = fs.DirectoryExists(path);
    vector<string> paths;
    if (directory) {
        CollectDefinitionFiles(fs, path, paths);
        std::sort(paths.begin(), paths.end());
    } else {
        paths.push_back(path);
    }
    vector<string> contents;
    for (auto &source : paths) contents.push_back(ReadDefinitionFile(fs, source));
    vector<SqlctxSource> sources;
    for (idx_t i = 0; i < paths.size(); ++i) {
        sources.push_back({paths[i].c_str(), contents[i].c_str()});
    }
    return sqlctx_snapshot_load_sources(snapshot.c_str(), sources.data(), sources.size(), directory);
}

static SqlctxCatalogSnapshot ReadLegacySnapshot(ClientContext &context, Catalog &catalog) {
    auto &database = catalog.GetAttached();
    if (!database.HasStorageManager() || catalog.InMemory()) {
        return {};
    }
    auto path = database.GetStorageManager().GetDBPath();
    if (path.empty() || path == ":memory:") {
        return {};
    }

    // Match the historical Rust sidecar path: replace the database filename's
    // final extension, leaving directory names (including dots) untouched.
    auto separator = path.find_last_of("/\\");
    auto filename_start = separator == string::npos ? 0 : separator + 1;
    auto extension = path.find_last_of('.');
    if (extension != string::npos && extension > filename_start) {
        path.resize(extension);
    }
    // The context filesystem supplies its opener and access checks itself.
    auto &fs = FileSystem::GetFileSystem(context);
    for (auto suffix : {".sqlctx.sql", ".sidemantic.sql"}) {
        auto sidecar = path + suffix;
        if (fs.FileExists(sidecar)) {
            auto content = ReadDefinitionFile(fs, sidecar);
            SnapshotResultOwner imported(sqlctx_snapshot_apply(nullptr, nullptr, "legacy_sql", content.c_str(), false));
            return imported.Get();
        }
    }
    return {};
}

static optional_ptr<CatalogEntry> FindCatalogMacro(ClientContext &context, Catalog &catalog, const string &macro_name) {
#if SQLCTX_NEW_IDENTIFIER_API
    const QualifiedName qualified_name(catalog.GetName(), Identifier::DefaultSchema(), Identifier(macro_name));
    EntryLookupInfo lookup(CatalogType::MACRO_ENTRY, qualified_name);
    return Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
#else
    EntryLookupInfo lookup(CatalogType::MACRO_ENTRY, macro_name);
    return Catalog::GetEntry(context, catalog.GetName(), DEFAULT_SCHEMA, lookup, OnEntryNotFound::RETURN_NULL);
#endif
}

SqlctxCatalogSnapshot ReadSqlctxCatalog(ClientContext &context, const string &catalog_name) {
    auto &catalog = SemanticCatalog(context, catalog_name);
    auto state = Session(context);
    auto entry = FindCatalogMacro(context, catalog, CATALOG_MACRO);
    if (!entry) {
        entry = FindCatalogMacro(context, catalog, LEGACY_CATALOG_MACRO);
    }
    SqlctxCatalogSnapshot snapshot;
    if (entry) {
        if (entry->type != CatalogType::MACRO_ENTRY) {
            throw InvalidInputException("Invalid sqlctx catalog entry type");
        }
        auto &macro_entry = entry->Cast<ScalarMacroCatalogEntry>();
        if (macro_entry.macros.size() != 1 || macro_entry.macros[0]->type != MacroType::SCALAR_MACRO) {
            throw InvalidInputException("Invalid sqlctx catalog snapshot macro");
        }
        auto &macro = macro_entry.macros[0]->Cast<ScalarMacroFunction>();
        if (!macro.expression || macro.expression->GetExpressionClass() != ExpressionClass::CONSTANT) {
            throw InvalidInputException("Invalid sqlctx catalog snapshot expression");
        }
        auto &constant = macro.expression->Cast<ConstantExpression>();
#if SQLCTX_LITERAL_API
        auto &literal = constant.GetLiteral();
        if (literal.kind != LiteralKind::STRING) {
            throw InvalidInputException("Invalid sqlctx catalog snapshot value");
        }
        snapshot.payload = literal.text;
#else
        auto &value = constant.value;
        if (value.IsNull() || value.type() != LogicalType::VARCHAR) {
            throw InvalidInputException("Invalid sqlctx catalog snapshot value");
        }
        snapshot.payload = value.GetValue<string>();
#endif
    } else {
        snapshot = ReadLegacySnapshot(context, catalog);
    }
    if (snapshot.payload.find('\0') != string::npos) {
        throw InvalidInputException("sqlctx catalog snapshot contains a NUL byte");
    }
    auto active = state->active_models.find(catalog.GetOid());
    if (active != state->active_models.end()) {
        snapshot.active_model = active->second;
    }
    snapshot.catalog = SqlctxName(catalog.GetName());
    return snapshot;
}

vector<string> SqlctxCatalogNames(ClientContext &context) {
    vector<string> names;
    for (auto &database : DatabaseManager::Get(context).GetDatabases(context)) {
        if (database->IsSystem()) continue;
        names.push_back(SqlctxName(database->GetName()));
    }
    std::sort(names.begin(), names.end());
    return names;
}

// Walk lexical CTE scopes rather than treating every base reference as a
// physical table. Compiled semantic SQL uses CTEs for aggregation and joins.
class SqlctxSourceQualifier {
public:
    SqlctxSourceQualifier(const string &catalog, const string &schema) : catalog(catalog), default_schema(schema) {}

    void Query(QueryNode &node, case_insensitive_set_t ctes = {}) {
        for (auto &cte : node.cte_map.map) {
#if SQLCTX_NEW_IDENTIFIER_API
            Query(*cte.second->query_node, ctes);
#else
            Query(*cte.second->query->node, ctes);
#endif
            ctes.insert(SqlctxName(cte.first));
        }
        if (node.type == QueryNodeType::SET_OPERATION_NODE) {
            for (auto &child : node.Cast<SetOperationNode>().children) Query(*child, ctes);
        } else if (node.type == QueryNodeType::RECURSIVE_CTE_NODE) {
            auto &recursive = node.Cast<RecursiveCTENode>();
            ctes.insert(SqlctxName(recursive.ctename));
            Query(*recursive.left, ctes);
            Query(*recursive.right, ctes);
        } else if (node.type == QueryNodeType::CTE_NODE) {
            auto &cte = node.Cast<CTENode>();
            Query(*cte.query, ctes);
            ctes.insert(SqlctxName(cte.ctename));
            Query(*cte.child, ctes);
        } else if (node.type == QueryNodeType::SELECT_NODE) {
            auto &select = node.Cast<SelectNode>();
            for (auto &expression : select.select_list) Expression(*expression, ctes);
            for (auto &expression : select.groups.group_expressions) Expression(*expression, ctes);
            if (select.where_clause) Expression(*select.where_clause, ctes);
            if (select.having) Expression(*select.having, ctes);
            if (select.qualify) Expression(*select.qualify, ctes);
            if (select.from_table) Table(*select.from_table, ctes);
        }
        ParsedExpressionIterator::EnumerateQueryNodeModifiers(node, [&](unique_ptr<ParsedExpression> &expression) {
            Expression(*expression, ctes);
        });
    }

private:
    const string &catalog;
    const string &default_schema;

    void Expression(ParsedExpression &expression, const case_insensitive_set_t &ctes) {
        if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY) {
#if SQLCTX_NEW_EXPRESSION_API
            Query(*expression.Cast<SubqueryExpression>().Subquery()->node, ctes);
#else
            Query(*expression.Cast<SubqueryExpression>().subquery->node, ctes);
#endif
        }
        ParsedExpressionIterator::EnumerateChildren(expression, [&](ParsedExpression &child) { Expression(child, ctes); });
    }

    void Table(TableRef &ref, const case_insensitive_set_t &ctes) {
        if (ref.type == TableReferenceType::BASE_TABLE) {
            auto &table = ref.Cast<BaseTableRef>();
#if SQLCTX_QUALIFIED_TABLE_API
            auto name = SqlctxName(table.Table());
            auto schema = SqlctxName(table.GetQualifiedName().Schema());
            if (!table.GetQualifiedName().Catalog().empty()) return;
            if (schema.empty() && ctes.count(name)) return;
            table.SetQualifiedName(Identifier(catalog), Identifier(schema.empty() ? default_schema : schema), Identifier(name));
#else
            if (!table.catalog_name.empty()) return;
            if (table.schema_name.empty() && ctes.count(table.table_name)) return;
            table.catalog_name = catalog;
            if (table.schema_name.empty()) table.schema_name = default_schema;
#endif
        } else if (ref.type == TableReferenceType::SUBQUERY) {
            Query(*ref.Cast<SubqueryRef>().subquery->node, ctes);
        } else if (ref.type == TableReferenceType::JOIN) {
            auto &join = ref.Cast<JoinRef>();
            Table(*join.left, ctes);
            Table(*join.right, ctes);
            if (join.condition) Expression(*join.condition, ctes);
        } else if (ref.type == TableReferenceType::TABLE_FUNCTION) {
            auto &function = ref.Cast<TableFunctionRef>();
            if (function.subquery) Query(*function.subquery->node, ctes);
            if (function.function) Expression(*function.function, ctes);
        } else {
            ParsedExpressionIterator::EnumerateTableRefChildren(ref, [&](unique_ptr<ParsedExpression> &expression) {
                Expression(*expression, ctes);
            });
        }
    }
};

void QualifySqlctxSources(ClientContext &context, QueryNode &node, const string &catalog) {
    if (catalog.empty()) return;
    auto schema = SemanticCatalog(context, catalog).GetDefaultSchema();
#if SQLCTX_NEW_IDENTIFIER_API
    auto default_schema = schema ? SqlctxName(*schema) : string(DEFAULT_SCHEMA);
#else
    auto default_schema = schema.empty() ? string(DEFAULT_SCHEMA) : schema;
#endif
    SqlctxSourceQualifier(catalog, default_schema).Query(node);
}

void RegisterSqlctxCatalogRead(ClientContext &context, StatementProperties &properties, const string &catalog_name) {
    properties.RegisterDBRead(SemanticCatalog(context, catalog_name), context);
    // A prepared semantic query must also observe connection-local MODEL changes.
    properties.always_require_rebind = true;
}

void RegisterSqlctxCatalogWrite(ClientContext &context, StatementProperties &properties) {
    properties.RegisterDBModify(SemanticCatalog(context), context, DatabaseModificationType::CREATE_CATALOG_ENTRY);
    properties.always_require_rebind = true;
}

// Definitions by "kind qualified_name", with their full JSON.
std::map<string, string> CatalogDefinitions(const string &payload) {
    std::map<string, string> result;
    if (payload.empty()) return result;
    auto entries = sqlctx_snapshot_catalog(payload.c_str(), "", "", nullptr);
    for (idx_t i = 0; !entries.error && i < entries.count; ++i) {
        auto &entry = entries.entries[i];
        result[string(entry.kind) + " " + entry.qualified_name] = entry.definition ? entry.definition : "";
    }
    sqlctx_free_catalog_entries(entries);
    return result;
}

// Summarize a mutation, e.g. "Created model orders; Replaced metric orders.revenue".
// Fields of a created or dropped model are implied by the model itself.
string DescribeCatalogChange(const string &before_payload, const string &after_payload) {
    auto before = CatalogDefinitions(before_payload);
    auto after = CatalogDefinitions(after_payload);
    vector<string> created, replaced, dropped;
    case_insensitive_set_t created_models, dropped_models, changed_models;
    auto model_of = [](const string &key) {
        auto name = key.substr(key.find(' ') + 1);
        auto dot = name.find('.');
        return dot == string::npos ? string() : name.substr(0, dot);
    };
    for (auto &entry : after) {
        if (StringUtil::StartsWith(entry.first, "model ") && !before.count(entry.first)) created_models.insert(entry.first.substr(6));
    }
    for (auto &entry : before) {
        if (StringUtil::StartsWith(entry.first, "model ") && !after.count(entry.first)) dropped_models.insert(entry.first.substr(6));
    }
    for (auto &entry : after) {
        auto previous = before.find(entry.first);
        if (previous != before.end() && previous->second == entry.second) continue;
        auto model = model_of(entry.first);
        if (created_models.count(model)) continue;
        if (!model.empty()) changed_models.insert(model);
        (previous == before.end() ? created : replaced).push_back(entry.first);
    }
    for (auto &entry : before) {
        if (after.count(entry.first)) continue;
        auto model = model_of(entry.first);
        if (dropped_models.count(model)) continue;
        if (!model.empty()) changed_models.insert(model);
        dropped.push_back(entry.first);
    }
    // A model's definition embeds its fields; report the fields instead.
    replaced.erase(std::remove_if(replaced.begin(), replaced.end(), [&](const string &key) {
        return StringUtil::StartsWith(key, "model ") && changed_models.count(key.substr(6));
    }), replaced.end());
    vector<string> parts;
    for (auto &group : {std::make_pair("Created", &created), std::make_pair("Replaced", &replaced),
                        std::make_pair("Dropped", &dropped)}) {
        if (!group.second->empty()) parts.push_back(string(group.first) + " " + StringUtil::Join(*group.second, ", "));
    }
    return parts.empty() ? "No semantic definitions changed" : StringUtil::Join(parts, "; ");
}

string ExecuteSqlctxMutation(ClientContext &context, const string &operation,
                                 const string &content, bool replace) {
    if (content.find('\0') != string::npos) {
        throw InvalidInputException("sqlctx definition contains a NUL byte");
    }
    auto &catalog = SemanticCatalog(context);
    auto snapshot = ReadSqlctxCatalog(context);
    SnapshotResultOwner applied(operation == "file"
        ? ImportDefinitionFiles(context, snapshot.payload, content)
        : sqlctx_snapshot_apply(snapshot.payload.c_str(), snapshot.active_model.c_str(),
                                    operation.c_str(), content.c_str(), replace));
    auto candidate = applied.Get();
    if (operation != "use") {
        CreateMacroInfo info(CatalogType::MACRO_ENTRY);
#if SQLCTX_NEW_IDENTIFIER_API
        info.SetQualifiedName(QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier(CATALOG_MACRO)));
#else
        info.catalog = catalog.GetName();
        info.schema = DEFAULT_SCHEMA;
        info.name = CATALOG_MACRO;
#endif
        info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
        // Not internal: internal catalog entries are omitted from checkpoints.
#if SQLCTX_LITERAL_API
        auto expression = ConstantExpression::String(candidate.payload);
#else
        auto expression = make_uniq<ConstantExpression>(Value(candidate.payload));
#endif
        info.macros.push_back(make_uniq<ScalarMacroFunction>(std::move(expression)));
        catalog.CreateFunction(context, info);
    }
    // Publish the session preference only after the catalog write succeeds.
    auto active_model = candidate.active_model;
    Session(context)->SetActive(catalog.GetOid(), std::move(candidate.active_model));
    if (operation == "use") return "Using model " + active_model;
    return DescribeCatalogChange(snapshot.payload, candidate.payload);
}

struct MutationBindData : public TableFunctionData {
    string operation;
    string content;
    bool replace;
};

struct MutationGlobalState : public GlobalTableFunctionState {
    bool done = false;
};

static unique_ptr<FunctionData> BindMutation(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types,
                                            vector<SqlctxIdentifier> &names) {
    auto data = make_uniq<MutationBindData>();
    data->operation = input.inputs[0].GetValue<string>();
    data->content = input.inputs[1].GetValue<string>();
    data->replace = input.inputs[2].GetValue<bool>();
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("result");
    if (input.binder) {
        auto &properties = input.binder->GetStatementProperties();
        RegisterSqlctxCatalogRead(context, properties);
    }
    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> InitMutation(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<MutationGlobalState>();
}

static void RunMutation(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<MutationGlobalState>();
    if (state.done) {
        return;
    }
    auto &data = input.bind_data->Cast<MutationBindData>();
    auto summary = ExecuteSqlctxMutation(context, data.operation, data.content, data.replace);
    state.done = true;
    output.SetCardinality(1);
    output.SetValue(0, 0, Value(summary));
}

ParserExtensionPlanResult PlanSqlctxMutation(ClientContext &context, const string &operation,
                                                const string &content, bool replace) {
    ParserExtensionPlanResult result;
    result.function = TableFunction("sqlctx_definition",
                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN},
                                   RunMutation, BindMutation, InitMutation);
    result.parameters = {Value(operation), Value(content), Value::BOOLEAN(replace)};
    result.return_type = StatementReturnType::QUERY_RESULT;
    if (operation != "use") {
        StatementProperties properties;
        RegisterSqlctxCatalogWrite(context, properties);
        result.modified_databases = std::move(properties.modified_databases);
    }
    return result;
}

} // namespace duckdb
