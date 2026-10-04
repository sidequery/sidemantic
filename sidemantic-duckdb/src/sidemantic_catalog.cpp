#include "sidemantic_catalog.hpp"
#include "sidemantic_compat.hpp"
#include "sidemantic.h"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include <algorithm>

namespace duckdb {

static constexpr const char *CATALOG_MACRO = "__sidemantic_catalog";
static constexpr const char *SESSION_STATE = "sidemantic_catalog_session";

// Only session preferences live outside DuckDB's catalog. Capture their values
// lazily: the first semantic statement may arrive after BEGIN already ran.
class SidemanticCatalogSession : public ClientContextState {
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

static Catalog &SemanticCatalog(ClientContext &context) {
    return Catalog::GetCatalog(context, DatabaseManager::GetDefaultDatabase(context));
}

static shared_ptr<SidemanticCatalogSession> Session(ClientContext &context) {
    return context.registered_state->GetOrCreate<SidemanticCatalogSession>(SESSION_STATE);
}

struct SnapshotResultOwner {
    explicit SnapshotResultOwner(SidemanticSnapshotResult result) : result(result) {
    }
    ~SnapshotResultOwner() {
        sidemantic_free_snapshot_result(result);
    }
    SidemanticSnapshotResult result;

    SidemanticCatalogSnapshot Get() const {
        if (result.error) {
            throw InvalidInputException("Sidemantic: %s", result.error);
        }
        if (!result.snapshot) {
            throw InternalException("Sidemantic returned no catalog snapshot");
        }
        return {result.snapshot, result.active_model ? result.active_model : ""};
    }
};

static string ReadDefinitionFile(FileSystem &fs, const string &path) {
    auto file = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
    auto size = file->GetFileSize();
    if (size < 0 || file->GetType() != FileType::FILE_TYPE_REGULAR) {
        throw InvalidInputException("Sidemantic definitions require a regular file: %s", path);
    }
    string content(size, '\0');
    if (!content.empty()) file->Read(&content[0], content.size(), 0);
    if (content.find('\0') != string::npos) {
        throw InvalidInputException("Sidemantic definitions contain a NUL byte: %s", path);
    }
    return content;
}

static void CollectDefinitionFiles(FileSystem &fs, const string &directory, vector<string> &paths, idx_t depth = 0) {
    // Bound recursive traversal, including directory symlink cycles.
    static constexpr idx_t MAX_DIRECTORY_DEPTH = 64;
    if (depth >= MAX_DIRECTORY_DEPTH) {
        throw InvalidInputException("Sidemantic model directory exceeds maximum nesting depth: %s", directory);
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
        throw IOException("Could not list Sidemantic model directory: %s", directory);
    }
}

static SidemanticSnapshotResult ImportDefinitionFiles(ClientContext &context, const string &snapshot,
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
    vector<SidemanticSource> sources;
    for (idx_t i = 0; i < paths.size(); ++i) {
        sources.push_back({paths[i].c_str(), contents[i].c_str()});
    }
    return sidemantic_snapshot_load_sources(snapshot.c_str(), sources.data(), sources.size(), directory);
}

static SidemanticCatalogSnapshot ReadLegacySnapshot(ClientContext &context, Catalog &catalog) {
    auto &database = catalog.GetAttached();
    if (catalog.InMemory() || !database.HasStorageManager()) {
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
    path += ".sidemantic.sql";

    // The context filesystem supplies its opener and access checks itself.
    auto &fs = FileSystem::GetFileSystem(context);
    if (!fs.FileExists(path)) {
        return {};
    }
    auto content = ReadDefinitionFile(fs, path);
    SnapshotResultOwner imported(sidemantic_snapshot_apply(nullptr, nullptr, "legacy_sql", content.c_str(), false));
    return imported.Get();
}

SidemanticCatalogSnapshot ReadSidemanticCatalog(ClientContext &context) {
    auto &catalog = SemanticCatalog(context);
    auto state = Session(context);
    const string macro_name = CATALOG_MACRO;
#if SIDEMANTIC_NEW_IDENTIFIER_API
    const QualifiedName qualified_name(catalog.GetName(), Identifier::DefaultSchema(), Identifier(macro_name));
    EntryLookupInfo lookup(CatalogType::MACRO_ENTRY, qualified_name);
    auto entry = Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
#else
    EntryLookupInfo lookup(CatalogType::MACRO_ENTRY, macro_name);
    auto entry = Catalog::GetEntry(context, catalog.GetName(), DEFAULT_SCHEMA, lookup, OnEntryNotFound::RETURN_NULL);
#endif
    SidemanticCatalogSnapshot snapshot;
    if (entry) {
        if (entry->type != CatalogType::MACRO_ENTRY) {
            throw InvalidInputException("Invalid Sidemantic catalog entry type");
        }
        auto &macro_entry = entry->Cast<ScalarMacroCatalogEntry>();
        if (macro_entry.macros.size() != 1 || macro_entry.macros[0]->type != MacroType::SCALAR_MACRO) {
            throw InvalidInputException("Invalid Sidemantic catalog snapshot macro");
        }
        auto &macro = macro_entry.macros[0]->Cast<ScalarMacroFunction>();
        if (!macro.expression || macro.expression->GetExpressionClass() != ExpressionClass::CONSTANT) {
            throw InvalidInputException("Invalid Sidemantic catalog snapshot expression");
        }
        auto &constant = macro.expression->Cast<ConstantExpression>();
#if SIDEMANTIC_LITERAL_API
        auto &literal = constant.GetLiteral();
        if (literal.kind != LiteralKind::STRING) {
            throw InvalidInputException("Invalid Sidemantic catalog snapshot value");
        }
        snapshot.payload = literal.text;
#else
        auto &value = constant.value;
        if (value.IsNull() || value.type() != LogicalType::VARCHAR) {
            throw InvalidInputException("Invalid Sidemantic catalog snapshot value");
        }
        snapshot.payload = value.GetValue<string>();
#endif
    } else {
        snapshot = ReadLegacySnapshot(context, catalog);
    }
    if (snapshot.payload.find('\0') != string::npos) {
        throw InvalidInputException("Sidemantic catalog snapshot contains a NUL byte");
    }
    auto active = state->active_models.find(catalog.GetOid());
    if (active != state->active_models.end()) {
        snapshot.active_model = active->second;
    }
    return snapshot;
}

void RegisterSidemanticCatalogRead(ClientContext &context, StatementProperties &properties) {
    properties.RegisterDBRead(SemanticCatalog(context), context);
    // A prepared semantic query must also observe connection-local MODEL changes.
    properties.always_require_rebind = true;
}

void RegisterSidemanticCatalogWrite(ClientContext &context, StatementProperties &properties) {
    properties.RegisterDBModify(SemanticCatalog(context), context, DatabaseModificationType::CREATE_CATALOG_ENTRY);
    properties.always_require_rebind = true;
}

void ExecuteSidemanticMutation(ClientContext &context, const string &operation,
                              const string &content, bool replace) {
    if (content.find('\0') != string::npos) {
        throw InvalidInputException("Sidemantic definition contains a NUL byte");
    }
    auto &catalog = SemanticCatalog(context);
    auto snapshot = ReadSidemanticCatalog(context);
    SnapshotResultOwner applied(operation == "file"
        ? ImportDefinitionFiles(context, snapshot.payload, content)
        : sidemantic_snapshot_apply(snapshot.payload.c_str(), snapshot.active_model.c_str(),
                                    operation.c_str(), content.c_str(), replace));
    auto candidate = applied.Get();
    if (operation != "use") {
        CreateMacroInfo info(CatalogType::MACRO_ENTRY);
#if SIDEMANTIC_NEW_IDENTIFIER_API
        info.SetQualifiedName(QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier(CATALOG_MACRO)));
#else
        info.catalog = catalog.GetName();
        info.schema = DEFAULT_SCHEMA;
        info.name = CATALOG_MACRO;
#endif
        info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
        // Not internal: internal catalog entries are omitted from checkpoints.
#if SIDEMANTIC_LITERAL_API
        auto expression = ConstantExpression::String(candidate.payload);
#else
        auto expression = make_uniq<ConstantExpression>(Value(candidate.payload));
#endif
        info.macros.push_back(make_uniq<ScalarMacroFunction>(std::move(expression)));
        catalog.CreateFunction(context, info);
    }
    // Publish the session preference only after the catalog write succeeds.
    Session(context)->SetActive(catalog.GetOid(), std::move(candidate.active_model));
}

struct MutationBindData : public TableFunctionData {
    string operation;
    string content;
    bool replace;
    string result_message;
};

struct MutationGlobalState : public GlobalTableFunctionState {
    bool done = false;
};

static unique_ptr<FunctionData> BindMutation(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types,
                                            vector<SidemanticIdentifier> &names) {
    auto data = make_uniq<MutationBindData>();
    data->operation = input.inputs[0].GetValue<string>();
    data->content = input.inputs[1].GetValue<string>();
    data->replace = input.inputs[2].GetValue<bool>();
    data->result_message = input.inputs[3].GetValue<string>();
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("result");
    if (input.binder) {
        auto &properties = input.binder->GetStatementProperties();
        RegisterSidemanticCatalogRead(context, properties);
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
    ExecuteSidemanticMutation(context, data.operation, data.content, data.replace);
    state.done = true;
    output.SetCardinality(1);
    output.SetValue(0, 0, Value(data.result_message));
}

ParserExtensionPlanResult PlanSidemanticMutation(ClientContext &context, const string &operation,
                                                const string &content, bool replace,
                                                const string &result_message) {
    ParserExtensionPlanResult result;
    result.function = TableFunction("sidemantic_definition",
                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::VARCHAR},
                                   RunMutation, BindMutation, InitMutation);
    result.parameters = {Value(operation), Value(content), Value::BOOLEAN(replace), Value(result_message)};
    result.return_type = StatementReturnType::QUERY_RESULT;
    if (operation != "use") {
        StatementProperties properties;
        RegisterSidemanticCatalogWrite(context, properties);
        result.modified_databases = std::move(properties.modified_databases);
    }
    return result;
}

} // namespace duckdb
