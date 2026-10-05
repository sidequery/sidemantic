#include "sidemantic_catalog.hpp"
#include "sidemantic_compat.hpp"
#include "sidemantic.h"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/planner_extension.hpp"
#include <algorithm>

namespace duckdb {
namespace {

constexpr const char *SEMANTIC_SCHEMA = "semantic";

const string &ScanName(const LogicalGet &get) {
#if SIDEMANTIC_NEW_IDENTIFIER_API
    return SidemanticName(get.function.GetName());
#else
    return get.function.name;
#endif
}

struct CatalogEntriesOwner {
    explicit CatalogEntriesOwner(const string &snapshot)
        : result(sidemantic_snapshot_catalog(snapshot.c_str(), "", "", nullptr)) {
        if (result.error) {
            string error(result.error);
            sidemantic_free_catalog_entries(result);
            throw InvalidInputException("Sidemantic catalog: %s", error);
        }
    }
    ~CatalogEntriesOwner() { sidemantic_free_catalog_entries(result); }
    SidemanticCatalogEntries result;
};

string QuoteName(const string &name) {
    return "\"" + StringUtil::Replace(name, "\"", "\"\"") + "\"";
}

// Synthetic OIDs remain within PostgreSQL's positive int32 range. Hash only
// stable names, never process-local DuckDB OIDs or std::hash implementation data.
int64_t StableOid(const string &key, unordered_set<int64_t> &used) {
    uint32_t hash = 2166136261U;
    for (unsigned char byte : StringUtil::Lower(key)) hash = (hash ^ byte) * 16777619U;
    int64_t oid = 0x40000000U | (hash & 0x3fffffffU);
    while (used.count(oid)) oid = 0x40000000U | ((oid + 1) & 0x3fffffffU);
    used.insert(oid);
    return oid;
}

struct SemanticColumn {
    string name;
    idx_t ordinal = 0;
    Value comment;
    LogicalType type = LogicalTypeId::INVALID;
};

struct SemanticRelation {
    string name;
    Value comment;
    int64_t oid = 0;
    vector<SemanticColumn> columns;
};

struct SemanticMetadata {
    string database;
    int64_t database_oid = 0;
    int64_t schema_oid = 0;
    bool physical_schema = false;
    vector<SemanticRelation> relations;
};

LogicalType BindFieldType(ClientContext &context, const string &snapshot, const string &model, const string &field) {
    auto sql = "SELECT " + QuoteName(model) + "." + QuoteName(field) + " FROM " + QuoteName(model);
    auto result = sidemantic_snapshot_rewrite(snapshot.c_str(), sql.c_str());
    string compiled = result.sql ? result.sql : "";
    string error = result.error ? result.error : "";
    sidemantic_free_result(result);
    if (!error.empty()) throw BinderException("%s", error);
    Parser parser(SidemanticBuiltinParserOptions());
    parser.ParseQuery(compiled);
    if (parser.statements.size() != 1) throw BinderException("Semantic field must compile to one query");
    auto binder = Binder::CreateBinder(context);
    auto bound = binder->Bind(*parser.statements[0]);
    if (bound.types.size() != 1) throw BinderException("Semantic field must produce one column");
    return bound.types[0];
}

SemanticMetadata ReadMetadata(ClientContext &context) {
    SemanticMetadata metadata;
    auto &catalog = Catalog::GetCatalog(context, DatabaseManager::GetDefaultDatabase(context));
    metadata.database = SidemanticName(catalog.GetName());
    metadata.database_oid = NumericCast<int64_t>(catalog.GetOid());
    auto snapshot = ReadSidemanticCatalog(context);
    if (snapshot.payload.empty()) return metadata;
    CatalogEntriesOwner entries(snapshot.payload);
    unordered_set<int64_t> used;
    case_insensitive_set_t physical_relations;
    for (auto &schema_ref : Catalog::GetAllSchemas(context)) {
        auto &schema = schema_ref.get();
        used.insert(NumericCast<int64_t>(schema.oid));
        bool same_schema = &schema.catalog == &catalog && StringUtil::CIEquals(SidemanticName(schema.name), SEMANTIC_SCHEMA);
        if (same_schema) {
            metadata.physical_schema = true;
            metadata.schema_oid = NumericCast<int64_t>(schema.oid);
        }
        for (auto type : {CatalogType::TABLE_ENTRY, CatalogType::SEQUENCE_ENTRY, CatalogType::INDEX_ENTRY}) {
            schema.Scan(context, type, [&](CatalogEntry &entry) {
                used.insert(NumericCast<int64_t>(entry.oid));
                if (same_schema && type == CatalogType::TABLE_ENTRY) physical_relations.insert(SidemanticName(entry.name));
            });
        }
    }
    if (!metadata.physical_schema) metadata.schema_oid = StableOid(metadata.database + ".semantic", used);
    // The Rust catalog is deterministically sorted, including across restarts.
    for (idx_t i = 0; i < entries.result.count; ++i) {
        auto &entry = entries.result.entries[i];
        if (string(entry.kind) != "model" || physical_relations.count(entry.name)) continue;
        SemanticRelation relation;
        relation.name = entry.name;
        relation.comment = entry.description ? Value(entry.description) : Value();
        relation.oid = StableOid(metadata.database + ".semantic." + relation.name, used);
        for (idx_t j = 0; j < entries.result.count; ++j) {
            auto &field = entries.result.entries[j];
            if (!field.model_name || relation.name != field.model_name) continue;
            auto kind = string(field.kind);
            if (kind != "dimension" && kind != "metric") continue;
            SemanticColumn column;
            column.name = field.name;
            column.ordinal = field.column_index;
            column.comment = field.description ? Value(field.description) : Value();
            try {
                column.type = BindFieldType(context, snapshot.payload, relation.name, column.name);
            } catch (const std::exception &exception) {
                // Like DuckDB's unbound views, keep the relation discoverable
                // without inventing a physical type for an unavailable source.
                // Do not hide cancellation or an engine invariant failure.
                auto error = ErrorData(exception);
                if (error.Type() == ExceptionType::INTERRUPT || error.Type() == ExceptionType::INTERNAL ||
                    error.Type() == ExceptionType::FATAL) throw;
            }
            relation.columns.push_back(std::move(column));
        }
        std::sort(relation.columns.begin(), relation.columns.end(),
                  [](const SemanticColumn &left, const SemanticColumn &right) { return left.ordinal < right.ordinal; });
        metadata.relations.push_back(std::move(relation));
    }
    return metadata;
}

struct DiscoveryRows : public TableFunctionData {
    vector<vector<Value>> rows;
};

struct DiscoveryState : public GlobalTableFunctionState {
    idx_t offset = 0;
};

unique_ptr<GlobalTableFunctionState> InitDiscovery(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<DiscoveryState>();
}

void ScanDiscovery(ClientContext &, TableFunctionInput &input, DataChunk &output) {
    auto &data = input.bind_data->Cast<DiscoveryRows>();
    auto &state = input.global_state->Cast<DiscoveryState>();
    auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, data.rows.size() - state.offset);
    for (idx_t row = 0; row < count; ++row) {
        for (idx_t column = 0; column < output.ColumnCount(); ++column) {
            output.SetValue(column, row, data.rows[state.offset + row][column]);
        }
    }
    state.offset += count;
    output.SetCardinality(count);
}

void NumericMetadata(const LogicalType &type, Value &precision, Value &radix, Value &scale) {
    int bits = 0;
    switch (type.id()) {
    case LogicalTypeId::DECIMAL:
        precision = Value::INTEGER(DecimalType::GetWidth(type));
        radix = Value::INTEGER(10);
        scale = Value::INTEGER(DecimalType::GetScale(type));
        return;
    case LogicalTypeId::TINYINT: bits = 8; break;
    case LogicalTypeId::SMALLINT: bits = 16; break;
    case LogicalTypeId::INTEGER: bits = 32; break;
    case LogicalTypeId::BIGINT: bits = 64; break;
    case LogicalTypeId::HUGEINT: bits = 128; break;
    case LogicalTypeId::FLOAT: bits = 24; break;
    case LogicalTypeId::DOUBLE: bits = 53; break;
    default: return;
    }
    precision = Value::INTEGER(bits);
    radix = Value::INTEGER(2);
    scale = Value::INTEGER(0);
}

unique_ptr<DiscoveryRows> BuildRows(const SemanticMetadata &metadata, const string &function,
                                  const vector<SidemanticIdentifier> &names, const vector<LogicalType> &types) {
    auto result = make_uniq<DiscoveryRows>();
    auto append = [&](const unordered_map<string, Value> &values) {
        vector<Value> row;
        for (idx_t i = 0; i < names.size(); ++i) {
            auto entry = values.find(SidemanticName(names[i]));
            row.push_back(entry == values.end() ? Value(types[i]) : entry->second);
        }
        result->rows.push_back(std::move(row));
    };
    unordered_map<string, Value> common {
        {"database_name", Value(metadata.database)}, {"database_oid", Value::BIGINT(metadata.database_oid)},
        {"schema_name", Value(SEMANTIC_SCHEMA)}, {"schema_oid", Value::BIGINT(metadata.schema_oid)},
        {"internal", Value::BOOLEAN(false)},
        {"tags", Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, {}, {})}
    };
    if (function == "duckdb_schemas") {
        if (!metadata.relations.empty() && !metadata.physical_schema) {
            common["oid"] = Value::BIGINT(metadata.schema_oid);
            append(common);
        }
        return result;
    }
    for (auto &relation : metadata.relations) {
        auto values = common;
        if (function == "duckdb_views") {
            values["view_name"] = Value(relation.name);
            values["view_oid"] = Value::BIGINT(relation.oid);
            values["comment"] = relation.comment;
            values["temporary"] = Value::BOOLEAN(false);
            values["column_count"] = Value::BIGINT(relation.columns.size());
            values["is_bound"] = Value::BOOLEAN(std::all_of(relation.columns.begin(), relation.columns.end(),
                [](const SemanticColumn &column) { return column.type.id() != LogicalTypeId::INVALID; }));
            append(values);
        } else {
            values["table_name"] = Value(relation.name);
            values["table_oid"] = Value::BIGINT(relation.oid);
            values["is_nullable"] = Value::BOOLEAN(true);
            values["is_generated"] = Value::BOOLEAN(false);
            for (idx_t i = 0; i < relation.columns.size(); ++i) {
                auto &column = relation.columns[i];
                auto row = values;
                row["column_name"] = Value(column.name);
                row["column_index"] = Value::INTEGER(i + 1);
                row["comment"] = column.comment;
                if (column.type.id() != LogicalTypeId::INVALID) {
                    row["data_type"] = Value(column.type.ToString());
                    row["data_type_id"] = Value::BIGINT(static_cast<int>(column.type.id()));
                    NumericMetadata(column.type, row["numeric_precision"], row["numeric_precision_radix"], row["numeric_scale"]);
                }
                append(row);
            }
        }
    }
    return result;
}

void OverlayScans(PlannerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                  unique_ptr<SemanticMetadata> &metadata) {
    // PREPARE has a nested planner. Its child can already contain this overlay.
    if (plan->type == LogicalOperatorType::LOGICAL_UNION) {
        for (auto &child : plan->children) {
            if (child->type == LogicalOperatorType::LOGICAL_GET &&
                ScanName(child->Cast<LogicalGet>()) == "sidemantic_catalog_scan") return;
        }
    }
    for (auto &child : plan->children) OverlayScans(input, child, metadata);
    if (plan->type != LogicalOperatorType::LOGICAL_GET) return;
    auto &get = plan->Cast<LogicalGet>();
    auto name = ScanName(get);
    if (name != "duckdb_columns" && name != "duckdb_views" && name != "duckdb_schemas") return;
    RegisterSidemanticCatalogRead(input.context, input.binder.GetStatementProperties());
    if (!metadata) metadata = make_uniq<SemanticMetadata>(ReadMetadata(input.context));
    if (metadata->relations.empty()) return;
    auto rows = BuildRows(*metadata, name, get.names, get.returned_types);
    if (rows->rows.empty()) return;
    TableFunction function("sidemantic_catalog_scan", {}, ScanDiscovery, nullptr, InitDiscovery);
#if SIDEMANTIC_NEW_IDENTIFIER_API
    auto extra = make_uniq<LogicalGet>(input.binder.GenerateTableIndex(), BoundTableFunction(function), std::move(rows),
                                      get.returned_types, get.names);
#else
    auto extra = make_uniq<LogicalGet>(input.binder.GenerateTableIndex(), function, std::move(rows),
                                      get.returned_types, get.names);
#endif
    auto columns = get.GetColumnIds();
    extra->SetColumnIds(std::move(columns));
    auto union_index = get.table_index;
    get.table_index = input.binder.GenerateTableIndex();
    auto column_count = get.GetColumnBindings().size();
    plan = make_uniq<LogicalSetOperation>(union_index, column_count, std::move(plan), std::move(extra),
                                         LogicalOperatorType::LOGICAL_UNION, true);
}

void DiscoveryPostBind(PlannerExtensionInput &input, BoundStatement &statement) {
    if (!statement.plan) return;
    unique_ptr<SemanticMetadata> metadata;
    OverlayScans(input, statement.plan, metadata);
}

} // namespace

bool SidemanticPhysicalRelationExists(ClientContext &context, const string &model) {
    auto &catalog = Catalog::GetCatalog(context, DatabaseManager::GetDefaultDatabase(context));
#if SIDEMANTIC_NEW_IDENTIFIER_API
    EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
                          QualifiedName(catalog.GetName(), Identifier(SEMANTIC_SCHEMA), Identifier(model)));
    return Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL) != nullptr;
#else
    EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, model);
    return Catalog::GetEntry(context, catalog.GetName(), SEMANTIC_SCHEMA, lookup, OnEntryNotFound::RETURN_NULL) != nullptr;
#endif
}

void RegisterSidemanticDiscovery(DatabaseInstance &db) {
    PlannerExtension extension;
    extension.post_bind_function = DiscoveryPostBind;
    PlannerExtension::Register(DBConfig::GetConfig(db), extension);
}

} // namespace duckdb
