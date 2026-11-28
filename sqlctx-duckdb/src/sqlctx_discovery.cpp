#include "sqlctx_catalog.hpp"
#include "sqlctx_compiler.hpp"
#include "sqlctx_compat.hpp"
#include "sqlctx.h"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/planner_extension.hpp"
#include <algorithm>

namespace duckdb {
namespace {

constexpr const char *SEMANTIC_SCHEMA = "semantic";

const string &ScanName(const LogicalGet &get) {
#if SQLCTX_NEW_IDENTIFIER_API
    return SqlctxName(get.function.GetName());
#else
    return get.function.name;
#endif
}

struct CatalogEntriesOwner {
    explicit CatalogEntriesOwner(const string &snapshot)
        : result(sqlctx_snapshot_catalog(snapshot.c_str(), "", "", nullptr)) {
        if (result.error) {
            string error(result.error);
            sqlctx_free_catalog_entries(result);
            throw InvalidInputException("sqlctx catalog: %s", error);
        }
    }
    ~CatalogEntriesOwner() { sqlctx_free_catalog_entries(result); }
    SqlctxCatalogEntries result;
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
};

struct SemanticRelation {
    string name;
    Value comment;
    int64_t oid = 0;
    vector<SemanticColumn> columns;
};

struct SemanticMetadata {
    string database;
    string snapshot;
    int64_t database_oid = 0;
    int64_t schema_oid = 0;
    bool physical_schema = false;
    vector<SemanticRelation> relations;
};

} // namespace

LogicalType SqlctxFieldType(ClientContext &context, const string &snapshot, const string &model,
                                const string &field, const string &catalog_name) {
    SqlctxCompilerOwner compiler(snapshot);
    return SqlctxFieldType(context, compiler, model, field, catalog_name);
}

LogicalType SqlctxFieldType(ClientContext &context, const SqlctxCompilerOwner &compiler, const string &model,
                           const string &field, const string &catalog_name) {
    auto sql = "SELECT " + QuoteName(model) + "." + QuoteName(field) + " FROM " + QuoteName(model);
    auto result = compiler.Rewrite(sql);
    string compiled = result.sql ? result.sql : "";
    string error = result.error ? result.error : "";
    sqlctx_free_result(result);
    if (!error.empty()) throw BinderException("%s", error);
    Parser parser(SqlctxBuiltinParserOptions());
    parser.ParseQuery(compiled);
    if (parser.statements.size() != 1) throw BinderException("Semantic field must compile to one query");
    if (!catalog_name.empty() && !StringUtil::CIEquals(catalog_name, SqlctxName(DatabaseManager::GetDefaultDatabase(context)))) {
        QualifySqlctxSources(context, *parser.statements[0]->Cast<SelectStatement>().node, catalog_name);
    }
    auto binder = Binder::CreateBinder(context);
    auto bound = binder->Bind(*parser.statements[0]);
    if (bound.types.size() != 1) throw BinderException("Semantic field must produce one column");
    return bound.types[0];
}

namespace {

SemanticMetadata ReadMetadata(ClientContext &context, const string &catalog_name, unordered_set<int64_t> &used) {
    SemanticMetadata metadata;
    auto &catalog = Catalog::GetCatalog(context, SqlctxIdentifier(catalog_name));
    metadata.database = SqlctxName(catalog.GetName());
    metadata.database_oid = NumericCast<int64_t>(catalog.GetOid());
    auto snapshot = ReadSqlctxCatalog(context, catalog_name);
    if (snapshot.payload.empty()) return metadata;
    metadata.snapshot = snapshot.payload;
    CatalogEntriesOwner entries(snapshot.payload);
    case_insensitive_set_t physical_relations;
    for (auto &schema_ref : Catalog::GetAllSchemas(context)) {
        auto &schema = schema_ref.get();
        used.insert(NumericCast<int64_t>(schema.oid));
        bool same_schema = &schema.catalog == &catalog && StringUtil::CIEquals(SqlctxName(schema.name), SEMANTIC_SCHEMA);
        if (same_schema) {
            metadata.physical_schema = true;
            metadata.schema_oid = NumericCast<int64_t>(schema.oid);
        }
        for (auto type : {CatalogType::TABLE_ENTRY, CatalogType::SEQUENCE_ENTRY, CatalogType::INDEX_ENTRY}) {
            schema.Scan(context, type, [&](CatalogEntry &entry) {
                used.insert(NumericCast<int64_t>(entry.oid));
                if (same_schema && type == CatalogType::TABLE_ENTRY) physical_relations.insert(SqlctxName(entry.name));
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
    vector<SqlctxIdentifier> names;
    vector<SemanticMetadata> metadata;
    string function;
    // Equality hints are kept in the ordinary plan too. Only names, never
    // deferred type values, can eliminate a row before physical binding.
    vector<pair<idx_t, Value>> equalities;
};

struct DiscoveryState : public GlobalTableFunctionState {
    vector<vector<Value>> rows;
    vector<column_t> column_ids;
    idx_t offset = 0;
};

unique_ptr<GlobalTableFunctionState> InitDiscovery(ClientContext &, TableFunctionInitInput &);

void ScanDiscovery(ClientContext &, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<DiscoveryState>();
    auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.offset);
    for (idx_t row = 0; row < count; ++row) {
        for (idx_t column = 0; column < output.ColumnCount(); ++column) {
            auto id = state.column_ids[column];
            auto value = id == COLUMN_IDENTIFIER_ROW_ID ? Value::BIGINT(0) : state.rows[state.offset + row][id];
#if SQLCTX_VECTOR_APPEND
            // DuckDB 2 derives expression cardinality from each vector, not
            // just the chunk. SetValue does not advance those vector sizes.
            output.data[column].Append(value);
#else
            output.SetValue(column, row, value);
#endif
        }
    }
    state.offset += count;
#if SQLCTX_VECTOR_APPEND
    output.CheckCardinality(count);
#else
    output.SetCardinality(count);
#endif
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
                                  const vector<SqlctxIdentifier> &names, const vector<LogicalType> &types) {
    auto result = make_uniq<DiscoveryRows>();
    auto append = [&](const unordered_map<string, Value> &values) {
        vector<Value> row;
        for (idx_t i = 0; i < names.size(); ++i) {
            auto entry = values.find(SqlctxName(names[i]));
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
            // Populated in InitDiscovery only when requested by the plan.
            values["is_bound"] = Value::BOOLEAN(false);
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
                append(row);
            }
        }
    }
    return result;
}

void RestrictDiscovery(ClientContext &, LogicalGet &get, FunctionData *bind_data,
                       vector<unique_ptr<Expression>> &filters) {
    auto &data = bind_data->Cast<DiscoveryRows>();
    for (auto &filter : filters) {
        if (filter->GetExpressionType() != ExpressionType::COMPARE_EQUAL) continue;
        const BoundColumnRefExpression *column = nullptr;
        const BoundConstantExpression *constant = nullptr;
        idx_t children = 0;
        ExpressionIterator::EnumerateChildren(*filter, [&](const Expression &child) {
            ++children;
            if (child.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
                column = &child.Cast<BoundColumnRefExpression>();
            } else if (child.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
                constant = &child.Cast<BoundConstantExpression>();
            }
        });
        if (children != 2 || !column || !constant) continue;
#if SQLCTX_NEW_EXPRESSION_API
        auto &binding = column->Binding();
        auto column_index = binding.column_index.GetIndex();
        auto &value = constant->GetValue();
#else
        auto &binding = column->binding;
        auto column_index = binding.column_index;
        auto &value = constant->value;
#endif
        if (binding.table_index != get.table_index || value.IsNull() || value.type() != LogicalType::VARCHAR) continue;
        auto id = get.GetColumnIds()[column_index].GetPrimaryIndex();
        if (id >= data.names.size()) continue;
        auto &name = SqlctxName(data.names[id]);
        if (name == "database_name" || name == "schema_name" || name == "table_name" ||
            name == "view_name" || name == "column_name") {
            data.equalities.emplace_back(id, value);
        }
    }
}

LogicalType BindFieldType(ClientContext &context, const SemanticMetadata &metadata,
                          const SqlctxCompilerOwner &compiler, const string &model, const string &field) {
    try {
        return SqlctxFieldType(context, compiler, model, field, metadata.database);
    } catch (const std::exception &exception) {
        // Match unbound views: missing sources remain visible with NULL types.
        // Cancellation and engine invariant failures must still reach the caller.
        auto error = ErrorData(exception);
        if (error.Type() == ExceptionType::INTERRUPT || error.Type() == ExceptionType::INTERNAL ||
            error.Type() == ExceptionType::FATAL) throw;
        return LogicalTypeId::INVALID;
    }
}

unique_ptr<GlobalTableFunctionState> InitDiscovery(ClientContext &context, TableFunctionInitInput &input) {
    auto &data = input.bind_data->Cast<DiscoveryRows>();
    auto state = make_uniq<DiscoveryState>();
    state->column_ids = input.column_ids;
    unordered_map<string, idx_t> indexes;
    for (idx_t i = 0; i < data.names.size(); ++i) indexes[SqlctxName(data.names[i])] = i;
    unordered_set<string> requested;
    for (auto id : input.column_ids) {
        if (id < data.names.size()) requested.insert(SqlctxName(data.names[id]));
    }
    bool bind_types = requested.count("is_bound") || requested.count("data_type") || requested.count("data_type_id") ||
        requested.count("numeric_precision") || requested.count("numeric_precision_radix") || requested.count("numeric_scale");
    unordered_map<string, unique_ptr<SqlctxCompilerOwner>> compilers;
    for (auto &original : data.rows) {
        bool matches = true;
        for (auto &equality : data.equalities) {
            if (original[equality.first].IsNull() ||
                original[equality.first].GetValue<string>() != equality.second.GetValue<string>()) {
                matches = false;
                break;
            }
        }
        if (!matches) continue;
        auto row = original;
        if (bind_types && data.function != "duckdb_schemas") {
            auto database = row[indexes.at("database_name")].GetValue<string>();
            auto model = row[indexes.at(data.function == "duckdb_views" ? "view_name" : "table_name")].GetValue<string>();
            for (auto &metadata : data.metadata) {
                if (metadata.database != database) continue;
                auto &compiler = compilers[database];
                if (!compiler) compiler = make_uniq<SqlctxCompilerOwner>(metadata.snapshot);
                if (data.function == "duckdb_views") {
                    for (auto &relation : metadata.relations) {
                        if (relation.name != model) continue;
                        bool bound = true;
                        for (auto &column : relation.columns) {
                            if (BindFieldType(context, metadata, *compiler, model, column.name).id() == LogicalTypeId::INVALID) {
                                bound = false;
                                break;
                            }
                        }
                        row[indexes.at("is_bound")] = Value::BOOLEAN(bound);
                    }
                } else {
                    auto field = row[indexes.at("column_name")].GetValue<string>();
                    auto type = BindFieldType(context, metadata, *compiler, model, field);
                    if (type.id() != LogicalTypeId::INVALID) {
                        row[indexes.at("data_type")] = Value(type.ToString());
                        row[indexes.at("data_type_id")] = Value::BIGINT(static_cast<int>(type.id()));
                        NumericMetadata(type, row[indexes.at("numeric_precision")],
                            row[indexes.at("numeric_precision_radix")], row[indexes.at("numeric_scale")]);
                    }
                }
                break;
            }
        }
        state->rows.push_back(std::move(row));
    }
    return std::move(state);
}

void OverlayScans(PlannerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                  unique_ptr<vector<SemanticMetadata>> &metadata) {
    // PREPARE has a nested planner. Its child can already contain this overlay.
    if (plan->type == LogicalOperatorType::LOGICAL_UNION) {
        for (auto &child : plan->children) {
            if (child->type == LogicalOperatorType::LOGICAL_GET &&
                ScanName(child->Cast<LogicalGet>()) == "sqlctx_catalog_scan") return;
        }
    }
    for (auto &child : plan->children) OverlayScans(input, child, metadata);
    if (plan->type != LogicalOperatorType::LOGICAL_GET) return;
    auto &get = plan->Cast<LogicalGet>();
    auto name = ScanName(get);
    if (name != "duckdb_columns" && name != "duckdb_views" && name != "duckdb_schemas") return;
    if (!metadata) {
        metadata = make_uniq<vector<SemanticMetadata>>();
        unordered_set<int64_t> used;
        for (auto &catalog : SqlctxCatalogNames(input.context)) {
            RegisterSqlctxCatalogRead(input.context, input.binder.GetStatementProperties(), catalog);
            auto entry = ReadMetadata(input.context, catalog, used);
            if (!entry.relations.empty()) metadata->push_back(std::move(entry));
        }
    }
    auto rows = make_uniq<DiscoveryRows>();
    rows->names = get.names;
    rows->metadata = *metadata;
    rows->function = name;
    for (auto &entry : *metadata) {
        auto catalog_rows = BuildRows(entry, name, get.names, get.returned_types);
        for (auto &row : catalog_rows->rows) rows->rows.push_back(std::move(row));
    }
    if (rows->rows.empty()) return;
    TableFunction function("sqlctx_catalog_scan", {}, ScanDiscovery, nullptr, InitDiscovery);
    function.projection_pushdown = true;
    function.pushdown_complex_filter = RestrictDiscovery;
#if SQLCTX_BOUND_TABLE_FUNCTION
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
    unique_ptr<vector<SemanticMetadata>> metadata;
    OverlayScans(input, statement.plan, metadata);
}

} // namespace

bool SqlctxPhysicalRelationExists(ClientContext &context, const string &model, const string &catalog_name) {
    auto &catalog = Catalog::GetCatalog(context, catalog_name.empty()
        ? DatabaseManager::GetDefaultDatabase(context) : SqlctxIdentifier(catalog_name));
#if SQLCTX_NEW_IDENTIFIER_API
    EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
                          QualifiedName(catalog.GetName(), Identifier(SEMANTIC_SCHEMA), Identifier(model)));
    return Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL) != nullptr;
#else
    EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, model);
    return Catalog::GetEntry(context, catalog.GetName(), SEMANTIC_SCHEMA, lookup, OnEntryNotFound::RETURN_NULL) != nullptr;
#endif
}

bool SqlctxFindPhysicalRelation(ClientContext &context, const string &name, SqlctxPhysicalRelation &relation) {
    // Resolve exactly as an unqualified table reference would: temporary
    // objects and the search path, including views.
#if SQLCTX_NEW_IDENTIFIER_API
    EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(name)));
    auto entry = Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
#else
    EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, name);
    auto entry = Catalog::GetEntry(context, INVALID_CATALOG, INVALID_SCHEMA, lookup, OnEntryNotFound::RETURN_NULL);
#endif
    if (!entry) return false;
    relation.catalog = SqlctxName(entry->ParentCatalog().GetName());
    relation.schema = SqlctxName(entry->ParentSchema().name);
    relation.columns.clear();
    relation.all_columns = false;
    if (entry->type == CatalogType::TABLE_ENTRY) {
        for (auto &column : entry->Cast<TableCatalogEntry>().GetColumns().GetColumnNames()) {
            relation.columns.insert(column);
        }
    } else if (entry->type == CatalogType::VIEW_ENTRY) {
        auto &view = entry->Cast<ViewCatalogEntry>();
        auto info = view.GetColumnInfo();
        if (!info) {
            relation.all_columns = true;
            return true;
        }
        for (idx_t i = 0; i < info->names.size(); ++i) {
            relation.columns.insert(SqlctxName(i < view.aliases.size() ? view.aliases[i] : info->names[i]));
        }
    } else {
        relation.all_columns = true;
    }
    return true;
}

void RegisterSqlctxDiscovery(DatabaseInstance &db) {
    PlannerExtension extension;
    extension.post_bind_function = DiscoveryPostBind;
    PlannerExtension::Register(DBConfig::GetConfig(db), extension);
}

} // namespace duckdb
