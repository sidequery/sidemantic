#include "sidemantic_api.hpp"
#include "sidemantic_catalog.hpp"
#include "sidemantic_compat.hpp"
#include "sidemantic.h"

#include "duckdb/common/error_data.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {
namespace {

struct CatalogBindData : TableFunctionData {
    string kind;
    string model;
    string metric;
    // Result types of fields without a declared data type, by qualified name.
    case_insensitive_map_t<string> result_types;
};

struct CatalogState : GlobalTableFunctionState {
    vector<vector<Value>> rows;
    idx_t offset = 0;
};

// Report the type a field produces, as information_schema does, when its
// definition declares none. Fields whose source cannot bind stay NULL.
static void BindResultTypes(ClientContext &context, CatalogBindData &data) {
    auto snapshot = ReadSidemanticCatalog(context);
    if (snapshot.payload.empty()) return;
    struct CatalogResult {
        SidemanticCatalogEntries result;
        ~CatalogResult() { sidemantic_free_catalog_entries(result); }
    } result {sidemantic_snapshot_catalog(snapshot.payload.c_str(), data.kind.c_str(), data.model.c_str(),
                                          data.metric.empty() ? nullptr : data.metric.c_str())};
    if (result.result.error) return;
    for (idx_t i = 0; i < result.result.count; ++i) {
        auto &entry = result.result.entries[i];
        string kind(entry.kind);
        if (entry.data_type || !entry.model_name || (kind != "dimension" && kind != "metric")) continue;
        try {
            data.result_types[entry.qualified_name] =
                SidemanticFieldType(context, snapshot.payload, entry.model_name, entry.name).ToString();
        } catch (const std::exception &exception) {
            auto error = ErrorData(exception);
            if (error.Type() == ExceptionType::INTERRUPT || error.Type() == ExceptionType::INTERNAL ||
                error.Type() == ExceptionType::FATAL) throw;
        }
    }
}

static unique_ptr<FunctionData> CatalogBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &types, vector<SidemanticIdentifier> &names) {
    if (input.binder) RegisterSidemanticCatalogRead(context, input.binder->GetStatementProperties());
    auto data = make_uniq<CatalogBindData>();
    data->kind = input.inputs[0].GetValue<string>();
    data->model = input.inputs[1].GetValue<string>();
    data->metric = input.inputs[2].GetValue<string>();
    if (data->kind == "export") {
        names.emplace_back("definition");
        types.emplace_back(LogicalType::VARCHAR);
        return std::move(data);
    }
    for (auto name : {"kind", "model_name", "name", "qualified_name", "label", "description", "type", "data_type",
                      "sql", "aggregation", "target_model", "relationship_type", "granularity"}) {
        names.emplace_back(name);
        types.emplace_back(LogicalType::VARCHAR);
    }
    names.emplace_back("is_public");
    types.emplace_back(LogicalType::BOOLEAN);
    names.emplace_back("definition");
    types.emplace_back(LogicalType::VARCHAR);
    BindResultTypes(context, *data);
    return std::move(data);
}

static unique_ptr<GlobalTableFunctionState> CatalogInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &data = input.bind_data->Cast<CatalogBindData>();
    auto snapshot = ReadSidemanticCatalog(context);
    auto state = make_uniq<CatalogState>();
    if (data.kind == "export") {
        auto result = sidemantic_snapshot_export(snapshot.payload.c_str());
        if (result.error) {
            string error(result.error);
            sidemantic_free_result(result);
            throw InvalidInputException("Sidemantic: %s", SidemanticErrorText(error));
        }
        string definition(result.sql);
        sidemantic_free_result(result);
        state->rows.push_back({Value(definition)});
        return std::move(state);
    }
    struct CatalogResult {
        SidemanticCatalogEntries result;
        ~CatalogResult() { sidemantic_free_catalog_entries(result); }
    } result {sidemantic_snapshot_catalog(snapshot.payload.c_str(), data.kind.c_str(), data.model.c_str(),
                                          data.metric.empty() ? nullptr : data.metric.c_str())};
    if (result.result.error) throw InvalidInputException("Sidemantic: %s", SidemanticErrorText(result.result.error));
    for (idx_t i = 0; i < result.result.count; ++i) {
        auto &entry = result.result.entries[i];
        vector<Value> row;
        for (auto value : {entry.kind, entry.model_name, entry.name, entry.qualified_name, entry.label,
                          entry.description, entry.semantic_type, entry.data_type, entry.sql, entry.aggregation,
                          entry.target_model, entry.relationship_type, entry.granularity}) {
            row.push_back(value ? Value(value) : Value(LogicalType::VARCHAR));
        }
        auto result_type = data.result_types.find(entry.qualified_name);
        if (!entry.data_type && result_type != data.result_types.end()) row[7] = Value(result_type->second);
        row.push_back(Value::BOOLEAN(entry.is_public));
        row.push_back(Value(entry.definition));
        state->rows.push_back(std::move(row));
    }
    return std::move(state);
}

static void CatalogFunction(ClientContext &, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<CatalogState>();
    auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.offset);
    for (idx_t row = 0; row < count; ++row) {
        auto &values = state.rows[state.offset + row];
        for (idx_t column = 0; column < values.size(); ++column) output.SetValue(column, row, values[column]);
    }
    state.offset += count;
    output.SetCardinality(count);
}

} // namespace

ParserExtensionPlanResult PlanSidemanticCatalog(const string &kind, const string &model, const string &metric) {
    // Parser-owned lowering only: these functions are never registered in the
    // SQL function catalog. Users inspect definitions with SHOW and DESCRIBE.
    ParserExtensionPlanResult result;
    result.function = TableFunction("sidemantic_catalog_scan",
                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   CatalogFunction, CatalogBind, CatalogInit);
    result.parameters = {Value(kind), Value(model), Value(metric)};
    result.return_type = StatementReturnType::QUERY_RESULT;
    return result;
}

} // namespace duckdb
