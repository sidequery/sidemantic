#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

struct SqlctxCatalogSnapshot {
    string payload;
    string active_model;
};

// Resolve definitions against the caller's current catalog transaction. Legacy
// sidecars are read only when no native snapshot exists and are never modified.
SqlctxCatalogSnapshot ReadSqlctxCatalog(ClientContext &context);

// Virtual semantic relations are exposed through the host's standard catalogs.
bool SqlctxPhysicalRelationExists(ClientContext &context, const string &model);

struct SqlctxPhysicalRelation {
    string catalog;
    string schema;
    case_insensitive_set_t columns;
    // Columns are unknown (e.g. an unbound view); treat every name as physical.
    bool all_columns = false;

    bool HasColumn(const string &name) const { return all_columns || columns.count(name); }
};

// Rust errors arrive as "Error: [context: ]Validation error: ..."; drop the labels.
string SqlctxErrorText(string error);

// The result type of one semantic field, bound against the current database.
LogicalType SqlctxFieldType(ClientContext &context, const string &snapshot, const string &model,
                                const string &field);

// The table or view an unqualified name binds to natively, if any.
bool SqlctxFindPhysicalRelation(ClientContext &context, const string &name, SqlctxPhysicalRelation &relation);
void RegisterSqlctxDiscovery(DatabaseInstance &db);

void RegisterSqlctxCatalogRead(ClientContext &context, StatementProperties &properties);
void RegisterSqlctxCatalogWrite(ClientContext &context, StatementProperties &properties);

// Supported operations: model, item, use, yaml, file. Returns a summary of
// the definitions it created, replaced or dropped.
string ExecuteSqlctxMutation(ClientContext &context, const string &operation,
                                 const string &content, bool replace);

ParserExtensionPlanResult PlanSqlctxMutation(ClientContext &context, const string &operation,
                                                const string &content, bool replace);

} // namespace duckdb
