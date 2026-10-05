#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

struct SidemanticCatalogSnapshot {
    string payload;
    string active_model;
};

// Resolve definitions against the caller's current catalog transaction. Legacy
// sidecars are read only when no native snapshot exists and are never modified.
SidemanticCatalogSnapshot ReadSidemanticCatalog(ClientContext &context);

// Virtual semantic relations are exposed through the host's standard catalogs.
bool SidemanticPhysicalRelationExists(ClientContext &context, const string &model);
void RegisterSidemanticDiscovery(DatabaseInstance &db);

void RegisterSidemanticCatalogRead(ClientContext &context, StatementProperties &properties);
void RegisterSidemanticCatalogWrite(ClientContext &context, StatementProperties &properties);

// Supported operations: model, item, use, yaml, file.
void ExecuteSidemanticMutation(ClientContext &context, const string &operation,
                              const string &content, bool replace);

ParserExtensionPlanResult PlanSidemanticMutation(ClientContext &context, const string &operation,
                                                const string &content, bool replace,
                                                const string &result_message);

} // namespace duckdb
