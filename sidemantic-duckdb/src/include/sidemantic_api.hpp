#pragma once

#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {
ParserExtensionPlanResult PlanSidemanticCatalog(const string &kind, const string &model, const string &metric);
}
