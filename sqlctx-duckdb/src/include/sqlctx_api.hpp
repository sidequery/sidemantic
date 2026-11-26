#pragma once

#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {
ParserExtensionPlanResult PlanSqlctxCatalog(const string &kind, const string &model, const string &metric);
}
