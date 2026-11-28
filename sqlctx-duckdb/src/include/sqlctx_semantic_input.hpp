#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
void RegisterSqlctxSemanticInput(ExtensionLoader &loader);
} // namespace duckdb
