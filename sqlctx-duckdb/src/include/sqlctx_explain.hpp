#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
void RegisterSqlctxExplain(ExtensionLoader &loader);
}
