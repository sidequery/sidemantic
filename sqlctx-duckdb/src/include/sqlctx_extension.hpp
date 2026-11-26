#pragma once

#include "duckdb.hpp"

namespace duckdb {

class SqlctxExtension : public Extension {
public:
    void Load(ExtensionLoader &loader) override;
    std::string Name() override { return "sqlctx"; }
    std::string Version() const override;
};

} // namespace duckdb
