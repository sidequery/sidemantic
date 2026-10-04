#pragma once

#include "duckdb.hpp"

namespace duckdb {

class SidemanticExtension : public Extension {
public:
    void Load(ExtensionLoader &loader) override;
    std::string Name() override { return "sidemantic"; }
    std::string Version() const override;
};

} // namespace duckdb
