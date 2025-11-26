#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"

namespace duckdb {

// Main extension class
class SidemanticExtension : public Extension {
public:
    void Load(ExtensionLoader &loader) override;
    std::string Name() override { return "sidemantic"; }
    std::string Version() const override;
};

// Forward declarations
BoundStatement sidemantic_bind(ClientContext &context, Binder &binder,
                               OperatorExtensionInfo *info, SQLStatement &statement);

ParserExtensionParseResult sidemantic_parse(ParserExtensionInfo *,
                                            const std::string &query);

ParserExtensionPlanResult sidemantic_plan(ParserExtensionInfo *, ClientContext &,
                                          unique_ptr<ParserExtensionParseData>);

// Operator extension: handles binding after parsing
struct SidemanticOperatorExtension : public OperatorExtension {
    SidemanticOperatorExtension() : OperatorExtension() { Bind = sidemantic_bind; }
    std::string GetName() override { return "sidemantic"; }
    unique_ptr<LogicalExtensionOperator>
    Deserialize(Deserializer &deserializer) override {
        throw InternalException("sidemantic operator should not be serialized");
    }
};

// Parser extension: intercepts query strings
struct SidemanticParserExtension : public ParserExtension {
    SidemanticParserExtension() : ParserExtension() {
        parse_function = sidemantic_parse;
        plan_function = sidemantic_plan;
    }
};

// Container for parsed statement (passed between parse and bind phases)
struct SidemanticParseData : ParserExtensionParseData {
    unique_ptr<SQLStatement> statement;

    unique_ptr<ParserExtensionParseData> Copy() const override {
        return make_uniq_base<ParserExtensionParseData, SidemanticParseData>(
            statement->Copy());
    }
    string ToString() const override { return "SidemanticParseData"; }
    SidemanticParseData(unique_ptr<SQLStatement> statement)
        : statement(std::move(statement)) {}
};

// State stored in ClientContext between parse and bind
class SidemanticState : public ClientContextState {
public:
    explicit SidemanticState(unique_ptr<ParserExtensionParseData> parse_data)
        : parse_data(std::move(parse_data)) {}
    void QueryEnd() override { parse_data.reset(); }
    unique_ptr<ParserExtensionParseData> parse_data;
};

} // namespace duckdb
