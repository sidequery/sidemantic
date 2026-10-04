#pragma once

#include "sidemantic_compat.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator_extension.hpp"

namespace duckdb {

struct SidemanticParseData : ParserExtensionParseData {
    // Definitions carry source only. Mutation is deferred until execution.
    string operation;
    string content;
    bool replace = false;
    bool explicit_semantic = false;
    unique_ptr<SQLStatement> statement;

    unique_ptr<ParserExtensionParseData> Copy() const override;
    string ToString() const override;
};

unique_ptr<SQLStatement> SidemanticStatement(unique_ptr<SidemanticParseData> data);
unique_ptr<SidemanticParseData> ParseSidemanticDefinition(const string &sql);
unique_ptr<SQLStatement> WrapSidemanticQuery(unique_ptr<SQLStatement> statement, bool explicit_semantic = false);

ParserOverrideResult sidemantic_parser_override(ParserExtensionInfo *, const string &, ParserOptions &);
#if SIDEMANTIC_TOKEN_PARSE_FN
ParserExtensionParseResult sidemantic_parse(ParserExtensionInfo *, const vector<SimpleToken> &);
#else
ParserExtensionParseResult sidemantic_parse(ParserExtensionInfo *, const string &);
#endif
ParserExtensionPlanResult sidemantic_plan(ParserExtensionInfo *, ClientContext &,
                                         unique_ptr<ParserExtensionParseData>);
BoundStatement sidemantic_bind(ClientContext &, Binder &, OperatorExtensionInfo *, SQLStatement &);

struct SidemanticParserExtension : ParserExtension {
    SidemanticParserExtension() {
        parse_function = sidemantic_parse;
        plan_function = sidemantic_plan;
        parser_override = sidemantic_parser_override;
    }
};

struct SidemanticOperatorExtension : OperatorExtension {
    SidemanticOperatorExtension() { Bind = sidemantic_bind; }
    string GetName() override { return "sidemantic"; }
    unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &) override;
};

// On hosts with the native grammar API, LOAD registers a composable grammar.
// Parsing retains an explicitly selected caller grammar and never changes it.
shared_ptr<ParserExtensionInfo> RegisterSidemanticGrammar(DatabaseInstance &db);
bool ParseSidemanticGrammar(ParserExtensionInfo *, const string &, const ParserOptions &,
                           vector<unique_ptr<SQLStatement>> &statements);

} // namespace duckdb
