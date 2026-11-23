#pragma once

#include "sqlctx_compat.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator_extension.hpp"

namespace duckdb {

struct SqlctxParseData : ParserExtensionParseData {
    // Definitions carry source only. Mutation is deferred until execution.
    string operation;
    string content;
    bool replace = false;
    bool explicit_semantic = false;
    unique_ptr<SQLStatement> statement;

    unique_ptr<ParserExtensionParseData> Copy() const override;
    string ToString() const override;
};

unique_ptr<SQLStatement> SqlctxStatement(unique_ptr<SqlctxParseData> data);
unique_ptr<SqlctxParseData> ParseSqlctxDefinition(const string &sql);
unique_ptr<SQLStatement> WrapSqlctxQuery(unique_ptr<SQLStatement> statement, bool explicit_semantic = false);

ParserOverrideResult sqlctx_parser_override(ParserExtensionInfo *, const string &, ParserOptions &);
#if SQLCTX_TOKEN_PARSE_FN
ParserExtensionParseResult sqlctx_parse(ParserExtensionInfo *, const vector<SimpleToken> &);
#else
ParserExtensionParseResult sqlctx_parse(ParserExtensionInfo *, const string &);
#endif
ParserExtensionPlanResult sqlctx_plan(ParserExtensionInfo *, ClientContext &,
                                         unique_ptr<ParserExtensionParseData>);
BoundStatement sqlctx_bind(ClientContext &, Binder &, OperatorExtensionInfo *, SQLStatement &);
void RegisterSqlctxRouting(DatabaseInstance &db);

struct SqlctxParserExtension : ParserExtension {
    SqlctxParserExtension() {
        parse_function = sqlctx_parse;
        plan_function = sqlctx_plan;
        parser_override = sqlctx_parser_override;
    }
};

struct SqlctxOperatorExtension : OperatorExtension {
    SqlctxOperatorExtension() { Bind = sqlctx_bind; }
    string GetName() override { return "sqlctx"; }
    unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &) override;
};

// On hosts with the native grammar API, LOAD registers a composable grammar.
// Parsing retains an explicitly selected caller grammar and never changes it.
shared_ptr<ParserExtensionInfo> RegisterSqlctxGrammar(DatabaseInstance &db);
bool ParseSqlctxGrammar(ParserExtensionInfo *, const string &, const ParserOptions &,
                           vector<unique_ptr<SQLStatement>> &statements);

} // namespace duckdb
