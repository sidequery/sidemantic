#include "sqlctx_parser.hpp"

#if SQLCTX_GRAMMAR_EXTENSION
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/grammar_extension.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {
namespace {

// The native grammar owns statement structure, quoting, comments and balanced
// definition bodies. Rust remains the shared authority for model properties.
class DefinitionAtomMatcher final : public AtomicMatcher {
public:
    DefinitionAtomMatcher() : AtomicMatcher(MatcherType::CUSTOM) {}
    MatcherResult MatchAtomic(MatchState &state) const override {
        auto token = state.token_iterator.Current();
        if (!token || token->type == TokenType::END_OF_INPUT || token->type == TokenType::TOKEN_ERROR ||
            token->text == ";" || token->text == "(" || token->text == ")" || token->text == "[" ||
            token->text == "]" || token->text == "{" || token->text == "}") {
            return MatcherResult::Failure();
        }
        auto result = state.AllocateParseResult<KeywordParseResult>(token->text, token->offset, token->length);
        state.token_iterator.Advance();
        state.UpdateMaxTokenIndex();
        return result;
    }
    SuggestionType AddSuggestionInternal(MatchState &) const override { return SuggestionType::OPTIONAL; }
    string ToString() const override { return "semantic definition property"; }
};

static unique_ptr<TransformResultValue> TransformDefinition(PEGTransformer &transformer, ParseResult &result) {
    auto location = result.GetLocation();
    string source(location.length, ' ');
    // Preserve token positions and quoted source; comments are intentionally
    // omitted before passing the definition to the shared configuration parser.
    for (idx_t i = 0; i < transformer.token_iterator.Size(); ++i) {
        auto &token = transformer.token_iterator.GetToken(i);
        if (token.offset < location.offset || token.offset >= location.End() || token.type == TokenType::COMMENT) continue;
        if (token.preceded_by_newline && token.offset > location.offset) {
            // Compact model declarations use line breaks to separate fields.
            source[token.offset - location.offset - 1] = '\n';
        }
        source.replace(token.offset - location.offset, token.length, token.text);
    }
    auto definition = ParseSqlctxDefinition(source);
    if (!definition) throw InternalException("sqlctx grammar produced an invalid declaration");
    return make_uniq<TypedTransformResult<unique_ptr<SQLStatement>>>(SqlctxStatement(std::move(definition)));
}

static unique_ptr<TransformProcess> StartDefinition(PEGTransformer &transformer, ParseResult &result) {
    return make_uniq<FinalizeTransformProcess>(transformer, result, TransformDefinition);
}

static unique_ptr<TransformResultValue> TransformSemanticQuery(PEGTransformer &transformer, ParseResult &result) {
    auto &list = result.Cast<ListParseResult>();
    auto &choice = list.Child<ListParseResult>(1).Child<ChoiceParseResult>(0);
    auto statement = transformer.Transform<unique_ptr<SQLStatement>>(choice.GetResult());
    return make_uniq<TypedTransformResult<unique_ptr<SQLStatement>>>(WrapSqlctxQuery(std::move(statement), true));
}

static unique_ptr<TransformProcess> StartSemanticQuery(PEGTransformer &transformer, ParseResult &result) {
    return make_uniq<FinalizeTransformProcess>(transformer, result, TransformSemanticQuery);
}

static unique_ptr<SQLStatement> TransformPreparable(PEGTransformer &transformer, ParseResult &result) {
    auto &choice = result.Cast<ListParseResult>().Child<ChoiceParseResult>(0);
    return transformer.Transform<unique_ptr<SQLStatement>>(choice.GetResult());
}

static unique_ptr<TransformResultValue> TransformPrepare(PEGTransformer &transformer, ParseResult &result) {
    auto &list = result.Cast<ListParseResult>();
    auto statement = make_uniq<PrepareStatement>();
    statement->name = transformer.Transform<Identifier>(list.GetChild(1));
    statement->statement = TransformPreparable(transformer, list.GetChild(3));
    statement->statement->named_param_map = transformer.named_parameter_map;
    statement->statement->has_anonymous_parameters = transformer.has_anonymous_parameters;
    transformer.ClearParameters();
    unique_ptr<SQLStatement> prepared = std::move(statement);
    return make_uniq<TypedTransformResult<unique_ptr<SQLStatement>>>(std::move(prepared));
}

static unique_ptr<TransformProcess> StartPrepare(PEGTransformer &transformer, ParseResult &result) {
    return make_uniq<FinalizeTransformProcess>(transformer, result, TransformPrepare);
}

static unique_ptr<TransformResultValue> TransformExplain(PEGTransformer &transformer, ParseResult &result) {
    auto &list = result.Cast<ListParseResult>();
    optional<Identifier> analyze;
    auto &analyze_result = list.Child<OptionalParseResult>(1);
    if (analyze_result.HasResult()) analyze = transformer.Transform<Identifier>(analyze_result.GetResult());
    optional<vector<GenericCopyOption>> options;
    auto &options_result = list.Child<OptionalParseResult>(2);
    if (options_result.HasResult()) {
        options = transformer.Transform<vector<GenericCopyOption>>(options_result.GetResult());
    }
    auto inner = TransformPreparable(transformer, list.GetChild(3));
    auto statement = PEGTransformerFactory::TransformExplainStatement(transformer, analyze, options, std::move(inner));
    return make_uniq<TypedTransformResult<unique_ptr<SQLStatement>>>(WrapSqlctxQuery(std::move(statement)));
}

static unique_ptr<TransformProcess> StartExplain(PEGTransformer &transformer, ParseResult &result) {
    return make_uniq<FinalizeTransformProcess>(transformer, result, TransformExplain);
}

class SqlctxGrammar final : public GrammarExtension {
public:
    SqlctxGrammar() : GrammarExtension("sqlctx", "Semantic models, metrics, dimensions, segments and queries") {}
    vector<GrammarChange> GetChanges() const override {
        return {
            GrammarChange::AddRule("SqlctxAtom <- Identifier"),
            GrammarChange::AddTerminalRuleOverride("SqlctxAtom", [](const PEGKeywordHelper &) {
                return make_uniq<DefinitionAtomMatcher>();
            }),
            GrammarChange::AddRule("SqlctxBody <- SqlctxBlock / SqlctxList / SqlctxObject / SqlctxAtom"),
            GrammarChange::AddRule("SqlctxBlock <- '(' SqlctxBody* ')'"),
            GrammarChange::AddRule("SqlctxList <- '[' SqlctxBody* ']'"),
            GrammarChange::AddRule("SqlctxObject <- '{' SqlctxBody* '}'"),
            GrammarChange::AddRule("SqlctxSource <- ColIdOrString ('.' ColIdOrString)*"),
            GrammarChange::AddRule("SqlctxModel <- 'MODEL' (SqlctxBlock / ColIdOrString ('FROM' (SqlctxSource / SqlctxBlock) SqlctxBlock / SqlctxBlock)?)"),
            GrammarChange::AddRule("SqlctxItem <- ('METRIC' / 'DIMENSION' / 'SEGMENT') (SqlctxBlock / SqlctxSource ('AS' Expression / SqlctxBlock))"),
            GrammarChange::AddRule("SqlctxDefinition <- 'SEMANTIC'? ('CREATE' ('OR' 'REPLACE')?)? (SqlctxModel / SqlctxItem)", StartDefinition),
            GrammarChange::AddRule("SqlctxDrop <- 'SEMANTIC'? 'DROP' ('MODEL' / 'METRIC' / 'DIMENSION' / 'SEGMENT') ('IF' 'EXISTS')? SqlctxSource", StartDefinition),
            GrammarChange::AddRule("SqlctxShow <- 'SHOW' 'SEMANTIC'? ('MODELS' / 'METRICS' / 'DIMENSIONS' / 'SEGMENTS' / 'RELATIONSHIPS') (('FROM' ColIdOrString) / ('FOR' SqlctxSource))?", StartDefinition),
            GrammarChange::AddRule("SqlctxDescribe <- ('DESCRIBE' / 'DESC') 'MODEL' ColIdOrString", StartDefinition),
            GrammarChange::AddRule("SqlctxExport <- 'EXPORT' 'SEMANTIC' 'CATALOG'", StartDefinition),
            GrammarChange::AddRule("SqlctxImport <- 'IMPORT' 'SEMANTIC' 'CATALOG' ColIdOrString", StartDefinition),
            GrammarChange::AddRule("SqlctxQuery <- 'SEMANTIC' (SelectStatement / CreateStatement / InsertStatement)", StartSemanticQuery),
            GrammarChange::AddRule("SqlctxPreparable <- SqlctxShow / SqlctxDescribe / SqlctxExport / SqlctxImport / SqlctxDrop / SqlctxDefinition / SqlctxQuery"),
            GrammarChange::AddRule("SqlctxPrepare <- 'PREPARE' ColIdOrString 'AS' SqlctxPreparable", StartPrepare),
            GrammarChange::AddRule("SqlctxExplain <- 'EXPLAIN' AnalyzeKeyword? ExplainOptionList? SqlctxPreparable", StartExplain),
            GrammarChange::PrependChoice("Statement", "SqlctxDefinition"),
            GrammarChange::PrependChoice("Statement", "SqlctxDrop"),
            GrammarChange::PrependChoice("Statement", "SqlctxShow"),
            GrammarChange::PrependChoice("Statement", "SqlctxDescribe"),
            GrammarChange::PrependChoice("Statement", "SqlctxExport"),
            GrammarChange::PrependChoice("Statement", "SqlctxImport"),
            GrammarChange::PrependChoice("Statement", "SqlctxQuery"),
            GrammarChange::PrependChoice("Statement", "SqlctxPrepare"),
            GrammarChange::PrependChoice("Statement", "SqlctxExplain"),
        };
    }
};

struct SqlctxGrammarInfo : public ParserExtensionInfo {
    shared_ptr<CompiledGrammar> default_grammar;
    shared_ptr<CompiledGrammar> grammar;
};

} // namespace

shared_ptr<ParserExtensionInfo> RegisterSqlctxGrammar(DatabaseInstance &db) {
    auto extension = make_shared_ptr<SqlctxGrammar>();
    GrammarExtension::Register(db, extension);
    auto info = make_shared_ptr<SqlctxGrammarInfo>();
    info->default_grammar = db.GetParserCache().GetMatcher();
#if SQLCTX_GRAMMAR_EXTENSION_LIST
    info->grammar = CompiledGrammar::Create(vector<reference<GrammarExtension>> {*extension});
#else
    // Earlier 2.0 builds compile registered grammars by name for a client.
    Connection connection(db);
    info->grammar = CompiledGrammar::Create(*connection.context, case_insensitive_set_t {"sqlctx"});
#endif
    return info;
}

bool ParseSqlctxGrammar(ParserExtensionInfo *info, const string &sql, const ParserOptions &options,
                           vector<unique_ptr<SQLStatement>> &statements) {
    auto grammar_info = dynamic_cast<SqlctxGrammarInfo *>(info);
    if (!grammar_info) return false;
    auto native = options;
    native.extensions = nullptr;
    // Respect explicitly composed grammars. Callers may opt in with
    // SET active_grammar_extensions = ['sqlctx', ...].
    // Use the database's cached default: statically linked loadable extensions
    // can have a different DefaultGrammar() singleton from the host DuckDB.
    if (!native.compiled_grammar || native.compiled_grammar == grammar_info->default_grammar) {
        native.compiled_grammar = grammar_info->grammar;
    }
    Parser parser(native);
    parser.ParseQuery(sql);
    statements = std::move(parser.statements);
    return true;
}

} // namespace duckdb
#else
namespace duckdb {
shared_ptr<ParserExtensionInfo> RegisterSqlctxGrammar(DatabaseInstance &) { return nullptr; }
bool ParseSqlctxGrammar(ParserExtensionInfo *, const string &, const ParserOptions &,
                           vector<unique_ptr<SQLStatement>> &) { return false; }
} // namespace duckdb
#endif
