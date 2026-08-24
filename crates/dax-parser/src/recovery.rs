//! Loss-tolerant parsing for editor and analysis tooling.
//!
//! The regular parser remains deliberately strict.  These entry points reuse it
//! one item at a time, retain every successfully parsed fragment, and skip to a
//! conservative synchronization token after an error.

use serde::Serialize;

use super::{
    Definition, Dialect, EvaluateStmt, Expr, LexError, Lexer, ParseError, Parser, Span, Token,
    TokenKind,
};

/// The parser phase that produced a recovery diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum RecoveryPhase {
    Lex,
    Parse,
}

/// A non-fatal problem encountered by a loss-tolerant parse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecoveryDiagnostic {
    pub phase: RecoveryPhase,
    pub message: String,
    pub span: Span,
}

impl From<LexError> for RecoveryDiagnostic {
    fn from(error: LexError) -> Self {
        Self {
            phase: RecoveryPhase::Lex,
            message: error.message,
            span: error.span,
        }
    }
}

impl From<ParseError> for RecoveryDiagnostic {
    fn from(error: ParseError) -> Self {
        Self {
            phase: RecoveryPhase::Parse,
            message: error.message,
            span: error.span,
        }
    }
}

/// An AST fragment that survived recovery, together with its source extent.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RecoveredItem<T> {
    pub value: T,
    pub span: Span,
}

/// Result shared by expression and query recovery entry points.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RecoveryResult<T> {
    pub items: Vec<RecoveredItem<T>>,
    pub diagnostics: Vec<RecoveryDiagnostic>,
}

impl<T> RecoveryResult<T> {
    pub fn is_clean(&self) -> bool {
        self.diagnostics.is_empty()
    }
}

/// Statement-sized query fragments retained independently after an error.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum RecoveredQueryItem {
    Definition(Definition),
    Evaluate(EvaluateStmt),
}

/// Parse an expression loss-tolerantly using the default dialect.
///
/// A valid formula produces one item.  Invalid input can produce smaller
/// expression fragments after commas or closing delimiters, which is useful to
/// editors that need an AST for the unaffected text around the cursor.
pub fn recover_expression(input: &str) -> RecoveryResult<Expr> {
    recover_expression_with_dialect(input, Dialect::default())
}

/// Parse an expression loss-tolerantly using `dialect`.
pub fn recover_expression_with_dialect(input: &str, dialect: Dialect) -> RecoveryResult<Expr> {
    let (tokens, mut diagnostics) = lex_recovering(input, dialect);
    let mut parser = Parser::new(tokens, dialect);
    let mut items = Vec::new();

    parser.skip_doc_comments();
    parser.eat(TokenKind::Eq);

    while !matches!(parser.peek().kind, TokenKind::Eof) {
        parser.skip_doc_comments();
        if matches!(parser.peek().kind, TokenKind::Eof) {
            break;
        }

        let start_index = parser.i;
        let start_span = parser.peek().span;
        match parser.parse_expr_bp(0) {
            Ok(value) => {
                items.push(RecoveredItem {
                    value,
                    span: consumed_span(&parser, start_index, start_span),
                });

                if !matches!(parser.peek().kind, TokenKind::Eof) {
                    diagnostics.push(RecoveryDiagnostic {
                        phase: RecoveryPhase::Parse,
                        message: "unexpected token after expression fragment".into(),
                        span: parser.peek().span,
                    });
                    synchronize_expression(&mut parser);
                }
            }
            Err(error) => {
                diagnostics.push(error.into());
                synchronize_expression(&mut parser);
            }
        }

        ensure_progress(&mut parser, start_index);
    }

    RecoveryResult { items, diagnostics }
}

/// Parse a query loss-tolerantly using the default dialect.
pub fn recover_query(input: &str) -> RecoveryResult<RecoveredQueryItem> {
    recover_query_with_dialect(input, Dialect::default())
}

/// Parse a query loss-tolerantly using `dialect`.
///
/// Definitions and `EVALUATE` statements are retained independently, so one
/// malformed statement does not discard later valid statements.
pub fn recover_query_with_dialect(
    input: &str,
    dialect: Dialect,
) -> RecoveryResult<RecoveredQueryItem> {
    let (tokens, mut diagnostics) = lex_recovering(input, dialect);
    let mut parser = Parser::new(tokens, dialect);
    let mut items = Vec::new();

    parser.skip_doc_comments();
    let had_define = parser.eat_kw("define");
    let mut in_define = had_define;
    let mut saw_definition = false;
    let mut saw_evaluate = false;

    while !matches!(parser.peek().kind, TokenKind::Eof) {
        parser.consume_stmt_terminators();
        let doc = parser.take_doc_comments();
        if matches!(parser.peek().kind, TokenKind::Eof) {
            break;
        }

        let start_index = parser.i;
        let start_span = parser.peek().span;

        let parsed = if parser.peek_kw("evaluate") {
            in_define = false;
            saw_evaluate = true;
            parser
                .parse_evaluate_stmt()
                .map(RecoveredQueryItem::Evaluate)
        } else if is_definition_starter(&parser) {
            saw_definition = true;
            if !in_define {
                diagnostics.push(RecoveryDiagnostic {
                    phase: RecoveryPhase::Parse,
                    message: "definition outside DEFINE block".into(),
                    span: start_span,
                });
            }
            parse_definition(&mut parser, doc).map(RecoveredQueryItem::Definition)
        } else {
            diagnostics.push(RecoveryDiagnostic {
                phase: RecoveryPhase::Parse,
                message: if in_define {
                    "expected MEASURE, FUNCTION, VAR, TABLE, COLUMN, or EVALUATE".into()
                } else {
                    "expected EVALUATE statement".into()
                },
                span: start_span,
            });
            synchronize_query(&mut parser);
            ensure_progress(&mut parser, start_index);
            continue;
        };

        match parsed {
            Ok(value) => items.push(RecoveredItem {
                value,
                span: consumed_span(&parser, start_index, start_span),
            }),
            Err(error) => {
                diagnostics.push(error.into());
                synchronize_query(&mut parser);
            }
        }

        ensure_progress(&mut parser, start_index);
    }

    let eof_span = parser.peek().span;
    if had_define && !saw_definition {
        diagnostics.push(RecoveryDiagnostic {
            phase: RecoveryPhase::Parse,
            message: "DEFINE block must contain at least one definition".into(),
            span: eof_span,
        });
    }
    if !saw_evaluate {
        diagnostics.push(RecoveryDiagnostic {
            phase: RecoveryPhase::Parse,
            message: "expected at least one EVALUATE statement".into(),
            span: eof_span,
        });
    }

    RecoveryResult { items, diagnostics }
}

fn lex_recovering(input: &str, dialect: Dialect) -> (Vec<Token>, Vec<RecoveryDiagnostic>) {
    let mut lexer = Lexer::new(input, dialect);
    let mut tokens = Vec::new();
    let mut diagnostics = Vec::new();

    loop {
        let before = lexer.idx;
        match lexer.next_token() {
            Ok(token) => {
                let eof = matches!(token.kind, TokenKind::Eof);
                tokens.push(token);
                if eof {
                    break;
                }
            }
            Err(error) => {
                diagnostics.push(error.into());
                // Most lexer errors consume their offending token.  Retain an
                // explicit guard because recovery must make progress even if a
                // future lexer branch reports before consuming input.
                if lexer.idx == before && lexer.bump_char().is_none() {
                    break;
                }
            }
        }
    }

    if !matches!(tokens.last().map(|token| &token.kind), Some(TokenKind::Eof)) {
        tokens.push(Token {
            kind: TokenKind::Eof,
            span: Span::new(input.len(), input.len()),
        });
    }

    (tokens, diagnostics)
}

fn parse_definition(parser: &mut Parser, doc: Option<String>) -> Result<Definition, ParseError> {
    if parser.peek_kw("measure") {
        parser.parse_define_measure(doc)
    } else if parser.peek_kw("function") {
        parser.parse_define_function(doc)
    } else if parser.peek_kw("var") {
        parser.parse_define_var(doc)
    } else if parser.peek_kw("table") {
        parser.parse_define_table(doc)
    } else {
        parser.parse_define_column(doc)
    }
}

fn is_definition_starter(parser: &Parser) -> bool {
    ["measure", "function", "var", "table", "column"]
        .iter()
        .any(|keyword| parser.peek_kw(keyword))
}

fn is_query_boundary(parser: &Parser) -> bool {
    is_definition_starter(parser)
        || parser.peek_kw("evaluate")
        || parser.peek_kw("order")
        || matches!(parser.peek().kind, TokenKind::Eof)
}

fn synchronize_expression(parser: &mut Parser) {
    while !matches!(parser.peek().kind, TokenKind::Eof) {
        if matches!(
            parser.peek().kind,
            TokenKind::Comma | TokenKind::Semicolon | TokenKind::RParen | TokenKind::RBrace
        ) {
            parser.bump();
            return;
        }

        if is_query_boundary(parser) {
            // Formula recovery has no statement-level construct to resume, so
            // consume the boundary itself before looking for another fragment.
            parser.bump();
            return;
        }

        parser.bump();
    }
}

fn synchronize_query(parser: &mut Parser) {
    while !matches!(parser.peek().kind, TokenKind::Eof) {
        if is_query_boundary(parser) {
            return;
        }

        if matches!(
            parser.peek().kind,
            TokenKind::Comma | TokenKind::Semicolon | TokenKind::RParen | TokenKind::RBrace
        ) {
            parser.bump();
            return;
        }

        parser.bump();
    }
}

fn consumed_span(parser: &Parser, start_index: usize, fallback: Span) -> Span {
    let end = parser
        .i
        .checked_sub(1)
        .and_then(|index| parser.tokens.get(index))
        .map_or(fallback.end, |token| token.span.end);
    let start = parser
        .tokens
        .get(start_index)
        .map_or(fallback.start, |token| token.span.start);
    Span::new(start, end.max(start))
}

fn ensure_progress(parser: &mut Parser, previous_index: usize) {
    if parser.i == previous_index && !matches!(parser.peek().kind, TokenKind::Eof) {
        parser.bump();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expression_recovery_keeps_fragments_after_lex_and_parse_errors() {
        let result = recover_expression("SUM(1 +, 2) #, 3");

        assert!(result.diagnostics.len() >= 2);
        assert!(result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.phase == RecoveryPhase::Lex));
        assert!(result
            .items
            .iter()
            .any(|item| item.value == Expr::Number("2".into())));
        assert!(result
            .items
            .iter()
            .any(|item| item.value == Expr::Number("3".into())));
    }

    #[test]
    fn query_recovery_retains_later_definitions_and_evaluates() {
        let result = recover_query(
            "DEFINE\n\
             MEASURE [Bad] = 1 + ;\n\
             MEASURE [Good] = 2;\n\
             EVALUATE {1, };\n\
             EVALUATE {2}",
        );

        assert!(result.diagnostics.len() >= 2);
        assert!(result.items.iter().any(|item| matches!(
            &item.value,
            RecoveredQueryItem::Definition(Definition::Measure { name, .. }) if name == "Good"
        )));
        assert!(result.items.iter().any(|item| matches!(
            &item.value,
            RecoveredQueryItem::Evaluate(EvaluateStmt {
                expr: Expr::TableConstructor(rows),
                ..
            }) if rows == &vec![vec![Expr::Number("2".into())]]
        )));
    }

    #[test]
    fn repeated_synchronization_tokens_always_make_progress() {
        let expression = recover_expression(",,,))}};;");
        assert!(!expression.diagnostics.is_empty());

        let query = recover_query("DEFINE , ) } ; EVALUATE {1}");
        assert!(query
            .items
            .iter()
            .any(|item| matches!(&item.value, RecoveredQueryItem::Evaluate(_))));
    }

    #[test]
    fn clean_inputs_remain_clean() {
        let expression = recover_expression("= 1 + 2");
        assert!(expression.is_clean());
        assert_eq!(expression.items.len(), 1);

        let query = recover_query("DEFINE MEASURE [M] = 1 EVALUATE { [M] }");
        assert!(query.is_clean());
        assert_eq!(query.items.len(), 2);
    }
}
