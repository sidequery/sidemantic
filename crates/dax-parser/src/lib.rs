//! dax_parser.rs — single-file DAX lexer+parser (expressions + basic queries)
//!
//! Patch highlights vs prior version:
//! - Fixes operator precedence per MS docs: `^` binds tighter than unary sign, comparisons bind tighter than `NOT`
//! - Adds `==` strict equality token/op
//! - Supports numeric literals starting with `.` (e.g. `.20`)
//! - Adds `@param` tokens/AST (needed for START AT params)
//! - Enforces START AT rules: requires ORDER BY, args must be constant or @param, count <= order keys
//! - Adds DEFINE FUNCTION (UDF) parsing, including parameter defaults and `///` doc comments
//! - Accepts optional semicolon statement terminators (between DEFINE entities / EVALUATE statements)
//!
//! Drop into `src/lib.rs` (or any module) and `cargo test`.
//! No external deps.

use serde::Serialize;
use std::fmt;

mod formatter;
mod function_catalog;
mod model_validation;
mod recovery;
mod validation;
pub use formatter::*;
pub use function_catalog::*;
pub use model_validation::*;
pub use recovery::*;
pub use validation::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Span {
    pub start: usize,
    pub end: usize, // half-open [start, end)
}
impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

/// The kind of AST construct identified by an entry in [`LosslessParse::nodes`].
///
/// Entries are emitted in parser construction order (children before parents where
/// applicable). Together with their byte spans this is an intentionally lightweight,
/// non-breaking alternative to storing source locations on every existing AST variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum AstNodeKind {
    Expression,
    Definition,
    DefineBlock,
    Evaluate,
    Query,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AstNodeSpan {
    pub kind: AstNodeKind,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum CommentKind {
    DashLine,
    SlashLine,
    Block,
    DocLine,
}

/// A source comment retained by the lossless parsing APIs.
///
/// `previous_node` and `next_node` index [`LosslessParse::nodes`] and provide stable
/// neighbouring anchors without forcing a formatter to adopt one attachment policy.
/// The exact spelling and whitespace remain recoverable from `source[span]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceComment {
    pub kind: CommentKind,
    pub span: Span,
    pub text: String,
    pub previous_node: Option<usize>,
    pub next_node: Option<usize>,
    pub containing_node: Option<usize>,
}

/// Parsed AST plus source locations and comments, while leaving the established AST and
/// parsing entrypoints source-compatible.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LosslessParse<T> {
    pub ast: T,
    pub source: String,
    pub span: Span,
    pub nodes: Vec<AstNodeSpan>,
    pub comments: Vec<SourceComment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LexError {
    pub message: String,
    pub span: Span,
}
impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at {}..{}",
            self.message, self.span.start, self.span.end
        )
    }
}
impl std::error::Error for LexError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ParseError {
    pub message: String,
    pub span: Span,
}
impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at {}..{}",
            self.message, self.span.start, self.span.end
        )
    }
}
impl std::error::Error for ParseError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum DaxError {
    Lex(LexError),
    Parse(ParseError),
}
impl fmt::Display for DaxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DaxError::Lex(e) => write!(f, "lex error: {e}"),
            DaxError::Parse(e) => write!(f, "parse error: {e}"),
        }
    }
}
impl std::error::Error for DaxError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Dialect {
    /// Accept `;` as argument separator (in addition to `,`)
    pub allow_semicolon_separators: bool,
    /// Accept `,` as decimal separator inside numeric literals (in addition to `.`)
    pub allow_decimal_comma: bool,
    /// Accept `--` as a line comment starter
    pub allow_dash_dash_comments: bool,
    /// Accept `//` as a line comment starter
    pub allow_double_slash_comments: bool,
    /// Accept `/* ... */` block comments
    pub allow_block_comments: bool,
}
impl Default for Dialect {
    fn default() -> Self {
        Self {
            allow_semicolon_separators: true,
            allow_decimal_comma: false, // canonical DAX is `.` decimal; flip if you want locale-tolerant
            allow_dash_dash_comments: true,
            allow_double_slash_comments: true,
            allow_block_comments: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum TokenKind {
    // trivia-ish
    DocComment(String), // `/// ...` (kept so DEFINE FUNCTION can attach doc)

    // atoms
    Ident(String),        // includes keywords (contextual)
    Param(String),        // @paramName (START AT)
    Number(String),       // raw numeric literal text
    String(String),       // decoded double-quoted literal
    DateTime(String),     // decoded `dt"YYYY-MM-DDThh:mm:ss"` literal
    QuotedIdent(String),  // decoded single-quoted identifier (e.g. 'Sales')
    BracketIdent(String), // decoded bracket identifier (e.g. [Total Sales])

    // punctuation
    LParen,
    RParen,
    LBrace,
    RBrace,
    Comma,
    Semicolon,
    Dot,
    Colon,

    // operators / punct
    Arrow, // =>

    Plus,
    Minus,
    Star,
    Slash,
    Caret,
    Amp, // concatenation (&)

    Eq,
    EqEq, // ==
    Neq,  // <>
    Lt,
    Lte,
    Gt,
    Gte,

    AndAnd, // &&
    OrOr,   // ||

    Eof,
}

pub struct Lexer<'a> {
    input: &'a str,
    idx: usize,
    dialect: Dialect,
}
impl<'a> Lexer<'a> {
    pub fn new(input: &'a str, dialect: Dialect) -> Self {
        Self {
            input,
            idx: 0,
            dialect,
        }
    }

    pub fn lex_all(mut self) -> Result<Vec<Token>, LexError> {
        let mut out = Vec::new();
        loop {
            let tok = self.next_token()?;
            let is_eof = matches!(tok.kind, TokenKind::Eof);
            out.push(tok);
            if is_eof {
                break;
            }
        }
        Ok(out)
    }

    fn len(&self) -> usize {
        self.input.len()
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.idx..].chars().next()
    }

    fn peek_byte(&self) -> Option<u8> {
        self.input.as_bytes().get(self.idx).copied()
    }

    fn peek_byte_n(&self, n: usize) -> Option<u8> {
        self.input.as_bytes().get(self.idx + n).copied()
    }

    fn bump_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.idx += ch.len_utf8();
        Some(ch)
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() {
                self.bump_char();
            } else {
                break;
            }
        }
    }

    fn skip_line_comment(&mut self) {
        while let Some(ch) = self.peek_char() {
            self.bump_char();
            if ch == '\n' {
                break;
            }
        }
    }

    fn skip_block_comment(&mut self) -> Result<(), LexError> {
        // assumes current is '/' and next is '*'
        let start = self.idx;
        self.bump_char(); // /
        self.bump_char(); // *
        while self.idx < self.len() {
            if self.peek_byte() == Some(b'*') && self.peek_byte_n(1) == Some(b'/') {
                self.bump_char(); // *
                self.bump_char(); // /
                return Ok(());
            }
            self.bump_char();
        }
        Err(LexError {
            message: "unterminated block comment".into(),
            span: Span::new(start, self.idx),
        })
    }

    fn lex_doc_comment(&mut self) -> (String, usize) {
        // assumes current bytes are "///"
        debug_assert_eq!(self.peek_byte(), Some(b'/'));
        self.bump_char();
        self.bump_char();
        self.bump_char();

        let mut out = String::new();
        while let Some(ch) = self.peek_char() {
            if ch == '\n' {
                break;
            }
            self.bump_char();
            out.push(ch);
        }

        (out.trim().to_string(), self.idx)
    }

    fn lex_param(&mut self) -> Result<(String, usize), LexError> {
        // @paramName - we accept [A-Za-z0-9_]+ after '@' to be permissive.
        let start = self.idx;
        debug_assert_eq!(self.peek_char(), Some('@'));
        self.bump_char(); // @

        let mut out = String::new();
        while let Some(ch) = self.peek_char() {
            if ch.is_alphanumeric() || ch == '_' {
                self.bump_char();
                out.push(ch);
            } else {
                break;
            }
        }

        if out.is_empty() {
            return Err(LexError {
                message: "expected parameter name after '@'".into(),
                span: Span::new(start, self.idx),
            });
        }

        Ok((out, self.idx))
    }

    fn next_token(&mut self) -> Result<Token, LexError> {
        loop {
            self.skip_whitespace();

            let start = self.idx;
            if start >= self.len() {
                return Ok(Token {
                    kind: TokenKind::Eof,
                    span: Span::new(start, start),
                });
            }

            // doc comment: /// ...
            if self.dialect.allow_double_slash_comments
                && self.peek_byte() == Some(b'/')
                && self.peek_byte_n(1) == Some(b'/')
                && self.peek_byte_n(2) == Some(b'/')
            {
                let (text, end) = self.lex_doc_comment();
                return Ok(Token {
                    kind: TokenKind::DocComment(text),
                    span: Span::new(start, end),
                });
            }

            // comments (ASCII-only starters, but idx is always at char boundary)
            match (self.peek_byte(), self.peek_byte_n(1)) {
                // `--` is a canonical DAX single-line comment (runs to end of line), like `//`.
                // It is always a comment regardless of the following character, matching Power BI /
                // Tabular's documented lexing; the rare contiguous double-unary-minus idiom is left
                // to the comment rather than guessed at.
                (Some(b'-'), Some(b'-')) if self.dialect.allow_dash_dash_comments => {
                    self.skip_line_comment();
                    continue;
                }
                (Some(b'/'), Some(b'/')) if self.dialect.allow_double_slash_comments => {
                    self.skip_line_comment();
                    continue;
                }
                (Some(b'/'), Some(b'*')) if self.dialect.allow_block_comments => {
                    self.skip_block_comment()?;
                    continue;
                }
                _ => {}
            }

            // ISO 8601 datetime literal: dt"YYYY-MM-DDThh:mm:ss". DAX is
            // case-insensitive, so accept any casing of the `dt` prefix.
            if matches!(self.peek_byte(), Some(b'd' | b'D'))
                && matches!(self.peek_byte_n(1), Some(b't' | b'T'))
                && self.peek_byte_n(2) == Some(b'"')
            {
                self.bump_char(); // d
                self.bump_char(); // t
                let (value, end) = self.lex_string_literal().map_err(|err| LexError {
                    message: "unterminated datetime literal".into(),
                    span: Span::new(start, err.span.end),
                })?;
                if !is_valid_datetime_literal(&value) {
                    return Err(LexError {
                        message: "invalid datetime literal; expected dt\"YYYY-M-D\", dt\"YYYY-M-DThh:mm:ss\", or dt\"YYYY-M-D hh:mm:ss\""
                            .into(),
                        span: Span::new(start, end),
                    });
                }
                return Ok(Token {
                    kind: TokenKind::DateTime(value),
                    span: Span::new(start, end),
                });
            }

            // punctuation/operators (prefer 2-char where relevant)
            match (self.peek_byte(), self.peek_byte_n(1)) {
                (Some(b'='), Some(b'=')) => {
                    self.bump_char();
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::EqEq,
                        span: Span::new(start, self.idx),
                    });
                }
                (Some(b'='), Some(b'>')) => {
                    self.bump_char();
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Arrow,
                        span: Span::new(start, self.idx),
                    });
                }
                (Some(b'&'), Some(b'&')) => {
                    self.bump_char();
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::AndAnd,
                        span: Span::new(start, self.idx),
                    });
                }
                (Some(b'|'), Some(b'|')) => {
                    self.bump_char();
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::OrOr,
                        span: Span::new(start, self.idx),
                    });
                }
                (Some(b'<'), Some(b'>')) => {
                    self.bump_char();
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Neq,
                        span: Span::new(start, self.idx),
                    });
                }
                (Some(b'<'), Some(b'=')) => {
                    self.bump_char();
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Lte,
                        span: Span::new(start, self.idx),
                    });
                }
                (Some(b'>'), Some(b'=')) => {
                    self.bump_char();
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Gte,
                        span: Span::new(start, self.idx),
                    });
                }
                _ => {}
            }

            // single-char tokens
            let ch = self.peek_char().unwrap();
            match ch {
                '(' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::LParen,
                        span: Span::new(start, self.idx),
                    });
                }
                ')' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::RParen,
                        span: Span::new(start, self.idx),
                    });
                }
                '{' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::LBrace,
                        span: Span::new(start, self.idx),
                    });
                }
                '}' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::RBrace,
                        span: Span::new(start, self.idx),
                    });
                }
                ',' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Comma,
                        span: Span::new(start, self.idx),
                    });
                }
                ';' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Semicolon,
                        span: Span::new(start, self.idx),
                    });
                }
                ':' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Colon,
                        span: Span::new(start, self.idx),
                    });
                }
                '@' => {
                    let (name, end) = self.lex_param()?;
                    return Ok(Token {
                        kind: TokenKind::Param(name),
                        span: Span::new(start, end),
                    });
                }
                '+' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Plus,
                        span: Span::new(start, self.idx),
                    });
                }
                '-' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Minus,
                        span: Span::new(start, self.idx),
                    });
                }
                '*' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Star,
                        span: Span::new(start, self.idx),
                    });
                }
                '/' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Slash,
                        span: Span::new(start, self.idx),
                    });
                }
                '^' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Caret,
                        span: Span::new(start, self.idx),
                    });
                }
                '&' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Amp,
                        span: Span::new(start, self.idx),
                    });
                }
                '=' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Eq,
                        span: Span::new(start, self.idx),
                    });
                }
                '<' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Lt,
                        span: Span::new(start, self.idx),
                    });
                }
                '>' => {
                    self.bump_char();
                    return Ok(Token {
                        kind: TokenKind::Gt,
                        span: Span::new(start, self.idx),
                    });
                }
                '"' => {
                    let (s, end) = self.lex_string_literal()?;
                    return Ok(Token {
                        kind: TokenKind::String(s),
                        span: Span::new(start, end),
                    });
                }
                '\'' => {
                    let (s, end) = self.lex_single_quoted_ident()?;
                    return Ok(Token {
                        kind: TokenKind::QuotedIdent(s),
                        span: Span::new(start, end),
                    });
                }
                '[' => {
                    let (s, end) = self.lex_bracket_ident()?;
                    return Ok(Token {
                        kind: TokenKind::BracketIdent(s),
                        span: Span::new(start, end),
                    });
                }
                _ => {}
            }

            // number?
            if ch.is_ascii_digit()
                || (ch == '.' && matches!(self.peek_byte_n(1), Some(b'0'..=b'9')))
            {
                let end = self.lex_number()?;
                let raw = self.input[start..end].to_string();
                return Ok(Token {
                    kind: TokenKind::Number(raw),
                    span: Span::new(start, end),
                });
            }

            if ch == '.' {
                self.bump_char();
                return Ok(Token {
                    kind: TokenKind::Dot,
                    span: Span::new(start, self.idx),
                });
            }

            // identifier?
            if is_ident_start(ch) {
                let end = self.lex_ident()?;
                let raw = self.input[start..end].to_string();
                return Ok(Token {
                    kind: TokenKind::Ident(raw),
                    span: Span::new(start, end),
                });
            }

            // anything else: unknown
            let bad = ch;
            self.bump_char();
            return Err(LexError {
                message: format!("unexpected character: {bad:?}"),
                span: Span::new(start, self.idx),
            });
        }
    }

    fn lex_string_literal(&mut self) -> Result<(String, usize), LexError> {
        // DAX string literal: "..." with escape by doubling quotes: ""
        let start = self.idx;
        debug_assert_eq!(self.peek_char(), Some('"'));
        self.bump_char(); // opening "
        let mut out = String::new();

        while self.idx < self.len() {
            if self.peek_byte() == Some(b'"') {
                if self.peek_byte_n(1) == Some(b'"') {
                    // escaped quote
                    self.bump_char();
                    self.bump_char();
                    out.push('"');
                    continue;
                } else {
                    // closing quote
                    self.bump_char();
                    return Ok((out, self.idx));
                }
            }

            let ch = self.bump_char().ok_or_else(|| LexError {
                message: "unterminated string literal".into(),
                span: Span::new(start, self.idx),
            })?;
            out.push(ch);
        }

        Err(LexError {
            message: "unterminated string literal".into(),
            span: Span::new(start, self.idx),
        })
    }

    fn lex_single_quoted_ident(&mut self) -> Result<(String, usize), LexError> {
        // DAX quoted identifier for tables: 'Sales' with escape by doubling single quotes: ''
        let start = self.idx;
        debug_assert_eq!(self.peek_char(), Some('\''));
        self.bump_char(); // opening '
        let mut out = String::new();

        while self.idx < self.len() {
            if self.peek_byte() == Some(b'\'') {
                if self.peek_byte_n(1) == Some(b'\'') {
                    self.bump_char();
                    self.bump_char();
                    out.push('\'');
                    continue;
                } else {
                    self.bump_char(); // closing
                    return Ok((out, self.idx));
                }
            }

            let ch = self.bump_char().ok_or_else(|| LexError {
                message: "unterminated quoted identifier".into(),
                span: Span::new(start, self.idx),
            })?;
            out.push(ch);
        }

        Err(LexError {
            message: "unterminated quoted identifier".into(),
            span: Span::new(start, self.idx),
        })
    }

    fn lex_bracket_ident(&mut self) -> Result<(String, usize), LexError> {
        // DAX bracket identifier: [Total Sales] with escape by doubling closing bracket: ]]
        let start = self.idx;
        debug_assert_eq!(self.peek_char(), Some('['));
        self.bump_char(); // opening [
        let mut out = String::new();

        while self.idx < self.len() {
            if self.peek_byte() == Some(b']') {
                if self.peek_byte_n(1) == Some(b']') {
                    self.bump_char();
                    self.bump_char();
                    out.push(']');
                    continue;
                } else {
                    self.bump_char(); // closing
                    return Ok((out, self.idx));
                }
            }

            let ch = self.bump_char().ok_or_else(|| LexError {
                message: "unterminated bracket identifier".into(),
                span: Span::new(start, self.idx),
            })?;
            out.push(ch);
        }

        Err(LexError {
            message: "unterminated bracket identifier".into(),
            span: Span::new(start, self.idx),
        })
    }

    fn lex_ident(&mut self) -> Result<usize, LexError> {
        while let Some(ch) = self.peek_char() {
            if is_ident_continue(ch) {
                self.bump_char();
            } else {
                break;
            }
        }
        Ok(self.idx)
    }

    fn lex_number(&mut self) -> Result<usize, LexError> {
        // Basic numeric literal:
        //   digits? [ ('.'|',') digits ] [ (e|E) ('+'|'-')? digits ]
        // We store the raw slice.
        while matches!(self.peek_char(), Some(c) if c.is_ascii_digit()) {
            self.bump_char();
        }

        if let Some(sep) = self.peek_char() {
            if (sep == '.' || (sep == ',' && self.dialect.allow_decimal_comma))
                && matches!(self.peek_byte_n(1), Some(b'0'..=b'9'))
            {
                self.bump_char(); // . or ,
                while matches!(self.peek_char(), Some(c) if c.is_ascii_digit()) {
                    self.bump_char();
                }
            }
        }

        if matches!(self.peek_char(), Some('e' | 'E')) {
            // exponent
            let save = self.idx;
            self.bump_char(); // e/E
            if matches!(self.peek_char(), Some('+' | '-')) {
                self.bump_char();
            }
            if !matches!(self.peek_char(), Some(c) if c.is_ascii_digit()) {
                // rollback: treat the 'e' as end of number, not exponent
                self.idx = save;
                return Ok(self.idx);
            }
            while matches!(self.peek_char(), Some(c) if c.is_ascii_digit()) {
                self.bump_char();
            }
        }

        Ok(self.idx)
    }
}

fn is_ident_start(ch: char) -> bool {
    ch.is_alphabetic() || ch == '_'
}
fn is_ident_continue(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch == '.'
}

fn parse_fixed_digits(value: &str, width: usize) -> Option<u32> {
    if value.len() != width || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn parse_one_or_two_digits(value: &str) -> Option<u32> {
    if !(1..=2).contains(&value.len()) || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn is_valid_datetime_literal(value: &str) -> bool {
    let (date, time) = if let Some((date, time)) = value.split_once('T') {
        (date, Some(time))
    } else if let Some((date, time)) = value.split_once(' ') {
        (date, Some(time))
    } else {
        (value, None)
    };

    let mut date_parts = date.split('-');
    let (Some(year), Some(month), Some(day), None) = (
        date_parts.next(),
        date_parts.next(),
        date_parts.next(),
        date_parts.next(),
    ) else {
        return false;
    };
    let Some(_year) = parse_fixed_digits(year, 4) else {
        return false;
    };
    let Some(month) = parse_one_or_two_digits(month) else {
        return false;
    };
    let Some(day) = parse_one_or_two_digits(day) else {
        return false;
    };
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return false;
    }

    let Some(time) = time else {
        return true;
    };
    let mut time_parts = time.split(':');
    let (Some(hour), Some(minute), Some(second), None) = (
        time_parts.next(),
        time_parts.next(),
        time_parts.next(),
        time_parts.next(),
    ) else {
        return false;
    };
    let (Some(hour), Some(minute), Some(second)) = (
        parse_fixed_digits(hour, 2),
        parse_fixed_digits(minute, 2),
        parse_fixed_digits(second, 2),
    ) else {
        return false;
    };
    hour <= 23 && minute <= 59 && second <= 59
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableName {
    pub name: String,
    pub quoted: bool,
}
impl TableName {
    pub fn unquoted(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            quoted: false,
        }
    }
    pub fn quoted(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            quoted: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VarDecl {
    pub name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum UnaryOp {
    Plus,
    Minus,
    Not,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum BinaryOp {
    Or,
    And,

    Eq,
    StrictEq, // ==
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,

    In,

    Concat, // &
    Add,
    Sub,
    Mul,
    Div,
    Pow,
}

impl BinaryOp {
    fn binding_power(&self) -> (u8, u8) {
        // Pratt binding powers: (left_bp, right_bp).
        // Left-assoc: (p, p+1). Right-assoc: (p, p).
        //
        // Precedence per Microsoft DAX operators:
        //   ^, sign, * /, + -, &, comparisons (=,==,<,>,<=,>=,<>,IN), NOT, &&, ||
        //
        // NOTE: NOT is handled as prefix with its own precedence (see parse_prefix).
        match self {
            BinaryOp::Or => (1, 2),
            BinaryOp::And => (2, 3),

            // comparisons
            BinaryOp::Eq
            | BinaryOp::StrictEq
            | BinaryOp::Neq
            | BinaryOp::Lt
            | BinaryOp::Lte
            | BinaryOp::Gt
            | BinaryOp::Gte
            | BinaryOp::In => (4, 5),

            BinaryOp::Concat => (5, 6),

            BinaryOp::Add | BinaryOp::Sub => (6, 7),
            BinaryOp::Mul | BinaryOp::Div => (7, 8),

            // Right associative, higher precedence than unary sign
            BinaryOp::Pow => (9, 9),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Expr {
    Number(String),
    String(String),
    DateTime(String),
    Boolean(bool),
    Blank,
    Omitted,

    Parameter(String), // @param (START AT)

    Identifier(String),  // variables, etc.
    TableRef(TableName), // bare table reference: 'Sales' or Sales
    BracketRef(String),  // [Measure] / [Column]
    TableColumnRef {
        table: TableName,
        column: String,
    },
    HierarchyRef {
        table: TableName,
        column: String,
        levels: Vec<String>,
    },

    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },

    DataTable {
        columns: Vec<DataTableColumn>,
        rows: Vec<Vec<Expr>>,
    },

    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },

    VarBlock {
        decls: Vec<VarDecl>,
        body: Box<Expr>,
    },

    // Table constructor: { <row>, <row>, ... } where row is scalar or tuple (..)
    // Stored as rows of expressions (columns per row).
    TableConstructor(Vec<Vec<Expr>>),

    Paren(Box<Expr>),
    Tuple(Vec<Expr>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum DataTableType {
    Boolean,
    Currency,
    DateTime,
    Double,
    Integer,
    String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DataTableColumn {
    pub name: String,
    pub data_type: DataTableType,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FuncParam {
    pub name: String,
    /// Raw type-hint tokens after `:` (0..N identifiers), e.g. `NUMERIC`, or `Scalar Numeric expr`.
    pub type_hints: Vec<String>,
    /// Optional default expression after `=`.
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VisualShapeColumn {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VisualShapeGroup {
    pub columns: Vec<VisualShapeColumn>,
    pub total: VisualShapeColumn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VisualShapeAxis {
    pub name: String,
    pub groups: Vec<VisualShapeGroup>,
    pub order_by: Vec<VisualShapeColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VisualShape {
    pub axes: Vec<VisualShapeAxis>,
    pub densify: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Query {
    pub define: Option<DefineBlock>,
    pub evaluates: Vec<EvaluateStmt>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DefineBlock {
    pub defs: Vec<Definition>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Definition {
    Measure {
        doc: Option<String>,
        table: Option<TableName>,
        name: String,
        expr: Expr,
    },
    Var {
        doc: Option<String>,
        name: String,
        expr: Expr,
    },
    Table {
        doc: Option<String>,
        name: String,
        expr: Expr,
        visual_shape: Option<VisualShape>,
    },
    Column {
        doc: Option<String>,
        table: Option<TableName>,
        name: String,
        expr: Expr,
    },
    Function {
        doc: Option<String>,
        name: String,
        params: Vec<FuncParam>,
        body: Expr,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OrderKey {
    pub expr: Expr,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvaluateStmt {
    pub expr: Expr,
    pub order_by: Vec<OrderKey>,
    pub start_at: Option<Vec<Expr>>,
}

/// Maximum recursion depth for the recursive-descent expression parser.
///
/// The mutually-recursive cycle (parse_expr_bp -> parse_prefix -> parse_primary ->
/// {Paren / parse_arg_list / parse_table_constructor / parse_var_block} -> parse_expr_bp)
/// has no natural bound, so pathological input (deeply nested parens/args/unary ops or
/// right-associative `^` chains) would overflow the native stack — a hardware fault that
/// PyO3 cannot convert into a catchable Python exception. We cap the depth and surface a
/// normal `ParseError` instead. 256 is far beyond any real-world formula nesting.
const MAX_PARSE_DEPTH: usize = 256;

pub struct Parser {
    tokens: Vec<Token>,
    i: usize,
    dialect: Dialect,
    depth: usize,
    node_spans: Vec<AstNodeSpan>,
}
impl Parser {
    pub fn new(tokens: Vec<Token>, dialect: Dialect) -> Self {
        Self {
            tokens,
            i: 0,
            dialect,
            depth: 0,
            node_spans: Vec::new(),
        }
    }

    fn consumed_end(&self, fallback: usize) -> usize {
        self.i
            .checked_sub(1)
            .and_then(|i| self.tokens.get(i))
            .map_or(fallback, |token| token.span.end)
    }

    fn record_node(&mut self, kind: AstNodeKind, start: usize) {
        self.node_spans.push(AstNodeSpan {
            kind,
            span: Span::new(start, self.consumed_end(start)),
        });
    }

    fn record_expr(&mut self, start: usize, expr: Expr) -> Expr {
        self.record_node(AstNodeKind::Expression, start);
        expr
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.i).unwrap_or_else(|| {
            self.tokens
                .last()
                .expect("token stream should always end with EOF")
        })
    }

    fn bump(&mut self) -> Token {
        let tok = self.peek().clone();
        if !matches!(tok.kind, TokenKind::Eof) {
            self.i += 1;
        }
        tok
    }

    fn same_variant(a: &TokenKind, b: &TokenKind) -> bool {
        std::mem::discriminant(a) == std::mem::discriminant(b)
    }

    fn peek_is(&self, kind: TokenKind) -> bool {
        Self::same_variant(&self.peek().kind, &kind)
    }

    fn eat(&mut self, kind: TokenKind) -> Option<Token> {
        if self.peek_is(kind.clone()) {
            Some(self.bump())
        } else {
            None
        }
    }

    fn expect(&mut self, kind: TokenKind, what: &'static str) -> Result<Token, ParseError> {
        self.eat(kind.clone()).ok_or_else(|| ParseError {
            message: format!("expected {what}"),
            span: self.peek().span,
        })
    }

    fn peek_ident_text(&self) -> Option<&str> {
        match &self.peek().kind {
            TokenKind::Ident(s) => Some(s.as_str()),
            _ => None,
        }
    }

    fn peek_kw(&self, kw: &str) -> bool {
        self.peek_ident_text()
            .is_some_and(|s| s.eq_ignore_ascii_case(kw))
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.peek_kw(kw) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: &'static str) -> Result<(), ParseError> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(ParseError {
                message: format!("expected keyword {kw}"),
                span: self.peek().span,
            })
        }
    }

    fn expect_ident(&mut self, what: &'static str) -> Result<String, ParseError> {
        match self.peek().kind.clone() {
            TokenKind::Ident(s) => {
                self.bump();
                Ok(s)
            }
            _ => Err(ParseError {
                message: format!("expected {what}"),
                span: self.peek().span,
            }),
        }
    }

    fn expect_bracket_ident(&mut self, what: &'static str) -> Result<String, ParseError> {
        match self.peek().kind.clone() {
            TokenKind::BracketIdent(s) => {
                self.bump();
                Ok(s)
            }
            _ => Err(ParseError {
                message: format!("expected {what}"),
                span: self.peek().span,
            }),
        }
    }

    fn expect_eof(&mut self) -> Result<(), ParseError> {
        if matches!(self.peek().kind, TokenKind::Eof) {
            Ok(())
        } else {
            Err(ParseError {
                message: "expected end of input".into(),
                span: self.peek().span,
            })
        }
    }

    fn eat_separator(&mut self) -> bool {
        if self.eat(TokenKind::Comma).is_some() {
            true
        } else {
            self.dialect.allow_semicolon_separators && self.eat(TokenKind::Semicolon).is_some()
        }
    }

    fn consume_stmt_terminators(&mut self) {
        // allow optional `;` between DEFINE entities / between EVALUATE statements
        while self.eat(TokenKind::Semicolon).is_some() {}
    }

    fn skip_doc_comments(&mut self) {
        while matches!(self.peek().kind, TokenKind::DocComment(_)) {
            self.bump();
        }
    }

    fn take_doc_comments(&mut self) -> Option<String> {
        let mut parts: Vec<String> = Vec::new();
        while let TokenKind::DocComment(s) = self.peek().kind.clone() {
            self.bump();
            parts.push(s);
        }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join("\n"))
        }
    }

    fn parse_table_name(&mut self) -> Result<TableName, ParseError> {
        match self.peek().kind.clone() {
            TokenKind::QuotedIdent(s) => {
                self.bump();
                Ok(TableName::quoted(s))
            }
            TokenKind::Ident(s) => {
                self.bump();
                Ok(TableName::unquoted(s))
            }
            _ => Err(ParseError {
                message: "expected table name (identifier or single-quoted identifier)".into(),
                span: self.peek().span,
            }),
        }
    }

    // ---- public entrypoints ----

    pub fn parse_formula_expression(&mut self) -> Result<Expr, ParseError> {
        self.skip_doc_comments();
        // Optional leading '=' (Excel/Power Pivot convention).
        self.eat(TokenKind::Eq);
        let expr = self.parse_expr_bp(0)?;
        self.expect_eof()?;
        Ok(expr)
    }

    pub fn parse_query(&mut self) -> Result<Query, ParseError> {
        self.skip_doc_comments();
        let start = self.peek().span.start;

        let define = if self.peek_kw("define") {
            Some(self.parse_define_block()?)
        } else {
            None
        };

        let mut evaluates = Vec::new();
        loop {
            self.consume_stmt_terminators();
            self.skip_doc_comments();
            if self.peek_kw("evaluate") {
                evaluates.push(self.parse_evaluate_stmt()?);
            } else {
                break;
            }
        }

        if evaluates.is_empty() {
            return Err(ParseError {
                message: "expected at least one EVALUATE statement".into(),
                span: self.peek().span,
            });
        }

        self.consume_stmt_terminators();
        self.skip_doc_comments();
        self.expect_eof()?;
        let query = Query { define, evaluates };
        self.record_node(AstNodeKind::Query, start);
        Ok(query)
    }

    // ---- query parsing ----

    fn parse_define_block(&mut self) -> Result<DefineBlock, ParseError> {
        let start = self.peek().span.start;
        self.expect_kw("define")?;
        let mut defs = Vec::new();

        loop {
            self.consume_stmt_terminators();

            // DEFINE ends before first EVALUATE
            if self.peek_kw("evaluate") || matches!(self.peek().kind, TokenKind::Eof) {
                break;
            }

            let doc = self.take_doc_comments();

            // If doc comments were followed by EVALUATE/EOF, just ignore them (treat as trivia).
            if self.peek_kw("evaluate") || matches!(self.peek().kind, TokenKind::Eof) {
                break;
            }

            if self.peek_kw("measure") {
                defs.push(self.parse_define_measure(doc)?);
            } else if self.peek_kw("function") {
                defs.push(self.parse_define_function(doc)?);
            } else if self.peek_kw("var") {
                defs.push(self.parse_define_var(doc)?);
            } else if self.peek_kw("table") {
                defs.push(self.parse_define_table(doc)?);
            } else if self.peek_kw("column") {
                defs.push(self.parse_define_column(doc)?);
            } else {
                return Err(ParseError {
                    message: "expected MEASURE, FUNCTION, VAR, TABLE, or COLUMN in DEFINE block"
                        .into(),
                    span: self.peek().span,
                });
            }
        }

        if defs.is_empty() {
            return Err(ParseError {
                message: "DEFINE block must contain at least one definition".into(),
                span: self.peek().span,
            });
        }

        let block = DefineBlock { defs };
        self.record_node(AstNodeKind::DefineBlock, start);
        Ok(block)
    }

    fn parse_define_measure(&mut self, doc: Option<String>) -> Result<Definition, ParseError> {
        let start = self.peek().span.start;
        self.expect_kw("measure")?;

        // Typically: MEASURE 'Table'[Measure] = <expr>
        // We accept:
        //   MEASURE [Measure] = ...
        //   MEASURE 'T'[M] = ...
        //   MEASURE T[M] = ...
        let (table, name) = if matches!(self.peek().kind, TokenKind::BracketIdent(_)) {
            (
                None,
                self.expect_bracket_ident("measure name like [My Measure]")?,
            )
        } else {
            let t = self.parse_table_name()?;
            let n = self.expect_bracket_ident("measure name like [My Measure]")?;
            (Some(t), n)
        };

        self.expect(TokenKind::Eq, "`=`")?;
        let expr = self.parse_expr_bp(0)?;

        self.consume_stmt_terminators();
        self.ensure_stmt_follower(&["measure", "function", "var", "table", "column", "evaluate"])?;

        let definition = Definition::Measure {
            doc,
            table,
            name,
            expr,
        };
        self.record_node(AstNodeKind::Definition, start);
        Ok(definition)
    }

    fn parse_define_var(&mut self, doc: Option<String>) -> Result<Definition, ParseError> {
        let start = self.peek().span.start;
        self.expect_kw("var")?;
        let name = self.expect_ident("variable name")?;
        self.expect(TokenKind::Eq, "`=`")?;
        let expr = self.parse_expr_bp(0)?;

        self.consume_stmt_terminators();
        self.ensure_stmt_follower(&["measure", "function", "var", "table", "column", "evaluate"])?;

        let definition = Definition::Var { doc, name, expr };
        self.record_node(AstNodeKind::Definition, start);
        Ok(definition)
    }

    fn parse_define_table(&mut self, doc: Option<String>) -> Result<Definition, ParseError> {
        let start = self.peek().span.start;
        self.expect_kw("table")?;
        // Spec uses `<table name>` — allow identifier or single-quoted identifier.
        let name = match self.peek().kind.clone() {
            TokenKind::Ident(s) => {
                self.bump();
                s
            }
            TokenKind::QuotedIdent(s) => {
                self.bump();
                s
            }
            _ => {
                return Err(ParseError {
                    message: "expected table name for TABLE definition".into(),
                    span: self.peek().span,
                })
            }
        };

        self.expect(TokenKind::Eq, "`=`")?;
        let expr = self.parse_expr_bp(0)?;
        let visual_shape = if self.peek_kw("with") {
            Some(self.parse_visual_shape()?)
        } else {
            None
        };

        self.consume_stmt_terminators();
        self.ensure_stmt_follower(&["measure", "function", "var", "table", "column", "evaluate"])?;

        let definition = Definition::Table {
            doc,
            name,
            expr,
            visual_shape,
        };
        self.record_node(AstNodeKind::Definition, start);
        Ok(definition)
    }

    fn parse_visual_shape_column(&mut self) -> Result<VisualShapeColumn, ParseError> {
        Ok(VisualShapeColumn {
            name: self.expect_bracket_ident("visual shape column like [Year]")?,
        })
    }

    fn parse_visual_shape(&mut self) -> Result<VisualShape, ParseError> {
        self.expect_kw("with")?;
        self.expect_kw("visual")?;
        self.expect_kw("shape")?;

        if !self.peek_kw("axis") {
            return Err(ParseError {
                message: "WITH VISUAL SHAPE requires at least one AXIS".into(),
                span: self.peek().span,
            });
        }

        let mut axes = Vec::new();
        while self.eat_kw("axis") {
            let name = self.expect_ident("visual shape axis name")?;
            if !self.peek_kw("group") {
                return Err(ParseError {
                    message: "visual shape AXIS requires at least one GROUP".into(),
                    span: self.peek().span,
                });
            }

            let mut groups = Vec::new();
            while self.eat_kw("group") {
                let mut columns = vec![self.parse_visual_shape_column()?];
                while self.eat(TokenKind::Comma).is_some() {
                    columns.push(self.parse_visual_shape_column()?);
                }
                self.expect_kw("total")?;
                let total = self.parse_visual_shape_column()?;
                groups.push(VisualShapeGroup { columns, total });
            }

            self.expect_kw("order")?;
            self.expect_kw("by")?;
            let mut order_by = vec![self.parse_visual_shape_column()?];
            while self.eat(TokenKind::Comma).is_some() {
                order_by.push(self.parse_visual_shape_column()?);
            }

            axes.push(VisualShapeAxis {
                name,
                groups,
                order_by,
            });
        }

        let densify = if self.eat_kw("densify") {
            match self.peek().kind.clone() {
                TokenKind::String(value) => {
                    self.bump();
                    Some(value)
                }
                _ => {
                    return Err(ParseError {
                        message: "expected string literal after DENSIFY".into(),
                        span: self.peek().span,
                    })
                }
            }
        } else {
            None
        };

        Ok(VisualShape { axes, densify })
    }

    fn parse_define_column(&mut self, doc: Option<String>) -> Result<Definition, ParseError> {
        let start = self.peek().span.start;
        self.expect_kw("column")?;

        // Common: COLUMN 'Table'[Column] = <expr>
        let (table, name) = if matches!(self.peek().kind, TokenKind::BracketIdent(_)) {
            (
                None,
                self.expect_bracket_ident("column name like [My Column]")?,
            )
        } else {
            let t = self.parse_table_name()?;
            let n = self.expect_bracket_ident("column name like [My Column]")?;
            (Some(t), n)
        };

        self.expect(TokenKind::Eq, "`=`")?;
        let expr = self.parse_expr_bp(0)?;

        self.consume_stmt_terminators();
        self.ensure_stmt_follower(&["measure", "function", "var", "table", "column", "evaluate"])?;

        let definition = Definition::Column {
            doc,
            table,
            name,
            expr,
        };
        self.record_node(AstNodeKind::Definition, start);
        Ok(definition)
    }

    fn parse_define_function(&mut self, doc: Option<String>) -> Result<Definition, ParseError> {
        let start = self.peek().span.start;
        // FUNCTION <name> = (<parameter>: <type> [= <default>], ...) => <body>
        self.expect_kw("function")?;
        let name = self.expect_ident("function name")?;

        self.expect(TokenKind::Eq, "`=`")?;
        self.expect(TokenKind::LParen, "`(`")?;

        let mut params: Vec<FuncParam> = Vec::new();
        if !self.peek_is(TokenKind::RParen) {
            loop {
                let pname = self.expect_ident("parameter name")?;

                let mut type_hints: Vec<String> = Vec::new();
                if self.eat(TokenKind::Colon).is_some() {
                    // DAX UDF type hints can be 1..N identifiers (e.g. `NUMERIC` or `Scalar Numeric expr`).
                    // Parse until `,`/`;` or `)`.
                    while let TokenKind::Ident(s) = self.peek().kind.clone() {
                        self.bump();
                        type_hints.push(s);
                    }

                    if type_hints.is_empty() {
                        return Err(ParseError {
                            message: "expected at least one type hint after ':'".into(),
                            span: self.peek().span,
                        });
                    }
                }

                let default = if self.eat(TokenKind::Eq).is_some() {
                    Some(self.parse_expr_bp(0)?)
                } else {
                    None
                };

                params.push(FuncParam {
                    name: pname,
                    type_hints,
                    default,
                });

                if self.eat_separator() {
                    if self.peek_is(TokenKind::RParen) {
                        return Err(ParseError {
                            message: "trailing separator in FUNCTION parameter list".into(),
                            span: self.peek().span,
                        });
                    }
                    continue;
                }

                break;
            }
        }

        self.expect(TokenKind::RParen, "`)`")?;
        self.expect(TokenKind::Arrow, "`=>`")?;

        let body = self.parse_expr_bp(0)?;

        self.consume_stmt_terminators();
        self.ensure_stmt_follower(&["measure", "function", "var", "table", "column", "evaluate"])?;

        let definition = Definition::Function {
            doc,
            name,
            params,
            body,
        };
        self.record_node(AstNodeKind::Definition, start);
        Ok(definition)
    }

    fn parse_evaluate_stmt(&mut self) -> Result<EvaluateStmt, ParseError> {
        let start = self.peek().span.start;
        self.expect_kw("evaluate")?;

        let expr = self.parse_expr_bp(0)?;

        let mut order_by = Vec::new();
        if self.peek_kw("order") {
            order_by = self.parse_order_by_clause()?;
        }

        let mut start_at = None;
        if self.peek_kw("start") {
            if order_by.is_empty() {
                return Err(ParseError {
                    message: "START AT requires an ORDER BY clause".into(),
                    span: self.peek().span,
                });
            }

            let values = self.parse_start_at_clause()?;

            // Spec: values must be constant or @param; count <= ORDER BY expressions.
            if values.len() > order_by.len() {
                return Err(ParseError {
                    message: "START AT has more arguments than ORDER BY".into(),
                    span: self.peek().span,
                });
            }

            start_at = Some(values);
        }

        self.consume_stmt_terminators();
        self.ensure_stmt_follower(&["evaluate"])?;

        let evaluate = EvaluateStmt {
            expr,
            order_by,
            start_at,
        };
        self.record_node(AstNodeKind::Evaluate, start);
        Ok(evaluate)
    }

    fn parse_order_by_clause(&mut self) -> Result<Vec<OrderKey>, ParseError> {
        self.expect_kw("order")?;
        self.expect_kw("by")?;

        let mut keys = Vec::new();
        loop {
            let expr = self.parse_expr_bp(0)?;

            let direction = if self.peek_kw("asc") {
                self.bump();
                SortDirection::Asc
            } else if self.peek_kw("desc") {
                self.bump();
                SortDirection::Desc
            } else {
                SortDirection::Asc
            };

            keys.push(OrderKey { expr, direction });

            if self.eat_separator() {
                continue;
            }
            break;
        }

        Ok(keys)
    }

    fn parse_start_at_clause(&mut self) -> Result<Vec<Expr>, ParseError> {
        self.expect_kw("start")?;
        self.expect_kw("at")?;

        let mut values = Vec::new();
        loop {
            let span = self.peek().span;
            let value = self.parse_expr_bp(0)?;
            if !Self::is_start_at_value(&value) {
                return Err(ParseError {
                    message: "START AT value must be a literal constant or @parameter".into(),
                    span,
                });
            }
            values.push(value);
            if self.eat_separator() {
                continue;
            }
            break;
        }

        Ok(values)
    }

    fn is_start_at_value(expr: &Expr) -> bool {
        match expr {
            Expr::Number(_)
            | Expr::String(_)
            | Expr::DateTime(_)
            | Expr::Boolean(_)
            | Expr::Parameter(_) => true,
            Expr::Unary {
                op: UnaryOp::Plus | UnaryOp::Minus,
                expr,
            } => matches!(expr.as_ref(), Expr::Number(_)),
            // BLANK is produced by the BLANK() function in documented DAX syntax,
            // making it an expression rather than a START AT literal constant.
            _ => false,
        }
    }

    fn ensure_stmt_follower(&self, allowed_keywords: &[&str]) -> Result<(), ParseError> {
        // After parsing a "statement-sized" expression, the next token must be:
        // - EOF
        // - doc comment (we treat as ignorable trivia between statements)
        // - one of the allowed statement starters (contextual keywords)
        if matches!(self.peek().kind, TokenKind::Eof) {
            return Ok(());
        }
        if matches!(self.peek().kind, TokenKind::DocComment(_)) {
            return Ok(());
        }
        if let Some(id) = self.peek_ident_text() {
            let ok = allowed_keywords
                .iter()
                .any(|kw| id.eq_ignore_ascii_case(kw));
            if ok {
                return Ok(());
            }
        }
        Err(ParseError {
            message: "unexpected token after statement".into(),
            span: self.peek().span,
        })
    }

    // ---- expression parsing (Pratt) ----

    fn parse_expr_bp(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        // Depth guard: every recursion path (parens, arg lists, table constructors, var
        // blocks, unary prefixes, right-assoc operators) funnels back through this entry,
        // so guarding here bounds the whole recursive-descent cycle. The result is captured
        // before decrementing so the counter stays balanced even when the body errors,
        // which keeps sibling recursions (e.g. left-deep `1+1+...` chains, whose RHS calls
        // unwind between operators) from falsely tripping the cap.
        self.depth += 1;
        if self.depth > MAX_PARSE_DEPTH {
            self.depth -= 1;
            return Err(ParseError {
                message: "expression nesting too deep".into(),
                span: self.peek().span,
            });
        }
        let result = self.parse_expr_bp_impl(min_bp);
        self.depth -= 1;
        result
    }

    fn parse_expr_bp_impl(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        let start = self.peek().span.start;
        let mut lhs = self.parse_prefix()?;

        while let Some((op, lbp, rbp)) = self.peek_infix_op() {
            if lbp < min_bp {
                break;
            }

            // consume operator token/keyword
            match op {
                BinaryOp::In => {
                    // IN is an identifier token
                    self.bump();
                }
                _ => {
                    self.bump();
                }
            }

            let rhs = self.parse_expr_bp(rbp)?;
            let binary = Expr::Binary {
                op,
                left: Box::new(lhs),
                right: Box::new(rhs),
            };
            lhs = self.record_expr(start, binary);
        }

        Ok(lhs)
    }

    fn parse_prefix(&mut self) -> Result<Expr, ParseError> {
        // VAR blocks are expressions (not just top-level)
        if self.peek_kw("var") {
            return self.parse_var_block();
        }

        // unary operators
        //
        // IMPORTANT: precedence per MS docs: exponentiation (^) happens before unary sign.
        // So unary sign must bind *less tightly* than '^' but tighter than '* /'.
        if let Some(token) = self.eat(TokenKind::Plus) {
            let expr = self.parse_expr_bp(8)?;
            let unary = Expr::Unary {
                op: UnaryOp::Plus,
                expr: Box::new(expr),
            };
            return Ok(self.record_expr(token.span.start, unary));
        }
        if let Some(token) = self.eat(TokenKind::Minus) {
            let expr = self.parse_expr_bp(8)?;
            let unary = Expr::Unary {
                op: UnaryOp::Minus,
                expr: Box::new(expr),
            };
            return Ok(self.record_expr(token.span.start, unary));
        }

        // IMPORTANT: precedence per MS docs: comparisons bind tighter than NOT, but NOT binds
        // tighter than && / ||.
        if self.peek_kw("not") {
            let token = self.bump();
            let expr = self.parse_expr_bp(3)?;
            let unary = Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            };
            return Ok(self.record_expr(token.span.start, unary));
        }

        self.parse_primary()
    }

    fn peek_infix_op(&self) -> Option<(BinaryOp, u8, u8)> {
        let op = match &self.peek().kind {
            TokenKind::OrOr => BinaryOp::Or,
            TokenKind::AndAnd => BinaryOp::And,

            TokenKind::Eq => BinaryOp::Eq,
            TokenKind::EqEq => BinaryOp::StrictEq,
            TokenKind::Neq => BinaryOp::Neq,
            TokenKind::Lt => BinaryOp::Lt,
            TokenKind::Lte => BinaryOp::Lte,
            TokenKind::Gt => BinaryOp::Gt,
            TokenKind::Gte => BinaryOp::Gte,

            TokenKind::Amp => BinaryOp::Concat,

            TokenKind::Plus => BinaryOp::Add,
            TokenKind::Minus => BinaryOp::Sub,
            TokenKind::Star => BinaryOp::Mul,
            TokenKind::Slash => BinaryOp::Div,
            TokenKind::Caret => BinaryOp::Pow,

            TokenKind::Ident(s) if s.eq_ignore_ascii_case("in") => BinaryOp::In,

            _ => return None,
        };

        let (lbp, rbp) = op.binding_power();
        Some((op, lbp, rbp))
    }

    fn parse_hierarchy_tail(
        &mut self,
        start: usize,
        table: TableName,
        column: String,
    ) -> Result<Expr, ParseError> {
        if !self.peek_is(TokenKind::Dot) {
            let expr = Expr::TableColumnRef { table, column };
            return Ok(self.record_expr(start, expr));
        }

        let mut levels = Vec::new();
        while self.eat(TokenKind::Dot).is_some() {
            let level = self.expect_bracket_ident("hierarchy level like [Year]")?;
            levels.push(level);
        }

        let expr = Expr::HierarchyRef {
            table,
            column,
            levels,
        };
        Ok(self.record_expr(start, expr))
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        let start = self.peek().span.start;
        match self.peek().kind.clone() {
            TokenKind::Number(n) => {
                self.bump();
                Ok(self.record_expr(start, Expr::Number(n)))
            }
            TokenKind::String(s) => {
                self.bump();
                Ok(self.record_expr(start, Expr::String(s)))
            }
            TokenKind::DateTime(value) => {
                self.bump();
                Ok(self.record_expr(start, Expr::DateTime(value)))
            }
            TokenKind::Param(p) => {
                self.bump();
                Ok(self.record_expr(start, Expr::Parameter(p)))
            }
            TokenKind::BracketIdent(name) => {
                self.bump();
                Ok(self.record_expr(start, Expr::BracketRef(name)))
            }
            TokenKind::QuotedIdent(name) => {
                self.bump();
                let table = TableName::quoted(name);

                // 'Table'[Column]
                if let TokenKind::BracketIdent(col) = self.peek().kind.clone() {
                    self.bump();
                    self.parse_hierarchy_tail(start, table, col)
                } else {
                    Ok(self.record_expr(start, Expr::TableRef(table)))
                }
            }
            TokenKind::Ident(id) => {
                // identifier could be:
                // - function call: IDENT '(' ...
                // - table/column reference: IDENT '[' col ']'
                // - bare identifier: variable/table
                self.bump();

                if self.peek_is(TokenKind::LParen) {
                    self.bump(); // (
                    if id.eq_ignore_ascii_case("datatable") {
                        return self.parse_datatable(start);
                    }
                    let args = self.parse_arg_list()?;
                    let expr = Expr::FunctionCall { name: id, args };
                    return Ok(self.record_expr(start, expr));
                }

                let table = TableName::unquoted(id.clone());
                if let TokenKind::BracketIdent(col) = self.peek().kind.clone() {
                    self.bump();
                    return self.parse_hierarchy_tail(start, table, col);
                }

                // contextual literals (after call/column checks so TRUE() / BLANK() parse)
                if id.eq_ignore_ascii_case("true") {
                    return Ok(self.record_expr(start, Expr::Boolean(true)));
                }
                if id.eq_ignore_ascii_case("false") {
                    return Ok(self.record_expr(start, Expr::Boolean(false)));
                }
                // DAX's "blank" is usually BLANK(), but some tooling treats BLANK as a literal-ish value.
                if id.eq_ignore_ascii_case("blank") {
                    return Ok(self.record_expr(start, Expr::Blank));
                }

                Ok(self.record_expr(start, Expr::Identifier(id)))
            }
            TokenKind::LParen => {
                self.bump();
                let first = self.parse_expr_bp(0)?;
                if self.eat_separator() {
                    let mut elements = vec![first];
                    if self.peek_is(TokenKind::RParen) {
                        return Err(ParseError {
                            message: "trailing separator in tuple expression".into(),
                            span: self.peek().span,
                        });
                    }
                    loop {
                        elements.push(self.parse_expr_bp(0)?);
                        if !self.eat_separator() {
                            break;
                        }
                        if self.peek_is(TokenKind::RParen) {
                            return Err(ParseError {
                                message: "trailing separator in tuple expression".into(),
                                span: self.peek().span,
                            });
                        }
                    }
                    self.expect(TokenKind::RParen, "`)`")?;
                    let expr = Expr::Tuple(elements);
                    return Ok(self.record_expr(start, expr));
                }
                self.expect(TokenKind::RParen, "`)`")?;
                let expr = Expr::Paren(Box::new(first));
                Ok(self.record_expr(start, expr))
            }
            TokenKind::LBrace => self.parse_table_constructor(),
            TokenKind::DocComment(_) => {
                // Treat doc comments as trivia; skip and parse next primary.
                self.bump();
                self.parse_primary()
            }
            TokenKind::Eof => Err(ParseError {
                message: "unexpected end of input".into(),
                span: self.peek().span,
            }),
            _ => Err(ParseError {
                message: "expected expression".into(),
                span: self.peek().span,
            }),
        }
    }

    fn parse_arg_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        // assumes '(' already consumed
        if self.peek_is(TokenKind::RParen) {
            self.bump();
            return Ok(Vec::new());
        }

        let mut args = Vec::new();
        loop {
            // Empty positional slots are distinct from BLANK() in DAX. They are
            // valid only within function argument lists (for example INDEX(1, , -1)).
            let expr = if self.peek_is(TokenKind::Comma)
                || (self.dialect.allow_semicolon_separators && self.peek_is(TokenKind::Semicolon))
            {
                let position = self.peek().span.start;
                self.record_expr(position, Expr::Omitted)
            } else {
                self.parse_expr_bp(0)?
            };
            args.push(expr);

            if self.eat_separator() {
                // disallow trailing separator: must have another expr next
                if self.peek_is(TokenKind::RParen) {
                    return Err(ParseError {
                        message: "trailing argument separator".into(),
                        span: self.peek().span,
                    });
                }
                continue;
            }

            break;
        }

        self.expect(TokenKind::RParen, "`)`")?;
        Ok(args)
    }

    fn parse_datatable_type(&mut self) -> Result<DataTableType, ParseError> {
        let span = self.peek().span;
        let TokenKind::Ident(name) = self.peek().kind.clone() else {
            return Err(ParseError {
                message: "expected DATATABLE column type".into(),
                span,
            });
        };
        self.bump();
        match name.to_ascii_uppercase().as_str() {
            "BOOLEAN" | "LOGICAL" => Ok(DataTableType::Boolean),
            "CURRENCY" | "DECIMAL" => Ok(DataTableType::Currency),
            "DATETIME" => Ok(DataTableType::DateTime),
            "DOUBLE" => Ok(DataTableType::Double),
            "INTEGER" | "INT64" => Ok(DataTableType::Integer),
            "STRING" | "TEXT" => Ok(DataTableType::String),
            _ => Err(ParseError {
                message: format!("unsupported DATATABLE column type `{name}`"),
                span,
            }),
        }
    }

    fn is_datatable_constant(expr: &Expr) -> bool {
        match expr {
            Expr::Number(_)
            | Expr::String(_)
            | Expr::DateTime(_)
            | Expr::Boolean(_)
            | Expr::Blank
            | Expr::Omitted => true,
            Expr::Paren(inner) => Self::is_datatable_constant(inner),
            Expr::Unary {
                op: UnaryOp::Plus | UnaryOp::Minus,
                expr,
            } => matches!(expr.as_ref(), Expr::Number(_)),
            Expr::FunctionCall { name, args }
                if name.eq_ignore_ascii_case("date") || name.eq_ignore_ascii_case("time") =>
            {
                args.iter().all(Self::is_datatable_constant)
            }
            Expr::FunctionCall { name, args }
                if name.eq_ignore_ascii_case("blank") && args.is_empty() =>
            {
                true
            }
            Expr::Binary {
                op: BinaryOp::Add,
                left,
                right,
            } => {
                let is_date_or_time = |value: &Expr| {
                    matches!(value, Expr::FunctionCall { name, .. }
                        if name.eq_ignore_ascii_case("date") || name.eq_ignore_ascii_case("time"))
                };
                is_date_or_time(left)
                    && is_date_or_time(right)
                    && Self::is_datatable_constant(left)
                    && Self::is_datatable_constant(right)
            }
            _ => false,
        }
    }

    /// Parse DATATABLE's dedicated schema-and-array grammar after `DATATABLE(`.
    /// This cannot use the generic function/table-constructor grammar because
    /// its nested `{ { ... }, { ... } }` rows permit missing values as BLANKs.
    fn parse_datatable(&mut self, start: usize) -> Result<Expr, ParseError> {
        let mut columns = Vec::new();

        loop {
            let span = self.peek().span;
            let TokenKind::String(name) = self.peek().kind.clone() else {
                return Err(ParseError {
                    message: "expected DATATABLE column name string".into(),
                    span,
                });
            };
            self.bump();
            if !self.eat_separator() {
                return Err(ParseError {
                    message: "expected separator after DATATABLE column name".into(),
                    span: self.peek().span,
                });
            }
            let data_type = self.parse_datatable_type()?;
            columns.push(DataTableColumn { name, data_type });

            if !self.eat_separator() {
                return Err(ParseError {
                    message: "expected separator after DATATABLE column type".into(),
                    span: self.peek().span,
                });
            }
            if self.peek_is(TokenKind::LBrace) {
                break;
            }
        }

        self.expect(TokenKind::LBrace, "`{`")?;
        let mut rows = Vec::new();
        if !self.peek_is(TokenKind::RBrace) {
            loop {
                self.expect(TokenKind::LBrace, "DATATABLE row opening `{`")?;
                let mut row = Vec::new();
                loop {
                    let value_span = self.peek().span;
                    let value = if self.peek_is(TokenKind::Comma)
                        || (self.dialect.allow_semicolon_separators
                            && self.peek_is(TokenKind::Semicolon))
                    {
                        let position = self.peek().span.start;
                        self.record_expr(position, Expr::Omitted)
                    } else {
                        self.parse_expr_bp(0)?
                    };
                    if !Self::is_datatable_constant(&value) {
                        return Err(ParseError {
                            message: "DATATABLE values must be constant expressions".into(),
                            span: value_span,
                        });
                    }
                    row.push(value);
                    if self.eat_separator() {
                        if self.peek_is(TokenKind::RBrace) {
                            // DATATABLE treats a missing value as BLANK(), including
                            // the final field in a fixed-width row.
                            let position = self.peek().span.start;
                            let omitted = self.record_expr(position, Expr::Omitted);
                            row.push(omitted);
                            break;
                        }
                        continue;
                    }
                    break;
                }
                self.expect(TokenKind::RBrace, "DATATABLE row closing `}`")?;
                if row.len() != columns.len() {
                    return Err(ParseError {
                        message: format!(
                            "DATATABLE row has {} values but schema defines {} columns",
                            row.len(),
                            columns.len()
                        ),
                        span: self.peek().span,
                    });
                }
                rows.push(row);
                if self.eat_separator() {
                    if self.peek_is(TokenKind::RBrace) {
                        return Err(ParseError {
                            message: "trailing separator in DATATABLE row list".into(),
                            span: self.peek().span,
                        });
                    }
                    continue;
                }
                break;
            }
        }
        self.expect(TokenKind::RBrace, "DATATABLE values closing `}`")?;
        self.expect(TokenKind::RParen, "`)`")?;
        let expr = Expr::DataTable { columns, rows };
        Ok(self.record_expr(start, expr))
    }

    fn parse_var_block(&mut self) -> Result<Expr, ParseError> {
        // VAR <name> = <expr> [VAR ...] RETURN <expr>
        let start = self.peek().span.start;
        let mut decls = Vec::new();

        if !self.peek_kw("var") {
            return Err(ParseError {
                message: "expected VAR".into(),
                span: self.peek().span,
            });
        }

        while self.eat_kw("var") {
            let name = self.expect_ident("variable name")?;
            self.expect(TokenKind::Eq, "`=`")?;
            let expr = self.parse_expr_bp(0)?;
            decls.push(VarDecl { name, expr });
        }

        self.expect_kw("return")?;
        let body = self.parse_expr_bp(0)?;

        let expr = Expr::VarBlock {
            decls,
            body: Box::new(body),
        };
        Ok(self.record_expr(start, expr))
    }

    fn parse_table_constructor(&mut self) -> Result<Expr, ParseError> {
        // { row (, row)* }
        // row := scalar_expr | '(' expr (, expr)* ')'
        let start = self.peek().span.start;
        self.expect(TokenKind::LBrace, "`{`")?;

        if self.peek_is(TokenKind::RBrace) {
            return Err(ParseError {
                message: "table constructor must contain at least one row".into(),
                span: self.peek().span,
            });
        }

        let mut rows: Vec<Vec<Expr>> = Vec::new();

        loop {
            let row = if self.peek_is(TokenKind::LParen) {
                self.bump(); // (
                if self.peek_is(TokenKind::RParen) {
                    return Err(ParseError {
                        message: "empty tuple row in table constructor".into(),
                        span: self.peek().span,
                    });
                }

                let mut cols = Vec::new();
                loop {
                    cols.push(self.parse_expr_bp(0)?);
                    if self.eat_separator() {
                        if self.peek_is(TokenKind::RParen) {
                            return Err(ParseError {
                                message: "trailing separator in tuple row".into(),
                                span: self.peek().span,
                            });
                        }
                        continue;
                    }
                    break;
                }

                self.expect(TokenKind::RParen, "`)`")?;
                cols
            } else {
                vec![self.parse_expr_bp(0)?]
            };

            if let Some(expected) = rows.first().map(Vec::len) {
                if row.len() != expected {
                    return Err(ParseError {
                        message: format!(
                            "table constructor row has {} values; expected {expected}",
                            row.len()
                        ),
                        span: self.peek().span,
                    });
                }
            }
            rows.push(row);

            if self.eat_separator() {
                if self.peek_is(TokenKind::RBrace) {
                    return Err(ParseError {
                        message: "trailing separator in table constructor".into(),
                        span: self.peek().span,
                    });
                }
                continue;
            }

            break;
        }

        self.expect(TokenKind::RBrace, "`}`")?;
        let expr = Expr::TableConstructor(rows);
        Ok(self.record_expr(start, expr))
    }
}

// ---- convenience API ----

pub fn lex(input: &str) -> Result<Vec<Token>, DaxError> {
    lex_with_dialect(input, Dialect::default())
}

pub fn lex_with_dialect(input: &str, dialect: Dialect) -> Result<Vec<Token>, DaxError> {
    Lexer::new(input, dialect).lex_all().map_err(DaxError::Lex)
}

pub fn parse_expression(input: &str) -> Result<Expr, DaxError> {
    parse_expression_with_dialect(input, Dialect::default())
}

pub fn parse_expression_with_dialect(input: &str, dialect: Dialect) -> Result<Expr, DaxError> {
    let tokens = Lexer::new(input, dialect)
        .lex_all()
        .map_err(DaxError::Lex)?;
    let mut p = Parser::new(tokens, dialect);
    p.parse_formula_expression().map_err(DaxError::Parse)
}

pub fn parse_query(input: &str) -> Result<Query, DaxError> {
    parse_query_with_dialect(input, Dialect::default())
}

pub fn parse_query_with_dialect(input: &str, dialect: Dialect) -> Result<Query, DaxError> {
    let tokens = Lexer::new(input, dialect)
        .lex_all()
        .map_err(DaxError::Lex)?;
    let mut p = Parser::new(tokens, dialect);
    p.parse_query().map_err(DaxError::Parse)
}

fn scan_source_comments(input: &str, dialect: Dialect) -> Vec<SourceComment> {
    let bytes = input.as_bytes();
    let mut comments = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        // DAX escapes quote delimiters by doubling them. Skip quoted regions so comment
        // starters embedded in strings, table names, or bracket identifiers stay data.
        let (delimiter, doubled) = match bytes[i] {
            b'"' => (Some(b'"'), true),
            b'\'' => (Some(b'\''), true),
            b'[' => (Some(b']'), true),
            _ => (None, false),
        };
        if let Some(delimiter) = delimiter {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == delimiter {
                    if doubled && bytes.get(i + 1) == Some(&delimiter) {
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }

        let comment = if dialect.allow_dash_dash_comments && bytes[i..].starts_with(b"--") {
            Some((CommentKind::DashLine, false, 2))
        } else if dialect.allow_double_slash_comments && bytes[i..].starts_with(b"///") {
            Some((CommentKind::DocLine, false, 3))
        } else if dialect.allow_double_slash_comments && bytes[i..].starts_with(b"//") {
            Some((CommentKind::SlashLine, false, 2))
        } else if dialect.allow_block_comments && bytes[i..].starts_with(b"/*") {
            Some((CommentKind::Block, true, 2))
        } else {
            None
        };

        let Some((kind, block, prefix_len)) = comment else {
            i += 1;
            continue;
        };
        let start = i;
        i += prefix_len;
        if block {
            while i < bytes.len() && !bytes[i..].starts_with(b"*/") {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
        } else {
            while i < bytes.len() && bytes[i] != b'\n' && bytes[i] != b'\r' {
                i += 1;
            }
        }
        comments.push(SourceComment {
            kind,
            span: Span::new(start, i),
            text: input[start..i].to_string(),
            previous_node: None,
            next_node: None,
            containing_node: None,
        });
    }

    comments
}

fn attach_comments(comments: &mut [SourceComment], nodes: &[AstNodeSpan]) {
    for comment in comments {
        comment.previous_node = nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.span.end <= comment.span.start)
            .max_by_key(|(_, node)| (node.span.end, node.span.start))
            .map(|(index, _)| index);
        comment.next_node = nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.span.start >= comment.span.end)
            .min_by_key(|(_, node)| (node.span.start, node.span.end))
            .map(|(index, _)| index);
        // Prefer the narrowest enclosing node; this makes an inline comment inside a call
        // attach to that call rather than only to the full query.
        comment.containing_node = nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| {
                node.span.start <= comment.span.start && node.span.end >= comment.span.end
            })
            .min_by_key(|(_, node)| node.span.end - node.span.start)
            .map(|(index, _)| index);
    }
}

pub fn parse_expression_lossless(input: &str) -> Result<LosslessParse<Expr>, DaxError> {
    parse_expression_lossless_with_dialect(input, Dialect::default())
}

pub fn parse_expression_lossless_with_dialect(
    input: &str,
    dialect: Dialect,
) -> Result<LosslessParse<Expr>, DaxError> {
    let tokens = Lexer::new(input, dialect)
        .lex_all()
        .map_err(DaxError::Lex)?;
    let mut parser = Parser::new(tokens, dialect);
    let ast = parser.parse_formula_expression().map_err(DaxError::Parse)?;
    let nodes = parser.node_spans;
    let span = nodes
        .iter()
        .rev()
        .find(|node| node.kind == AstNodeKind::Expression)
        .map_or(Span::new(0, 0), |node| node.span);
    let mut comments = scan_source_comments(input, dialect);
    attach_comments(&mut comments, &nodes);
    Ok(LosslessParse {
        ast,
        source: input.to_string(),
        span,
        nodes,
        comments,
    })
}

pub fn parse_query_lossless(input: &str) -> Result<LosslessParse<Query>, DaxError> {
    parse_query_lossless_with_dialect(input, Dialect::default())
}

pub fn parse_query_lossless_with_dialect(
    input: &str,
    dialect: Dialect,
) -> Result<LosslessParse<Query>, DaxError> {
    let tokens = Lexer::new(input, dialect)
        .lex_all()
        .map_err(DaxError::Lex)?;
    let mut parser = Parser::new(tokens, dialect);
    let ast = parser.parse_query().map_err(DaxError::Parse)?;
    let nodes = parser.node_spans;
    let span = nodes
        .iter()
        .rev()
        .find(|node| node.kind == AstNodeKind::Query)
        .map_or(Span::new(0, 0), |node| node.span);
    let mut comments = scan_source_comments(input, dialect);
    attach_comments(&mut comments, &nodes);
    Ok(LosslessParse {
        ast,
        source: input.to_string(),
        span,
        nodes,
        comments,
    })
}

// ---- tests ----

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! num {
        ($s:expr) => {
            Expr::Number($s.to_string())
        };
    }
    macro_rules! strlit {
        ($s:expr) => {
            Expr::String($s.to_string())
        };
    }
    macro_rules! ident {
        ($s:expr) => {
            Expr::Identifier($s.to_string())
        };
    }
    macro_rules! param {
        ($s:expr) => {
            Expr::Parameter($s.to_string())
        };
    }
    macro_rules! br {
        ($s:expr) => {
            Expr::BracketRef($s.to_string())
        };
    }
    macro_rules! qtbl {
        ($s:expr) => {
            TableName::quoted($s.to_string())
        };
    }
    macro_rules! utbl {
        ($s:expr) => {
            TableName::unquoted($s.to_string())
        };
    }
    macro_rules! bin {
        ($op:expr, $l:expr, $r:expr) => {
            Expr::Binary {
                op: $op,
                left: Box::new($l),
                right: Box::new($r),
            }
        };
    }
    macro_rules! un {
        ($op:expr, $e:expr) => {
            Expr::Unary {
                op: $op,
                expr: Box::new($e),
            }
        };
    }

    #[test]
    fn lex_bracket_escape() {
        let toks = lex("[a]]b]").unwrap();
        assert_eq!(toks.len(), 2); // ident + eof
        match &toks[0].kind {
            TokenKind::BracketIdent(s) => assert_eq!(s, "a]b"),
            _ => panic!("expected bracket ident"),
        }
    }

    #[test]
    fn lex_string_escape() {
        let toks = lex(r#""a""b""#).unwrap();
        match &toks[0].kind {
            TokenKind::String(s) => assert_eq!(s, r#"a"b"#),
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn lex_datetime_literal() {
        let input = r#"dt"2020-12-15T12:30:59""#;
        let toks = lex(input).unwrap();
        assert_eq!(toks[0].span, Span::new(0, input.len()));
        assert_eq!(
            toks[0].kind,
            TokenKind::DateTime("2020-12-15T12:30:59".to_string())
        );
    }

    #[test]
    fn parse_datetime_literal_case_insensitively() {
        for prefix in ["dt", "DT", "Dt", "dT"] {
            let input = format!(r#"{prefix}"2020-12-15T12:30:59""#);
            assert_eq!(
                parse_expression(&input).unwrap(),
                Expr::DateTime("2020-12-15T12:30:59".to_string())
            );
        }
    }

    #[test]
    fn datetime_literal_accepts_documented_forms() {
        for value in [
            "2015-1-9",
            "2015-1-9T02:30:00",
            "2015-1-9 02:30:00",
            "2020-02-31",
        ] {
            assert_eq!(
                parse_expression(&format!(r#"dt"{value}""#)).unwrap(),
                Expr::DateTime(value.into())
            );
        }
    }

    #[test]
    fn datetime_literal_rejects_invalid_lexical_forms_with_full_span() {
        for source in [
            r#"dt"""#,
            r#"dt"not-a-date""#,
            r#"dt"2020-1""#,
            r#"dt"2020-13-1""#,
            r#"dt"2020-1-32""#,
            r#"dt"2020-1-1T2:30:00""#,
            r#"dt"2020-1-1T02:30""#,
            r#"dt"2020-1-1T24:00:00""#,
            r#"dt"2020-1-1T02:30:00.123""#,
            r#"dt"2020-1-1T02:30:00Z""#,
        ] {
            let DaxError::Lex(err) = lex(source).unwrap_err() else {
                panic!("expected lexical error for {source}");
            };
            assert!(
                err.message.contains("invalid datetime literal"),
                "got: {err}"
            );
            assert_eq!(err.span, Span::new(0, source.len()), "source: {source}");
        }

        let source = r#"dt"2020-1-1"#;
        let DaxError::Lex(err) = lex(source).unwrap_err() else {
            panic!("expected lexical error");
        };
        assert_eq!(err.message, "unterminated datetime literal");
        assert_eq!(err.span, Span::new(0, source.len()));
    }

    #[test]
    fn datetime_literal_participates_in_expressions() {
        assert_eq!(
            parse_expression(r#"[CreatedAt] >= dt"2020-12-15T12:30:59""#).unwrap(),
            bin!(
                BinaryOp::Gte,
                br!("CreatedAt"),
                Expr::DateTime("2020-12-15T12:30:59".to_string())
            )
        );
    }

    #[test]
    fn comments_are_skipped() {
        let e = parse_expression("1 + 2 -- hello\n * 3").unwrap();
        assert_eq!(
            e,
            bin!(
                BinaryOp::Add,
                num!("1"),
                bin!(BinaryOp::Mul, num!("2"), num!("3"))
            )
        );
    }

    #[test]
    fn dash_dash_comment_still_works_at_end_of_line() {
        // canonical line comment: `--` followed by whitespace stays a comment
        let e = parse_expression("1 -- trailing\n").unwrap();
        assert_eq!(e, num!("1"));
    }

    #[test]
    fn contiguous_dash_dash_is_a_line_comment() {
        // `--` always starts a line comment in DAX, even with no following space, so the rest of
        // the line is elided. `[Sales]--[Cost]` is therefore just `[Sales]`.
        let e = parse_expression("[Sales]--[Cost]").unwrap();
        assert_eq!(e, br!("Sales"));

        // `--text` (no space) is still a comment to end of line.
        let e2 = parse_expression("1 --[Cost]\n").unwrap();
        assert_eq!(e2, num!("1"));
    }

    /// Run `f` on a thread with a generous stack. The depth guard's job is to fire
    /// *before* the native stack is exhausted; production entry threads (e.g. the
    /// PyO3 main thread, ~8 MB) clear `MAX_PARSE_DEPTH` frames comfortably. cargo's
    /// default test-thread stack is only ~2 MB, which the deepest (function-call
    /// arg) path can exhaust at the cap, so we give these guard-logic tests room to
    /// reach the guarded `Err` instead of flaking on raw frame size.
    fn with_big_stack(f: impl FnOnce() + Send + 'static) {
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(f)
            .expect("spawn test thread")
            .join()
            .expect("test thread panicked");
    }

    #[test]
    fn deeply_nested_parens_error_instead_of_overflow() {
        // Pathological nesting must surface a catchable ParseError, not overflow
        // the native stack (uncatchable across PyO3).
        with_big_stack(|| {
            let depth = MAX_PARSE_DEPTH + 50;
            let src = format!("{}1{}", "(".repeat(depth), ")".repeat(depth));
            let err = parse_expression(&src).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("nesting too deep"), "got: {msg}");
        });
    }

    #[test]
    fn deeply_nested_unary_minus_error_instead_of_overflow() {
        // Space-separated minuses lex as distinct Minus tokens (a contiguous run of
        // dashes would be a `--` line comment), so each one nests one unary level.
        with_big_stack(|| {
            let src = format!("{}1", "- ".repeat(MAX_PARSE_DEPTH + 50));
            let err = parse_expression(&src).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("nesting too deep"), "got: {msg}");
        });
    }

    #[test]
    fn left_deep_operator_chain_not_affected_by_depth_guard() {
        // A long left-associative `+` chain unwinds between operators, so it must
        // parse fine well past MAX_PARSE_DEPTH (its actual recursion depth is small).
        with_big_stack(|| {
            let n = MAX_PARSE_DEPTH * 4;
            let src = vec!["1"; n].join("+");
            // Should parse without tripping the depth guard.
            parse_expression(&src).expect("left-deep chain should not hit depth cap");
        });
    }

    #[test]
    fn deeply_nested_query_args_error_instead_of_overflow() {
        // The parse_query entry path funnels into parse_expr_bp too; nested args
        // must error rather than overflow.
        with_big_stack(|| {
            let depth = MAX_PARSE_DEPTH + 50;
            let src = format!(
                "evaluate {{ {}1{} }}",
                "f(".repeat(depth),
                ")".repeat(depth)
            );
            let err = parse_query(&src).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("nesting too deep"), "got: {msg}");
        });
    }

    #[test]
    fn precedence_mul_over_add() {
        let e = parse_expression("1 + 2 * 3").unwrap();
        assert_eq!(
            e,
            bin!(
                BinaryOp::Add,
                num!("1"),
                bin!(BinaryOp::Mul, num!("2"), num!("3"))
            )
        );
    }

    #[test]
    fn right_assoc_pow() {
        let e = parse_expression("2 ^ 3 ^ 4").unwrap();
        assert_eq!(
            e,
            bin!(
                BinaryOp::Pow,
                num!("2"),
                bin!(BinaryOp::Pow, num!("3"), num!("4"))
            )
        );
    }

    #[test]
    fn unary_minus_binds_between_pow_and_mul() {
        // DAX precedence: exponentiation before sign
        // So -2^2 == -(2^2)
        let e = parse_expression("-2^2").unwrap();
        assert_eq!(
            e,
            un!(UnaryOp::Minus, bin!(BinaryOp::Pow, num!("2"), num!("2")))
        );

        // but sign still binds tighter than multiplication: -2*3 == (-2)*3
        let e2 = parse_expression("-2 * 3").unwrap();
        assert_eq!(
            e2,
            bin!(BinaryOp::Mul, un!(UnaryOp::Minus, num!("2")), num!("3"))
        );
    }

    #[test]
    fn not_binds_looser_than_comparisons_but_tighter_than_and_or() {
        let e = parse_expression("not 1 = 2").unwrap();
        assert_eq!(
            e,
            un!(UnaryOp::Not, bin!(BinaryOp::Eq, num!("1"), num!("2")))
        );

        let e2 = parse_expression("not true && false").unwrap();
        assert_eq!(
            e2,
            bin!(
                BinaryOp::And,
                un!(UnaryOp::Not, Expr::Boolean(true)),
                Expr::Boolean(false)
            )
        );
    }

    #[test]
    fn strict_equality_operator() {
        let e = parse_expression("1 == 2").unwrap();
        assert_eq!(e, bin!(BinaryOp::StrictEq, num!("1"), num!("2")));
    }

    #[test]
    fn leading_dot_number_literal() {
        let e = parse_expression(".20 * 3").unwrap();
        assert_eq!(e, bin!(BinaryOp::Mul, num!(".20"), num!("3")));
    }

    #[test]
    fn var_block_parses() {
        let e = parse_expression("var x = 1 var y = x + 2 return y * 3").unwrap();
        assert_eq!(
            e,
            Expr::VarBlock {
                decls: vec![
                    VarDecl {
                        name: "x".into(),
                        expr: num!("1"),
                    },
                    VarDecl {
                        name: "y".into(),
                        expr: bin!(BinaryOp::Add, ident!("x"), num!("2")),
                    },
                ],
                body: Box::new(bin!(BinaryOp::Mul, ident!("y"), num!("3"))),
            }
        );
    }

    #[test]
    fn function_call_args() {
        let e = parse_expression(r#"sumx('sales', 'sales'[amount] + 1)"#).unwrap();
        assert_eq!(
            e,
            Expr::FunctionCall {
                name: "sumx".into(),
                args: vec![
                    Expr::TableRef(qtbl!("sales")),
                    bin!(
                        BinaryOp::Add,
                        Expr::TableColumnRef {
                            table: qtbl!("sales"),
                            column: "amount".into(),
                        },
                        num!("1")
                    )
                ],
            }
        );
    }

    #[test]
    fn function_calls_parse_official_omitted_argument_examples() {
        assert_eq!(
            parse_expression("index(1, , -1)").unwrap(),
            Expr::FunctionCall {
                name: "index".into(),
                args: vec![num!("1"), Expr::Omitted, un!(UnaryOp::Minus, num!("1"))],
            }
        );
        assert_eq!(
            parse_expression("window(0, ABS, 0, REL,, -1)").unwrap(),
            Expr::FunctionCall {
                name: "window".into(),
                args: vec![
                    num!("0"),
                    ident!("ABS"),
                    num!("0"),
                    ident!("REL"),
                    Expr::Omitted,
                    un!(UnaryOp::Minus, num!("1")),
                ],
            }
        );
    }

    #[test]
    fn udf_call_preserves_omitted_middle_default_position() {
        let q = parse_query(
            "define function f = (a: numeric, b: numeric = 2, c: numeric = 3) => a + b + c
             evaluate { f(1,,3) }",
        )
        .unwrap();

        let Expr::TableConstructor(rows) = &q.evaluates[0].expr else {
            panic!("expected table constructor");
        };
        assert_eq!(
            rows[0][0],
            Expr::FunctionCall {
                name: "f".into(),
                args: vec![num!("1"), Expr::Omitted, num!("3")],
            }
        );
    }

    #[test]
    fn omitted_expressions_remain_confined_to_function_arguments() {
        for source in ["1 + , 2", "{1,,3}"] {
            let err = parse_expression(source).unwrap_err();
            assert!(
                err.to_string().contains("expected expression"),
                "got: {err}"
            );
        }

        for source in ["f(1,)", "f(,)"] {
            let err = parse_expression(source).unwrap_err();
            assert!(
                err.to_string().contains("trailing argument separator"),
                "got: {err}"
            );
        }
    }

    #[test]
    fn table_constructor_scalar_rows() {
        let e = parse_expression("{1, 2, 3}").unwrap();
        assert_eq!(
            e,
            Expr::TableConstructor(vec![vec![num!("1")], vec![num!("2")], vec![num!("3")]])
        );
    }

    #[test]
    fn table_constructor_tuple_rows() {
        let e = parse_expression("{(1, 2), (3, 4)}").unwrap();
        assert_eq!(
            e,
            Expr::TableConstructor(vec![vec![num!("1"), num!("2")], vec![num!("3"), num!("4")]])
        );
    }

    #[test]
    fn table_constructor_requires_rows_with_equal_width() {
        for source in ["{}", "{(1, 2), (3)}", "{(1), (2, 3)}"] {
            let err = parse_expression(source).unwrap_err();
            assert!(
                err.to_string().contains("table constructor"),
                "{source}: got {err}"
            );
        }
    }

    #[test]
    fn datatable_parses_schema_constants_and_missing_values() {
        let expr = parse_expression(
            r#"DATATABLE(
                "Name", STRING,
                "When", DATETIME,
                "Amount", CURRENCY,
                {
                    {"A", DATE(2024, 1, 2) + TIME(3, 4, 5), -1.5},
                    {"B", "2024-01-03", }
                }
            )"#,
        )
        .unwrap();
        let Expr::DataTable { columns, rows } = expr else {
            panic!("expected DATATABLE AST");
        };
        assert_eq!(columns.len(), 3);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1][2], Expr::Omitted);

        let expr = parse_expression(
            r#"DATATABLE(
                "A", INTEGER,
                "B", LOGICAL,
                {{1, TRUE}, {, FALSE}}
            )"#,
        )
        .unwrap();
        assert_eq!(
            expr,
            Expr::DataTable {
                columns: vec![
                    DataTableColumn {
                        name: "A".into(),
                        data_type: DataTableType::Integer,
                    },
                    DataTableColumn {
                        name: "B".into(),
                        data_type: DataTableType::Boolean,
                    },
                ],
                rows: vec![
                    vec![num!("1"), Expr::Boolean(true)],
                    vec![Expr::Omitted, Expr::Boolean(false)],
                ],
            }
        );
    }

    #[test]
    fn datatable_rejects_bad_schema_rows_and_non_constants() {
        for (source, expected) in [
            (
                r#"DATATABLE("A", UUID, {{1}})"#,
                "unsupported DATATABLE column type",
            ),
            (
                r#"DATATABLE("A", INTEGER, "B", STRING, {{1}})"#,
                "schema defines 2 columns",
            ),
            (
                r#"DATATABLE("A", INTEGER, {{[Measure]}})"#,
                "must be constant expressions",
            ),
            (
                r#"DATATABLE("A", INTEGER, {{RAND()}})"#,
                "must be constant expressions",
            ),
            (
                r#"DATATABLE("A", DATETIME, {{DATE([Year], 1, 1) + TIME(0, 0, 0)}})"#,
                "must be constant expressions",
            ),
            (
                r#"DATATABLE("A", DATETIME, {{DATE(RAND(), 1, 1) + TIME(0, 0, 0)}})"#,
                "must be constant expressions",
            ),
        ] {
            let err = parse_expression(source).unwrap_err();
            assert!(err.to_string().contains(expected), "{source}: got {err}");
        }
    }

    #[test]
    fn table_and_bracket_ref() {
        let e = parse_expression("'Sales'[Amount] & [Total Sales]").unwrap();
        assert_eq!(
            e,
            bin!(
                BinaryOp::Concat,
                Expr::TableColumnRef {
                    table: qtbl!("Sales"),
                    column: "Amount".into()
                },
                br!("Total Sales")
            )
        );
    }

    #[test]
    fn parse_query_define_and_evaluate() {
        let q = parse_query(
            "define
               measure 't'[m] = 1
               var v = 2
             evaluate
               't'
             order by
               [m] desc
             start at
               5",
        )
        .unwrap();

        assert_eq!(
            q,
            Query {
                define: Some(DefineBlock {
                    defs: vec![
                        Definition::Measure {
                            doc: None,
                            table: Some(qtbl!("t")),
                            name: "m".into(),
                            expr: num!("1"),
                        },
                        Definition::Var {
                            doc: None,
                            name: "v".into(),
                            expr: num!("2"),
                        }
                    ]
                }),
                evaluates: vec![EvaluateStmt {
                    expr: Expr::TableRef(qtbl!("t")),
                    order_by: vec![OrderKey {
                        expr: br!("m"),
                        direction: SortDirection::Desc
                    }],
                    start_at: Some(vec![num!("5")]),
                }]
            }
        );
    }

    #[test]
    fn parse_query_multiple_evaluate_and_semicolons() {
        let q = parse_query("evaluate { [m] }; evaluate 't';").unwrap();
        assert_eq!(q.evaluates.len(), 2);
        assert_eq!(
            q.evaluates[0].expr,
            Expr::TableConstructor(vec![vec![br!("m")]])
        );
        assert_eq!(q.evaluates[1].expr, Expr::TableRef(qtbl!("t")));
    }

    #[test]
    fn start_at_requires_order_by() {
        let err = parse_query("evaluate 't' start at 1").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("START AT requires an ORDER BY"), "got: {msg}");
    }

    #[test]
    fn start_at_arg_count_must_not_exceed_order_by() {
        let err = parse_query("evaluate 't' order by [a] start at 1, 2").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("more arguments"), "got: {msg}");
    }

    #[test]
    fn start_at_allows_literal_constants() {
        let q = parse_query(
            r#"evaluate 't'
               order by [a], [b], [c], [d], [e], [f]
               start at -1, +2.5, "abc", dt"2020-12-15T12:30:59", true, false"#,
        )
        .unwrap();
        assert_eq!(
            q.evaluates[0].start_at,
            Some(vec![
                un!(UnaryOp::Minus, num!("1")),
                un!(UnaryOp::Plus, num!("2.5")),
                strlit!("abc"),
                Expr::DateTime("2020-12-15T12:30:59".into()),
                Expr::Boolean(true),
                Expr::Boolean(false),
            ])
        );
    }

    #[test]
    fn start_at_allows_at_param() {
        let q = parse_query("evaluate 't' order by [a] start at @p").unwrap();
        assert_eq!(q.evaluates[0].start_at, Some(vec![param!("p")]));
    }

    #[test]
    fn start_at_rejects_non_constant_expressions() {
        for value in [
            "1 + 2",
            "[x]",
            "x",
            "abs(1)",
            "blank()",
            "blank",
            "{1}",
            "var x = 1 return x",
        ] {
            let query = format!("evaluate 't' order by [a] start at {value}");
            let err = parse_query(&query).unwrap_err();
            assert!(
                err.to_string().contains("literal constant or @parameter"),
                "{value}: got {err}"
            );
        }
    }

    #[test]
    fn define_function_udf_parses_with_doc() {
        let q = parse_query(
            "define
                /// adds two numbers
                function sumtwo = ( a, b : numeric ) => a + b
             evaluate
                { sumtwo(10, 20) }",
        )
        .unwrap();

        assert_eq!(
            q.define.unwrap().defs[0],
            Definition::Function {
                doc: Some("adds two numbers".into()),
                name: "sumtwo".into(),
                params: vec![
                    FuncParam {
                        name: "a".into(),
                        type_hints: vec![],
                        default: None,
                    },
                    FuncParam {
                        name: "b".into(),
                        type_hints: vec!["numeric".into()],
                        default: None,
                    }
                ],
                body: bin!(BinaryOp::Add, ident!("a"), ident!("b")),
            }
        );
    }

    #[test]
    fn define_function_udf_parses_parameter_defaults() {
        let q = parse_query(
            "define
                function addtax = (
                    amount : numeric,
                    taxrate : numeric = 0.1,
                    scale = divide(1 + 2, 3) * 4
                ) => amount + amount * taxrate * scale
             evaluate { addtax(100) }",
        )
        .unwrap();

        let Definition::Function { params, .. } = &q.define.unwrap().defs[0] else {
            panic!("expected function definition");
        };
        assert_eq!(params.len(), 3);
        assert_eq!(params[0].default, None);
        assert_eq!(params[1].type_hints, vec!["numeric"]);
        assert_eq!(params[1].default, Some(num!("0.1")));
        assert_eq!(
            params[2].default,
            Some(bin!(
                BinaryOp::Mul,
                Expr::FunctionCall {
                    name: "divide".into(),
                    args: vec![bin!(BinaryOp::Add, num!("1"), num!("2")), num!("3")],
                },
                num!("4")
            ))
        );
    }

    #[test]
    fn define_function_udf_rejects_missing_default_expression() {
        for query in [
            "define function f = (x: numeric =) => x evaluate { f() }",
            "define function f = (x: numeric =, y: numeric) => x evaluate { f() }",
        ] {
            let err = parse_query(query).unwrap_err();
            assert!(
                err.to_string().contains("expected expression"),
                "got: {err}"
            );
        }
    }

    #[test]
    fn define_table_parses_official_visual_shape_example() {
        let q = parse_query(
            r#"
            define table data = summarizecolumns(
                rollupaddissubtotal(T[Year], "IsYearTotal"),
                rollupaddissubtotal(T[Product], "IsProductTotal"),
                "Measure", sum(T[SalesAmount])
            )
            with visual shape
                axis rows group [Year] total [IsYearTotal] order by [Year]
                axis columns group [Product] total [IsProductTotal] order by [Product]
                densify "IsDensified"
            evaluate data
            "#,
        )
        .unwrap();

        let Definition::Table { visual_shape, .. } = &q.define.unwrap().defs[0] else {
            panic!("expected table definition");
        };
        let shape = visual_shape
            .as_ref()
            .expect("visual shape should be present");
        assert_eq!(shape.axes.len(), 2);
        assert_eq!(shape.axes[0].name, "rows");
        assert_eq!(shape.axes[0].groups.len(), 1);
        assert_eq!(shape.axes[0].groups[0].columns[0].name, "Year");
        assert_eq!(shape.axes[0].groups[0].total.name, "IsYearTotal");
        assert_eq!(shape.axes[0].order_by[0].name, "Year");
        assert_eq!(shape.axes[1].name, "columns");
        assert_eq!(shape.axes[1].groups[0].columns[0].name, "Product");
        assert_eq!(shape.densify.as_deref(), Some("IsDensified"));
    }

    #[test]
    fn define_table_visual_shape_supports_multiple_groups_and_columns() {
        let q = parse_query(
            r#"
            define table data = T
            with visual shape
                axis rows
                    group [Year], [Month] total [IsDateTotal]
                    group [Product] total [IsProductTotal]
                    order by [Year], [Month], [Product]
            evaluate data
            "#,
        )
        .unwrap();

        let Definition::Table { visual_shape, .. } = &q.define.unwrap().defs[0] else {
            panic!("expected table definition");
        };
        let axis = &visual_shape.as_ref().unwrap().axes[0];
        assert_eq!(axis.groups.len(), 2);
        assert_eq!(axis.groups[0].columns.len(), 2);
        assert_eq!(axis.order_by.len(), 3);
    }

    #[test]
    fn ordinary_define_table_has_no_visual_shape() {
        let q = parse_query("define table data = T evaluate data").unwrap();
        let Definition::Table { visual_shape, .. } = &q.define.unwrap().defs[0] else {
            panic!("expected table definition");
        };
        assert_eq!(*visual_shape, None);
    }

    #[test]
    fn define_table_visual_shape_rejects_malformed_clauses() {
        let cases = [
            (
                "define table data = T with visual shape densify \"d\" evaluate data",
                "requires at least one AXIS",
            ),
            (
                "define table data = T with visual shape axis rows order by [Year] evaluate data",
                "requires at least one GROUP",
            ),
            (
                "define table data = T with visual shape axis rows group [Year] order by [Year] evaluate data",
                "expected keyword total",
            ),
            (
                "define table data = T with visual shape axis rows group [Year] total [IsTotal] evaluate data",
                "expected keyword order",
            ),
            (
                "define table data = T with visual shape axis rows group [Year] total [IsTotal] order by [Year] densify IsDensified evaluate data",
                "expected string literal after DENSIFY",
            ),
        ];

        for (query, expected) in cases {
            let err = parse_query(query).unwrap_err();
            assert!(err.to_string().contains(expected), "got: {err}");
        }
    }

    #[test]
    fn in_operator() {
        let e = parse_expression("[x] in {1,2,3}").unwrap();
        assert_eq!(
            e,
            bin!(
                BinaryOp::In,
                br!("x"),
                Expr::TableConstructor(vec![vec![num!("1")], vec![num!("2")], vec![num!("3")]])
            )
        );
    }

    #[test]
    fn multi_column_in_uses_tuple_expression() {
        let expr =
            parse_expression(r#"('Product'[Color], 'Product'[Brand]) in {("Red", "Contoso")}"#)
                .unwrap();
        assert_eq!(
            expr,
            bin!(
                BinaryOp::In,
                Expr::Tuple(vec![
                    Expr::TableColumnRef {
                        table: qtbl!("Product"),
                        column: "Color".into(),
                    },
                    Expr::TableColumnRef {
                        table: qtbl!("Product"),
                        column: "Brand".into(),
                    },
                ]),
                Expr::TableConstructor(vec![vec![strlit!("Red"), strlit!("Contoso")]])
            )
        );
    }

    #[test]
    fn tuple_expression_requires_multiple_complete_elements() {
        assert_eq!(
            parse_expression("(1)").unwrap(),
            Expr::Paren(Box::new(num!("1")))
        );
        for source in ["(1,)", "(1,,2)"] {
            assert!(parse_expression(source).is_err(), "accepted: {source}");
        }
    }

    #[test]
    fn leading_equals_is_accepted() {
        let e = parse_expression("=1+2").unwrap();
        assert_eq!(e, bin!(BinaryOp::Add, num!("1"), num!("2")));
    }

    #[test]
    fn semicolon_separators() {
        let dialect = Dialect {
            allow_semicolon_separators: true,
            ..Default::default()
        };

        let e = parse_expression_with_dialect("sum(1; 2; 3)", dialect).unwrap();
        assert_eq!(
            e,
            Expr::FunctionCall {
                name: "sum".into(),
                args: vec![num!("1"), num!("2"), num!("3")]
            }
        );
    }

    #[test]
    fn errors_on_trailing_arg_separator() {
        let err = parse_expression("sum(1, )").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("trailing argument separator"), "got: {msg}");
    }

    #[test]
    fn errors_on_unterminated_string() {
        let err = parse_expression(r#""oops"#).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unterminated"), "got: {msg}");
    }

    #[test]
    fn errors_on_unexpected_after_statement_in_define() {
        // expression parser would parse "1" then next token "2" is not a valid stmt starter -> error
        let err = parse_query("define measure 't'[m] = 1 2 evaluate 't'").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unexpected token after statement"),
            "got: {msg}"
        );
    }

    #[test]
    fn errors_on_empty_evaluate() {
        let err = parse_query("define var x = 1").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("expected at least one EVALUATE"), "got: {msg}");
    }

    #[test]
    fn identifiers_can_be_tables_unquoted() {
        let e = parse_expression("sales").unwrap();
        // In real DAX, this may refer to a table; we keep it as Identifier for now.
        // (You can later add a resolution phase that rewrites Identifier->TableRef)
        assert_eq!(e, ident!("sales"));
    }

    #[test]
    fn quoted_table_ref_is_table_ref() {
        let e = parse_expression("'Sales'").unwrap();
        assert_eq!(e, Expr::TableRef(qtbl!("Sales")));
    }

    #[test]
    fn parens() {
        let e = parse_expression("(1 + 2) * 3").unwrap();
        assert_eq!(
            e,
            bin!(
                BinaryOp::Mul,
                Expr::Paren(Box::new(bin!(BinaryOp::Add, num!("1"), num!("2")))),
                num!("3")
            )
        );
    }

    #[test]
    fn logical_ops() {
        let e = parse_expression("true && false || true").unwrap();
        // && binds tighter than ||
        assert_eq!(
            e,
            bin!(
                BinaryOp::Or,
                bin!(BinaryOp::And, Expr::Boolean(true), Expr::Boolean(false)),
                Expr::Boolean(true)
            )
        );
    }

    #[test]
    fn comparisons_chain_left_assoc() {
        let e = parse_expression("1 = 2 = 3").unwrap();
        assert_eq!(
            e,
            bin!(
                BinaryOp::Eq,
                bin!(BinaryOp::Eq, num!("1"), num!("2")),
                num!("3")
            )
        );
    }

    #[test]
    fn concat_precedence_between_add_and_compare() {
        let e = parse_expression(r#""a" & "b" = "ab""#).unwrap();
        assert_eq!(
            e,
            bin!(
                BinaryOp::Eq,
                bin!(BinaryOp::Concat, strlit!("a"), strlit!("b")),
                strlit!("ab")
            )
        );
    }

    #[test]
    fn table_column_ref_unquoted_table() {
        let e = parse_expression("t[amount]").unwrap();
        assert_eq!(
            e,
            Expr::TableColumnRef {
                table: utbl!("t"),
                column: "amount".into()
            }
        );
    }

    fn expression_node_count(expr: &Expr) -> usize {
        let children = match expr {
            Expr::FunctionCall { args, .. } => args.iter().map(expression_node_count).sum(),
            Expr::DataTable { rows, .. } | Expr::TableConstructor(rows) => {
                rows.iter().flatten().map(expression_node_count).sum()
            }
            Expr::Unary { expr, .. } | Expr::Paren(expr) => expression_node_count(expr),
            Expr::Binary { left, right, .. } => {
                expression_node_count(left) + expression_node_count(right)
            }
            Expr::VarBlock { decls, body } => {
                decls
                    .iter()
                    .map(|decl| expression_node_count(&decl.expr))
                    .sum::<usize>()
                    + expression_node_count(body)
            }
            Expr::Tuple(elements) => elements.iter().map(expression_node_count).sum(),
            _ => 0,
        };
        1 + children
    }

    #[test]
    fn lossless_expression_locates_every_expression_node_and_comment() {
        let source = "/// measure docs\nSUM(/* inside */ 1, -- next\n 2 + 3) // tail";
        let parsed = parse_expression_lossless(source).unwrap();
        let expression_spans: Vec<_> = parsed
            .nodes
            .iter()
            .filter(|node| node.kind == AstNodeKind::Expression)
            .collect();

        assert_eq!(expression_spans.len(), expression_node_count(&parsed.ast));
        assert_eq!(
            &source[parsed.span.start..parsed.span.end],
            "SUM(/* inside */ 1, -- next\n 2 + 3)"
        );
        assert_eq!(
            parsed
                .comments
                .iter()
                .map(|comment| (comment.kind, comment.text.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (CommentKind::DocLine, "/// measure docs"),
                (CommentKind::Block, "/* inside */"),
                (CommentKind::DashLine, "-- next"),
                (CommentKind::SlashLine, "// tail"),
            ]
        );
        assert!(parsed.comments[1].containing_node.is_some());
        assert!(parsed.comments[3].previous_node.is_some());
    }

    #[test]
    fn lossless_comments_ignore_comment_markers_inside_literals() {
        let source = r#"CONCATENATE("// text", '/* table */'[-- column]) /* real */"#;
        let parsed = parse_expression_lossless(source).unwrap();
        assert_eq!(parsed.comments.len(), 1);
        assert_eq!(parsed.comments[0].text, "/* real */");
    }

    #[test]
    fn lossless_query_locates_query_definitions_and_evaluates() {
        let source = "/// docs\nDEFINE MEASURE 'T'[M] = 1 + 2\n-- between\nEVALUATE { [M] }";
        let parsed = parse_query_lossless(source).unwrap();

        assert_eq!(
            parsed
                .nodes
                .iter()
                .filter(|node| node.kind == AstNodeKind::Definition)
                .count(),
            1
        );
        assert_eq!(
            parsed
                .nodes
                .iter()
                .filter(|node| node.kind == AstNodeKind::Evaluate)
                .count(),
            1
        );
        assert_eq!(
            parsed.nodes.last().map(|node| node.kind),
            Some(AstNodeKind::Query)
        );
        assert_eq!(parsed.comments.len(), 2);
        assert_eq!(
            &source[parsed.span.start..parsed.span.end],
            "DEFINE MEASURE 'T'[M] = 1 + 2\n-- between\nEVALUATE { [M] }"
        );
    }
}
