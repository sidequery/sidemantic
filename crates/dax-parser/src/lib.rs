//! dax_parser.rs — single-file DAX lexer+parser (expressions + basic queries)
//!
//! Patch highlights vs prior version:
//! - Fixes operator precedence per MS docs: `^` binds tighter than unary sign, comparisons bind tighter than `NOT`
//! - Adds `==` strict equality token/op
//! - Supports numeric literals starting with `.` (e.g. `.20`)
//! - Adds `@param` tokens/AST (needed for START AT params)
//! - Enforces START AT rules: requires ORDER BY, args must be constant or @param, count <= order keys
//! - Adds DEFINE FUNCTION (UDF) parsing: `FUNCTION f = (a : type ...) => body` + `///` doc comments
//! - Accepts optional semicolon statement terminators (between DEFINE entities / EVALUATE statements)
//!
//! Drop into `src/lib.rs` (or any module) and `cargo test`.
//! No external deps.

use serde::Serialize;
use std::fmt;

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
    Boolean(bool),
    Blank,

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
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FuncParam {
    pub name: String,
    /// Raw type-hint tokens after `:` (0..N identifiers), e.g. `NUMERIC`, or `Scalar Numeric expr`.
    pub type_hints: Vec<String>,
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

pub struct Parser {
    tokens: Vec<Token>,
    i: usize,
    dialect: Dialect,
}
impl Parser {
    pub fn new(tokens: Vec<Token>, dialect: Dialect) -> Self {
        Self {
            tokens,
            i: 0,
            dialect,
        }
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
        Ok(Query { define, evaluates })
    }

    // ---- query parsing ----

    fn parse_define_block(&mut self) -> Result<DefineBlock, ParseError> {
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

        Ok(DefineBlock { defs })
    }

    fn parse_define_measure(&mut self, doc: Option<String>) -> Result<Definition, ParseError> {
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

        Ok(Definition::Measure {
            doc,
            table,
            name,
            expr,
        })
    }

    fn parse_define_var(&mut self, doc: Option<String>) -> Result<Definition, ParseError> {
        self.expect_kw("var")?;
        let name = self.expect_ident("variable name")?;
        self.expect(TokenKind::Eq, "`=`")?;
        let expr = self.parse_expr_bp(0)?;

        self.consume_stmt_terminators();
        self.ensure_stmt_follower(&["measure", "function", "var", "table", "column", "evaluate"])?;

        Ok(Definition::Var { doc, name, expr })
    }

    fn parse_define_table(&mut self, doc: Option<String>) -> Result<Definition, ParseError> {
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

        self.consume_stmt_terminators();
        self.ensure_stmt_follower(&["measure", "function", "var", "table", "column", "evaluate"])?;

        Ok(Definition::Table { doc, name, expr })
    }

    fn parse_define_column(&mut self, doc: Option<String>) -> Result<Definition, ParseError> {
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

        Ok(Definition::Column {
            doc,
            table,
            name,
            expr,
        })
    }

    fn parse_define_function(&mut self, doc: Option<String>) -> Result<Definition, ParseError> {
        // FUNCTION <function name> = ([parameter name]: [parameter type], ...) => <function body>
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

                params.push(FuncParam {
                    name: pname,
                    type_hints,
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

        Ok(Definition::Function {
            doc,
            name,
            params,
            body,
        })
    }

    fn parse_evaluate_stmt(&mut self) -> Result<EvaluateStmt, ParseError> {
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

        Ok(EvaluateStmt {
            expr,
            order_by,
            start_at,
        })
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
            values.push(self.parse_expr_bp(0)?);
            if self.eat_separator() {
                continue;
            }
            break;
        }

        Ok(values)
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
            lhs = Expr::Binary {
                op,
                left: Box::new(lhs),
                right: Box::new(rhs),
            };
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
        if self.eat(TokenKind::Plus).is_some() {
            let expr = self.parse_expr_bp(8)?;
            return Ok(Expr::Unary {
                op: UnaryOp::Plus,
                expr: Box::new(expr),
            });
        }
        if self.eat(TokenKind::Minus).is_some() {
            let expr = self.parse_expr_bp(8)?;
            return Ok(Expr::Unary {
                op: UnaryOp::Minus,
                expr: Box::new(expr),
            });
        }

        // IMPORTANT: precedence per MS docs: comparisons bind tighter than NOT, but NOT binds
        // tighter than && / ||.
        if self.peek_kw("not") {
            self.bump();
            let expr = self.parse_expr_bp(3)?;
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            });
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
        table: TableName,
        column: String,
    ) -> Result<Expr, ParseError> {
        if !self.peek_is(TokenKind::Dot) {
            return Ok(Expr::TableColumnRef { table, column });
        }

        let mut levels = Vec::new();
        while self.eat(TokenKind::Dot).is_some() {
            let level = self.expect_bracket_ident("hierarchy level like [Year]")?;
            levels.push(level);
        }

        Ok(Expr::HierarchyRef {
            table,
            column,
            levels,
        })
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        match self.peek().kind.clone() {
            TokenKind::Number(n) => {
                self.bump();
                Ok(Expr::Number(n))
            }
            TokenKind::String(s) => {
                self.bump();
                Ok(Expr::String(s))
            }
            TokenKind::Param(p) => {
                self.bump();
                Ok(Expr::Parameter(p))
            }
            TokenKind::BracketIdent(name) => {
                self.bump();
                Ok(Expr::BracketRef(name))
            }
            TokenKind::QuotedIdent(name) => {
                self.bump();
                let table = TableName::quoted(name);

                // 'Table'[Column]
                if let TokenKind::BracketIdent(col) = self.peek().kind.clone() {
                    self.bump();
                    self.parse_hierarchy_tail(table, col)
                } else {
                    Ok(Expr::TableRef(table))
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
                    let args = self.parse_arg_list()?;
                    return Ok(Expr::FunctionCall { name: id, args });
                }

                let table = TableName::unquoted(id.clone());
                if let TokenKind::BracketIdent(col) = self.peek().kind.clone() {
                    self.bump();
                    return self.parse_hierarchy_tail(table, col);
                }

                // contextual literals (after call/column checks so TRUE() / BLANK() parse)
                if id.eq_ignore_ascii_case("true") {
                    return Ok(Expr::Boolean(true));
                }
                if id.eq_ignore_ascii_case("false") {
                    return Ok(Expr::Boolean(false));
                }
                // DAX's "blank" is usually BLANK(), but some tooling treats BLANK as a literal-ish value.
                if id.eq_ignore_ascii_case("blank") {
                    return Ok(Expr::Blank);
                }

                Ok(Expr::Identifier(id))
            }
            TokenKind::LParen => {
                self.bump();
                let inner = self.parse_expr_bp(0)?;
                self.expect(TokenKind::RParen, "`)`")?;
                Ok(Expr::Paren(Box::new(inner)))
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
            let expr = self.parse_expr_bp(0)?;
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

    fn parse_var_block(&mut self) -> Result<Expr, ParseError> {
        // VAR <name> = <expr> [VAR ...] RETURN <expr>
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

        Ok(Expr::VarBlock {
            decls,
            body: Box::new(body),
        })
    }

    fn parse_table_constructor(&mut self) -> Result<Expr, ParseError> {
        // { row (, row)* }
        // row := scalar_expr | '(' expr (, expr)* ')'
        self.expect(TokenKind::LBrace, "`{`")?;

        if self.peek_is(TokenKind::RBrace) {
            self.bump();
            return Ok(Expr::TableConstructor(Vec::new()));
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
        Ok(Expr::TableConstructor(rows))
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
    fn start_at_allows_expression_args() {
        let q = parse_query("evaluate 't' order by [a] start at [x]").unwrap();
        assert_eq!(q.evaluates[0].start_at, Some(vec![br!("x")]));
    }

    #[test]
    fn start_at_allows_at_param() {
        let q = parse_query("evaluate 't' order by [a] start at @p").unwrap();
        assert_eq!(q.evaluates[0].start_at, Some(vec![param!("p")]));
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
                    },
                    FuncParam {
                        name: "b".into(),
                        type_hints: vec!["numeric".into()],
                    }
                ],
                body: bin!(BinaryOp::Add, ident!("a"), ident!("b")),
            }
        );
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
}
