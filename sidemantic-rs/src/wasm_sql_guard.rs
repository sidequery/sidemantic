//! Conservative WASM parser work limits; native hosts retain their larger stack.
//! Use the owning dialect's tokenizer so literal/comment contents are never SQL
//! structure. Token count also bounds recursive unary and operator chains, which
//! need not contain any parentheses.
use crate::error::{Result, SidemanticError};
use polyglot_sql::{dialects::Dialect, DialectType, TokenType};

pub(crate) const MAX_NESTING: usize = 16;
pub(crate) const MAX_TOKENS: usize = 256;

pub(crate) fn check(sql: &str, dialect: DialectType) -> Result<()> {
    let tokens = Dialect::get(dialect)
        .tokenize(sql)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    let mut nesting = 0usize;
    let mut count = 0usize;
    for token in tokens {
        if matches!(
            token.token_type,
            TokenType::BlockComment | TokenType::LineComment
        ) {
            continue;
        }
        count += 1;
        if count > MAX_TOKENS {
            return Err(SidemanticError::SqlParse(format!(
                "WASM SQL parser token limit exceeded ({MAX_TOKENS})"
            )));
        }
        match token.token_type {
            TokenType::LParen | TokenType::LBracket | TokenType::LBrace | TokenType::Case => {
                nesting += 1;
                if nesting > MAX_NESTING {
                    return Err(SidemanticError::SqlParse(format!(
                        "WASM SQL parser nesting limit exceeded ({MAX_NESTING})"
                    )));
                }
            }
            TokenType::RParen | TokenType::RBracket | TokenType::RBrace | TokenType::End => {
                nesting = nesting.saturating_sub(1);
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialect_lexer_ignores_literal_and_comment_delimiters() {
        for sql in [
            "SELECT '(((((((((((((((((((((', \"((((((((((((((((((((\"",
            "SELECT $$((((((((((((((((((((((($$",
            "SELECT 1 /* ((((((((((((((((((( */ -- (((((((((((((((((((\n",
        ] {
            check(sql, DialectType::DuckDB).unwrap();
        }
    }

    #[test]
    fn limits_cover_parentheses_and_unary_chains() {
        check(
            &format!(
                "SELECT {}1{}",
                "(".repeat(MAX_NESTING),
                ")".repeat(MAX_NESTING)
            ),
            DialectType::DuckDB,
        )
        .unwrap();
        assert!(check(
            &format!(
                "SELECT {}1{}",
                "(".repeat(MAX_NESTING + 1),
                ")".repeat(MAX_NESTING + 1)
            ),
            DialectType::DuckDB
        )
        .is_err());
        assert!(check(
            &format!("SELECT {}true", "NOT ".repeat(MAX_TOKENS)),
            DialectType::DuckDB
        )
        .is_err());
    }
}
