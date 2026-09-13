//! Conservative WASM parser work limits; native hosts retain their larger stack.
//! Use the owning dialect's tokenizer so literal/comment contents are never SQL
//! structure. Operator-chain length also bounds recursive unary/operator chains, which
//! need not contain any parentheses.
use crate::error::{Result, SidemanticError};
use polyglot_sql::{dialects::Dialect, DialectType, TokenType};

pub(crate) const MAX_NESTING: usize = 16;
pub(crate) const MAX_OPERATORS: usize = 32;

pub(crate) fn check(sql: &str, dialect: DialectType) -> Result<()> {
    let tokens = Dialect::get(dialect)
        .tokenize(sql)
        .map_err(|error| SidemanticError::SqlParse(error.to_string()))?;
    let mut nesting = 0usize;
    let mut operators = 0usize;
    let mut set_operations = 0usize;
    let mut parent_operators = Vec::new();
    for token in tokens {
        if matches!(
            token.token_type,
            TokenType::BlockComment | TokenType::LineComment
        ) {
            continue;
        }
        match token.token_type {
            TokenType::Comma
            | TokenType::Select
            | TokenType::From
            | TokenType::Where
            | TokenType::Having
            | TokenType::Join
            | TokenType::On
            | TokenType::Then
            | TokenType::Else
            | TokenType::Semicolon => operators = 0,
            TokenType::Union | TokenType::Intersect | TokenType::Except => {
                set_operations += 1;
                if set_operations > MAX_NESTING {
                    return Err(SidemanticError::SqlParse(
                        "WASM SQL parser set-operation limit exceeded (16)".into(),
                    ));
                }
            }
            TokenType::Dash
            | TokenType::Plus
            | TokenType::Star
            | TokenType::Slash
            | TokenType::Mod
            | TokenType::Percent
            | TokenType::Lt
            | TokenType::Lte
            | TokenType::Gt
            | TokenType::Gte
            | TokenType::Not
            | TokenType::Eq
            | TokenType::Neq
            | TokenType::NullsafeEq
            | TokenType::And
            | TokenType::Or
            | TokenType::Amp
            | TokenType::DPipe
            | TokenType::Pipe
            | TokenType::Caret
            | TokenType::LtLt
            | TokenType::GtGt
            | TokenType::Tilde
            | TokenType::Arrow
            | TokenType::DArrow
            | TokenType::DColon
            | TokenType::Like
            | TokenType::ILike
            | TokenType::Is
            | TokenType::In
            | TokenType::Between
            | TokenType::Xor
            | TokenType::DStar
            | TokenType::NotLike
            | TokenType::NotILike
            | TokenType::NotRLike
            | TokenType::NotIRLike
            | TokenType::RLike
            | TokenType::IRLike
            | TokenType::Colon
            | TokenType::DotColon
            | TokenType::ColonEq
            | TokenType::ColonGt
            | TokenType::NColonGt
            | TokenType::DAt
            | TokenType::AtAt
            | TokenType::LtAt
            | TokenType::AtGt
            | TokenType::DAmp
            | TokenType::AmpLt
            | TokenType::AmpGt
            | TokenType::HashArrow
            | TokenType::DHashArrow
            | TokenType::FArrow
            | TokenType::PipeGt
            | TokenType::PipeSlash
            | TokenType::DPipeSlash
            | TokenType::QMarkAmp
            | TokenType::QMarkPipe
            | TokenType::HashDash
            | TokenType::Exclamation
            | TokenType::Adjacent
            | TokenType::LrArrow => {
                operators += 1;
                if operators > MAX_OPERATORS {
                    return Err(SidemanticError::SqlParse(format!(
                        "WASM SQL parser operator-chain limit exceeded ({MAX_OPERATORS})"
                    )));
                }
            }
            _ => {}
        }
        match token.token_type {
            TokenType::LParen | TokenType::LBracket | TokenType::LBrace | TokenType::Case => {
                parent_operators.push(operators);
                operators = 0;
                nesting += 1;
                if nesting > MAX_NESTING {
                    return Err(SidemanticError::SqlParse(format!(
                        "WASM SQL parser nesting limit exceeded ({MAX_NESTING})"
                    )));
                }
            }
            TokenType::RParen | TokenType::RBracket | TokenType::RBrace | TokenType::End => {
                nesting = nesting.saturating_sub(1);
                operators = parent_operators.pop().unwrap_or(0);
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
    fn nested_commas_do_not_reset_the_parent_operator_chain() {
        let expression = std::iter::repeat_n("coalesce(1, 2)", MAX_OPERATORS + 2)
            .collect::<Vec<_>>()
            .join(" + ");
        assert!(check(&format!("SELECT {expression}"), DialectType::DuckDB).is_err());
        let projections = (0..100)
            .map(|i| format!("{i} AS c{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        check(&format!("SELECT {projections}"), DialectType::DuckDB).unwrap();
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
            &format!("SELECT {}true", "NOT ".repeat(MAX_OPERATORS)),
            DialectType::DuckDB
        )
        .is_err());
    }
}
