use dax_parser::{lex, parse_expression, Expr, TokenKind};
use std::fs;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("crate should be nested under repo root")
        .to_path_buf()
}

#[test]
fn keyword_functions_parse_as_calls() {
    let path = repo_root().join("tests/dax/fixtures/tabulareditor/keyword_functions.txt");
    let text = fs::read_to_string(path).expect("keyword function fixture missing");

    for kw in text.lines() {
        let kw = kw.trim();
        if kw.is_empty() {
            continue;
        }

        let tokens = lex(kw).unwrap_or_else(|err| panic!("keyword lex failed: {kw}\n{err}"));
        match &tokens[0].kind {
            TokenKind::Ident(value) => assert_eq!(value, kw),
            other => panic!("keyword did not lex as ident: {kw} -> {other:?}"),
        }

        let expr = parse_expression(&format!("{kw}()"))
            .unwrap_or_else(|err| panic!("keyword call parse failed: {kw}()\n{err}"));
        match expr {
            Expr::FunctionCall { name, args } => {
                assert_eq!(name, kw);
                assert!(args.is_empty());
            }
            other => panic!("keyword did not parse as function call: {kw} -> {other:?}"),
        }
    }
}
