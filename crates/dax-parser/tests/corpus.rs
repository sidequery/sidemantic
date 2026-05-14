use dax_parser::{lex, parse_expression, TokenKind};
use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("crate should be nested under repo root")
        .to_path_buf()
}

fn load_blocks(path: &Path) -> Vec<(String, String)> {
    let text = fs::read_to_string(path).expect("fixture file missing");
    let mut blocks = Vec::new();
    let mut current = Vec::new();

    for line in text.lines() {
        if line.trim() == "---" {
            if !current.is_empty() {
                blocks.push(current.join("\n"));
                current.clear();
            }
            continue;
        }
        current.push(line.to_string());
    }
    if !current.is_empty() {
        blocks.push(current.join("\n"));
    }

    let mut out = Vec::new();
    for block in blocks {
        let mut source = "<unknown>".to_string();
        let mut expr_lines = Vec::new();
        for line in block.lines() {
            if let Some(rest) = line.strip_prefix("# source:") {
                source = rest.trim().to_string();
                continue;
            }
            expr_lines.push(line);
        }
        let expr = expr_lines.join("\n").trim().to_string();
        if expr.is_empty() {
            continue;
        }
        out.push((source, expr));
    }
    out
}

#[test]
fn parse_expression_corpus_pydaxlexer() {
    let path = repo_root().join("tests/dax/fixtures/pydaxlexer/expressions.txt");
    for (source, expr) in load_blocks(&path) {
        parse_expression(&expr)
            .unwrap_or_else(|err| panic!("pydaxlexer expression failed: {source}\n{expr}\n{err}"));
    }
}

#[test]
fn parse_expression_corpus_pydaxlexer_stress() {
    let path = repo_root().join("tests/dax/fixtures/pydaxlexer/stress.txt");
    for (source, expr) in load_blocks(&path) {
        parse_expression(&expr).unwrap_or_else(|err| {
            panic!("pydaxlexer stress expression failed: {source}\n{expr}\n{err}")
        });
    }
}

#[test]
fn parse_expression_corpus_pbi_parsers() {
    let path = repo_root().join("tests/dax/fixtures/pbi_parsers/expressions.txt");
    for (source, expr) in load_blocks(&path) {
        parse_expression(&expr)
            .unwrap_or_else(|err| panic!("pbi_parsers expression failed: {source}\n{expr}\n{err}"));
    }
}

#[test]
fn lex_tabular_editor_keywords() {
    let path = repo_root().join("tests/dax/fixtures/tabulareditor/keywords.txt");
    let text = fs::read_to_string(path).expect("keywords fixture missing");
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
    }
}
