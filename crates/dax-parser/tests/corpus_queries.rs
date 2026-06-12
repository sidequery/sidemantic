use dax_parser::parse_query;
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
fn parse_query_corpus_query_docs() {
    let path = repo_root().join("tests/dax/fixtures/query-docs/queries.txt");
    for (source, query) in load_blocks(&path) {
        parse_query(&query)
            .unwrap_or_else(|err| panic!("query-docs query failed: {source}\n{query}\n{err}"));
    }
}
