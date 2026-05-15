use dax_parser::{parse_expression, parse_query};

#[test]
fn parse_expression_error_corpus() {
    let cases = [
        ("unterminated string", r#""oops"#, "unterminated"),
        (
            "invalid hierarchy tail",
            "Table[Date].Year",
            "hierarchy level",
        ),
    ];

    for (name, input, expected) in cases {
        let err = parse_expression(input).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(expected),
            "{name} did not contain '{expected}': {msg}"
        );
    }
}

#[test]
fn parse_query_error_corpus() {
    let cases = [(
        "start at without order by",
        "EVALUATE 't' START AT 1",
        "START AT requires an ORDER BY",
    )];

    for (name, input, expected) in cases {
        let err = parse_query(input).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(expected),
            "{name} did not contain '{expected}': {msg}"
        );
    }
}
