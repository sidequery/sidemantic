//! Deterministic offline formatting for the DAX AST.

use crate::{
    lookup, BinaryOp, DataTableColumn, DataTableType, DefineBlock, Definition, EvaluateStmt, Expr,
    FuncParam, OrderKey, Query, SortDirection, TableName, UnaryOp, VisualShape, VisualShapeAxis,
    VisualShapeColumn,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatStyle {
    Canonical,
    Sqlbi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormatOptions {
    pub list_separator: char,
    pub decimal_separator: char,
    pub indent_width: usize,
}
impl FormatOptions {
    pub const fn canonical() -> Self {
        Self {
            list_separator: ',',
            decimal_separator: '.',
            indent_width: 4,
        }
    }
    pub const fn localized() -> Self {
        Self {
            list_separator: ';',
            decimal_separator: ',',
            indent_width: 4,
        }
    }
}
impl Default for FormatOptions {
    fn default() -> Self {
        Self::canonical()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DaxFormatter {
    options: FormatOptions,
    style: FormatStyle,
}
impl Default for DaxFormatter {
    fn default() -> Self {
        Self::new(FormatOptions::default())
    }
}
impl DaxFormatter {
    pub const fn new(options: FormatOptions) -> Self {
        Self {
            options,
            style: FormatStyle::Canonical,
        }
    }
    pub const fn with_style(mut self, style: FormatStyle) -> Self {
        self.style = style;
        self
    }
    pub const fn options(&self) -> FormatOptions {
        self.options
    }
    pub fn format_expression(&self, expr: &Expr) -> String {
        self.expr(expr, 0)
    }
    pub fn format_definition(&self, def: &Definition) -> String {
        self.definition(def)
    }
    pub fn format_query(&self, query: &Query) -> String {
        let mut out = Vec::new();
        if let Some(d) = &query.define {
            out.push(self.define_block(d));
        }
        out.extend(query.evaluates.iter().map(|e| self.evaluate(e)));
        out.join("\n\n")
    }
    fn sep(&self) -> String {
        format!("{} ", self.options.list_separator)
    }
    fn is_sqlbi(&self) -> bool {
        self.style == FormatStyle::Sqlbi
    }
    fn pad(&self, n: usize) -> String {
        " ".repeat(n * self.options.indent_width)
    }
    fn indent(&self, s: &str, n: usize) -> String {
        let p = self.pad(n);
        s.lines()
            .map(|x| format!("{p}{x}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
    fn number(&self, s: &str) -> String {
        s.chars()
            .map(|c| {
                if c == '.' || c == ',' {
                    self.options.decimal_separator
                } else {
                    c
                }
            })
            .collect()
    }
    fn join(&self, xs: &[Expr]) -> String {
        xs.iter()
            .map(|x| self.expr(x, 0))
            .collect::<Vec<_>>()
            .join(&self.sep())
    }
    fn expr(&self, e: &Expr, parent: u8) -> String {
        let prec = expression_precedence(e);
        let s = match e {
            Expr::Number(x) => self.number(x),
            Expr::String(x) => string(x),
            Expr::DateTime(x) => format!("dt{}", string(x)),
            Expr::Boolean(x) => match (*x, self.is_sqlbi()) {
                (true, true) => "TRUE ()".into(),
                (false, true) => "FALSE ()".into(),
                (true, false) => "TRUE".into(),
                (false, false) => "FALSE".into(),
            },
            Expr::Blank => if self.is_sqlbi() { "BLANK ()" } else { "BLANK" }.into(),
            Expr::Omitted => String::new(),
            Expr::Parameter(x) => format!("@{x}"),
            Expr::Identifier(x) => x.clone(),
            Expr::TableRef(t) => table(t),
            Expr::BracketRef(x) => bracket(x),
            Expr::TableColumnRef { table: t, column } => format!("{}{}", table(t), bracket(column)),
            Expr::HierarchyRef {
                table: t,
                column,
                levels,
            } => {
                let mut x = format!("{}{}", table(t), bracket(column));
                for l in levels {
                    x.push('.');
                    x.push_str(&bracket(l));
                }
                x
            }
            Expr::FunctionCall { name, args } => {
                if self.is_sqlbi() {
                    let name = lookup(name).map_or(name.as_str(), |function| function.name);
                    if args.is_empty() {
                        format!("{name} ()")
                    } else {
                        format!("{name} ( {} )", self.join(args))
                    }
                } else {
                    format!("{name}({})", self.join(args))
                }
            }
            Expr::DataTable { columns, rows } => self.datatable(columns, rows),
            Expr::Unary { op, expr } => {
                let x = self.expr(expr, unary_precedence(op));
                match op {
                    UnaryOp::Plus => format!("+{x}"),
                    UnaryOp::Minus => format!("-{x}"),
                    UnaryOp::Not => format!("NOT {x}"),
                }
            }
            Expr::Binary { op, left, right } => {
                let p = binary_precedence(op);
                let (lp, rp) = if matches!(op, BinaryOp::Pow) {
                    (p + 1, p)
                } else {
                    (p, p + 1)
                };
                format!(
                    "{} {} {}",
                    self.expr(left, lp),
                    binary_text(op),
                    self.expr(right, rp)
                )
            }
            Expr::VarBlock { decls, body } => {
                if self.is_sqlbi() {
                    let mut parts = decls
                        .iter()
                        .map(|d| format!("VAR {} = {}", d.name, self.expr(&d.expr, 0)))
                        .collect::<Vec<_>>();
                    parts.push(format!("RETURN {}", self.expr(body, 0)));
                    parts.join(" ")
                } else {
                    let mut lines = decls
                        .iter()
                        .map(|d| format!("VAR {} = {}", d.name, self.expr(&d.expr, 0)))
                        .collect::<Vec<_>>();
                    lines.push("RETURN".into());
                    lines.push(self.indent(&self.expr(body, 0), 1));
                    lines.join("\n")
                }
            }
            Expr::TableConstructor(rows) => {
                let rows = rows
                    .iter()
                    .map(|r| {
                        if r.len() == 1 {
                            self.expr(&r[0], 0)
                        } else if self.is_sqlbi() {
                            format!("( {} )", self.join(r))
                        } else {
                            format!("({})", self.join(r))
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(&self.sep());
                if self.is_sqlbi() {
                    format!("{{ {rows} }}")
                } else {
                    format!("{{{rows}}}")
                }
            }
            Expr::Paren(x) => {
                if self.is_sqlbi() {
                    format!("( {} )", self.expr(x, 0))
                } else {
                    format!("({})", self.expr(x, 0))
                }
            }
            Expr::Tuple(xs) => {
                if self.is_sqlbi() {
                    format!("( {} )", self.join(xs))
                } else {
                    format!("({})", self.join(xs))
                }
            }
        };
        if prec < parent && !matches!(e, Expr::Paren(_)) {
            format!("({s})")
        } else {
            s
        }
    }
    fn datatable(&self, cols: &[DataTableColumn], rows: &[Vec<Expr>]) -> String {
        let mut args = cols
            .iter()
            .flat_map(|c| [string(&c.name), datatype(&c.data_type).into()])
            .collect::<Vec<_>>();
        let rows = rows
            .iter()
            .map(|r| {
                if self.is_sqlbi() {
                    format!("{{ {} }}", self.join(r))
                } else {
                    format!("{{{}}}", self.join(r))
                }
            })
            .collect::<Vec<_>>()
            .join(&self.sep());
        args.push(if self.is_sqlbi() {
            format!("{{ {rows} }}")
        } else {
            format!("{{{rows}}}")
        });
        if self.is_sqlbi() {
            format!("DATATABLE ( {} )", args.join(&self.sep()))
        } else {
            format!("DATATABLE({})", args.join(&self.sep()))
        }
    }
    fn define_block(&self, b: &DefineBlock) -> String {
        format!(
            "DEFINE\n{}",
            b.defs
                .iter()
                .map(|d| self.indent(&self.definition(d), 1))
                .collect::<Vec<_>>()
                .join("\n")
        )
    }
    fn definition(&self, d: &Definition) -> String {
        let (doc, stmt) = match d {
            Definition::Measure {
                doc,
                table: t,
                name,
                expr,
            } => {
                let head = format!(
                    "MEASURE {}{} =",
                    t.as_ref().map(table).unwrap_or_default(),
                    bracket(name)
                );
                let stmt = if self.is_sqlbi() {
                    format!("{head}\n{}", self.indent(&self.expr(expr, 0), 1))
                } else {
                    format!("{head} {}", self.expr(expr, 0))
                };
                (doc, stmt)
            }
            Definition::Var { doc, name, expr } => {
                (doc, format!("VAR {name} = {}", self.expr(expr, 0)))
            }
            Definition::Table {
                doc,
                name,
                expr,
                visual_shape,
            } => {
                let name = if self.is_sqlbi() {
                    sqlbi_table_name(name)
                } else {
                    quoted(name)
                };
                let mut s = if self.is_sqlbi() {
                    format!("TABLE {name} =\n{}", self.indent(&self.expr(expr, 0), 1))
                } else {
                    format!("TABLE {name} = {}", self.expr(expr, 0))
                };
                if let Some(v) = visual_shape {
                    s.push('\n');
                    if self.is_sqlbi() {
                        s.push_str(&self.indent(&self.visual_shape(v), 1));
                    } else {
                        s.push_str(&self.visual_shape(v));
                    }
                }
                (doc, s)
            }
            Definition::Column {
                doc,
                table: t,
                name,
                expr,
            } => (
                doc,
                format!(
                    "COLUMN {}{} = {}",
                    t.as_ref().map(table).unwrap_or_default(),
                    bracket(name),
                    self.expr(expr, 0)
                ),
            ),
            Definition::Function {
                doc,
                name,
                params,
                body,
            } => {
                let stmt = if self.is_sqlbi() {
                    let params = params
                        .iter()
                        .enumerate()
                        .map(|(index, p)| {
                            let suffix = if index + 1 == params.len() {
                                String::new()
                            } else {
                                self.options.list_separator.to_string()
                            };
                            format!("{}{}", self.indent(&self.param(p), 2), suffix)
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    format!(
                        "FUNCTION {name} = (\n{params}\n{}) =>\n{}",
                        self.pad(1),
                        self.indent(&self.expr(body, 0), 1)
                    )
                } else {
                    format!(
                        "FUNCTION {name} = ({}) => {}",
                        params
                            .iter()
                            .map(|p| self.param(p))
                            .collect::<Vec<_>>()
                            .join(&self.sep()),
                        self.expr(body, 0)
                    )
                };
                (doc, stmt)
            }
        };
        match doc {
            Some(x) => format!("{}\n{stmt}", document(x)),
            None => stmt,
        }
    }
    fn param(&self, p: &FuncParam) -> String {
        let mut s = p.name.clone();
        if !p.type_hints.is_empty() {
            s.push_str(if self.is_sqlbi() { " : " } else { ": " });
            s.push_str(&p.type_hints.join(" "));
        }
        if let Some(x) = &p.default {
            s.push_str(" = ");
            s.push_str(&self.expr(x, 0));
        }
        s
    }
    fn visual_shape(&self, v: &VisualShape) -> String {
        let mut x = vec!["WITH VISUAL SHAPE".into()];
        for a in &v.axes {
            x.extend(self.axis(a));
        }
        if let Some(d) = &v.densify {
            x.push(format!("DENSIFY {}", string(d)));
        }
        x.join("\n")
    }
    fn columns(&self, xs: &[VisualShapeColumn]) -> String {
        xs.iter()
            .map(|x| bracket(&x.name))
            .collect::<Vec<_>>()
            .join(&self.sep())
    }
    fn axis(&self, a: &VisualShapeAxis) -> Vec<String> {
        if self.is_sqlbi() {
            let mut x = vec![format!("AXIS {}", a.name)];
            for g in &a.groups {
                x.push(format!("{}GROUP {}", self.pad(1), self.columns(&g.columns)));
                x.push(format!("{}TOTAL {}", self.pad(2), bracket(&g.total.name)));
            }
            x.push(format!(
                "{}ORDER BY {}",
                self.pad(1),
                self.columns(&a.order_by)
            ));
            return x;
        }
        let mut x = vec![format!("{}AXIS {}", self.pad(1), a.name)];
        for g in &a.groups {
            x.push(format!(
                "{}GROUP {} TOTAL {}",
                self.pad(2),
                self.columns(&g.columns),
                bracket(&g.total.name)
            ));
        }
        x.push(format!(
            "{}ORDER BY {}",
            self.pad(2),
            self.columns(&a.order_by)
        ));
        x
    }
    fn evaluate(&self, e: &EvaluateStmt) -> String {
        if self.is_sqlbi() {
            let mut x = vec![format!("EVALUATE\n{}", self.expr(&e.expr, 0))];
            if e.order_by.len() == 1 {
                x.push(format!("ORDER BY {}", self.order(&e.order_by)));
            } else if !e.order_by.is_empty() {
                x.push(format!(
                    "ORDER BY\n{}",
                    self.indent(&self.order(&e.order_by), 1)
                ));
            }
            if let Some(s) = &e.start_at {
                x.push(format!("START AT {}", self.join(s)));
            }
            return x.join("\n");
        }
        let mut x = vec![format!(
            "EVALUATE\n{}",
            self.indent(&self.expr(&e.expr, 0), 1)
        )];
        if !e.order_by.is_empty() {
            x.push(format!(
                "ORDER BY\n{}",
                self.indent(&self.order(&e.order_by), 1)
            ));
        }
        if let Some(s) = &e.start_at {
            x.push(format!("START AT\n{}", self.indent(&self.join(s), 1)));
        }
        x.join("\n")
    }
    fn order(&self, xs: &[OrderKey]) -> String {
        let separator = if self.is_sqlbi() && xs.len() > 1 {
            format!("{}\n", self.options.list_separator)
        } else {
            self.sep()
        };
        xs.iter()
            .map(|x| {
                format!(
                    "{} {}",
                    self.expr(&x.expr, 0),
                    match x.direction {
                        SortDirection::Asc => "ASC",
                        SortDirection::Desc => "DESC",
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(&separator)
    }
}

pub fn format_expression(e: &Expr) -> String {
    DaxFormatter::default().format_expression(e)
}
pub fn format_expression_with_options(e: &Expr, o: FormatOptions) -> String {
    DaxFormatter::new(o).format_expression(e)
}
pub fn format_expression_with_style(e: &Expr, o: FormatOptions, style: FormatStyle) -> String {
    DaxFormatter::new(o).with_style(style).format_expression(e)
}
pub fn format_query(q: &Query) -> String {
    DaxFormatter::default().format_query(q)
}
pub fn format_query_with_options(q: &Query, o: FormatOptions) -> String {
    DaxFormatter::new(o).format_query(q)
}
pub fn format_query_with_style(q: &Query, o: FormatOptions, style: FormatStyle) -> String {
    DaxFormatter::new(o).with_style(style).format_query(q)
}

fn string(x: &str) -> String {
    format!("\"{}\"", x.replace('"', "\"\""))
}
fn quoted(x: &str) -> String {
    format!("'{}'", x.replace('\'', "''"))
}
fn sqlbi_table_name(x: &str) -> String {
    let reserved = matches!(
        x.to_ascii_uppercase().as_str(),
        "DEFINE"
            | "EVALUATE"
            | "ORDER"
            | "BY"
            | "START"
            | "AT"
            | "RETURN"
            | "VAR"
            | "IN"
            | "ASC"
            | "DESC"
            | "MEASURE"
            | "COLUMN"
            | "TABLE"
            | "FUNCTION"
            | "WITH"
            | "VISUAL"
            | "SHAPE"
            | "AXIS"
            | "GROUP"
            | "TOTAL"
            | "DENSIFY"
            | "TRUE"
            | "FALSE"
            | "NOT"
    );
    if !reserved
        && !x.is_empty()
        && x.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && x.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
    {
        x.into()
    } else {
        quoted(x)
    }
}
fn bracket(x: &str) -> String {
    format!("[{}]", x.replace(']', "]]"))
}
fn table(x: &TableName) -> String {
    if x.quoted {
        quoted(&x.name)
    } else {
        x.name.clone()
    }
}
fn document(x: &str) -> String {
    x.lines()
        .map(|l| {
            if l.is_empty() {
                "///".into()
            } else {
                format!("/// {l}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}
fn datatype(x: &DataTableType) -> &'static str {
    match x {
        DataTableType::Boolean => "BOOLEAN",
        DataTableType::Currency => "CURRENCY",
        DataTableType::DateTime => "DATETIME",
        DataTableType::Double => "DOUBLE",
        DataTableType::Integer => "INTEGER",
        DataTableType::String => "STRING",
    }
}
fn binary_text(x: &BinaryOp) -> &'static str {
    match x {
        BinaryOp::Or => "||",
        BinaryOp::And => "&&",
        BinaryOp::Eq => "=",
        BinaryOp::StrictEq => "==",
        BinaryOp::Neq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::Lte => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Gte => ">=",
        BinaryOp::In => "IN",
        BinaryOp::Concat => "&",
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Pow => "^",
    }
}
fn binary_precedence(x: &BinaryOp) -> u8 {
    match x {
        BinaryOp::Or => 1,
        BinaryOp::And => 2,
        BinaryOp::Eq
        | BinaryOp::StrictEq
        | BinaryOp::Neq
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte
        | BinaryOp::In => 4,
        BinaryOp::Concat => 5,
        BinaryOp::Add | BinaryOp::Sub => 6,
        BinaryOp::Mul | BinaryOp::Div => 7,
        BinaryOp::Pow => 9,
    }
}
fn unary_precedence(x: &UnaryOp) -> u8 {
    match x {
        UnaryOp::Not => 3,
        UnaryOp::Plus | UnaryOp::Minus => 8,
    }
}
fn expression_precedence(x: &Expr) -> u8 {
    match x {
        Expr::Binary { op, .. } => binary_precedence(op),
        Expr::Unary { op, .. } => unary_precedence(op),
        Expr::VarBlock { .. } => 0,
        _ => 10,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        parse_expression, parse_expression_with_dialect, parse_query, parse_query_with_dialect,
        Dialect,
    };
    #[test]
    fn precedence_round_trips() {
        for s in [
            "1 + 2 * 3",
            "(1 + 2) * 3",
            "1 - (2 - 3)",
            "(2 ^ 3) ^ 4",
            "2 ^ 3 ^ 4",
            "-2 ^ 3",
            "(-2) ^ 3",
            "NOT [x] = 1 && [y] = 2",
        ] {
            let x = parse_expression(s).unwrap();
            let f = format_expression(&x);
            assert_eq!(parse_expression(&f).unwrap(), x, "{f}");
        }
    }
    #[test]
    fn escapes_names_and_strings() {
        assert_eq!(
            format_expression(&Expr::String("say \"hi\"".into())),
            "\"say \"\"hi\"\"\""
        );
        assert_eq!(
            format_expression(&Expr::TableColumnRef {
                table: TableName::quoted("O'Brien"),
                column: "a]b".into()
            }),
            "'O''Brien'[a]]b]"
        );
    }
    #[test]
    fn localized_round_trip() {
        let x = parse_expression("F(1.5, , {2.25, 3.5})").unwrap();
        let f = format_expression_with_options(&x, FormatOptions::localized());
        assert_eq!(f, "F(1,5; ; {2,25; 3,5})");
        let d = Dialect {
            allow_decimal_comma: true,
            ..Dialect::default()
        };
        let reparsed = parse_expression_with_dialect(&f, d).unwrap();
        // Number nodes preserve their original spelling, so compare canonical output.
        assert_eq!(format_expression(&reparsed), format_expression(&x));
    }
    #[test]
    fn localized_visual_shape_round_trip() {
        let q = parse_query(
            "DEFINE TABLE shaped = T WITH VISUAL SHAPE AXIS rows \
             GROUP [Year], [Month] TOTAL [DateTotal] \
             ORDER BY [Year], [Month] EVALUATE shaped",
        )
        .unwrap();
        let formatted = format_query_with_options(&q, FormatOptions::localized());
        assert!(formatted.contains("GROUP [Year]; [Month]"), "{formatted}");
        assert!(
            formatted.contains("ORDER BY [Year]; [Month]"),
            "{formatted}"
        );

        let reparsed = parse_query_with_dialect(&formatted, Dialect::default()).unwrap();
        assert_eq!(reparsed, q, "{formatted}");
    }
    #[test]
    fn datatable_round_trip() {
        let x = parse_expression(
            r#"DATATABLE("Name", STRING, "Count", INTEGER, {{"a", 1}, {"b", 2}})"#,
        )
        .unwrap();
        let f = format_expression(&x);
        assert_eq!(parse_expression(&f).unwrap(), x);
    }
    #[test]
    fn full_query_round_trip() {
        let s = r#"DEFINE /// total
MEASURE 'S'[M] = SUM('S'[X]) VAR threshold = 10 TABLE shaped = SUMMARIZE('S', 'S'[X]) WITH VISUAL SHAPE AXIS rows GROUP [X] TOTAL [Total] ORDER BY [X] DENSIFY "rows" COLUMN 'S'[C] = 1 FUNCTION f = (x: SCALAR NUMERIC = 1) => x + 1 EVALUATE shaped ORDER BY [X] DESC START AT 1"#;
        let q = parse_query(s).unwrap();
        let f = format_query(&q);
        assert_eq!(parse_query(&f).unwrap(), q, "{f}");
    }
    #[test]
    fn var_tuple_hierarchy_datetime_round_trip() {
        let s = r#"VAR x = dt"2015-1-9T02:30:00" RETURN ('T'[H].[L], x) IN {(dt"2015-1-9", 1)}"#;
        let x = parse_expression(s).unwrap();
        let f = format_expression(&x);
        assert_eq!(parse_expression(&f).unwrap(), x, "{f}");
    }

    #[test]
    fn sqlbi_builtin_casing_and_function_spacing() {
        let expr =
            parse_expression("sumx(filter('Sales','Sales'[Amount]>100),'Sales'[Amount])").unwrap();
        assert_eq!(
            format_expression_with_style(&expr, FormatOptions::canonical(), FormatStyle::Sqlbi),
            "SUMX ( FILTER ( 'Sales', 'Sales'[Amount] > 100 ), 'Sales'[Amount] )"
        );
    }

    #[test]
    fn sqlbi_table_constructor_and_query_clauses() {
        let constructor = parse_query("EVALUATE {(1,2),(3,4)}").unwrap();
        assert_eq!(
            format_query_with_style(&constructor, FormatOptions::canonical(), FormatStyle::Sqlbi),
            "EVALUATE\n{ ( 1, 2 ), ( 3, 4 ) }"
        );

        let clauses = parse_query("EVALUATE ROW(\"x\",1) ORDER BY [x] START AT 1").unwrap();
        assert_eq!(
            format_query_with_style(&clauses, FormatOptions::canonical(), FormatStyle::Sqlbi),
            "EVALUATE\nROW ( \"x\", 1 )\nORDER BY [x] ASC\nSTART AT 1"
        );
    }

    #[test]
    fn sqlbi_define_measure_var_and_udf_layout() {
        let definitions = parse_query(
            "DEFINE MEASURE 'S'[M]=SUM('S'[A]) VAR threshold=10 EVALUATE ROW(\"m\",[M])",
        )
        .unwrap();
        assert_eq!(
            format_query_with_style(&definitions, FormatOptions::canonical(), FormatStyle::Sqlbi),
            "DEFINE\n    MEASURE 'S'[M] =\n        SUM ( 'S'[A] )\n    VAR threshold = 10\n\nEVALUATE\nROW ( \"m\", [M] )"
        );

        let udf = parse_query(
            "DEFINE FUNCTION AddTax=(amount:NUMERIC,taxRate:NUMERIC=0.1)=>amount*(1+taxRate) EVALUATE {AddTax(10)}",
        )
        .unwrap();
        assert_eq!(
            format_query_with_style(&udf, FormatOptions::canonical(), FormatStyle::Sqlbi),
            "DEFINE\n    FUNCTION AddTax = (\n            amount : NUMERIC,\n            taxRate : NUMERIC = 0.1\n        ) =>\n        amount * ( 1 + taxRate )\n\nEVALUATE\n{ AddTax ( 10 ) }"
        );
    }

    #[test]
    fn sqlbi_visual_shape_layout() {
        let query = parse_query(
            "DEFINE TABLE data=ROW(\"Year\",2000,\"IsTotal\",FALSE()) WITH VISUAL SHAPE AXIS ROWS GROUP [Year] TOTAL [IsTotal] ORDER BY [Year] DENSIFY \"IsDensified\" EVALUATE data",
        )
        .unwrap();
        assert_eq!(
            format_query_with_style(&query, FormatOptions::canonical(), FormatStyle::Sqlbi),
            "DEFINE\n    TABLE data =\n        ROW ( \"Year\", 2000, \"IsTotal\", FALSE () )\n        WITH VISUAL SHAPE\n        AXIS ROWS\n            GROUP [Year]\n                TOTAL [IsTotal]\n            ORDER BY [Year]\n        DENSIFY \"IsDensified\"\n\nEVALUATE\ndata"
        );
    }

    #[test]
    fn sqlbi_quotes_table_names_only_when_required() {
        assert_eq!(sqlbi_table_name("data2"), "data2");
        assert_eq!(sqlbi_table_name("TABLE"), "'TABLE'");
        assert_eq!(sqlbi_table_name("Sales Data"), "'Sales Data'");
    }

    #[test]
    fn format_options_preserves_three_field_struct_literals() {
        let options = FormatOptions {
            list_separator: ',',
            decimal_separator: '.',
            indent_width: 2,
        };
        assert_eq!(DaxFormatter::new(options).options(), options);
    }
}
