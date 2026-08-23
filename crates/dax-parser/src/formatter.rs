//! Deterministic offline formatting for the DAX AST.

use crate::{
    BinaryOp, DataTableColumn, DataTableType, DefineBlock, Definition, EvaluateStmt, Expr,
    FuncParam, OrderKey, Query, SortDirection, TableName, UnaryOp, VisualShape, VisualShapeAxis,
    VisualShapeColumn,
};

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
}
impl Default for DaxFormatter {
    fn default() -> Self {
        Self::new(FormatOptions::default())
    }
}
impl DaxFormatter {
    pub const fn new(options: FormatOptions) -> Self {
        Self { options }
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
            Expr::Boolean(x) => if *x { "TRUE" } else { "FALSE" }.into(),
            Expr::Blank => "BLANK".into(),
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
            Expr::FunctionCall { name, args } => format!("{name}({})", self.join(args)),
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
                let mut lines = decls
                    .iter()
                    .map(|d| format!("VAR {} = {}", d.name, self.expr(&d.expr, 0)))
                    .collect::<Vec<_>>();
                lines.push("RETURN".into());
                lines.push(self.indent(&self.expr(body, 0), 1));
                lines.join("\n")
            }
            Expr::TableConstructor(rows) => format!(
                "{{{}}}",
                rows.iter()
                    .map(|r| if r.len() == 1 {
                        self.expr(&r[0], 0)
                    } else {
                        format!("({})", self.join(r))
                    })
                    .collect::<Vec<_>>()
                    .join(&self.sep())
            ),
            Expr::Paren(x) => format!("({})", self.expr(x, 0)),
            Expr::Tuple(xs) => format!("({})", self.join(xs)),
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
        args.push(format!(
            "{{{}}}",
            rows.iter()
                .map(|r| format!("{{{}}}", self.join(r)))
                .collect::<Vec<_>>()
                .join(&self.sep())
        ));
        format!("DATATABLE({})", args.join(&self.sep()))
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
            } => (
                doc,
                format!(
                    "MEASURE {}{} = {}",
                    t.as_ref().map(table).unwrap_or_default(),
                    bracket(name),
                    self.expr(expr, 0)
                ),
            ),
            Definition::Var { doc, name, expr } => {
                (doc, format!("VAR {name} = {}", self.expr(expr, 0)))
            }
            Definition::Table {
                doc,
                name,
                expr,
                visual_shape,
            } => {
                let mut s = format!("TABLE {} = {}", quoted(name), self.expr(expr, 0));
                if let Some(v) = visual_shape {
                    s.push('\n');
                    s.push_str(&self.visual_shape(v));
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
            } => (
                doc,
                format!(
                    "FUNCTION {name} = ({}) => {}",
                    params
                        .iter()
                        .map(|p| self.param(p))
                        .collect::<Vec<_>>()
                        .join(&self.sep()),
                    self.expr(body, 0)
                ),
            ),
        };
        match doc {
            Some(x) => format!("{}\n{stmt}", document(x)),
            None => stmt,
        }
    }
    fn param(&self, p: &FuncParam) -> String {
        let mut s = p.name.clone();
        if !p.type_hints.is_empty() {
            s.push_str(": ");
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
            .join(&self.sep())
    }
}

pub fn format_expression(e: &Expr) -> String {
    DaxFormatter::default().format_expression(e)
}
pub fn format_expression_with_options(e: &Expr, o: FormatOptions) -> String {
    DaxFormatter::new(o).format_expression(e)
}
pub fn format_query(q: &Query) -> String {
    DaxFormatter::default().format_query(q)
}
pub fn format_query_with_options(q: &Query, o: FormatOptions) -> String {
    DaxFormatter::new(o).format_query(q)
}

fn string(x: &str) -> String {
    format!("\"{}\"", x.replace('"', "\"\""))
}
fn quoted(x: &str) -> String {
    format!("'{}'", x.replace('\'', "''"))
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
    use crate::{parse_expression, parse_expression_with_dialect, parse_query, Dialect};
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
}
