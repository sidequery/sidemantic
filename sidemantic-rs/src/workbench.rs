use std::io::{self, IsTerminal};
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Wrap};
use ratatui::Terminal;
use sidemantic::SidemanticRuntime;
#[cfg(feature = "workbench-adbc")]
use sidemantic::{execute_with_adbc, AdbcExecutionRequest, AdbcValue};

use crate::CliResult;

const PREVIEW_ROW_LIMIT: usize = 25;
const PREVIEW_CELL_WIDTH: usize = 48;

#[derive(Debug, Clone)]
struct ModelSummary {
    name: String,
    table: String,
    dimensions: usize,
    metrics: usize,
    relationships: usize,
    dimension_names: Vec<String>,
    metric_names: Vec<String>,
    relationship_names: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FocusPanel {
    Models,
    Sql,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputView {
    Sql,
    Table,
    Chart,
}

impl OutputView {
    fn next(self) -> Self {
        match self {
            OutputView::Sql => OutputView::Table,
            OutputView::Table => OutputView::Chart,
            OutputView::Chart => OutputView::Sql,
        }
    }

    fn label(self) -> &'static str {
        match self {
            OutputView::Sql => "SQL",
            OutputView::Table => "TABLE",
            OutputView::Chart => "CHART",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChartRenderMode {
    Bar,
    Dot,
}

impl ChartRenderMode {
    fn next(self) -> Self {
        match self {
            ChartRenderMode::Bar => ChartRenderMode::Dot,
            ChartRenderMode::Dot => ChartRenderMode::Bar,
        }
    }

    fn label(self) -> &'static str {
        match self {
            ChartRenderMode::Bar => "BAR",
            ChartRenderMode::Dot => "DOT",
        }
    }
}

#[derive(Debug, Clone)]
struct ExecutionPreview {
    rewritten_sql: String,
    columns: Vec<String>,
    rows: Vec<Vec<WorkbenchValue>>,
}

#[cfg_attr(not(feature = "workbench-adbc"), allow(dead_code))]
#[derive(Debug, Clone, PartialEq)]
enum WorkbenchValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

#[cfg(feature = "workbench-adbc")]
impl From<AdbcValue> for WorkbenchValue {
    fn from(value: AdbcValue) -> Self {
        match value {
            AdbcValue::Null => Self::Null,
            AdbcValue::Bool(value) => Self::Bool(value),
            AdbcValue::I64(value) => Self::I64(value),
            AdbcValue::U64(value) => Self::U64(value),
            AdbcValue::F64(value) => Self::F64(value),
            AdbcValue::String(value) => Self::String(value),
            AdbcValue::Bytes(value) => Self::Bytes(value),
        }
    }
}

#[derive(Debug)]
struct WorkbenchApp {
    runtime: SidemanticRuntime,
    models: Vec<ModelSummary>,
    selected_model_index: usize,
    sql_input: String,
    output: String,
    output_view: OutputView,
    execution_preview: Option<ExecutionPreview>,
    status: String,
    focus: FocusPanel,
    should_quit: bool,
    connection: Option<String>,
    chart_mode: ChartRenderMode,
    chart_value_column: Option<usize>,
    chart_label_column: Option<usize>,
}

impl WorkbenchApp {
    fn new(runtime: SidemanticRuntime, connection: Option<String>) -> Self {
        let mut models = runtime
            .graph()
            .models()
            .map(|model| ModelSummary {
                name: model.name.clone(),
                table: model.table_name().to_string(),
                dimensions: model.dimensions.len(),
                metrics: model.metrics.len(),
                relationships: model.relationships.len(),
                dimension_names: model
                    .dimensions
                    .iter()
                    .map(|dimension| dimension.name.clone())
                    .collect(),
                metric_names: model
                    .metrics
                    .iter()
                    .map(|metric| metric.name.clone())
                    .collect(),
                relationship_names: model
                    .relationships
                    .iter()
                    .map(|relationship| relationship.name.clone())
                    .collect(),
            })
            .collect::<Vec<_>>();
        models.sort_by(|left, right| left.name.cmp(&right.name));

        let sql_input = models.first().map_or_else(
            || "select 1".to_string(),
            |model| format!("select * from {}", model.name),
        );
        let status = if connection.is_some() {
            "Ready. F5 rewrite, F6 execute, F7 view cycle, Ctrl+M/V/L chart controls, Tab switch focus, Esc quit."
                .to_string()
        } else {
            "Ready. F5 rewrite. F6 execute requires --connection/--db. F7 view cycle, Ctrl+M/V/L chart controls, Tab switch focus, Esc quit."
                .to_string()
        };

        let mut app = Self {
            runtime,
            models,
            selected_model_index: 0,
            sql_input,
            output: String::new(),
            output_view: OutputView::Sql,
            execution_preview: None,
            status,
            focus: FocusPanel::Sql,
            should_quit: false,
            connection,
            chart_mode: ChartRenderMode::Bar,
            chart_value_column: None,
            chart_label_column: None,
        };
        app.run_rewrite();
        app
    }

    fn run_rewrite(&mut self) {
        match self.runtime.rewrite(&self.sql_input) {
            Ok(sql) => {
                self.output = sql;
                self.status = "Rewrite ok".to_string();
            }
            Err(err) => {
                self.output = err.to_string();
                self.status = "Rewrite failed".to_string();
            }
        }
    }

    fn selected_model_name(&self) -> Option<&str> {
        self.models
            .get(self.selected_model_index)
            .map(|model| model.name.as_str())
    }

    fn apply_model_template(&mut self) {
        if let Some(model_name) = self.selected_model_name().map(str::to_string) {
            self.sql_input = format!("select * from {model_name}");
            self.status = format!("Loaded template for model '{model_name}'");
        }
    }

    fn run_execute(&mut self) {
        let rewritten = match self.runtime.rewrite(&self.sql_input) {
            Ok(sql) => sql,
            Err(err) => {
                self.execution_preview = None;
                self.output = err.to_string();
                self.status = "Execute failed (rewrite)".to_string();
                return;
            }
        };

        let Some(connection) = self.connection.as_deref() else {
            self.execution_preview = None;
            self.output = rewritten;
            self.status = "Execute skipped: no connection configured".to_string();
            return;
        };

        #[cfg(not(feature = "workbench-adbc"))]
        {
            let _ = connection;
            self.execution_preview = None;
            self.output = rewritten;
            self.status = "Execute unavailable: build without workbench-adbc".to_string();
            self.output.push_str(
                "\n\nADBC execution support is not enabled. Rebuild with feature 'workbench-adbc' to run database-backed queries.",
            );
            return;
        }

        #[cfg(feature = "workbench-adbc")]
        {
            let (driver, uri, database_options) =
                match crate::parse_connection_url_to_adbc(connection) {
                    Ok(payload) => payload,
                    Err(err) => {
                        self.execution_preview = None;
                        self.output = rewritten;
                        self.status = "Execute failed (connection)".to_string();
                        self.output.push_str("\n\nConnection parsing failed:\n");
                        self.output.push_str(&err);
                        return;
                    }
                };

            match execute_with_adbc(AdbcExecutionRequest {
                driver,
                sql: rewritten.clone(),
                uri,
                entrypoint: None,
                database_options,
                connection_options: Vec::new(),
            }) {
                Ok(result) => {
                    let rows = result
                        .rows
                        .into_iter()
                        .map(|row| {
                            row.into_iter()
                                .map(WorkbenchValue::from)
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();
                    let row_count = rows.len();
                    self.execution_preview = Some(ExecutionPreview {
                        rewritten_sql: rewritten.clone(),
                        columns: result.columns.clone(),
                        rows: rows.clone(),
                    });
                    self.chart_value_column = None;
                    self.chart_label_column = None;
                    self.output = format_execution_output(&rewritten, &result.columns, &rows);
                    self.status = format!(
                        "Execute ok ({} row{})",
                        row_count,
                        if row_count == 1 { "" } else { "s" }
                    );
                }
                Err(err) => {
                    self.execution_preview = None;
                    self.output = rewritten;
                    self.status = "Execute failed".to_string();
                    self.output.push_str("\n\nExecution failed:\n");
                    self.output.push_str(&err.to_string());
                }
            }
        }
    }

    fn cycle_output_view(&mut self) {
        self.output_view = self.output_view.next();
        self.status = format!("Output view: {}", self.output_view.label());
    }

    fn set_output_view(&mut self, view: OutputView) {
        self.output_view = view;
        self.status = format!("Output view: {}", self.output_view.label());
    }

    fn cycle_chart_mode(&mut self) {
        self.chart_mode = self.chart_mode.next();
        self.output_view = OutputView::Chart;
        self.status = format!("Chart mode: {}", self.chart_mode.label());
    }

    fn cycle_chart_value_column(&mut self) {
        let Some(preview) = self.execution_preview.as_ref() else {
            self.status = "Chart value column unavailable: run execute first".to_string();
            return;
        };

        let numeric_candidates = chart_numeric_column_indices(preview);
        if numeric_candidates.is_empty() {
            self.status = "Chart value column unavailable: no numeric columns".to_string();
            return;
        }

        let current_position = self.chart_value_column.and_then(|current| {
            numeric_candidates
                .iter()
                .position(|index| *index == current)
        });
        let next_position = match current_position {
            Some(position) => (position + 1) % numeric_candidates.len(),
            None => 0,
        };
        let next_index = numeric_candidates[next_position];
        self.chart_value_column = Some(next_index);
        if self.chart_label_column == Some(next_index) {
            self.chart_label_column = None;
        }
        self.output_view = OutputView::Chart;
        self.status = format!("Chart value column: {}", preview.columns[next_index]);
    }

    fn cycle_chart_label_column(&mut self) {
        let Some(preview) = self.execution_preview.as_ref() else {
            self.status = "Chart label column unavailable: run execute first".to_string();
            return;
        };
        if preview.columns.is_empty() {
            self.status = "Chart label column unavailable: no columns".to_string();
            return;
        }

        let value_index = self
            .chart_value_column
            .and_then(|index| preview.columns.get(index).map(|_| index))
            .or_else(|| first_numeric_column_index(preview))
            .unwrap_or(0);

        let mut label_candidates = (0..preview.columns.len()).collect::<Vec<_>>();
        if label_candidates.len() > 1 {
            label_candidates.retain(|index| *index != value_index);
        }
        if label_candidates.is_empty() {
            self.status = "Chart label column unavailable: no label candidates".to_string();
            return;
        }

        let default_label = default_chart_label_index(preview, value_index);
        let current_label = self
            .chart_label_column
            .and_then(|index| preview.columns.get(index).map(|_| index))
            .unwrap_or(default_label);
        let current_position = label_candidates
            .iter()
            .position(|index| *index == current_label)
            .unwrap_or(0);
        let next_position = (current_position + 1) % label_candidates.len();
        let next_index = label_candidates[next_position];
        self.chart_label_column = Some(next_index);
        self.output_view = OutputView::Chart;
        self.status = format!("Chart label column: {}", preview.columns[next_index]);
    }

    fn next_focus(&mut self) {
        self.focus = match self.focus {
            FocusPanel::Models => FocusPanel::Sql,
            FocusPanel::Sql => FocusPanel::Models,
        };
    }

    fn handle_models_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down => {
                if !self.models.is_empty() {
                    self.selected_model_index =
                        (self.selected_model_index + 1).min(self.models.len() - 1);
                }
            }
            KeyCode::Up => {
                if self.selected_model_index > 0 {
                    self.selected_model_index -= 1;
                }
            }
            KeyCode::Enter => self.apply_model_template(),
            _ => {}
        }
    }

    fn handle_sql_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char(ch) => {
                if key.modifiers.contains(KeyModifiers::CONTROL) {
                    return;
                }
                self.sql_input.push(ch);
            }
            KeyCode::Backspace => {
                self.sql_input.pop();
            }
            KeyCode::Enter => {
                self.sql_input.push('\n');
            }
            KeyCode::Tab => self.next_focus(),
            _ => {}
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        if key.kind != KeyEventKind::Press {
            return;
        }

        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }

        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('r') {
            self.run_rewrite();
            return;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('e') {
            self.run_execute();
            return;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('1') {
            self.set_output_view(OutputView::Sql);
            return;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('2') {
            self.set_output_view(OutputView::Table);
            return;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('3') {
            self.set_output_view(OutputView::Chart);
            return;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('m') {
            self.cycle_chart_mode();
            return;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('v') {
            self.cycle_chart_value_column();
            return;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('l') {
            self.cycle_chart_label_column();
            return;
        }

        match key.code {
            KeyCode::Esc => self.should_quit = true,
            KeyCode::F(5) => self.run_rewrite(),
            KeyCode::F(6) => self.run_execute(),
            KeyCode::F(7) => self.cycle_output_view(),
            KeyCode::Tab => self.next_focus(),
            _ => match self.focus {
                FocusPanel::Models => self.handle_models_key(key),
                FocusPanel::Sql => self.handle_sql_key(key),
            },
        }
    }
}

fn selected_model_details(model: Option<&ModelSummary>) -> String {
    let Some(model) = model else {
        return "No model selected".to_string();
    };
    let mut details = String::new();
    details.push_str(&format!("table: {}\n", model.table));
    details.push_str(&format!(
        "dimensions ({}): {}\n",
        model.dimensions,
        summarize_name_list(&model.dimension_names, 8)
    ));
    details.push_str(&format!(
        "metrics ({}): {}\n",
        model.metrics,
        summarize_name_list(&model.metric_names, 8)
    ));
    details.push_str(&format!(
        "relationships ({}): {}\n",
        model.relationships,
        summarize_name_list(&model.relationship_names, 6)
    ));
    details
}

fn summarize_name_list(names: &[String], limit: usize) -> String {
    if names.is_empty() {
        return "-".to_string();
    }
    let shown = names.iter().take(limit).cloned().collect::<Vec<_>>();
    if names.len() > limit {
        format!("{}, +{} more", shown.join(", "), names.len() - limit)
    } else {
        shown.join(", ")
    }
}

fn workbench_value_as_f64(value: &WorkbenchValue) -> Option<f64> {
    match value {
        WorkbenchValue::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        WorkbenchValue::I64(value) => Some(*value as f64),
        WorkbenchValue::U64(value) => Some(*value as f64),
        WorkbenchValue::F64(value) => Some(*value),
        WorkbenchValue::Null | WorkbenchValue::String(_) | WorkbenchValue::Bytes(_) => None,
    }
}

fn chart_numeric_column_indices(preview: &ExecutionPreview) -> Vec<usize> {
    preview
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, _)| {
            preview
                .rows
                .iter()
                .take(PREVIEW_ROW_LIMIT)
                .find_map(|row| row.get(index))
                .and_then(workbench_value_as_f64)
                .map(|_| index)
        })
        .collect()
}

fn first_numeric_column_index(preview: &ExecutionPreview) -> Option<usize> {
    chart_numeric_column_indices(preview).into_iter().next()
}

fn default_chart_label_index(preview: &ExecutionPreview, value_index: usize) -> usize {
    if value_index == 0 && preview.columns.len() > 1 {
        1
    } else {
        0
    }
}

fn build_chart_lines(
    preview: Option<&ExecutionPreview>,
    chart_mode: ChartRenderMode,
    value_column_override: Option<usize>,
    label_column_override: Option<usize>,
) -> Vec<String> {
    let Some(preview) = preview else {
        return vec!["No execution results yet. Press F6 to run query.".to_string()];
    };
    if preview.rows.is_empty() || preview.columns.is_empty() {
        return vec!["Execution returned no rows.".to_string()];
    }

    let numeric_candidates = chart_numeric_column_indices(preview);
    let Some(default_value_index) = numeric_candidates.first().copied() else {
        return vec!["Chart unavailable: no numeric columns in execution result.".to_string()];
    };
    let value_index = value_column_override
        .filter(|index| numeric_candidates.contains(index))
        .unwrap_or(default_value_index);
    let default_label_index = default_chart_label_index(preview, value_index);
    let label_index = label_column_override
        .filter(|index| preview.columns.get(*index).is_some())
        .unwrap_or(default_label_index);

    let mut points = Vec::new();
    for (row_index, row) in preview.rows.iter().take(12).enumerate() {
        let Some(value) = row.get(value_index).and_then(workbench_value_as_f64) else {
            continue;
        };
        let label = row
            .get(label_index)
            .map(format_workbench_value)
            .filter(|value| !value.trim().is_empty() && value != "NULL")
            .unwrap_or_else(|| format!("row {}", row_index + 1));
        points.push((label, value));
    }
    if points.is_empty() {
        return vec!["Chart unavailable: no numeric rows in preview subset.".to_string()];
    }

    let max_abs = points
        .iter()
        .map(|(_, value)| value.abs())
        .fold(0.0_f64, f64::max)
        .max(1.0);
    let min_value = points
        .iter()
        .map(|(_, value)| *value)
        .fold(f64::INFINITY, f64::min);
    let max_value = points
        .iter()
        .map(|(_, value)| *value)
        .fold(f64::NEG_INFINITY, f64::max);
    let value_span = (max_value - min_value).abs().max(1e-9);

    let mut lines = vec![
        format!(
            "chart source: value={} label={} mode={}",
            preview.columns[value_index],
            preview.columns[label_index],
            chart_mode.label()
        ),
        "chart controls: Ctrl+M mode, Ctrl+V value column, Ctrl+L label column".to_string(),
    ];
    for (label, value) in points {
        match chart_mode {
            ChartRenderMode::Bar => {
                let bar_units = ((value.abs() / max_abs) * 24.0).round().max(1.0) as usize;
                let bar_symbol = if value < 0.0 { '-' } else { '#' };
                let bar = std::iter::repeat_n(bar_symbol, bar_units).collect::<String>();
                lines.push(format!("{label:>20} | {bar:<24} {value:.3}"));
            }
            ChartRenderMode::Dot => {
                let marker_position = (((value - min_value) / value_span) * 23.0).round() as usize;
                let mut markers = [' '; 24];
                markers[marker_position.min(23)] = 'o';
                let track = markers.iter().collect::<String>();
                lines.push(format!("{label:>20} | {track} {value:.3}"));
            }
        }
    }
    lines
}

#[cfg_attr(not(feature = "workbench-adbc"), allow(dead_code))]
fn format_execution_output(
    rewritten: &str,
    columns: &[String],
    rows: &[Vec<WorkbenchValue>],
) -> String {
    let shown = rows.len().min(PREVIEW_ROW_LIMIT);
    let mut output = String::new();
    output.push_str("Rendered SQL:\n");
    output.push_str(rewritten);
    output.push_str("\n\nResult preview:\n");
    output.push_str(&format!(
        "rows={} shown={} limit={}\n",
        rows.len(),
        shown,
        PREVIEW_ROW_LIMIT
    ));

    if columns.is_empty() {
        output.push_str("(no columns)\n");
        return output;
    }

    output.push_str(&columns.join(" | "));
    output.push('\n');
    output.push_str(
        &columns
            .iter()
            .map(|_| "--------")
            .collect::<Vec<_>>()
            .join("-+-"),
    );
    output.push('\n');

    for row in rows.iter().take(shown) {
        let rendered_row = columns
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let value = row.get(index).unwrap_or(&WorkbenchValue::Null);
                format_workbench_value(value)
            })
            .collect::<Vec<_>>();
        output.push_str(&rendered_row.join(" | "));
        output.push('\n');
    }

    if rows.len() > shown {
        output.push('\n');
        output.push_str(&format!("... {} more rows", rows.len() - shown));
    }

    output
}

fn format_workbench_value(value: &WorkbenchValue) -> String {
    let raw = match value {
        WorkbenchValue::Null => "NULL".to_string(),
        WorkbenchValue::Bool(value) => value.to_string(),
        WorkbenchValue::I64(value) => value.to_string(),
        WorkbenchValue::U64(value) => value.to_string(),
        WorkbenchValue::F64(value) => value.to_string(),
        WorkbenchValue::String(value) => value.clone(),
        WorkbenchValue::Bytes(value) => {
            let hex = value
                .iter()
                .take(16)
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            if value.len() > 16 {
                format!("0x{hex}...")
            } else {
                format!("0x{hex}")
            }
        }
    };
    let normalized = raw.replace('\n', " ");
    if normalized.chars().count() <= PREVIEW_CELL_WIDTH {
        normalized
    } else {
        let prefix = normalized
            .chars()
            .take(PREVIEW_CELL_WIDTH.saturating_sub(3))
            .collect::<String>();
        format!("{prefix}...")
    }
}

fn draw_app(frame: &mut ratatui::Frame<'_>, app: &WorkbenchApp) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(3),
        ])
        .split(frame.area());

    let connection_text = app
        .connection
        .as_deref()
        .map_or("connection=none".to_string(), |value| {
            format!("connection={value}")
        });
    let selected_text = app
        .selected_model_name()
        .map_or("selected=none".to_string(), |name| {
            format!("selected={name}")
        });
    let header = Paragraph::new(format!(
        "Sidemantic Workbench (ratatui) | {selected_text} | {connection_text}"
    ))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(header, layout[0]);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(layout[1]);

    let model_items = app
        .models
        .iter()
        .map(|model| {
            ListItem::new(Line::from(format!(
                "{} [table={} dims={} metrics={} rels={}]",
                model.name, model.table, model.dimensions, model.metrics, model.relationships
            )))
        })
        .collect::<Vec<_>>();
    let left = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(52), Constraint::Percentage(48)])
        .split(body[0]);
    let model_title = if app.focus == FocusPanel::Models {
        "Models [focus]"
    } else {
        "Models"
    };
    let model_list =
        List::new(model_items).block(Block::default().title(model_title).borders(Borders::ALL));
    let mut model_state = ratatui::widgets::ListState::default();
    if !app.models.is_empty() {
        model_state.select(Some(app.selected_model_index));
    }
    frame.render_stateful_widget(model_list, left[0], &mut model_state);

    let details = selected_model_details(app.models.get(app.selected_model_index));
    let details_panel = Paragraph::new(details)
        .block(
            Block::default()
                .title("Model Details (Enter loads template)")
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(details_panel, left[1]);

    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(body[1]);

    let sql_style = if app.focus == FocusPanel::Sql {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    let sql_editor = Paragraph::new(app.sql_input.as_str())
        .block(
            Block::default()
                .title(
                    "SQL Input [Tab switch, F5 rewrite, F6 execute, F7 view cycle, Ctrl+M/V/L chart config]",
                )
                .borders(Borders::ALL)
                .border_style(sql_style),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(sql_editor, right[0]);

    let output_title = format!(
        "Output [{}] (Ctrl+1 SQL / Ctrl+2 TABLE / Ctrl+3 CHART)",
        app.output_view.label()
    );
    match app.output_view {
        OutputView::Sql => {
            let output = Paragraph::new(app.output.as_str())
                .block(Block::default().title(output_title).borders(Borders::ALL))
                .wrap(Wrap { trim: false });
            frame.render_widget(output, right[1]);
        }
        OutputView::Table => {
            if let Some(preview) = app.execution_preview.as_ref() {
                let row_count = preview.rows.len();
                let shown_count = row_count.min(PREVIEW_ROW_LIMIT);
                if preview.columns.is_empty() {
                    let output = Paragraph::new("Execution returned no columns.")
                        .block(Block::default().title(output_title).borders(Borders::ALL))
                        .wrap(Wrap { trim: false });
                    frame.render_widget(output, right[1]);
                } else {
                    let header = Row::new(preview.columns.iter().map(|column| {
                        Cell::from(column.clone()).style(
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::BOLD),
                        )
                    }));
                    let rows = preview
                        .rows
                        .iter()
                        .take(PREVIEW_ROW_LIMIT)
                        .map(|row| {
                            Row::new(preview.columns.iter().enumerate().map(|(index, _)| {
                                let value = row.get(index).unwrap_or(&WorkbenchValue::Null);
                                Cell::from(format_workbench_value(value))
                            }))
                        })
                        .collect::<Vec<_>>();
                    let widths = preview
                        .columns
                        .iter()
                        .map(|_| Constraint::Min(12))
                        .collect::<Vec<_>>();
                    let table = Table::new(rows, widths)
                        .header(header)
                        .column_spacing(1)
                        .block(
                            Block::default()
                                .title(format!(
                                    "{output_title} rows={row_count} shown={shown_count} sql={}",
                                    preview.rewritten_sql.lines().next().unwrap_or("")
                                ))
                                .borders(Borders::ALL),
                        );
                    frame.render_widget(table, right[1]);
                }
            } else {
                let output = Paragraph::new("No execution results. Press F6 to execute query.")
                    .block(Block::default().title(output_title).borders(Borders::ALL))
                    .wrap(Wrap { trim: false });
                frame.render_widget(output, right[1]);
            }
        }
        OutputView::Chart => {
            let chart_lines = build_chart_lines(
                app.execution_preview.as_ref(),
                app.chart_mode,
                app.chart_value_column,
                app.chart_label_column,
            );
            let output = Paragraph::new(chart_lines.join("\n"))
                .block(Block::default().title(output_title).borders(Borders::ALL))
                .wrap(Wrap { trim: false });
            frame.render_widget(output, right[1]);
        }
    }

    let footer = Paragraph::new(app.status.as_str()).block(Block::default().borders(Borders::ALL));
    frame.render_widget(footer, layout[2]);
}

pub fn launch(models_path: &str, connection: Option<String>) -> CliResult<()> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err("workbench requires an interactive terminal (TTY)".to_string());
    }

    let runtime = super::load_runtime(models_path)?;
    let mut app = WorkbenchApp::new(runtime, connection);

    enable_raw_mode().map_err(|e| format!("failed to enable raw mode: {e}"))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)
        .map_err(|e| format!("failed to enter alternate screen: {e}"))?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal =
        Terminal::new(backend).map_err(|e| format!("failed to initialize terminal: {e}"))?;

    let mut loop_result: CliResult<()> = Ok(());
    while !app.should_quit {
        if let Err(err) = terminal.draw(|frame| draw_app(frame, &app)) {
            loop_result = Err(format!("failed to draw workbench UI: {err}"));
            break;
        }

        match event::poll(Duration::from_millis(100)) {
            Ok(true) => match event::read() {
                Ok(Event::Key(key)) => app.handle_key(key),
                Ok(_) => {}
                Err(err) => {
                    loop_result = Err(format!("failed to read terminal event: {err}"));
                    break;
                }
            },
            Ok(false) => {}
            Err(err) => {
                loop_result = Err(format!("failed to poll terminal events: {err}"));
                break;
            }
        }
    }

    let mut restore_error = None;
    if let Err(err) = disable_raw_mode() {
        restore_error = Some(format!("failed to disable raw mode: {err}"));
    }
    if let Err(err) = execute!(terminal.backend_mut(), LeaveAlternateScreen) {
        restore_error = Some(format!("failed to leave alternate screen: {err}"));
    }
    if let Err(err) = terminal.show_cursor() {
        restore_error = Some(format!("failed to restore cursor: {err}"));
    }

    if let Some(err) = restore_error {
        return Err(err);
    }

    loop_result
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    #[test]
    fn format_workbench_value_truncates_and_normalizes_newlines() {
        let value = WorkbenchValue::String(
            "line1\nline2 this is a very long trailing value that should exceed the preview cell width"
                .to_string(),
        );
        let rendered = format_workbench_value(&value);
        assert!(!rendered.contains('\n'));
        assert!(rendered.contains("line1 line2"));
        assert!(rendered.ends_with("..."));
    }

    #[test]
    fn format_execution_output_includes_row_counts_and_preview() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![
                WorkbenchValue::I64(1),
                WorkbenchValue::String("alice".to_string()),
            ],
            vec![
                WorkbenchValue::I64(2),
                WorkbenchValue::String("bob".to_string()),
            ],
        ];
        let rendered = format_execution_output("select * from users", &columns, &rows);
        assert!(rendered.contains("Rendered SQL:"));
        assert!(rendered.contains("rows=2 shown=2"));
        assert!(rendered.contains("id | name"));
        assert!(rendered.contains("alice"));
        assert!(rendered.contains("bob"));
    }

    #[test]
    fn output_view_cycles_in_expected_order() {
        assert_eq!(OutputView::Sql.next(), OutputView::Table);
        assert_eq!(OutputView::Table.next(), OutputView::Chart);
        assert_eq!(OutputView::Chart.next(), OutputView::Sql);
    }

    #[test]
    fn build_chart_lines_uses_numeric_column_when_available() {
        let preview = ExecutionPreview {
            rewritten_sql: "select name, amount from t".to_string(),
            columns: vec!["name".to_string(), "amount".to_string()],
            rows: vec![
                vec![
                    WorkbenchValue::String("alpha".to_string()),
                    WorkbenchValue::F64(10.0),
                ],
                vec![
                    WorkbenchValue::String("beta".to_string()),
                    WorkbenchValue::F64(20.0),
                ],
            ],
        };
        let lines = build_chart_lines(Some(&preview), ChartRenderMode::Bar, None, None);
        assert!(lines
            .first()
            .is_some_and(|line| line.contains("chart source")));
        assert!(lines.iter().any(|line| line.contains("alpha")));
        assert!(lines.iter().any(|line| line.contains("beta")));
    }

    #[test]
    fn build_chart_lines_reports_missing_numeric_data() {
        let preview = ExecutionPreview {
            rewritten_sql: "select name from t".to_string(),
            columns: vec!["name".to_string()],
            rows: vec![vec![WorkbenchValue::String("alpha".to_string())]],
        };
        let lines = build_chart_lines(Some(&preview), ChartRenderMode::Bar, None, None);
        assert_eq!(
            lines,
            vec!["Chart unavailable: no numeric columns in execution result.".to_string()]
        );
    }

    #[test]
    fn build_chart_lines_honors_column_overrides() {
        let preview = ExecutionPreview {
            rewritten_sql: "select label, amount_a, amount_b from t".to_string(),
            columns: vec![
                "label".to_string(),
                "amount_a".to_string(),
                "amount_b".to_string(),
            ],
            rows: vec![
                vec![
                    WorkbenchValue::String("first".to_string()),
                    WorkbenchValue::F64(10.0),
                    WorkbenchValue::F64(3.0),
                ],
                vec![
                    WorkbenchValue::String("second".to_string()),
                    WorkbenchValue::F64(20.0),
                    WorkbenchValue::F64(7.0),
                ],
            ],
        };

        let lines = build_chart_lines(Some(&preview), ChartRenderMode::Bar, Some(2), Some(0));
        assert!(lines
            .first()
            .is_some_and(|line| line.contains("value=amount_b label=label mode=BAR")));
    }

    #[test]
    fn build_chart_lines_dot_mode_renders_dot_track() {
        let preview = ExecutionPreview {
            rewritten_sql: "select name, amount from t".to_string(),
            columns: vec!["name".to_string(), "amount".to_string()],
            rows: vec![
                vec![
                    WorkbenchValue::String("alpha".to_string()),
                    WorkbenchValue::F64(10.0),
                ],
                vec![
                    WorkbenchValue::String("beta".to_string()),
                    WorkbenchValue::F64(20.0),
                ],
            ],
        };

        let lines = build_chart_lines(Some(&preview), ChartRenderMode::Dot, None, None);
        assert!(lines.first().is_some_and(|line| line.contains("mode=DOT")));
        assert!(lines.iter().any(|line| line.contains("o")));
    }

    fn fixture_runtime() -> SidemanticRuntime {
        SidemanticRuntime::from_yaml(
            r#"
models:
  - name: z_orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
  - name: customers
    table: customers
    primary_key: id
    dimensions:
      - name: country
        type: categorical
"#,
        )
        .expect("fixture runtime should load")
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl(ch: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(ch), KeyModifiers::CONTROL)
    }

    #[test]
    fn workbench_app_startup_sorts_models_and_rewrites_initial_query() {
        let app = WorkbenchApp::new(fixture_runtime(), None);
        assert_eq!(app.models[0].name, "customers");
        assert_eq!(app.models[1].name, "z_orders");
        assert_eq!(app.sql_input, "select * from customers");
        assert_eq!(app.output_view, OutputView::Sql);
        assert_eq!(app.focus, FocusPanel::Sql);
        assert_eq!(app.status, "Rewrite ok");
        assert!(!app.output.trim().is_empty());

        let with_connection =
            WorkbenchApp::new(fixture_runtime(), Some("duckdb:///tmp/demo.db".to_string()));
        assert_eq!(with_connection.status, "Rewrite ok");
    }

    #[test]
    fn workbench_key_handling_updates_state_deterministically() {
        let mut app = WorkbenchApp::new(fixture_runtime(), None);

        app.handle_key(key(KeyCode::Char('x')));
        assert!(app.sql_input.ends_with('x'));
        app.handle_key(key(KeyCode::Backspace));
        assert!(!app.sql_input.ends_with('x'));
        app.handle_key(key(KeyCode::Enter));
        assert!(app.sql_input.ends_with('\n'));
        app.handle_key(ctrl('z'));
        assert!(!app.sql_input.ends_with('z'));

        app.handle_key(key(KeyCode::Tab));
        assert_eq!(app.focus, FocusPanel::Models);
        app.handle_key(key(KeyCode::Down));
        assert_eq!(app.selected_model_index, 1);
        app.handle_key(key(KeyCode::Down));
        assert_eq!(app.selected_model_index, 1);
        app.handle_key(key(KeyCode::Up));
        assert_eq!(app.selected_model_index, 0);
        app.handle_key(key(KeyCode::Enter));
        assert_eq!(app.sql_input, "select * from customers");

        app.handle_key(key(KeyCode::F(5)));
        assert_eq!(app.status, "Rewrite ok");
        app.handle_key(key(KeyCode::F(6)));
        assert_eq!(app.status, "Execute skipped: no connection configured");
        app.handle_key(key(KeyCode::F(7)));
        assert_eq!(app.output_view, OutputView::Table);
        app.handle_key(ctrl('3'));
        assert_eq!(app.output_view, OutputView::Chart);
        app.handle_key(ctrl('m'));
        assert_eq!(app.chart_mode, ChartRenderMode::Dot);
        app.handle_key(key(KeyCode::Esc));
        assert!(app.should_quit);
    }

    #[test]
    fn draw_app_renders_main_panels_and_small_viewports() {
        let mut app = WorkbenchApp::new(fixture_runtime(), None);
        let backend = TestBackend::new(100, 32);
        let mut terminal = Terminal::new(backend).expect("test terminal should initialize");
        terminal
            .draw(|frame| draw_app(frame, &app))
            .expect("workbench should draw");
        let rendered = terminal.backend().to_string();
        for expected in [
            "Sidemantic Workbench",
            "Models",
            "Model Details",
            "SQL Input",
            "Output [SQL]",
            "connection=none",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected}\n{rendered}"
            );
        }

        app.execution_preview = Some(ExecutionPreview {
            rewritten_sql: "select country, count(*) from customers group by 1".to_string(),
            columns: vec!["country".to_string(), "count".to_string()],
            rows: vec![vec![
                WorkbenchValue::String("US".to_string()),
                WorkbenchValue::I64(3),
            ]],
        });
        app.output_view = OutputView::Table;
        terminal
            .draw(|frame| draw_app(frame, &app))
            .expect("table view should draw");
        let rendered = terminal.backend().to_string();
        assert!(rendered.contains("country"));
        assert!(rendered.contains("US"));

        app.output_view = OutputView::Chart;
        terminal
            .draw(|frame| draw_app(frame, &app))
            .expect("chart view should draw");
        let rendered = terminal.backend().to_string();
        assert!(rendered.contains("chart source"));

        let backend = TestBackend::new(60, 20);
        let mut small_terminal = Terminal::new(backend).expect("small terminal should initialize");
        small_terminal
            .draw(|frame| draw_app(frame, &app))
            .expect("small viewport should draw");
    }
}
