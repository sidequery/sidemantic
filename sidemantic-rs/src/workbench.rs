use std::io::{self, IsTerminal};
use std::time::Duration;

use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, size as terminal_size, EnterAlternateScreen,
    LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Position, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols;
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Axis, Bar, BarChart, BarGroup, Block, Borders, Cell, Chart, Dataset, GraphType, List, ListItem,
    ListState, Paragraph, Row, Table, Wrap,
};
use ratatui::Terminal;
use sidemantic::SidemanticRuntime;
#[cfg(feature = "workbench-adbc")]
use sidemantic::{execute_with_adbc, AdbcExecutionRequest, AdbcValue};

use crate::CliResult;

const PREVIEW_ROW_LIMIT: usize = 25;
const PREVIEW_CELL_WIDTH: usize = 48;
const CHART_MAX_BARS: usize = 16;

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

/// Order the view tabs are displayed in (results-first, the most common case).
const VIEW_TAB_ORDER: [OutputView; 3] = [OutputView::Table, OutputView::Sql, OutputView::Chart];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChartRenderMode {
    Bar,
    Line,
}

impl ChartRenderMode {
    fn next(self) -> Self {
        match self {
            ChartRenderMode::Bar => ChartRenderMode::Line,
            ChartRenderMode::Line => ChartRenderMode::Bar,
        }
    }

    fn label(self) -> &'static str {
        match self {
            ChartRenderMode::Bar => "BAR",
            ChartRenderMode::Line => "LINE",
        }
    }
}

#[derive(Debug, Clone)]
struct ExecutionPreview {
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

/// Pre-computed rectangles for every panel, shared by drawing and mouse
/// hit-testing so a click always lands on the panel the user sees.
struct UiRects {
    models: Rect,
    details: Rect,
    sql: Rect,
    tabs: Rect,
    output: Rect,
}

fn ui_rects(area: Rect) -> UiRects {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(3),
        ])
        .split(area);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(root[1]);

    let left = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(52), Constraint::Percentage(48)])
        .split(body[0]);

    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(46),
            Constraint::Length(1),
            Constraint::Min(4),
        ])
        .split(body[1]);

    UiRects {
        models: left[0],
        details: left[1],
        sql: right[0],
        tabs: right[1],
        output: right[2],
    }
}

#[derive(Debug)]
struct WorkbenchApp {
    runtime: SidemanticRuntime,
    models: Vec<ModelSummary>,
    selected_model_index: usize,
    sql_input: String,
    /// Cursor position as a character index into `sql_input`.
    cursor: usize,
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

        let sql_input = models
            .first()
            .map_or_else(|| "select 1".to_string(), starter_query_for);
        let cursor = sql_input.chars().count();

        let mut app = Self {
            runtime,
            models,
            selected_model_index: 0,
            sql_input,
            cursor,
            output: String::new(),
            output_view: OutputView::Sql,
            execution_preview: None,
            status: "Ready. Ctrl+R run · 1/2/3 switch views · Tab focus · Esc quit.".to_string(),
            focus: FocusPanel::Sql,
            should_quit: false,
            connection,
            chart_mode: ChartRenderMode::Bar,
            chart_value_column: None,
            chart_label_column: None,
        };
        app.compile_only();
        app
    }

    fn compile(&self) -> Result<String, String> {
        self.runtime
            .rewrite(&self.sql_input)
            .map_err(|err| err.to_string())
    }

    /// Compile the editor SQL into the SQL view without touching the database.
    /// Used on startup and whenever the query is reloaded.
    fn compile_only(&mut self) {
        match self.compile() {
            Ok(sql) => self.output = sql,
            Err(err) => self.output = err,
        }
    }

    /// The single primary action: compile, then execute if a connection is
    /// configured, then jump to the most useful view.
    fn run_query(&mut self) {
        let compiled = match self.compile() {
            Ok(sql) => sql,
            Err(err) => {
                self.execution_preview = None;
                self.output = err;
                self.output_view = OutputView::Sql;
                self.status = "Query did not compile — see SQL view.".to_string();
                return;
            }
        };

        let Some(connection) = self.connection.clone() else {
            self.execution_preview = None;
            self.output = compiled;
            self.output_view = OutputView::Sql;
            self.status =
                "Compiled. No connection configured — pass --db/--connection to run.".to_string();
            return;
        };

        #[cfg(not(feature = "workbench-adbc"))]
        {
            let _ = connection;
            self.execution_preview = None;
            self.output = compiled;
            self.output_view = OutputView::Sql;
            self.status = "Built without ADBC — rebuild with feature 'workbench-adbc'.".to_string();
            return;
        }

        #[cfg(feature = "workbench-adbc")]
        {
            let (driver, uri, database_options) =
                match crate::parse_connection_url_to_adbc(&connection) {
                    Ok(payload) => payload,
                    Err(err) => {
                        self.execution_preview = None;
                        self.output = format!("{compiled}\n\nConnection parsing failed:\n{err}");
                        self.output_view = OutputView::Sql;
                        self.status = "Bad connection URL — see SQL view.".to_string();
                        return;
                    }
                };

            match execute_with_adbc(AdbcExecutionRequest {
                driver,
                sql: compiled.clone(),
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
                    let has_columns = !result.columns.is_empty();
                    self.execution_preview = Some(ExecutionPreview {
                        columns: result.columns,
                        rows,
                    });
                    self.chart_value_column = None;
                    self.chart_label_column = None;
                    // SQL view keeps the compiled SQL; data lands in the table.
                    self.output = compiled;
                    self.output_view = if has_columns {
                        OutputView::Table
                    } else {
                        OutputView::Sql
                    };
                    self.status = format!(
                        "Ran query — {} row{}.",
                        row_count,
                        if row_count == 1 { "" } else { "s" }
                    );
                }
                Err(err) => {
                    self.execution_preview = None;
                    self.output = format!("{compiled}\n\nExecution failed:\n{err}");
                    self.output_view = OutputView::Sql;
                    self.status = "Query failed — see SQL view.".to_string();
                }
            }
        }
    }

    fn cycle_output_view(&mut self) {
        self.output_view = self.output_view.next();
        self.status = format!("View: {}", self.output_view.label());
    }

    fn set_output_view(&mut self, view: OutputView) {
        self.output_view = view;
        self.status = format!("View: {}", self.output_view.label());
    }

    fn cycle_chart_mode(&mut self) {
        self.chart_mode = self.chart_mode.next();
        self.output_view = OutputView::Chart;
        self.status = format!("Chart mode: {}", self.chart_mode.label());
    }

    fn cycle_chart_value_column(&mut self) {
        let Some(preview) = self.execution_preview.as_ref() else {
            self.status = "Run a query before configuring the chart.".to_string();
            return;
        };

        let numeric_candidates = chart_numeric_column_indices(preview);
        if numeric_candidates.is_empty() {
            self.status = "No numeric columns to chart.".to_string();
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
            self.status = "Run a query before configuring the chart.".to_string();
            return;
        };
        if preview.columns.is_empty() {
            self.status = "No columns to chart.".to_string();
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
            self.status = "No label columns to chart.".to_string();
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

    fn selected_model_name(&self) -> Option<&str> {
        self.models
            .get(self.selected_model_index)
            .map(|model| model.name.as_str())
    }

    /// Replace the editor contents with a starter query for the selected model.
    fn load_selected_starter(&mut self) {
        if let Some(model) = self.models.get(self.selected_model_index) {
            let name = model.name.clone();
            self.sql_input = starter_query_for(model);
            self.cursor = self.sql_input.chars().count();
            self.compile_only();
            self.execution_preview = None;
            self.output_view = OutputView::Sql;
            self.focus = FocusPanel::Sql;
            self.status = format!("Loaded starter query for '{name}'. Press Ctrl+R to run.");
        }
    }

    // --- minimal text editor over `sql_input` -----------------------------

    fn sql_char_len(&self) -> usize {
        self.sql_input.chars().count()
    }

    fn cursor_byte(&self) -> usize {
        self.sql_input
            .char_indices()
            .nth(self.cursor)
            .map_or(self.sql_input.len(), |(byte, _)| byte)
    }

    fn insert_char(&mut self, ch: char) {
        let byte = self.cursor_byte();
        self.sql_input.insert(byte, ch);
        self.cursor += 1;
    }

    fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let start = self
            .sql_input
            .char_indices()
            .nth(self.cursor - 1)
            .map_or(0, |(byte, _)| byte);
        let end = self.cursor_byte();
        self.sql_input.replace_range(start..end, "");
        self.cursor -= 1;
    }

    fn delete(&mut self) {
        if self.cursor >= self.sql_char_len() {
            return;
        }
        let start = self.cursor_byte();
        let end = self
            .sql_input
            .char_indices()
            .nth(self.cursor + 1)
            .map_or(self.sql_input.len(), |(byte, _)| byte);
        self.sql_input.replace_range(start..end, "");
    }

    fn cursor_line_col(&self) -> (usize, usize) {
        let mut line = 0;
        let mut col = 0;
        for ch in self.sql_input.chars().take(self.cursor) {
            if ch == '\n' {
                line += 1;
                col = 0;
            } else {
                col += 1;
            }
        }
        (line, col)
    }

    fn line_start_char(&self, line: usize) -> usize {
        let mut index = 0;
        for current in self.sql_input.split('\n').take(line) {
            index += current.chars().count() + 1;
        }
        index
    }

    fn move_home(&mut self) {
        let (line, _) = self.cursor_line_col();
        self.cursor = self.line_start_char(line);
    }

    fn move_end(&mut self) {
        let (line, _) = self.cursor_line_col();
        let line_len = self
            .sql_input
            .split('\n')
            .nth(line)
            .map_or(0, |value| value.chars().count());
        self.cursor = self.line_start_char(line) + line_len;
    }

    fn move_vertical(&mut self, delta: isize) {
        let lines: Vec<&str> = self.sql_input.split('\n').collect();
        let (line, col) = self.cursor_line_col();
        let target = line as isize + delta;
        if target < 0 || target as usize >= lines.len() {
            return;
        }
        let target_line = target as usize;
        let target_len = lines[target_line].chars().count();
        self.cursor = self.line_start_char(target_line) + col.min(target_len);
    }

    fn handle_models_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down if !self.models.is_empty() => {
                self.selected_model_index =
                    (self.selected_model_index + 1).min(self.models.len() - 1);
            }
            KeyCode::Up if self.selected_model_index > 0 => {
                self.selected_model_index -= 1;
            }
            KeyCode::Enter => self.load_selected_starter(),
            _ => {}
        }
    }

    fn handle_sql_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char(ch) => {
                if key.modifiers.contains(KeyModifiers::CONTROL) {
                    return;
                }
                self.insert_char(ch);
            }
            KeyCode::Backspace => self.backspace(),
            KeyCode::Delete => self.delete(),
            KeyCode::Enter => self.insert_char('\n'),
            KeyCode::Left if self.cursor > 0 => self.cursor -= 1,
            KeyCode::Right if self.cursor < self.sql_char_len() => self.cursor += 1,
            KeyCode::Up => self.move_vertical(-1),
            KeyCode::Down => self.move_vertical(1),
            KeyCode::Home => self.move_home(),
            KeyCode::End => self.move_end(),
            _ => {}
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        if key.kind != KeyEventKind::Press {
            return;
        }

        if key.modifiers.contains(KeyModifiers::CONTROL) {
            match key.code {
                KeyCode::Char('c') => {
                    self.should_quit = true;
                    return;
                }
                // Run: Ctrl+R (primary), Ctrl+E (alias).
                KeyCode::Char('r') | KeyCode::Char('e') => {
                    self.run_query();
                    return;
                }
                KeyCode::Char('1') => {
                    self.set_output_view(OutputView::Table);
                    return;
                }
                KeyCode::Char('2') => {
                    self.set_output_view(OutputView::Sql);
                    return;
                }
                KeyCode::Char('3') => {
                    self.set_output_view(OutputView::Chart);
                    return;
                }
                KeyCode::Char('m') => {
                    self.cycle_chart_mode();
                    return;
                }
                KeyCode::Char('v') => {
                    self.cycle_chart_value_column();
                    return;
                }
                KeyCode::Char('l') => {
                    self.cycle_chart_label_column();
                    return;
                }
                _ => {}
            }
        }

        match key.code {
            KeyCode::Esc => self.should_quit = true,
            KeyCode::F(5) => self.run_query(),
            KeyCode::F(7) => self.cycle_output_view(),
            KeyCode::Tab | KeyCode::BackTab => self.next_focus(),
            _ => match self.focus {
                FocusPanel::Models => self.handle_models_key(key),
                FocusPanel::Sql => self.handle_sql_key(key),
            },
        }
    }

    fn handle_mouse(&mut self, area: Rect, mouse: MouseEvent) {
        if mouse.kind != MouseEventKind::Down(MouseButton::Left) {
            return;
        }
        let rects = ui_rects(area);
        let (col, row) = (mouse.column, mouse.row);

        if rect_contains(rects.models, col, row) || rect_contains(rects.details, col, row) {
            self.focus = FocusPanel::Models;
            // Map the click to a model row (first inner row is just below the border).
            let inner_top = rects.models.y + 1;
            if row >= inner_top && row < rects.models.y + rects.models.height.saturating_sub(1) {
                let index = (row - inner_top) as usize;
                if index < self.models.len() {
                    self.selected_model_index = index;
                }
            }
            return;
        }

        if rect_contains(rects.tabs, col, row) {
            for (view, start, width) in view_tab_ranges() {
                let absolute = rects.tabs.x + start;
                if col >= absolute && col < absolute + width {
                    self.set_output_view(view);
                    return;
                }
            }
            return;
        }

        if rect_contains(rects.sql, col, row) || rect_contains(rects.output, col, row) {
            self.focus = FocusPanel::Sql;
        }
    }
}

fn rect_contains(rect: Rect, x: u16, y: u16) -> bool {
    x >= rect.x && x < rect.x + rect.width && y >= rect.y && y < rect.y + rect.height
}

/// A starter query that demonstrates qualified `model.field` semantic SQL,
/// using the model's own dimensions and metrics where available.
fn starter_query_for(model: &ModelSummary) -> String {
    let mut fields: Vec<String> = Vec::new();
    if let Some(dimension) = model.dimension_names.first() {
        fields.push(format!("{}.{}", model.name, dimension));
    }
    for metric in model.metric_names.iter().take(2) {
        fields.push(format!("{}.{}", model.name, metric));
    }
    if fields.is_empty() {
        return format!("select *\nfrom {}", model.name);
    }
    let select_list = fields
        .iter()
        .map(|field| format!("  {field}"))
        .collect::<Vec<_>>()
        .join(",\n");
    let mut query = format!("select\n{select_list}\nfrom {}", model.name);
    if let Some(metric) = model.metric_names.first() {
        query.push_str(&format!(
            "\norder by {}.{} desc\nlimit 20",
            model.name, metric
        ));
    }
    query
}

/// Relative x offsets and widths of each view tab within the tab bar.
fn view_tab_ranges() -> Vec<(OutputView, u16, u16)> {
    let mut ranges = Vec::new();
    let mut x = 0u16;
    for (index, view) in VIEW_TAB_ORDER.iter().enumerate() {
        let width = tab_label(index, *view).chars().count() as u16;
        ranges.push((*view, x, width));
        x += width + 1; // trailing space between tabs
    }
    ranges
}

fn tab_label(index: usize, view: OutputView) -> String {
    format!(" {} {} ", index + 1, view.label())
}

fn view_tab_line(active: OutputView) -> Line<'static> {
    let mut spans = Vec::new();
    for (index, view) in VIEW_TAB_ORDER.iter().enumerate() {
        let style = if *view == active {
            Style::default()
                .fg(Color::Black)
                .bg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Gray)
        };
        spans.push(Span::styled(tab_label(index, *view), style));
        spans.push(Span::raw(" "));
    }
    Line::from(spans)
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
    details.push_str("\nEnter loads a starter query for this model.");
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

#[derive(Debug, Clone, PartialEq)]
struct ChartData {
    value_col: usize,
    label_col: usize,
    points: Vec<(String, f64)>,
}

/// Resolve which columns to chart and extract `(label, value)` points, or return
/// a message explaining why a chart cannot be drawn. Rendering (bar/line) is done
/// with ratatui's `BarChart`/`Chart` widgets in `draw_app`.
fn chart_data(
    preview: &ExecutionPreview,
    value_column_override: Option<usize>,
    label_column_override: Option<usize>,
) -> Result<ChartData, String> {
    if preview.rows.is_empty() || preview.columns.is_empty() {
        return Err("Query returned no rows.".to_string());
    }

    let numeric_candidates = chart_numeric_column_indices(preview);
    let Some(default_value_index) = numeric_candidates.first().copied() else {
        return Err("No numeric columns to chart.".to_string());
    };
    let value_col = value_column_override
        .filter(|index| numeric_candidates.contains(index))
        .unwrap_or(default_value_index);
    let default_label_index = default_chart_label_index(preview, value_col);
    let label_col = label_column_override
        .filter(|index| preview.columns.get(*index).is_some())
        .unwrap_or(default_label_index);

    let mut points = Vec::new();
    for (row_index, row) in preview.rows.iter().enumerate() {
        let Some(value) = row.get(value_col).and_then(workbench_value_as_f64) else {
            continue;
        };
        let label = row
            .get(label_col)
            .map(format_workbench_value)
            .filter(|value| !value.trim().is_empty() && value != "NULL")
            .unwrap_or_else(|| format!("row {}", row_index + 1));
        points.push((label, value));
    }
    if points.is_empty() {
        return Err("No numeric rows to chart.".to_string());
    }
    Ok(ChartData {
        value_col,
        label_col,
        points,
    })
}

/// Bar length in `BarChart`'s u64 units, scaled so the largest magnitude in the
/// series fills the chart. Uses magnitude (not the raw value) so negative values
/// render a proportional bar instead of collapsing to zero; the sign is conveyed
/// by the bar's label text and colour.
fn bar_chart_value(value: f64, max_magnitude: f64) -> u64 {
    let max_magnitude = max_magnitude.max(1e-9);
    ((value.abs() / max_magnitude) * 10_000.0).round() as u64
}

fn truncate_label(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        value.to_string()
    } else {
        let prefix: String = value.chars().take(max.saturating_sub(1)).collect();
        format!("{prefix}…")
    }
}

/// Evenly-spaced x-axis tick labels (first / middle / last category) for line charts.
fn chart_x_axis_labels(points: &[(String, f64)]) -> Vec<Line<'static>> {
    if points.is_empty() {
        return Vec::new();
    }
    let last = points.len() - 1;
    let mut indices = vec![0];
    if last >= 2 {
        indices.push(last / 2);
    }
    if last >= 1 {
        indices.push(last);
    }
    indices
        .into_iter()
        .map(|index| Line::from(truncate_label(&points[index].0, 12)))
        .collect()
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

fn footer_hint(app: &WorkbenchApp) -> String {
    let mut parts = vec!["Ctrl+R run", "1/2/3 views"];
    match app.focus {
        FocusPanel::Models => parts.push("↑↓ pick · Enter load · Tab→editor"),
        FocusPanel::Sql => parts.push("Tab→models"),
    }
    if app.output_view == OutputView::Chart {
        parts.push("Ctrl+M/V/L chart");
    }
    parts.push("Esc quit");
    parts.join("  ·  ")
}

fn draw_app(frame: &mut ratatui::Frame<'_>, app: &WorkbenchApp) {
    let area = frame.area();
    let rects = ui_rects(area);
    let header_rect = Rect::new(area.x, area.y, area.width, 3.min(area.height));
    let footer_rect = if area.height >= 3 {
        Rect::new(area.x, area.y + area.height - 3, area.width, 3)
    } else {
        area
    };

    // --- header -----------------------------------------------------------
    let connection_text = app
        .connection
        .as_deref()
        .map_or("connection=none".to_string(), |value| {
            format!("connection={value}")
        });
    let selected_text = app
        .selected_model_name()
        .map_or("model=none".to_string(), |name| format!("model={name}"));
    let focus_text = match app.focus {
        FocusPanel::Models => "focus=models",
        FocusPanel::Sql => "focus=editor",
    };
    let header = Paragraph::new(format!(
        "Sidemantic Workbench    {selected_text}    {connection_text}    {focus_text}"
    ))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(header, header_rect);

    // --- models list ------------------------------------------------------
    let model_items = app
        .models
        .iter()
        .map(|model| {
            ListItem::new(Line::from(format!(
                "{}  ({}d {}m {}r)",
                model.name, model.dimensions, model.metrics, model.relationships
            )))
        })
        .collect::<Vec<_>>();
    let model_title = if app.focus == FocusPanel::Models {
        "Models [focus]"
    } else {
        "Models"
    };
    let highlight_bg = if app.focus == FocusPanel::Models {
        Color::Yellow
    } else {
        Color::DarkGray
    };
    let model_list = List::new(model_items)
        .block(Block::default().title(model_title).borders(Borders::ALL))
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(highlight_bg)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▶ ");
    let mut model_state = ListState::default();
    if !app.models.is_empty() {
        model_state.select(Some(app.selected_model_index));
    }
    frame.render_stateful_widget(model_list, rects.models, &mut model_state);

    // --- model details ----------------------------------------------------
    let details = selected_model_details(app.models.get(app.selected_model_index));
    let details_panel = Paragraph::new(details)
        .block(
            Block::default()
                .title("Model Details")
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(details_panel, rects.details);

    // --- SQL editor -------------------------------------------------------
    let sql_style = if app.focus == FocusPanel::Sql {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    let inner_w = rects.sql.width.saturating_sub(2);
    let inner_h = rects.sql.height.saturating_sub(2);
    let (cursor_line, cursor_col) = app.cursor_line_col();
    let scroll_y = (cursor_line as u16).saturating_sub(inner_h.saturating_sub(1));
    let scroll_x = (cursor_col as u16).saturating_sub(inner_w.saturating_sub(1));
    let sql_editor = Paragraph::new(app.sql_input.as_str())
        .block(
            Block::default()
                .title("SQL Input — Ctrl+R run, Tab switch focus")
                .borders(Borders::ALL)
                .border_style(sql_style),
        )
        .scroll((scroll_y, scroll_x));
    frame.render_widget(sql_editor, rects.sql);
    if app.focus == FocusPanel::Sql && inner_w > 0 && inner_h > 0 {
        let cursor_x = rects.sql.x + 1 + (cursor_col as u16).saturating_sub(scroll_x);
        let cursor_y = rects.sql.y + 1 + (cursor_line as u16).saturating_sub(scroll_y);
        frame.set_cursor_position(Position::new(
            cursor_x.min(rects.sql.x + rects.sql.width.saturating_sub(1)),
            cursor_y.min(rects.sql.y + rects.sql.height.saturating_sub(1)),
        ));
    }

    // --- view tabs --------------------------------------------------------
    frame.render_widget(Paragraph::new(view_tab_line(app.output_view)), rects.tabs);

    // --- output -----------------------------------------------------------
    let output_title = format!("Output [{}]", app.output_view.label());
    match app.output_view {
        OutputView::Sql => {
            let output = Paragraph::new(app.output.as_str())
                .block(Block::default().title(output_title).borders(Borders::ALL))
                .wrap(Wrap { trim: false });
            frame.render_widget(output, rects.output);
        }
        OutputView::Table => {
            if let Some(preview) = app.execution_preview.as_ref() {
                let row_count = preview.rows.len();
                let shown_count = row_count.min(PREVIEW_ROW_LIMIT);
                if preview.columns.is_empty() {
                    let output = Paragraph::new("Query returned no columns.")
                        .block(Block::default().title(output_title).borders(Borders::ALL))
                        .wrap(Wrap { trim: false });
                    frame.render_widget(output, rects.output);
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
                                    "{output_title}  rows={row_count} shown={shown_count}"
                                ))
                                .borders(Borders::ALL),
                        );
                    frame.render_widget(table, rects.output);
                }
            } else {
                let output = Paragraph::new("No results yet. Press Ctrl+R to run the query.")
                    .block(Block::default().title(output_title).borders(Borders::ALL))
                    .wrap(Wrap { trim: false });
                frame.render_widget(output, rects.output);
            }
        }
        OutputView::Chart => {
            let resolved = app
                .execution_preview
                .as_ref()
                .ok_or_else(|| "No results yet. Press Ctrl+R to run the query.".to_string())
                .and_then(|preview| {
                    chart_data(preview, app.chart_value_column, app.chart_label_column)
                        .map(|data| (preview, data))
                });
            match resolved {
                Err(message) => {
                    let output = Paragraph::new(message)
                        .block(Block::default().title(output_title).borders(Borders::ALL))
                        .wrap(Wrap { trim: false });
                    frame.render_widget(output, rects.output);
                }
                Ok((preview, data)) => {
                    let title = format!(
                        "{output_title}  {} by {}  [{}]  Ctrl+M mode · Ctrl+V/L cols",
                        preview.columns[data.value_col],
                        preview.columns[data.label_col],
                        app.chart_mode.label(),
                    );
                    let block = Block::default().title(title).borders(Borders::ALL);
                    match app.chart_mode {
                        ChartRenderMode::Bar => {
                            let max_magnitude = data
                                .points
                                .iter()
                                .map(|(_, value)| value.abs())
                                .fold(0.0_f64, f64::max);
                            let bars: Vec<Bar> = data
                                .points
                                .iter()
                                .take(CHART_MAX_BARS)
                                .map(|(label, value)| {
                                    let color = if *value < 0.0 {
                                        Color::Red
                                    } else {
                                        Color::Cyan
                                    };
                                    Bar::default()
                                        .value(bar_chart_value(*value, max_magnitude))
                                        .label(Line::from(truncate_label(label, 18)))
                                        .text_value(format!("{value:.2}"))
                                        .style(Style::default().fg(color))
                                })
                                .collect();
                            let barchart = BarChart::default()
                                .block(block)
                                .direction(Direction::Horizontal)
                                .bar_width(1)
                                .bar_gap(0)
                                .data(BarGroup::default().bars(&bars));
                            frame.render_widget(barchart, rects.output);
                        }
                        ChartRenderMode::Line => {
                            let series: Vec<(f64, f64)> = data
                                .points
                                .iter()
                                .enumerate()
                                .map(|(index, (_, value))| (index as f64, *value))
                                .collect();
                            let min_y =
                                series.iter().map(|(_, y)| *y).fold(f64::INFINITY, f64::min);
                            let max_y = series
                                .iter()
                                .map(|(_, y)| *y)
                                .fold(f64::NEG_INFINITY, f64::max);
                            let (min_y, max_y) = if (max_y - min_y).abs() < 1e-9 {
                                (min_y - 1.0, max_y + 1.0)
                            } else {
                                (min_y, max_y)
                            };
                            let last_x = series.len().saturating_sub(1).max(1) as f64;
                            let datasets = vec![Dataset::default()
                                .marker(symbols::Marker::Braille)
                                .graph_type(GraphType::Line)
                                .style(Style::default().fg(Color::Cyan))
                                .data(&series)];
                            let chart = Chart::new(datasets)
                                .block(block)
                                .x_axis(
                                    Axis::default()
                                        .style(Style::default().fg(Color::DarkGray))
                                        .bounds([0.0, last_x])
                                        .labels(chart_x_axis_labels(&data.points)),
                                )
                                .y_axis(
                                    Axis::default()
                                        .style(Style::default().fg(Color::DarkGray))
                                        .bounds([min_y, max_y])
                                        .labels(vec![
                                            Line::from(format!("{min_y:.2}")),
                                            Line::from(format!("{:.2}", (min_y + max_y) / 2.0)),
                                            Line::from(format!("{max_y:.2}")),
                                        ]),
                                );
                            frame.render_widget(chart, rects.output);
                        }
                    }
                }
            }
        }
    }

    // --- footer -----------------------------------------------------------
    let footer = Paragraph::new(app.status.as_str()).block(
        Block::default()
            .borders(Borders::ALL)
            .title(footer_hint(app)),
    );
    frame.render_widget(footer, footer_rect);
}

pub fn launch(models_path: &str, connection: Option<String>) -> CliResult<()> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err("workbench requires an interactive terminal (TTY)".to_string());
    }

    let runtime = super::load_runtime(models_path)?;
    let mut app = WorkbenchApp::new(runtime, connection);

    enable_raw_mode().map_err(|e| format!("failed to enable raw mode: {e}"))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)
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
                Ok(Event::Mouse(mouse)) => {
                    let (cols, rows) = terminal_size().unwrap_or((80, 24));
                    app.handle_mouse(Rect::new(0, 0, cols, rows), mouse);
                }
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
    if let Err(err) = execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    ) {
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
    fn output_view_cycles_in_expected_order() {
        assert_eq!(OutputView::Sql.next(), OutputView::Table);
        assert_eq!(OutputView::Table.next(), OutputView::Chart);
        assert_eq!(OutputView::Chart.next(), OutputView::Sql);
    }

    #[test]
    fn starter_query_uses_qualified_dimensions_and_metrics() {
        let model = ModelSummary {
            name: "orders".to_string(),
            table: "orders".to_string(),
            dimensions: 1,
            metrics: 1,
            relationships: 0,
            dimension_names: vec!["status".to_string()],
            metric_names: vec!["revenue".to_string()],
            relationship_names: vec![],
        };
        let query = starter_query_for(&model);
        assert!(query.contains("orders.status"), "{query}");
        assert!(query.contains("orders.revenue"), "{query}");
        assert!(query.contains("from orders"), "{query}");
        assert!(query.contains("order by orders.revenue desc"), "{query}");
    }

    #[test]
    fn starter_query_without_metrics_selects_star() {
        let model = ModelSummary {
            name: "lookup".to_string(),
            table: "lookup".to_string(),
            dimensions: 0,
            metrics: 0,
            relationships: 0,
            dimension_names: vec![],
            metric_names: vec![],
            relationship_names: vec![],
        };
        assert_eq!(starter_query_for(&model), "select *\nfrom lookup");
    }

    #[test]
    fn view_tab_ranges_are_contiguous_and_match_labels() {
        let ranges = view_tab_ranges();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].0, OutputView::Table);
        assert_eq!(ranges[1].0, OutputView::Sql);
        assert_eq!(ranges[2].0, OutputView::Chart);
        // Table tab starts at 0 and has the width of " 1 TABLE ".
        assert_eq!(ranges[0].1, 0);
        assert_eq!(ranges[0].2, " 1 TABLE ".chars().count() as u16);
    }

    #[test]
    fn chart_data_picks_numeric_column_and_label() {
        let preview = ExecutionPreview {
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
        let data = chart_data(&preview, None, None).expect("chart data");
        assert_eq!(data.value_col, 1);
        assert_eq!(data.label_col, 0);
        assert_eq!(
            data.points,
            vec![("alpha".to_string(), 10.0), ("beta".to_string(), 20.0)]
        );
    }

    #[test]
    fn chart_data_reports_missing_numeric_data() {
        let preview = ExecutionPreview {
            columns: vec!["name".to_string()],
            rows: vec![vec![WorkbenchValue::String("alpha".to_string())]],
        };
        assert_eq!(
            chart_data(&preview, None, None),
            Err("No numeric columns to chart.".to_string())
        );
    }

    #[test]
    fn chart_data_honors_column_overrides() {
        let preview = ExecutionPreview {
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

        let data = chart_data(&preview, Some(2), Some(0)).expect("chart data");
        assert_eq!(data.value_col, 2);
        assert_eq!(data.label_col, 0);
        assert_eq!(
            data.points,
            vec![("first".to_string(), 3.0), ("second".to_string(), 7.0)]
        );
    }

    #[test]
    fn chart_render_mode_toggles_between_bar_and_line() {
        assert_eq!(ChartRenderMode::Bar.next(), ChartRenderMode::Line);
        assert_eq!(ChartRenderMode::Line.next(), ChartRenderMode::Bar);
        assert_eq!(ChartRenderMode::Line.label(), "LINE");
    }

    #[test]
    fn bar_chart_value_preserves_negative_magnitudes() {
        // Largest magnitude (whether positive or negative) fills the chart.
        assert_eq!(bar_chart_value(-20.0, 20.0), 10_000);
        assert_eq!(bar_chart_value(20.0, 20.0), 10_000);
        // A negative value renders a proportional, non-zero bar (not clamped to 0).
        assert_eq!(bar_chart_value(-10.0, 20.0), 5_000);
        assert_eq!(bar_chart_value(5.0, 20.0), 2_500);
        // An all-zero series does not divide by zero.
        assert_eq!(bar_chart_value(0.0, 0.0), 0);
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
    fn workbench_app_startup_sorts_models_and_seeds_starter_query() {
        let app = WorkbenchApp::new(fixture_runtime(), None);
        assert_eq!(app.models[0].name, "customers");
        assert_eq!(app.models[1].name, "z_orders");
        // First model has a dimension but no metrics → dimension-only starter.
        assert_eq!(app.sql_input, "select\n  customers.country\nfrom customers");
        assert_eq!(app.cursor, app.sql_char_len());
        assert_eq!(app.output_view, OutputView::Sql);
        assert_eq!(app.focus, FocusPanel::Sql);
        assert!(app.status.contains("Ready"));
        assert!(!app.output.trim().is_empty());
    }

    #[test]
    fn run_without_connection_compiles_and_reports_missing_connection() {
        let mut app = WorkbenchApp::new(fixture_runtime(), None);
        app.handle_key(ctrl('r'));
        assert!(app.status.contains("No connection configured"));
        assert_eq!(app.output_view, OutputView::Sql);
        assert!(!app.output.trim().is_empty());
    }

    #[test]
    fn editor_supports_cursor_movement_and_mid_string_edits() {
        let mut app = WorkbenchApp::new(fixture_runtime(), None);
        app.sql_input = "abc".to_string();
        app.cursor = 3;

        app.handle_key(key(KeyCode::Left));
        app.handle_key(key(KeyCode::Left));
        assert_eq!(app.cursor, 1);
        app.handle_key(key(KeyCode::Char('X')));
        assert_eq!(app.sql_input, "aXbc");
        assert_eq!(app.cursor, 2);
        app.handle_key(key(KeyCode::Backspace));
        assert_eq!(app.sql_input, "abc");
        app.handle_key(key(KeyCode::Home));
        assert_eq!(app.cursor, 0);
        app.handle_key(key(KeyCode::Delete));
        assert_eq!(app.sql_input, "bc");
        app.handle_key(key(KeyCode::End));
        assert_eq!(app.cursor, 2);
        app.handle_key(key(KeyCode::Enter));
        app.handle_key(key(KeyCode::Char('z')));
        assert_eq!(app.sql_input, "bc\nz");
        app.handle_key(key(KeyCode::Up));
        assert_eq!(app.cursor_line_col(), (0, 1));
    }

    #[test]
    fn key_handling_switches_views_models_and_focus() {
        let mut app = WorkbenchApp::new(fixture_runtime(), None);

        app.handle_key(key(KeyCode::Tab));
        assert_eq!(app.focus, FocusPanel::Models);
        app.handle_key(key(KeyCode::Down));
        assert_eq!(app.selected_model_index, 1);
        app.handle_key(key(KeyCode::Enter));
        // Loading a starter focuses the editor with the model's query.
        assert_eq!(app.focus, FocusPanel::Sql);
        assert!(app.sql_input.contains("z_orders.revenue"));

        app.handle_key(ctrl('1'));
        assert_eq!(app.output_view, OutputView::Table);
        app.handle_key(ctrl('2'));
        assert_eq!(app.output_view, OutputView::Sql);
        app.handle_key(ctrl('3'));
        assert_eq!(app.output_view, OutputView::Chart);
        app.handle_key(key(KeyCode::F(7)));
        assert_eq!(app.output_view, OutputView::Sql);
        app.handle_key(ctrl('m'));
        assert_eq!(app.chart_mode, ChartRenderMode::Line);

        app.handle_key(key(KeyCode::Esc));
        assert!(app.should_quit);
    }

    #[test]
    fn mouse_click_selects_model_and_switches_view_tab() {
        let mut app = WorkbenchApp::new(fixture_runtime(), None);
        let area = Rect::new(0, 0, 100, 32);
        let rects = ui_rects(area);

        // Click the second model row.
        let second_row = rects.models.y + 1 + 1;
        app.handle_mouse(
            area,
            MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: rects.models.x + 2,
                row: second_row,
                modifiers: KeyModifiers::NONE,
            },
        );
        assert_eq!(app.focus, FocusPanel::Models);
        assert_eq!(app.selected_model_index, 1);

        // Click the Chart tab.
        let chart_range = view_tab_ranges()[2];
        app.handle_mouse(
            area,
            MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: rects.tabs.x + chart_range.1 + 1,
                row: rects.tabs.y,
                modifiers: KeyModifiers::NONE,
            },
        );
        assert_eq!(app.output_view, OutputView::Chart);
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
            "TABLE",
            "SQL",
            "CHART",
            "connection=none",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected}\n{rendered}"
            );
        }

        app.execution_preview = Some(ExecutionPreview {
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
            .expect("bar chart should draw");
        let rendered = terminal.backend().to_string();
        assert!(rendered.contains("count by country"), "{rendered}");

        app.chart_mode = ChartRenderMode::Line;
        terminal
            .draw(|frame| draw_app(frame, &app))
            .expect("line chart should draw");

        let backend = TestBackend::new(60, 20);
        let mut small_terminal = Terminal::new(backend).expect("small terminal should initialize");
        small_terminal
            .draw(|frame| draw_app(frame, &app))
            .expect("small viewport should draw");
    }
}
