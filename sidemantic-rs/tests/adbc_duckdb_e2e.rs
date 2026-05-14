#![cfg(all(feature = "mcp-adbc", feature = "runtime-server-adbc"))]

mod common;

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

use adbc_core::options::{OptionDatabase, OptionValue};
use common::{command_with_clean_sidemantic_env, free_loopback_addr, unique_temp_dir, ChildGuard};
use serde_json::{json, Value};
use sidemantic::{execute_with_adbc, AdbcExecutionRequest};

const DUCKDB_ENTRYPOINT: &str = "duckdb_adbc_init";

fn duckdb_driver_path() -> Option<String> {
    std::env::var("SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn duckdb_database_options(db_path: &Path) -> Vec<(OptionDatabase, OptionValue)> {
    vec![(
        OptionDatabase::from("path"),
        OptionValue::String(db_path.to_string_lossy().to_string()),
    )]
}

fn execute_duckdb_sql(driver: &str, db_path: &Path, sql: &str) {
    execute_with_adbc(AdbcExecutionRequest {
        driver: driver.to_string(),
        sql: sql.to_string(),
        uri: None,
        entrypoint: Some(DUCKDB_ENTRYPOINT.to_string()),
        database_options: duckdb_database_options(db_path),
        connection_options: Vec::new(),
    })
    .unwrap_or_else(|err| panic!("DuckDB ADBC SQL failed: {sql}\n{err}"));
}

fn seed_duckdb(driver: &str, db_path: &Path) {
    execute_duckdb_sql(driver, db_path, "drop table if exists orders");
    execute_duckdb_sql(
        driver,
        db_path,
        "create table orders(order_id integer, status varchar, customer_id integer, amount double)",
    );
    execute_duckdb_sql(
        driver,
        db_path,
        "insert into orders values (1, 'complete', 10, 10.5), (2, 'complete', 11, 20.0), (3, 'cancelled', 10, 7.0)",
    );
}

fn assert_revenue_rows(rows: &[Value]) {
    assert_eq!(rows.len(), 2, "{rows:?}");
    let revenue_for = |status: &str| -> f64 {
        rows.iter()
            .find(|row| row["status"] == status)
            .and_then(|row| row["revenue"].as_f64())
            .unwrap_or_else(|| panic!("missing revenue row for {status}: {rows:?}"))
    };
    assert!((revenue_for("complete") - 30.5).abs() < f64::EPSILON);
    assert!((revenue_for("cancelled") - 7.0).abs() < f64::EPSILON);
}

fn run_cli(driver: &str, models_path: &Path, db_path: &Path) {
    let output = Command::new(env!("CARGO_BIN_EXE_sidemantic"))
        .arg("run")
        .arg("--models")
        .arg(models_path)
        .arg("--dimension")
        .arg("orders.status")
        .arg("--metric")
        .arg("orders.revenue")
        .arg("--driver")
        .arg(driver)
        .arg("--entrypoint")
        .arg(DUCKDB_ENTRYPOINT)
        .arg("--dbopt")
        .arg(format!("path={}", db_path.to_string_lossy()))
        .output()
        .expect("sidemantic run should execute");

    assert!(
        output.status.success(),
        "sidemantic run failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value =
        serde_json::from_slice(&output.stdout).expect("CLI run output should be JSON");
    assert_eq!(payload["row_count"], 2);
    assert_revenue_rows(payload["rows"].as_array().expect("rows should be an array"));
}

fn http_request(addr: &str, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
    let body = body.map(|value| value.to_string());
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: application/json\r\n"
    );
    if let Some(body) = body.as_ref() {
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");
    if let Some(body) = body.as_ref() {
        request.push_str(body);
    }

    let mut stream = TcpStream::connect(addr).expect("server should accept connections");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout should be set");
    stream
        .write_all(request.as_bytes())
        .expect("request should be written");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("response should be read");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .expect("response should contain headers and body");
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .expect("status code should be parsed");
    let json_body = serde_json::from_str(body.trim()).unwrap_or_else(|err| {
        panic!("response body should be JSON: {err}\nraw response:\n{response}")
    });
    (status, json_body)
}

fn wait_for_server(addr: &str) {
    for _ in 0..100 {
        if TcpStream::connect(addr).is_ok() {
            let (status, body) = http_request(addr, "GET", "/readyz", None);
            if status == 200 && body == json!({ "status": "ok" }) {
                return;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not become ready at {addr}");
}

fn run_http(driver: &str, models_path: &Path, db_path: &Path) {
    let bind = free_loopback_addr();
    let mut command = command_with_clean_sidemantic_env(env!("CARGO_BIN_EXE_sidemantic-server"));
    command
        .arg("--models")
        .arg(models_path)
        .arg("--bind")
        .arg(&bind)
        .arg("--driver")
        .arg(driver)
        .arg("--entrypoint")
        .arg(DUCKDB_ENTRYPOINT)
        .arg("--dbopt")
        .arg(format!("path={}", db_path.to_string_lossy()));
    let mut child = ChildGuard::new(
        command
            .spawn()
            .expect("sidemantic-server child should spawn"),
    );
    wait_for_server(&bind);

    let (status, body) = http_request(
        &bind,
        "POST",
        "/query",
        Some(json!({
            "dimensions": ["orders.status"],
            "metrics": ["orders.revenue"]
        })),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["row_count"], 2);
    assert_revenue_rows(body["rows"].as_array().expect("rows should be an array"));

    let (status, body) = http_request(
        &bind,
        "POST",
        "/sql",
        Some(json!({
            "query": "select orders.status, orders.revenue from orders"
        })),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["row_count"], 2);
    assert_revenue_rows(body["rows"].as_array().expect("rows should be an array"));
    assert_eq!(
        body["original_sql"],
        "select orders.status, orders.revenue from orders"
    );

    let (status, body) = http_request(
        &bind,
        "POST",
        "/raw",
        Some(json!({
            "query": "select status, amount from orders order by order_id limit 1"
        })),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["row_count"], 1);
    assert_eq!(body["rows"][0]["status"], "complete");
    assert_eq!(body["rows"][0]["amount"], 10.5);

    child.kill_and_wait();
}

struct McpClient {
    child: ChildGuard,
    stdin: ChildStdin,
    responses: Receiver<Value>,
}

impl McpClient {
    fn spawn(models_path: &Path, driver: &str, db_path: &Path) -> Self {
        let mut command = command_with_clean_sidemantic_env(env!("CARGO_BIN_EXE_sidemantic-mcp"));
        command
            .arg("--models")
            .arg(models_path)
            .arg("--driver")
            .arg(driver)
            .arg("--entrypoint")
            .arg(DUCKDB_ENTRYPOINT)
            .arg("--dbopt")
            .arg(format!("path={}", db_path.to_string_lossy()))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        let mut child = command.spawn().expect("sidemantic-mcp should spawn");
        let stdin = child.stdin.take().expect("child stdin should be piped");
        let stdout = child.stdout.take().expect("child stdout should be piped");
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = match line {
                    Ok(line) => line,
                    Err(_) => break,
                };
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(value) = serde_json::from_str::<Value>(&line) {
                    let _ = tx.send(value);
                }
            }
        });

        Self {
            child: ChildGuard::new(child),
            stdin,
            responses: rx,
        }
    }

    fn send(&mut self, message: Value) {
        writeln!(self.stdin, "{message}").expect("mcp message should be written");
        self.stdin.flush().expect("mcp stdin should flush");
    }

    fn request(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }));
        self.read_response(id)
    }

    fn notify(&mut self, method: &str, params: Value) {
        self.send(json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        }));
    }

    fn read_response(&mut self, id: u64) -> Value {
        for _ in 0..20 {
            let value = self
                .responses
                .recv_timeout(Duration::from_secs(5))
                .expect("mcp response should arrive");
            if value["id"] == json!(id) {
                return value;
            }
        }
        panic!("mcp response id {id} was not observed");
    }

    fn shutdown(mut self) {
        self.child.kill_and_wait();
    }
}

fn structured_content(response: &Value) -> Value {
    if let Some(value) = response["result"]["structuredContent"].as_object() {
        return Value::Object(value.clone());
    }
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("tool response should include text content");
    serde_json::from_str(text).expect("tool text content should be JSON")
}

fn run_mcp(driver: &str, models_path: &Path, db_path: &Path) {
    let mut client = McpClient::spawn(models_path, driver, db_path);

    let init = client.request(
        1,
        "initialize",
        json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "sidemantic-adbc-test", "version": "0.0.0" }
        }),
    );
    assert_eq!(init["jsonrpc"], "2.0");
    client.notify("notifications/initialized", json!({}));

    let response = client.request(
        2,
        "tools/call",
        json!({
            "name": "run_query",
            "arguments": {
                "dimensions": ["orders.status"],
                "metrics": ["orders.revenue"]
            }
        }),
    );
    let payload = structured_content(&response);
    assert_eq!(payload["row_count"], 2);
    assert_revenue_rows(payload["rows"].as_array().expect("rows should be an array"));

    let sql_response = client.request(
        3,
        "tools/call",
        json!({
            "name": "run_sql",
            "arguments": {
                "query": "select orders.status, orders.revenue from orders"
            }
        }),
    );
    let sql_payload = structured_content(&sql_response);
    assert_eq!(sql_payload["row_count"], 2);
    assert_revenue_rows(
        sql_payload["rows"]
            .as_array()
            .expect("SQL rows should be an array"),
    );
    assert_eq!(
        sql_payload["original_sql"],
        "select orders.status, orders.revenue from orders"
    );

    let chart_response = client.request(
        4,
        "tools/call",
        json!({
            "name": "create_chart",
            "arguments": {
                "dimensions": ["orders.status"],
                "metrics": ["orders.revenue"],
                "chart_type": "bar",
                "width": 80,
                "height": 60
            }
        }),
    );
    let chart_payload = structured_content(&chart_response);
    assert_eq!(chart_payload["row_count"], 2);
    assert_eq!(chart_payload["vega_spec"]["title"], "Revenue by Status");
    assert!(chart_payload["vega_spec"]["data"]["values"]
        .as_array()
        .expect("chart should embed data")
        .iter()
        .any(|row| row["status"] == "complete"));
    assert!(chart_payload["png_base64"]
        .as_str()
        .expect("chart should include PNG data URL")
        .starts_with("data:image/png;base64,"));

    client.shutdown();
}

#[test]
fn duckdb_adbc_executes_cli_http_and_mcp() {
    let Some(driver) = duckdb_driver_path() else {
        eprintln!("skipping DuckDB ADBC E2E; SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER is not set");
        return;
    };

    let dir = unique_temp_dir("sidemantic_adbc_duckdb_e2e");
    let models_path = common::write_retail_fixture(&dir);
    let db_path: PathBuf = dir.join("warehouse.duckdb");
    seed_duckdb(&driver, &db_path);

    run_cli(&driver, &models_path, &db_path);
    run_http(&driver, &models_path, &db_path);
    run_mcp(&driver, &models_path, &db_path);

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}
