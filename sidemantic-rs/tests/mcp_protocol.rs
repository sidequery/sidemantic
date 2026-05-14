#![cfg(feature = "mcp-server")]

mod common;

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::process::{ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

use common::{command_with_clean_sidemantic_env, unique_temp_dir, ChildGuard};
use serde_json::{json, Value};

struct McpClient {
    child: ChildGuard,
    stdin: ChildStdin,
    responses: Receiver<Value>,
}

impl McpClient {
    fn spawn(models_path: &std::path::Path) -> Self {
        let mut command = command_with_clean_sidemantic_env(env!("CARGO_BIN_EXE_sidemantic-mcp"));
        command
            .arg("--models")
            .arg(models_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        Self::spawn_command(command)
    }

    fn spawn_positional(models_path: &std::path::Path) -> Self {
        let mut command = command_with_clean_sidemantic_env(env!("CARGO_BIN_EXE_sidemantic-mcp"));
        command
            .arg(models_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        Self::spawn_command(command)
    }

    fn spawn_command(mut command: Command) -> Self {
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

#[test]
fn mcp_server_accepts_positional_models_path() {
    let dir = unique_temp_dir("sidemantic_mcp_positional_models");
    let models_path = common::write_retail_fixture(&dir);
    let mut client = McpClient::spawn_positional(&models_path);

    let init = client.request(
        1,
        "initialize",
        json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "sidemantic-test", "version": "0.0.0" }
        }),
    );
    assert_eq!(init["jsonrpc"], "2.0");

    client.shutdown();
    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn mcp_server_fails_fast_for_missing_models_path() {
    let dir = unique_temp_dir("sidemantic_mcp_bad_models");
    let missing = dir.join("missing.yml");

    let mut command = command_with_clean_sidemantic_env(env!("CARGO_BIN_EXE_sidemantic-mcp"));
    let output = command
        .arg("--models")
        .arg(&missing)
        .output()
        .expect("sidemantic-mcp should run to a startup error");

    assert!(!output.status.success(), "missing models path should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("models path") && stderr.contains("is not a readable file or directory"),
        "{stderr}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn mcp_server_exercises_tool_protocol_and_errors() {
    let dir = unique_temp_dir("sidemantic_mcp_protocol");
    let models_path = common::write_retail_fixture(&dir);
    let mut client = McpClient::spawn(&models_path);

    let init = client.request(
        1,
        "initialize",
        json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "sidemantic-test", "version": "0.0.0" }
        }),
    );
    assert_eq!(init["jsonrpc"], "2.0");
    assert!(init["result"]["capabilities"].to_string().contains("tools"));
    assert!(init["result"]["capabilities"]
        .to_string()
        .contains("resources"));
    client.notify("notifications/initialized", json!({}));

    let tools = client.request(2, "tools/list", json!({}));
    let tool_names = tools["result"]["tools"]
        .as_array()
        .expect("tools/list should return tools")
        .iter()
        .map(|tool| tool["name"].as_str().unwrap_or_default())
        .collect::<Vec<_>>();
    for expected in [
        "list_models",
        "get_models",
        "get_semantic_graph",
        "validate_query",
        "compile_query",
        "run_query",
        "run_sql",
        "create_chart",
    ] {
        assert!(
            tool_names.contains(&expected),
            "missing tool {expected}: {tools}"
        );
    }

    let list = client.request(
        3,
        "tools/call",
        json!({ "name": "list_models", "arguments": {} }),
    );
    let list_payload = structured_content(&list);
    assert!(list_payload["models"]
        .as_array()
        .expect("list_models should return model array")
        .iter()
        .any(|model| model["name"] == "orders"));

    let details = client.request(
        4,
        "tools/call",
        json!({ "name": "get_models", "arguments": { "model_names": ["orders"] } }),
    );
    let details_payload = structured_content(&details);
    assert_eq!(details_payload["models"][0]["name"], "orders");
    assert!(details_payload["models"][0]["relationships"]
        .to_string()
        .contains("customers"));

    let graph = client.request(
        9,
        "tools/call",
        json!({ "name": "get_semantic_graph", "arguments": {} }),
    );
    let graph_payload = structured_content(&graph);
    assert!(graph_payload["models"]
        .as_array()
        .expect("graph should include model array")
        .iter()
        .any(|model| model["name"] == "orders"));
    assert!(graph_payload["joinable_pairs"]
        .as_array()
        .expect("graph should include joinable pairs")
        .iter()
        .any(|pair| pair["from"] == "orders" && pair["to"] == "customers"));

    let compiled = client.request(
        5,
        "tools/call",
        json!({
            "name": "compile_query",
            "arguments": {
                "dimensions": ["orders.status"],
                "metrics": ["orders.revenue"],
                "limit": 5
            }
        }),
    );
    let compiled_payload = structured_content(&compiled);
    let sql = compiled_payload["sql"]
        .as_str()
        .expect("compile tool should return sql");
    assert!(sql.contains("SUM"), "{sql}");
    assert!(sql.contains("GROUP BY"), "{sql}");
    assert!(sql.contains("LIMIT 5"), "{sql}");

    let dry_run = client.request(
        10,
        "tools/call",
        json!({
            "name": "run_query",
            "arguments": {
                "dimensions": ["orders.status"],
                "metrics": ["orders.revenue"],
                "where": "orders.customer_id > 0",
                "offset": 2,
                "dry_run": true
            }
        }),
    );
    let dry_run_payload = structured_content(&dry_run);
    let sql = dry_run_payload["sql"]
        .as_str()
        .expect("dry run should return sql");
    assert!(sql.contains("WHERE"), "{sql}");
    assert!(sql.contains("OFFSET 2"), "{sql}");

    let valid = client.request(
        11,
        "tools/call",
        json!({
            "name": "validate_query",
            "arguments": {
                "dimensions": ["orders.status"],
                "metrics": ["orders.revenue"]
            }
        }),
    );
    let valid_payload = structured_content(&valid);
    assert_eq!(valid_payload["valid"], true);
    assert_eq!(
        valid_payload["errors"]
            .as_array()
            .expect("valid errors should be an array")
            .len(),
        0
    );

    let invalid = client.request(
        12,
        "tools/call",
        json!({
            "name": "validate_query",
            "arguments": { "metrics": ["orders.unknown_metric"] }
        }),
    );
    let invalid_payload = structured_content(&invalid);
    assert_eq!(invalid_payload["valid"], false);
    assert!(invalid_payload["errors"]
        .to_string()
        .contains("unknown_metric"));

    let invalid_metric = client.request(
        6,
        "tools/call",
        json!({
            "name": "compile_query",
            "arguments": { "metrics": ["orders.unknown_metric"] }
        }),
    );
    assert_eq!(invalid_metric["error"]["code"], -32602);
    assert!(invalid_metric["error"]["message"]
        .as_str()
        .unwrap_or("")
        .contains("failed to compile query"));

    let missing_adbc = client.request(
        7,
        "tools/call",
        json!({
            "name": "run_query",
            "arguments": { "metrics": ["orders.revenue"] }
        }),
    );
    assert_eq!(missing_adbc["error"]["code"], -32602);
    assert!(missing_adbc["error"]["message"]
        .as_str()
        .unwrap_or("")
        .contains("mcp-adbc"));

    let missing_adbc_sql = client.request(
        13,
        "tools/call",
        json!({
            "name": "run_sql",
            "arguments": { "query": "select orders.status, orders.revenue from orders" }
        }),
    );
    assert_eq!(missing_adbc_sql["error"]["code"], -32602);
    assert!(missing_adbc_sql["error"]["message"]
        .as_str()
        .unwrap_or("")
        .contains("mcp-adbc"));

    let missing_adbc_chart = client.request(
        14,
        "tools/call",
        json!({
            "name": "create_chart",
            "arguments": {
                "dimensions": ["orders.status"],
                "metrics": ["orders.revenue"]
            }
        }),
    );
    assert_eq!(missing_adbc_chart["error"]["code"], -32602);
    assert!(missing_adbc_chart["error"]["message"]
        .as_str()
        .unwrap_or("")
        .contains("mcp-adbc"));

    let resources = client.request(15, "resources/list", json!({}));
    let resource_items = resources["result"]["resources"]
        .as_array()
        .expect("resources/list should return resources");
    assert!(resource_items
        .iter()
        .any(|resource| resource["uri"] == "semantic://catalog"));

    let catalog = client.request(16, "resources/read", json!({ "uri": "semantic://catalog" }));
    let catalog_text = catalog["result"]["contents"][0]["text"]
        .as_str()
        .expect("catalog resource should return text");
    let catalog_payload: Value =
        serde_json::from_str(catalog_text).expect("catalog text should be JSON");
    assert!(catalog_payload
        .get("tables")
        .and_then(Value::as_array)
        .expect("catalog should include table metadata")
        .iter()
        .any(|table| table["table_name"] == "orders"));
    assert!(catalog_payload
        .get("columns")
        .and_then(Value::as_array)
        .expect("catalog should include column metadata")
        .iter()
        .any(|column| column["table_name"] == "orders" && column["column_name"] == "revenue"));

    let missing_resource =
        client.request(17, "resources/read", json!({ "uri": "semantic://missing" }));
    assert_eq!(missing_resource["error"]["code"], -32002);

    let unknown_tool = client.request(
        8,
        "tools/call",
        json!({ "name": "does_not_exist", "arguments": {} }),
    );
    assert!(unknown_tool.get("error").is_some(), "{unknown_tool}");

    client.shutdown();
    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}
