#![cfg(feature = "runtime-server")]

mod common;

use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use common::{command_with_clean_sidemantic_env, free_loopback_addr, unique_temp_dir};
use serde_json::{json, Value};

fn http_request(addr: &str, method: &str, path: &str, body: Option<Value>) -> (u16, Value, String) {
    http_request_with_headers(addr, method, path, body, &[])
}

fn http_request_with_headers(
    addr: &str,
    method: &str,
    path: &str,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> (u16, Value, String) {
    let body = body.map(|value| value.to_string());
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: application/json\r\n"
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
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
    (status, json_body, response)
}

fn wait_for_server(addr: &str) {
    for _ in 0..100 {
        if TcpStream::connect(addr).is_ok() {
            let (status, body, _) = http_request(addr, "GET", "/readyz", None);
            if status == 200 && body == json!({ "status": "ok" }) {
                return;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not become ready at {addr}");
}

#[test]
fn http_server_fails_fast_for_missing_models_path() {
    let dir = unique_temp_dir("sidemantic_http_server_bad_models");
    let missing = dir.join("missing.yml");
    let bind = free_loopback_addr();

    let mut command = command_with_clean_sidemantic_env(env!("CARGO_BIN_EXE_sidemantic-server"));
    let output = command
        .arg("--models")
        .arg(&missing)
        .arg("--bind")
        .arg(&bind)
        .output()
        .expect("sidemantic-server should run to a startup error");

    assert!(!output.status.success(), "missing models path should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("models path") && stderr.contains("is not a readable file or directory"),
        "{stderr}"
    );

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn http_server_exercises_real_endpoints_and_errors() {
    let dir = unique_temp_dir("sidemantic_http_server");
    let models_path = common::write_retail_fixture(&dir);
    let bind = free_loopback_addr();

    let mut command = command_with_clean_sidemantic_env(env!("CARGO_BIN_EXE_sidemantic-server"));
    command
        .arg("--models")
        .arg(&models_path)
        .arg("--bind")
        .arg(&bind);
    let mut child = common::ChildGuard::new(
        command
            .spawn()
            .expect("sidemantic-server child should spawn"),
    );
    wait_for_server(&bind);

    let (status, body, _) = http_request(&bind, "GET", "/health", None);
    assert_eq!(status, 200);
    assert_eq!(body["status"], "ok");
    assert_eq!(body["model_count"], 2);

    let (status, body, _) = http_request(&bind, "GET", "/readyz", None);
    assert_eq!(status, 200);
    assert_eq!(body, json!({ "status": "ok" }));

    let (status, body, _) = http_request(&bind, "GET", "/models", None);
    assert_eq!(status, 200);
    assert!(body
        .as_array()
        .expect("models response should be an array")
        .iter()
        .any(|model| model["name"] == "orders"));

    let (status, body, _) = http_request(&bind, "GET", "/graph", None);
    assert_eq!(status, 200);
    assert!(body["models"]
        .as_array()
        .expect("graph models should be an array")
        .iter()
        .any(|model| model["name"] == "orders"));
    assert!(body["joinable_pairs"]
        .as_array()
        .expect("joinable pairs should be an array")
        .iter()
        .any(|pair| pair["from"] == "orders" && pair["to"] == "customers"));

    let (status, body, _) = http_request(&bind, "GET", "/models/orders", None);
    assert_eq!(status, 200);
    assert_eq!(body["name"], "orders");
    assert!(body["relationships"].to_string().contains("customers"));

    let (status, body, _) = http_request(&bind, "GET", "/models/missing", None);
    assert_eq!(status, 404);
    assert_eq!(body["error"], "model not found: missing");

    let (status, body, _) = http_request(
        &bind,
        "POST",
        "/models",
        Some(json!({ "model_names": ["orders"] })),
    );
    assert_eq!(status, 200);
    assert_eq!(
        body.as_array()
            .expect("filtered models should be array")
            .len(),
        1
    );
    assert_eq!(body[0]["name"], "orders");

    let (status, body, _) = http_request(
        &bind,
        "POST",
        "/query/compile",
        Some(json!({
            "dimensions": ["orders.status"],
            "metrics": ["orders.revenue"],
            "limit": 5
        })),
    );
    assert_eq!(status, 200);
    let sql = body["sql"]
        .as_str()
        .expect("compile response should include sql");
    assert!(sql.contains("SUM"), "{sql}");
    assert!(sql.contains("GROUP BY"), "{sql}");
    assert!(sql.contains("LIMIT 5"), "{sql}");

    let (status, body, _) = http_request(
        &bind,
        "POST",
        "/compile",
        Some(json!({
            "dimensions": ["orders.status"],
            "metrics": ["orders.revenue"],
            "filters": ["orders.status = 'complete'"],
            "where": "orders.customer_id > 0",
            "order_by": ["orders.revenue desc"],
            "limit": 5,
            "offset": 2,
            "ungrouped": false
        })),
    );
    assert_eq!(status, 200);
    let sql = body["sql"]
        .as_str()
        .expect("compile alias response should include sql");
    assert!(sql.contains("WHERE"), "{sql}");
    assert!(
        sql.to_ascii_uppercase().contains("ORDER BY REVENUE DESC"),
        "{sql}"
    );
    assert!(sql.contains("LIMIT 5"), "{sql}");
    assert!(sql.contains("OFFSET 2"), "{sql}");

    let (status, body, _) = http_request(
        &bind,
        "POST",
        "/query/compile",
        Some(json!({ "metrics": ["orders.unknown_metric"] })),
    );
    assert_eq!(status, 400);
    assert!(body["error"]
        .as_str()
        .unwrap_or("")
        .contains("failed to compile query"));

    let (status, body, _) = http_request(
        &bind,
        "POST",
        "/query",
        Some(json!({ "metrics": ["orders.revenue"] })),
    );
    assert_eq!(status, 400);
    assert!(body["error"]
        .as_str()
        .unwrap_or("")
        .contains("runtime-server-adbc"));

    let (status, body, _) = http_request(
        &bind,
        "POST",
        "/sql/compile",
        Some(json!({ "query": "select orders.status, orders.revenue from orders" })),
    );
    assert_eq!(status, 200);
    assert!(body["sql"].as_str().unwrap_or("").contains("orders_cte"));

    let (status, body, _) = http_request(
        &bind,
        "POST",
        "/raw",
        Some(json!({ "query": "delete from orders" })),
    );
    assert_eq!(status, 400);
    assert!(body["error"].as_str().unwrap_or("").contains("SELECT"));

    child.kill_and_wait();
    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn http_server_enforces_auth_cors_and_body_limit() {
    let dir = unique_temp_dir("sidemantic_http_server_controls");
    let models_path = common::write_retail_fixture(&dir);
    let bind = free_loopback_addr();

    let mut command = command_with_clean_sidemantic_env(env!("CARGO_BIN_EXE_sidemantic-server"));
    command
        .arg("--models")
        .arg(&models_path)
        .arg("--bind")
        .arg(&bind)
        .arg("--auth-token")
        .arg("secret")
        .arg("--cors-origin")
        .arg("https://app.example.com")
        .arg("--max-request-body-bytes")
        .arg("32");
    let mut child = common::ChildGuard::new(
        command
            .spawn()
            .expect("sidemantic-server child should spawn"),
    );
    wait_for_server(&bind);

    let (status, body, _) = http_request(&bind, "GET", "/readyz", None);
    assert_eq!(status, 200);
    assert_eq!(body, json!({ "status": "ok" }));

    let (status, body, response) = http_request(&bind, "GET", "/health", None);
    assert_eq!(status, 401);
    assert_eq!(body["error"], "Unauthorized");
    assert!(response.to_ascii_lowercase().contains("www-authenticate"));

    let (status, body, response) = http_request_with_headers(
        &bind,
        "GET",
        "/health",
        None,
        &[
            ("Authorization", "Bearer secret"),
            ("Origin", "https://app.example.com"),
        ],
    );
    assert_eq!(status, 200);
    assert_eq!(body["status"], "ok");
    assert!(response
        .to_ascii_lowercase()
        .contains("access-control-allow-origin: https://app.example.com"));

    let (status, body, _) = http_request_with_headers(
        &bind,
        "POST",
        "/compile",
        Some(json!({
            "dimensions": ["orders.status"],
            "metrics": ["orders.revenue"],
            "filters": ["orders.status = 'complete'"]
        })),
        &[("Authorization", "Bearer secret")],
    );
    assert_eq!(status, 413);
    assert!(body["error"]
        .as_str()
        .unwrap_or("")
        .contains("Request body exceeds 32 bytes"));

    child.kill_and_wait();
    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn http_server_interpolates_query_parameters() {
    let dir = unique_temp_dir("sidemantic_http_server_parameters");
    let models_path = dir.join("models.yml");
    fs::write(
        &models_path,
        r#"
parameters:
  - name: status
    type: string
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
    metrics:
      - name: revenue
        agg: sum
        sql: amount
"#,
    )
    .expect("parameter fixture should be written");
    let bind = free_loopback_addr();

    let mut command = command_with_clean_sidemantic_env(env!("CARGO_BIN_EXE_sidemantic-server"));
    command
        .arg("--models")
        .arg(&models_path)
        .arg("--bind")
        .arg(&bind);
    let mut child = common::ChildGuard::new(
        command
            .spawn()
            .expect("sidemantic-server child should spawn"),
    );
    wait_for_server(&bind);

    let (status, body, _) = http_request(
        &bind,
        "POST",
        "/compile",
        Some(json!({
            "metrics": ["orders.revenue"],
            "filters": ["orders.status = {{ status }}"],
            "parameters": { "status": "complete" }
        })),
    );
    assert_eq!(status, 200, "{body}");
    let sql = body["sql"]
        .as_str()
        .expect("compile response should include sql");
    assert!(sql.contains("status = 'complete'"), "{sql}");

    child.kill_and_wait();
    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}
