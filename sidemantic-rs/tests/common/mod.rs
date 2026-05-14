#![allow(dead_code)]

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn unique_temp_dir(prefix: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be valid")
        .as_nanos();
    dir.push(format!("{prefix}_{suffix}"));
    fs::create_dir_all(&dir).expect("temp dir should be created");
    dir
}

pub fn write_retail_fixture(dir: &Path) -> PathBuf {
    let models_path = dir.join("retail.yml");
    fs::write(
        &models_path,
        r#"
models:
  - name: orders
    table: orders
    primary_key: order_id
    dimensions:
      - name: status
        type: categorical
      - name: customer_id
        type: numeric
    metrics:
      - name: revenue
        agg: sum
        sql: amount
      - name: order_count
        agg: count
    relationships:
      - name: customers
        type: many_to_one
        foreign_key: customer_id
  - name: customers
    table: customers
    primary_key: id
    dimensions:
      - name: country
        type: categorical
metrics:
  - name: revenue_per_order
    type: ratio
    numerator: revenue
    denominator: order_count
"#,
    )
    .expect("retail fixture should be written");
    models_path
}

#[allow(dead_code)]
pub fn free_loopback_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("free port should be allocated");
    listener
        .local_addr()
        .expect("local addr should be available")
        .to_string()
}

pub struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    pub fn new(child: Child) -> Self {
        Self { child }
    }

    pub fn kill_and_wait(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.kill_and_wait();
    }
}

pub fn command_with_clean_sidemantic_env(binary: &str) -> Command {
    let mut command = Command::new(binary);
    command
        .env_remove("SIDEMANTIC_ADBC_DBOPTS")
        .env_remove("SIDEMANTIC_ADBC_CONNOPTS")
        .env_remove("SIDEMANTIC_MCP_ADBC_DRIVER")
        .env_remove("SIDEMANTIC_MCP_ADBC_URI")
        .env_remove("SIDEMANTIC_MCP_ADBC_ENTRYPOINT")
        .env_remove("SIDEMANTIC_MCP_ADBC_DBOPTS")
        .env_remove("SIDEMANTIC_MCP_ADBC_CONNOPTS")
        .env_remove("SIDEMANTIC_SERVER_ADBC_DRIVER")
        .env_remove("SIDEMANTIC_SERVER_ADBC_URI")
        .env_remove("SIDEMANTIC_SERVER_ADBC_ENTRYPOINT")
        .env_remove("SIDEMANTIC_SERVER_ADBC_DBOPTS")
        .env_remove("SIDEMANTIC_SERVER_ADBC_CONNOPTS")
        .env_remove("SIDEMANTIC_SERVER_AUTH_TOKEN")
        .env_remove("SIDEMANTIC_SERVER_CORS_ORIGINS")
        .env_remove("SIDEMANTIC_SERVER_MAX_REQUEST_BODY_BYTES")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command
}
