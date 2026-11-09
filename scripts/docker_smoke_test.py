"""Smoke-test a built Sidemantic Docker image in its API, MCP, and PostgreSQL modes.

Usage: python3 scripts/docker_smoke_test.py IMAGE [--api-port PORT] [--modes api,mcp,postgres]

Each mode runs the image with --demo and queries the LookML ``orders`` model, so a
missing parser or broken entrypoint fails the check. Only the standard library and
the docker CLI are required: psql runs from the official postgres image inside the
server container's network namespace, so no PostgreSQL client or published port is
needed for that mode.
"""

import argparse
import json
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

STARTUP_TIMEOUT_SECONDS = 180
API_TOKEN = "smoke-test-token"
PSQL_IMAGE = "postgres:17-alpine"


class SmokeTestError(Exception):
    pass


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def container_running(name: str) -> bool:
    result = docker("inspect", "--format", "{{.State.Running}}", name, check=False)
    return result.stdout.strip() == "true"


def print_logs(name: str) -> None:
    logs = docker("logs", name, check=False)
    print(f"--- logs for {name} ---\n{logs.stdout}{logs.stderr}--- end logs ---", file=sys.stderr)


def wait_until(name: str, description: str, ready) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if ready():
            return
        if not container_running(name):
            raise SmokeTestError(f"container exited before {description}")
        time.sleep(1)
    raise SmokeTestError(f"timed out waiting for {description}")


def http_json(url: str, payload: dict | None = None, token: str | None = None) -> tuple[int, dict | None]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(url, data=data, headers=headers, method="POST" if data else "GET")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, None


def check_api(image: str, port: int) -> str:
    name = f"sidemantic-smoke-api-{port}"
    docker("rm", "-f", name, check=False)
    docker(
        "run",
        "--detach",
        "--name",
        name,
        "--env",
        "SIDEMANTIC_MODE=api",
        "--env",
        f"SIDEMANTIC_API_TOKEN={API_TOKEN}",
        "--publish",
        f"127.0.0.1:{port}:4400",
        image,
        "--demo",
    )
    base_url = f"http://127.0.0.1:{port}"

    def ready() -> bool:
        try:
            with urllib.request.urlopen(f"{base_url}/readyz", timeout=5) as response:
                return response.status == 200
        except OSError:
            return False

    try:
        wait_until(name, "the API server to answer /readyz", ready)
        status, _ = http_json(f"{base_url}/models")
        if status != 401:
            raise SmokeTestError(f"unauthenticated /models returned {status}, expected 401")
        query = {"metrics": ["orders.order_count"], "dimensions": ["customers.region"]}
        status, body = http_json(f"{base_url}/query", query, token=API_TOKEN)
        if status != 200 or not body or not body.get("rows"):
            raise SmokeTestError(f"/query returned {status}: {body}")
        total = sum(row["order_count"] for row in body["rows"])
        if total <= 0:
            raise SmokeTestError(f"/query returned no orders: {body['rows']}")
        return f"/query orders.order_count by customers.region -> {len(body['rows'])} rows, {total} orders"
    except Exception:
        print_logs(name)
        raise
    finally:
        docker("rm", "-f", name, check=False)


def check_mcp(image: str) -> str:
    process = subprocess.Popen(
        ["docker", "run", "--interactive", "--rm", "--env", "SIDEMANTIC_MODE=mcp", image, "--demo"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )

    def send(message: dict) -> None:
        process.stdin.write(json.dumps(message) + "\n")
        process.stdin.flush()

    def receive(request_id: int) -> dict:
        received = {}

        def read() -> None:
            for line in process.stdout:
                if line.strip():
                    message = json.loads(line)
                    if message.get("id") == request_id:
                        received["message"] = message
                        return

        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        reader.join(STARTUP_TIMEOUT_SECONDS)
        if "message" not in received:
            raise SmokeTestError(f"no MCP response to request {request_id} (exit code {process.poll()})")
        if "error" in received["message"]:
            raise SmokeTestError(f"MCP request {request_id} failed: {received['message']['error']}")
        return received["message"]["result"]

    try:
        send(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "docker-smoke-test", "version": "0"},
                },
            }
        )
        server_name = receive(1)["serverInfo"]["name"]
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        send(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "run_query",
                    "arguments": {"metrics": ["orders.order_count"], "dimensions": ["orders.status"]},
                },
            }
        )
        result = receive(2)
        if result.get("isError"):
            raise SmokeTestError(f"run_query failed: {result['content']}")
        rows = json.loads(result["content"][0]["text"])["rows"]
        if not rows:
            raise SmokeTestError("run_query returned no rows")
        return f"{server_name} run_query orders.order_count by orders.status -> {len(rows)} rows"
    finally:
        process.stdin.close()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()


def check_postgres(image: str) -> str:
    name = "sidemantic-smoke-postgres"
    docker("rm", "-f", name, check=False)
    docker("run", "--detach", "--name", name, image, "--demo")

    def psql(sql: str) -> subprocess.CompletedProcess:
        return docker(
            "run",
            "--rm",
            "--network",
            f"container:{name}",
            "--env",
            "PGPASSWORD=any",
            PSQL_IMAGE,
            "psql",
            "--host=127.0.0.1",
            "--port=5433",
            "--username=any",
            "--dbname=sidemantic",
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "--command",
            sql,
            check=False,
        )

    try:
        docker("pull", "--quiet", PSQL_IMAGE)
        wait_until(name, "the PostgreSQL server to accept queries", lambda: psql("SELECT 1").returncode == 0)
        summary = []
        for sql in [
            "SELECT category, product_count FROM semantic_layer.products GROUP BY category ORDER BY category",
            "SELECT region, customer_count FROM semantic_layer.customers GROUP BY region ORDER BY region",
            "SELECT status, order_count FROM semantic_layer.orders GROUP BY status ORDER BY status",
        ]:
            result = psql(sql)
            rows = [line for line in result.stdout.splitlines() if line.strip()]
            # The server reports some query errors as empty results, so require rows.
            if result.returncode != 0 or not rows:
                raise SmokeTestError(f"{sql!r} returned {result.returncode}: {result.stdout}{result.stderr}")
            summary.append(f"{sql.split(' FROM ')[1].split()[0]}: {len(rows)} rows")
        return ", ".join(summary)
    except Exception:
        print_logs(name)
        raise
    finally:
        docker("rm", "-f", name, check=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("image")
    parser.add_argument("--api-port", type=int, default=4400, help="host port published for the API check")
    parser.add_argument("--modes", default="api,mcp,postgres", help="comma-separated modes to check")
    args = parser.parse_args()

    checks = {
        "api": lambda: check_api(args.image, args.api_port),
        "mcp": lambda: check_mcp(args.image),
        "postgres": lambda: check_postgres(args.image),
    }
    failed = False
    for mode in args.modes.split(","):
        try:
            print(f"PASS {mode}: {checks[mode]()}", flush=True)
        except Exception as error:
            failed = True
            print(f"FAIL {mode}: {error}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
