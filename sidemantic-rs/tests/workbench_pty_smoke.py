import fcntl
import os
import pty
import select
import struct
import subprocess
import sys
import tempfile
import termios
import time
from pathlib import Path

MODEL_YAML = """
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
"""


def write_models(root: Path) -> Path:
    path = root / "models.yml"
    path.write_text(MODEL_YAML)
    return path


def spawn_pty(args, env=None):
    master_fd, slave_fd = pty.openpty()
    winsize = struct.pack("HHHH", 32, 120, 0, 0)
    fcntl.ioctl(slave_fd, termios.TIOCSWINSZ, winsize)
    proc = subprocess.Popen(
        args,
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=subprocess.PIPE,
        env=env,
        close_fds=True,
    )
    os.close(slave_fd)
    os.set_blocking(master_fd, False)
    return proc, master_fd


def read_available(fd):
    chunks = []
    while True:
        ready, _, _ = select.select([fd], [], [], 0)
        if not ready:
            break
        try:
            chunk = os.read(fd, 65536)
        except BlockingIOError:
            break
        except OSError:
            break
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks).decode(errors="replace")


def wait_for(fd, needle, timeout=5):
    deadline = time.time() + timeout
    output = ""
    while time.time() < deadline:
        output += read_available(fd)
        if needle in output:
            return output
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {needle!r}; output={output!r}")


def send_key(fd, data):
    os.write(fd, data)
    time.sleep(0.1)


def close_pty_process(proc, fd):
    try:
        if proc.poll() is None:
            send_key(fd, b"\x1b")
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def run_bad_model_path(binary: str, root: Path):
    missing = root / "missing-models"
    proc, fd = spawn_pty([binary, "workbench", str(missing)])
    try:
        proc.wait(timeout=5)
        output = read_available(fd)
        stderr = proc.stderr.read().decode(errors="replace")
        assert proc.returncode != 0, "bad model path should fail"
        assert "does not exist" in stderr or "does not exist" in output, (output, stderr)
    finally:
        close_pty_process(proc, fd)


def run_no_db_workbench(binary: str, models_path: Path):
    proc, fd = spawn_pty([binary, "workbench", str(models_path)])
    try:
        first = wait_for(fd, "Workbench")
        assert "connection=none" in first, first
        assert "SQL Input" in first, first

        send_key(fd, b"\x05")
        wait_for(fd, "configured")

        send_key(fd, b"\x1b[18~")
        wait_for(fd, "TABLE")

        send_key(fd, b"\x1b")
        proc.wait(timeout=5)
        assert proc.returncode == 0, f"workbench quit failed: {proc.returncode}"
    finally:
        close_pty_process(proc, fd)


def seed_duckdb(binary: str, models_path: Path, db_path: Path, driver: str):
    for sql in [
        "drop table if exists orders",
        "create table orders(order_id integer, status varchar, amount double)",
        "insert into orders values (1, 'complete', 10.5), (2, 'complete', 20.0), (3, 'cancelled', 7.0)",
    ]:
        result = subprocess.run(
            [
                binary,
                "query",
                "--models",
                str(models_path),
                "--sql",
                sql,
                "--driver",
                driver,
                "--entrypoint",
                "duckdb_adbc_init",
                "--uri",
                str(db_path),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr


def write_driver_manifest(root: Path, driver: str) -> Path:
    driver_dir = root / "adbc-drivers"
    driver_dir.mkdir()
    (driver_dir / "adbc_driver_duckdb.toml").write_text(
        f"""
manifest_version = 1
name = "DuckDB"
version = "1.0.0"

[Driver]
entrypoint = "duckdb_adbc_init"
shared = "{driver}"
"""
    )
    return driver_dir


def run_db_workbench(binary: str, models_path: Path, root: Path):
    driver = os.environ.get("SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER")
    if not driver:
        raise AssertionError("SIDEMANTIC_TEST_ADBC_DUCKDB_DRIVER is required for workbench-adbc PTY test")

    db_path = root / "warehouse.duckdb"
    seed_duckdb(binary, models_path, db_path, driver)
    driver_dir = write_driver_manifest(root, driver)
    env = os.environ.copy()
    existing = env.get("ADBC_DRIVER_PATH")
    env["ADBC_DRIVER_PATH"] = str(driver_dir) if not existing else f"{driver_dir}{os.pathsep}{existing}"

    proc, fd = spawn_pty([binary, "workbench", str(models_path), "--db", str(db_path)], env=env)
    try:
        first = wait_for(fd, "Workbench")
        assert "connection=duckdb:///" in first, first

        send_key(fd, b"\x05")
        output = wait_for(fd, "complete")
        assert "complete" in output or "cancelled" in output, output

        send_key(fd, b"\x1b")
        proc.wait(timeout=5)
        assert proc.returncode == 0, f"workbench DB quit failed: {proc.returncode}"
    finally:
        close_pty_process(proc, fd)


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: workbench_pty_smoke.py <sidemantic-binary>")

    binary = sys.argv[1]
    with tempfile.TemporaryDirectory(prefix="sidemantic-workbench-pty-") as tmp:
        root = Path(tmp)
        models_path = write_models(root)
        run_bad_model_path(binary, root)
        run_no_db_workbench(binary, models_path)
        if os.environ.get("SIDEMANTIC_WORKBENCH_PTY_EXPECT_ADBC") == "1":
            run_db_workbench(binary, models_path, root)


if __name__ == "__main__":
    main()
