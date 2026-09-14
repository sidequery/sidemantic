# /// script
# requires-python = ">=3.11"
# ///
"""Exercise a packaged CLI without depending on a repository checkout."""

import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> None:
    executable = Path(sys.argv[1]).resolve(strict=True)
    with tempfile.TemporaryDirectory() as directory:
        models = Path(directory) / "models.yaml"
        models.write_text("""models:
  - name: orders
    table: orders
    metrics:
      - name: revenue
        agg: sum
        sql: amount
""")
        result = subprocess.run(
            [str(executable), "compile", "--models", str(models), "--metric", "orders.revenue"],
            cwd=directory,
            check=True,
            text=True,
            capture_output=True,
        )
        if "SUM(" not in result.stdout.upper() or "amount" not in result.stdout:
            raise AssertionError(f"packaged CLI did not compile the model: {result.stdout}")


if __name__ == "__main__":
    main()
